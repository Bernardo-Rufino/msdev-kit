"""
Unit tests for the Dataset class — parameter validation, DAX parsing,
and truncation detection logic.

These tests use mocked API responses and do not call any real API.

Usage:
    pytest tests/test_dataset.py -v
"""

import json
import pytest
from unittest.mock import patch, MagicMock
from msdev_kit.fabric.dataset import Dataset


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class MockDataset(Dataset):
    """Dataset instance that skips __init__ I/O (token, dirs)."""
    def __init__(self):
        self.main_url = 'https://api.powerbi.com/v1.0/myorg'
        self.fabric_api_base_url = 'https://api.fabric.microsoft.com'
        self.token = 'fake-token'
        self.headers = {'Authorization': f'Bearer {self.token}'}


@pytest.fixture
def ds():
    return MockDataset()


def _make_api_response(status_code, body):
    """Create a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = json.dumps(body).encode('utf-8')
    resp.json.return_value = body
    resp.text = json.dumps(body)
    resp.headers = {'content-type': 'application/json'}
    return resp


# ===========================================================================
# _extract_table_expression
# ===========================================================================

class TestExtractTableExpression:

    def test_simple_evaluate(self):
        query = "EVALUATE 'Sales'"
        assert Dataset._extract_table_expression(query) == "'Sales'"

    def test_evaluate_with_function(self):
        query = "EVALUATE SUMMARIZECOLUMNS('Table'[Col1], 'Table'[Col2])"
        result = Dataset._extract_table_expression(query)
        assert result == "SUMMARIZECOLUMNS('Table'[Col1], 'Table'[Col2])"

    def test_evaluate_case_insensitive(self):
        query = "evaluate MyTable"
        assert Dataset._extract_table_expression(query) == "MyTable"

    def test_evaluate_with_filter(self):
        query = "EVALUATE FILTER('Products', 'Products'[Price] > 100)"
        result = Dataset._extract_table_expression(query)
        assert result == "FILTER('Products', 'Products'[Price] > 100)"

    def test_multiline_query(self):
        query = "EVALUATE\n  SUMMARIZECOLUMNS(\n    'Date'[Year],\n    'Sales'[Amount]\n  )"
        result = Dataset._extract_table_expression(query)
        assert "SUMMARIZECOLUMNS" in result

    def test_no_evaluate_returns_empty(self):
        query = "SELECT * FROM Sales"
        assert Dataset._extract_table_expression(query) == ''

    def test_empty_query(self):
        assert Dataset._extract_table_expression('') == ''

    def test_evaluate_with_define(self):
        query = "DEFINE MEASURE Sales[Total] = SUM(Sales[Amount])\nEVALUATE SUMMARIZECOLUMNS('Date'[Year])"
        result = Dataset._extract_table_expression(query)
        assert "SUMMARIZECOLUMNS" in result


# ===========================================================================
# execute_query — parameter validation
# ===========================================================================

class TestExecuteQueryValidation:

    def test_missing_query(self, ds):
        result = ds.execute_query(workspace_id='ws-1', dataset_id='ds-1', query='')
        assert result['message'] == 'Missing parameters, please check.'

    def test_missing_workspace_id(self, ds):
        result = ds.execute_query(workspace_id='', dataset_id='ds-1', query='EVALUATE Sales')
        assert result['message'] == 'Missing parameters, please check.'

    def test_missing_dataset_id(self, ds):
        result = ds.execute_query(workspace_id='ws-1', dataset_id='', query='EVALUATE Sales')
        assert result['message'] == 'Missing parameters, please check.'


# ===========================================================================
# execute_query — truncation detection
# ===========================================================================

class TestExecuteQueryTruncation:

    @patch.object(MockDataset, '_post_query')
    def test_no_truncation_small_result(self, mock_post, ds):
        """A small result set should not be marked as truncated."""
        rows = [{'[Col1]': i, '[Col2]': i * 10} for i in range(50)]

        # COUNTROWS response
        count_resp = _make_api_response(200, {
            'results': [{'tables': [{'rows': [{'[_count]': 50}]}]}]
        })
        # Actual query response
        query_resp = _make_api_response(200, {
            'results': [{'tables': [{'rows': rows}]}]
        })
        mock_post.side_effect = [count_resp, query_resp]

        result = ds.execute_query(workspace_id='ws-1', dataset_id='ds-1', query='EVALUATE Sales')

        assert result['message'] == 'Success'
        assert result['truncated'] is False
        assert result['total_rows'] == 50
        assert result['rows_returned'] == 50
        assert result['num_columns'] == 2
        assert result['max_rows_allowed'] == 100_000

    @patch.object(MockDataset, '_post_query')
    def test_truncation_detected_by_countrows(self, mock_post, ds):
        """When COUNTROWS returns more than max_rows, truncated should be True."""
        # 20 columns -> max 50,000 rows (1M / 20)
        row = {f'[Col{i}]': i for i in range(20)}
        rows = [row.copy() for _ in range(100)]

        count_resp = _make_api_response(200, {
            'results': [{'tables': [{'rows': [{'[_count]': 60_000}]}]}]
        })
        query_resp = _make_api_response(200, {
            'results': [{'tables': [{'rows': rows}]}]
        })
        mock_post.side_effect = [count_resp, query_resp]

        result = ds.execute_query(workspace_id='ws-1', dataset_id='ds-1', query='EVALUATE BigTable')

        assert result['truncated'] is True
        assert result['total_rows'] == 60_000
        assert result['max_rows_allowed'] == 50_000
        assert result['num_columns'] == 20

    @patch.object(MockDataset, '_post_query')
    def test_max_rows_with_few_columns(self, mock_post, ds):
        """With 5 columns, max_rows should be 100k (1M/5 = 200k, capped at 100k)."""
        row = {f'[Col{i}]': i for i in range(5)}
        rows = [row.copy() for _ in range(10)]

        count_resp = _make_api_response(200, {
            'results': [{'tables': [{'rows': [{'[_count]': 10}]}]}]
        })
        query_resp = _make_api_response(200, {
            'results': [{'tables': [{'rows': rows}]}]
        })
        mock_post.side_effect = [count_resp, query_resp]

        result = ds.execute_query(workspace_id='ws-1', dataset_id='ds-1', query='EVALUATE Sales')

        assert result['max_rows_allowed'] == 100_000
        assert result['truncated'] is False

    @patch.object(MockDataset, '_post_query')
    def test_countrows_failure_still_runs_query(self, mock_post, ds):
        """If COUNTROWS fails, the actual query should still run."""
        count_resp = _make_api_response(400, {'error': {'message': 'Bad request'}})
        rows = [{'[Col1]': 1}]
        query_resp = _make_api_response(200, {
            'results': [{'tables': [{'rows': rows}]}]
        })
        mock_post.side_effect = [count_resp, query_resp]

        result = ds.execute_query(workspace_id='ws-1', dataset_id='ds-1', query='EVALUATE Sales')

        assert result['message'] == 'Success'
        assert result['total_rows'] is None
        assert result['truncated'] is False

    @patch.object(MockDataset, '_post_query')
    def test_empty_result(self, mock_post, ds):
        """An empty result should return 0 rows and not be truncated."""
        count_resp = _make_api_response(200, {
            'results': [{'tables': [{'rows': [{'[_count]': 0}]}]}]
        })
        query_resp = _make_api_response(200, {
            'results': [{'tables': [{'rows': []}]}]
        })
        mock_post.side_effect = [count_resp, query_resp]

        result = ds.execute_query(workspace_id='ws-1', dataset_id='ds-1', query='EVALUATE EmptyTable')

        assert result['rows_returned'] == 0
        assert result['num_columns'] == 0
        assert result['truncated'] is False

    @patch.object(MockDataset, '_post_query')
    def test_api_error_returns_error(self, mock_post, ds):
        """API errors should be returned properly."""
        count_resp = _make_api_response(200, {
            'results': [{'tables': [{'rows': [{'[_count]': 10}]}]}]
        })
        query_resp = _make_api_response(403, {'error': {'message': 'Forbidden'}})
        mock_post.side_effect = [count_resp, query_resp]

        result = ds.execute_query(workspace_id='ws-1', dataset_id='ds-1', query='EVALUATE Sales')

        assert result['message'] == 'Error'

    @patch.object(MockDataset, '_post_query')
    def test_truncation_by_row_limit_hit(self, mock_post, ds):
        """When rows_returned >= max_rows and no COUNTROWS, infer truncation."""
        # No EVALUATE in query -> no COUNTROWS pre-check
        rows = [{'[Col1]': i} for i in range(100_000)]

        query_resp = _make_api_response(200, {
            'results': [{'tables': [{'rows': rows}]}]
        })
        # Only one call since query has no EVALUATE
        mock_post.side_effect = [query_resp]

        result = ds.execute_query(workspace_id='ws-1', dataset_id='ds-1', query='SELECT 1')

        assert result['truncated'] is True
        assert result['rows_returned'] == 100_000


# ===========================================================================
# Parameter validation — other methods
# ===========================================================================

class TestDatasetParameterValidation:

    def test_get_dataset_details_missing_workspace(self, ds):
        result = ds.get_dataset_details(workspace_id='', dataset_id='ds-1')
        assert 'Missing workspace id' in result['message']

    def test_get_dataset_details_missing_dataset(self, ds):
        result = ds.get_dataset_details(workspace_id='ws-1', dataset_id='')
        assert 'Missing dataset id' in result['message']

    def test_list_datasets_missing_workspace(self, ds):
        result = ds.list_datasets(workspace_id='')
        assert 'Missing workspace id' in result['message']


# ===========================================================================
# get_dataset_name
# ===========================================================================

class TestGetDatasetName:

    @patch('msdev_kit.fabric.dataset.requests.request')
    def test_returns_name_from_pbi_api(self, mock_req, ds):
        mock_req.return_value = _make_api_response(200, {'name': 'Sales Model'})

        name = ds.get_dataset_name('ws-1', 'ds-1')

        assert name == 'Sales Model'
        assert mock_req.call_count == 1

    @patch('msdev_kit.fabric.dataset.requests.request')
    def test_falls_back_to_fabric_api(self, mock_req, ds):
        pbi_error = _make_api_response(404, {'error': {'message': 'Not found'}})
        fabric_success = _make_api_response(200, {'displayName': 'Fabric Model'})
        mock_req.side_effect = [pbi_error, fabric_success]

        name = ds.get_dataset_name('ws-1', 'ds-1')

        assert name == 'Fabric Model'
        assert mock_req.call_count == 2

    @patch('msdev_kit.fabric.dataset.requests.request')
    def test_returns_empty_when_both_fail(self, mock_req, ds):
        pbi_error = _make_api_response(404, {'error': {'message': 'Not found'}})
        fabric_error = _make_api_response(404, {'error': {'message': 'Not found'}})
        mock_req.side_effect = [pbi_error, fabric_error]

        name = ds.get_dataset_name('ws-1', 'ds-999')

        assert name == ''

    @patch('msdev_kit.fabric.dataset.time.sleep')
    @patch('msdev_kit.fabric.dataset.requests.request')
    def test_retries_on_429(self, mock_req, mock_sleep, ds):
        rate_limited = MagicMock()
        rate_limited.status_code = 429
        rate_limited.headers = {'Retry-After': '1'}
        success = _make_api_response(200, {'name': 'Sales Model'})
        mock_req.side_effect = [rate_limited, success]

        name = ds.get_dataset_name('ws-1', 'ds-1')

        assert name == 'Sales Model'
        assert mock_sleep.called
