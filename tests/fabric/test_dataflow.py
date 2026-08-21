"""
Unit tests for the Dataflow class — get_dataflow_name (PBI + Fabric fallback)
and 429 retry handling.

These tests use mocked API responses and do not call any real API.

Usage:
    pytest tests/test_dataflow.py -v
"""

import json
from threading import Barrier
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from msdev_kit.fabric.dataflow import Dataflow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class MockDataflow(Dataflow):
    """Dataflow instance that skips __init__ I/O (token, dirs, workspace)."""
    def __init__(self):
        self.main_url = 'https://api.powerbi.com/v1.0/myorg'
        self.fabric_api_base_url = 'https://api.fabric.microsoft.com'
        self.token = 'fake-token'
        self.headers = {'Authorization': f'Bearer {self.token}'}


@pytest.fixture
def df(tmp_path):
    dataflow = MockDataflow()
    dataflow.dataflows_dir = str(tmp_path)
    return dataflow


def _make_response(status_code, body, content_type='application/json'):
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = json.dumps(body).encode('utf-8')
    resp.headers = {'content-type': content_type, 'Retry-After': '1'}
    resp.json.return_value = body
    resp.text = json.dumps(body)
    return resp


# ===========================================================================
# get_dataflow_name
# ===========================================================================

class TestGetDataflowName:

    @patch('msdev_kit.fabric.dataflow.requests.request')
    def test_returns_name_from_pbi_api(self, mock_req, df):
        """Gen1/Gen2 non-CI/CD dataflows should resolve via PBI API."""
        mock_req.return_value = _make_response(200, {
            'objectId': 'df-1',
            'name': 'Sales Dataflow'
        })

        name = df.get_dataflow_name('ws-1', 'df-1')

        assert name == 'Sales Dataflow'
        # Should have only called PBI API (one request)
        assert mock_req.call_count == 1

    @patch('msdev_kit.fabric.dataflow.requests.request')
    def test_falls_back_to_fabric_api(self, mock_req, df):
        """Gen2 CI/CD dataflows should resolve via Fabric API when PBI fails."""
        pbi_error = _make_response(404, {'error': {'message': 'Not found'}})
        fabric_success = _make_response(200, {
            'id': 'df-1',
            'displayName': 'CI/CD Dataflow'
        })
        mock_req.side_effect = [pbi_error, fabric_success]

        name = df.get_dataflow_name('ws-1', 'df-1')

        assert name == 'CI/CD Dataflow'
        assert mock_req.call_count == 2

    @patch('msdev_kit.fabric.dataflow.requests.request')
    def test_returns_empty_when_both_fail(self, mock_req, df):
        """Should return empty string when both APIs fail."""
        pbi_error = _make_response(404, {'error': {'message': 'Not found'}})
        fabric_error = _make_response(404, {'error': {'message': 'Not found'}})
        mock_req.side_effect = [pbi_error, fabric_error]

        name = df.get_dataflow_name('ws-1', 'df-999')

        assert name == ''

    def test_missing_workspace_id(self, df):
        """Should return empty string for missing workspace_id (PBI validation)."""
        name = df.get_dataflow_name('', 'df-1')
        assert name == ''

    def test_missing_dataflow_id(self, df):
        """Should return empty string for missing dataflow_id (PBI validation)."""
        name = df.get_dataflow_name('ws-1', '')
        assert name == ''


# ===========================================================================
# _request_with_retry (429 handling)
# ===========================================================================

class TestDataflowRetry:

    @patch('msdev_kit.fabric.dataflow.time.sleep')
    @patch('msdev_kit.fabric.dataflow.requests.request')
    def test_retries_on_429(self, mock_req, mock_sleep, df):
        rate_limited = _make_response(429, {})
        rate_limited.headers = {'Retry-After': '1'}
        success = _make_response(200, {'objectId': 'df-1', 'name': 'My DF'})
        mock_req.side_effect = [rate_limited, success]

        name = df.get_dataflow_name('ws-1', 'df-1')

        assert name == 'My DF'
        assert mock_sleep.called
        assert mock_req.call_count == 2

    @patch('msdev_kit.fabric.dataflow.time.sleep')
    @patch('msdev_kit.fabric.dataflow.requests.request')
    def test_respects_retry_after_header(self, mock_req, mock_sleep, df):
        rate_limited = _make_response(429, {})
        rate_limited.headers = {'Retry-After': '10'}
        success = _make_response(200, {'objectId': 'df-1', 'name': 'My DF'})
        mock_req.side_effect = [rate_limited, success]

        df.get_dataflow_name('ws-1', 'df-1')

        mock_sleep.assert_called_with(10)

    @patch('msdev_kit.fabric.dataflow.time.sleep')
    @patch('msdev_kit.fabric.dataflow.requests.request')
    def test_uses_exponential_backoff_without_retry_after(self, mock_req, mock_sleep, df):
        rate_limited = _make_response(429, {})
        rate_limited.headers = {}
        success = _make_response(200, {'objectId': 'df-1', 'name': 'My DF'})
        mock_req.side_effect = [rate_limited, success]

        df.get_dataflow_name('ws-1', 'df-1')

        mock_sleep.assert_called_once_with(1)


# ===========================================================================
# Parameter validation — _get_dataflow_pbi_definition
# ===========================================================================

class TestGetDataflowPbiDefinitionValidation:

    def test_missing_workspace_id(self, df):
        result = df._get_dataflow_pbi_definition('', 'df-1')
        assert 'Missing workspace id' in result['message']

    def test_missing_dataflow_id(self, df):
        result = df._get_dataflow_pbi_definition('ws-1', '')
        assert 'Missing dataflow id' in result['message']


# ===========================================================================
# list_dataflows
# ===========================================================================

class TestListDataflows:

    def test_uses_generation_2_1_for_fabric_only_dataflows(self, df, tmp_path, monkeypatch):
        df.workspace = MagicMock()
        df.workspace.get_workspace_details.return_value = {
            'message': 'Success',
            'content': {'name': 'Test Workspace'},
        }
        df.dataflows_dir = str(tmp_path)
        responses = iter([
            _make_response(200, {'value': []}),
            _make_response(200, {
                'value': [{'id': 'df-1', 'displayName': 'CI/CD Dataflow'}],
            }),
        ])
        monkeypatch.setattr(df, '_request_with_retry', lambda *args, **kwargs: next(responses))

        result = df.list_dataflows('ws-1')

        assert result['message'] == 'Success'
        assert result['content'] == [{
            'id': 'df-1',
            'name': 'CI/CD Dataflow',
            'generation': 2.1,
            'source': 'fabric',
        }]


# ===========================================================================
# get_workspace_data_destinations
# ===========================================================================

class TestGetWorkspaceDataDestinations:

    def test_validates_workspace_and_worker_count(self, df):
        assert 'Missing workspace id' in df.get_workspace_data_destinations('')['message']
        assert 'max_workers' in df.get_workspace_data_destinations('ws-1', max_workers=0)['message']

    def test_lists_then_inspects_dataflows_concurrently(self, df, monkeypatch):
        dataflows = [
            {'id': 'df-1', 'name': 'First', 'source': 'pbi'},
            {'id': 'df-2', 'name': 'Second', 'source': 'fabric'},
        ]
        barrier = Barrier(2)
        calls = []

        monkeypatch.setattr(
            df,
            'list_dataflows',
            lambda workspace_id: {'message': 'Success', 'content': dataflows},
        )

        def get_destinations(
            workspace_id,
            dataflow_id,
            source=None,
            verbose=True,
            on_rate_limit=None,
        ):
            calls.append((dataflow_id, source, verbose))
            barrier.wait(timeout=2)
            return {
                'message': 'Success',
                'content': [{'name': f'{dataflow_id}_table', 'destination_type': 'Lakehouse'}],
            }

        monkeypatch.setattr(df, 'get_data_destinations', get_destinations)

        result = df.get_workspace_data_destinations('ws-1', max_workers=2)

        assert result['message'] == 'Success'
        assert [item['dataflow']['id'] for item in result['content']] == ['df-1', 'df-2']
        assert result['content'][0]['tables'][0]['name'] == 'df-1_table'
        assert result['content'][1]['tables'][0]['name'] == 'df-2_table'
        assert sorted(calls) == [('df-1', 'pbi', False), ('df-2', 'fabric', False)]
        exported = pd.read_excel(''.join([
            df.dataflows_dir,
            '/workspace_dataflow_destinations_ws-1.xlsx',
        ]))
        assert exported.columns.tolist() == [
            'dataflow_id',
            'dataflow_name',
            'dataflow_configuredBy',
            'dataflow_generation',
            'dataflow_source',
            'table_name',
            'table_destination_type',
        ]
        assert exported['table_name'].tolist() == ['df-1_table', 'df-2_table']

    def test_returns_partial_results_when_a_dataflow_cannot_be_inspected(self, df, monkeypatch):
        monkeypatch.setattr(
            df,
            'list_dataflows',
            lambda workspace_id: {
                'message': 'Success',
                'content': [
                    {'id': 'df-1', 'name': 'Working'},
                    {'id': 'df-2', 'name': 'Broken'},
                    {'name': 'Missing ID'},
                ],
            },
        )
        monkeypatch.setattr(
            df,
            'get_data_destinations',
            lambda workspace_id, dataflow_id, source=None, verbose=True, on_rate_limit=None: (
                {'message': 'Success', 'content': [{'name': 'Orders'}]}
                if dataflow_id == 'df-1'
                else {'message': 'Definition unavailable', 'content': ''}
            ),
        )

        result = df.get_workspace_data_destinations('ws-1')

        assert result['message'] == 'Partial success'
        assert result['content'][0]['tables'] == [{'name': 'Orders'}]
        assert result['content'][1]['error'] == 'Definition unavailable'
        assert result['content'][2]['error'] == 'Dataflow record has no id.'

    def test_uses_pbi_source_without_a_fabric_definition_probe(self, df, monkeypatch):
        monkeypatch.setattr(
            df,
            'get_dataflow_gen2_definition',
            lambda *args, **kwargs: pytest.fail('PBI dataflows must not call getDefinition.'),
        )
        monkeypatch.setattr(
            df,
            '_get_dataflow_pbi_definition',
            lambda *args, **kwargs: {'message': 'Success', 'content': {'pbi:mashup': {}}},
        )
        monkeypatch.setattr(
            df,
            '_get_data_destinations_standard',
            lambda definition: {'message': 'Success', 'content': [{'name': 'Orders'}]},
        )

        result = df.get_data_destinations('ws-1', 'df-1', source='pbi', verbose=False)

        assert result == {'message': 'Success', 'content': [{'name': 'Orders'}]}

    def test_reports_rate_limit_then_resumes_progress(self, df, monkeypatch, capsys):
        monkeypatch.setattr(
            df,
            'list_dataflows',
            lambda workspace_id: {
                'message': 'Success',
                'content': [{'id': 'df-1', 'name': 'First', 'source': 'pbi'}],
            },
        )

        def get_destinations(
            workspace_id,
            dataflow_id,
            source=None,
            verbose=True,
            on_rate_limit=None,
        ):
            on_rate_limit(5)
            return {'message': 'Success', 'content': []}

        monkeypatch.setattr(df, 'get_data_destinations', get_destinations)

        result = df.get_workspace_data_destinations('ws-1')

        output = capsys.readouterr().out
        assert result['message'] == 'Success'
        assert '0 processados até então, aguardando rate limit...' in output
        assert '\rExtracted definition from 1/1 dataflows from workspace ws-1...' in output


# ===========================================================================
# Data destination parsing
# ===========================================================================

class TestDataDestinationParsing:

    def test_returns_only_queries_with_a_destination_and_uses_name(self, df):
        definition = {
            'pbi:mashup': {
                'document': '''
shared SourceOnly = let
    Source = #table({"id"}, {{1}})
in
    Source;
shared Loaded_DataDestination = let
    Source = Fabric.Warehouse([CreateNavigationProperties = false]),
    Workspace = Source{[workspaceId = "workspace-1"]}[Data],
    Destination = Workspace{[warehouseId = "warehouse-1", Schema = "dbo"]}[Data]
in
    Destination;
'''
            }
        }

        result = df._get_data_destinations_standard(definition)

        assert result['message'] == 'Success'
        assert result['content'] == [{
            'name': 'Loaded',
            'destination_type': 'Warehouse',
            'workspace_id': 'workspace-1',
            'item_id': 'warehouse-1',
            'sql_schema': 'dbo',
            'mapping_type': 'Automatic',
            'columns': [],
        }]
