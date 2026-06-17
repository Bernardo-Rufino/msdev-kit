"""
Unit tests for the Dataflow class — get_dataflow_name (PBI + Fabric fallback)
and 429 retry handling.

These tests use mocked API responses and do not call any real API.

Usage:
    pytest tests/test_dataflow.py -v
"""

import json
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
def df():
    return MockDataflow()


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
