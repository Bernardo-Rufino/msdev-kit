"""
Unit tests for the Notebook class — parameter validation, list/get operations,
and 429 retry handling.

These tests use mocked API responses and do not call any real API.

Usage:
    pytest tests/test_notebook.py -v
"""

import json
import pytest
from unittest.mock import patch, MagicMock
from msdev_kit.fabric.notebook import Notebook


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def nb():
    return Notebook('fake-token')


def _make_response(status_code, body, content_type='application/json'):
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = json.dumps(body).encode('utf-8')
    resp.headers = {'content-type': content_type}
    resp.json.return_value = body
    resp.text = json.dumps(body)
    return resp


# ===========================================================================
# list_notebooks
# ===========================================================================

class TestListNotebooks:

    def test_missing_workspace_id(self, nb):
        result = nb.list_notebooks('')
        assert 'Missing workspace id' in result['message']

    @patch('msdev_kit.fabric.notebook.requests.request')
    def test_success_single_page(self, mock_req, nb):
        mock_req.return_value = _make_response(200, {
            'value': [
                {'id': 'nb-1', 'displayName': 'Notebook A'},
                {'id': 'nb-2', 'displayName': 'Notebook B'},
            ]
        })

        result = nb.list_notebooks('ws-1')

        assert result['message'] == 'Success'
        assert len(result['content']) == 2
        assert result['content'][0]['displayName'] == 'Notebook A'

    @patch('msdev_kit.fabric.notebook.requests.request')
    def test_success_paginated(self, mock_req, nb):
        page1 = _make_response(200, {
            'value': [{'id': 'nb-1', 'displayName': 'Notebook A'}],
            'continuationUri': 'https://api.fabric.microsoft.com/v1/next-page'
        })
        page2 = _make_response(200, {
            'value': [{'id': 'nb-2', 'displayName': 'Notebook B'}],
        })
        mock_req.side_effect = [page1, page2]

        result = nb.list_notebooks('ws-1')

        assert result['message'] == 'Success'
        assert len(result['content']) == 2

    @patch('msdev_kit.fabric.notebook.requests.request')
    def test_api_error(self, mock_req, nb):
        mock_req.return_value = _make_response(403, {
            'error': {'message': 'Forbidden'}
        })

        result = nb.list_notebooks('ws-1')

        assert 'error' in result['message']


# ===========================================================================
# get_notebook
# ===========================================================================

class TestGetNotebook:

    def test_missing_workspace_id(self, nb):
        result = nb.get_notebook('', 'nb-1')
        assert 'Missing workspace id' in result['message']

    def test_missing_notebook_id(self, nb):
        result = nb.get_notebook('ws-1', '')
        assert 'Missing notebook id' in result['message']

    @patch('msdev_kit.fabric.notebook.requests.request')
    def test_success(self, mock_req, nb):
        mock_req.return_value = _make_response(200, {
            'id': 'nb-1',
            'displayName': 'My Notebook',
            'description': 'A test notebook'
        })

        result = nb.get_notebook('ws-1', 'nb-1')

        assert result['message'] == 'Success'
        assert result['content']['displayName'] == 'My Notebook'

    @patch('msdev_kit.fabric.notebook.requests.request')
    def test_not_found(self, mock_req, nb):
        mock_req.return_value = _make_response(404, {
            'error': {'message': 'Not found'}
        })

        result = nb.get_notebook('ws-1', 'nb-999')

        assert 'error' in result['message']


# ===========================================================================
# _request_with_retry (429 handling)
# ===========================================================================

class TestNotebookRetry:

    @patch('msdev_kit.fabric.notebook.time.sleep')
    @patch('msdev_kit.fabric.notebook.requests.request')
    def test_retries_on_429(self, mock_req, mock_sleep, nb):
        rate_limited = _make_response(429, {})
        rate_limited.headers = {'Retry-After': '1'}
        success = _make_response(200, {'id': 'nb-1', 'displayName': 'NB'})
        mock_req.side_effect = [rate_limited, success]

        result = nb.get_notebook('ws-1', 'nb-1')

        assert result['message'] == 'Success'
        assert mock_sleep.called
        assert mock_req.call_count == 2
