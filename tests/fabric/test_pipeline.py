"""
Unit tests for the Pipeline class — get_pipeline, get_pipeline_activities,
find_pipelines_by_dataflow, name/ID resolution, and 429 retry handling.

These tests use mocked API responses and do not call any real API.

Usage:
    pytest tests/test_pipeline.py -v
"""

import json
import base64
import pytest
from unittest.mock import patch, MagicMock
from msdev_kit.fabric.pipeline import Pipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def pl():
    return Pipeline('fake-token')


def _make_response(status_code, body, content_type='application/json'):
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = json.dumps(body).encode('utf-8')
    resp.headers = {'content-type': content_type}
    resp.json.return_value = body
    resp.text = json.dumps(body)
    return resp


def _make_pipeline_definition(activities):
    """Build a pipeline definition response with given activities."""
    pipeline_content = {
        'properties': {
            'activities': activities
        }
    }
    encoded = base64.b64encode(json.dumps(pipeline_content).encode('utf-8')).decode('utf-8')
    return {
        'definition': {
            'parts': [
                {
                    'path': 'pipeline-content.json',
                    'payload': encoded
                }
            ]
        }
    }


# ===========================================================================
# get_pipeline
# ===========================================================================

class TestGetPipeline:

    def test_missing_workspace_id(self, pl):
        result = pl.get_pipeline('', 'pl-1')
        assert 'Missing workspace id' in result['message']

    def test_missing_pipeline_id(self, pl):
        result = pl.get_pipeline('ws-1', '')
        assert 'Missing pipeline id' in result['message']

    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_success(self, mock_req, pl):
        mock_req.return_value = _make_response(200, {
            'id': 'pl-1',
            'displayName': 'My Pipeline',
            'description': 'A test pipeline'
        })

        result = pl.get_pipeline('ws-1', 'pl-1')

        assert result['message'] == 'Success'
        assert result['content']['displayName'] == 'My Pipeline'

    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_not_found(self, mock_req, pl):
        mock_req.return_value = _make_response(404, {
            'error': {'message': 'Not found'}
        })

        result = pl.get_pipeline('ws-1', 'pl-999')

        assert 'error' in result['message']


# ===========================================================================
# _resolve_pipeline — ID and name resolution
# ===========================================================================

class TestResolvePipeline:

    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_resolves_by_id(self, mock_req, pl):
        mock_req.return_value = _make_response(200, {
            'id': 'pl-1', 'displayName': 'My Pipeline'
        })

        pid, pname = pl._resolve_pipeline('ws-1', 'pl-1')

        assert pid == 'pl-1'
        assert pname == 'My Pipeline'

    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_resolves_by_name(self, mock_req, pl):
        """When get_pipeline fails (not an ID), falls back to list and match by name."""
        not_found = _make_response(404, {'error': {'message': 'Not found'}})
        list_resp = _make_response(200, {
            'value': [
                {'id': 'pl-1', 'displayName': 'Pipeline Alpha'},
                {'id': 'pl-2', 'displayName': 'Pipeline Beta'},
            ]
        })
        mock_req.side_effect = [not_found, list_resp]

        pid, pname = pl._resolve_pipeline('ws-1', 'Pipeline Beta')

        assert pid == 'pl-2'
        assert pname == 'Pipeline Beta'

    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_resolves_by_name_case_insensitive(self, mock_req, pl):
        not_found = _make_response(404, {'error': {'message': 'Not found'}})
        list_resp = _make_response(200, {
            'value': [{'id': 'pl-1', 'displayName': 'My Pipeline'}]
        })
        mock_req.side_effect = [not_found, list_resp]

        pid, pname = pl._resolve_pipeline('ws-1', 'my pipeline')

        assert pid == 'pl-1'
        assert pname == 'My Pipeline'

    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_returns_none_when_not_found(self, mock_req, pl):
        not_found = _make_response(404, {'error': {'message': 'Not found'}})
        list_resp = _make_response(200, {
            'value': [{'id': 'pl-1', 'displayName': 'Other Pipeline'}]
        })
        mock_req.side_effect = [not_found, list_resp]

        pid, pname = pl._resolve_pipeline('ws-1', 'Nonexistent')

        assert pid is None
        assert pname is None


# ===========================================================================
# _resolve_dataflow_id — ID and name resolution
# ===========================================================================

class TestResolveDataflowId:

    @patch('msdev_kit.fabric.pipeline.Dataflow')
    def test_resolves_by_id(self, MockDataflow, pl):
        mock_df = MockDataflow.return_value
        mock_df.get_dataflow_name.return_value = 'Sales DF'

        dfid, dfname = pl._resolve_dataflow_id('ws-1', 'df-1')

        assert dfid == 'df-1'
        assert dfname == 'Sales DF'
        mock_df.get_dataflow_name.assert_called_once_with('ws-1', 'df-1')

    @patch('msdev_kit.fabric.pipeline.Dataflow')
    def test_resolves_by_name(self, MockDataflow, pl):
        mock_df = MockDataflow.return_value
        mock_df.get_dataflow_name.return_value = ''  # Not found as ID
        mock_df.list_dataflows.return_value = {
            'message': 'Success',
            'content': [
                {'id': 'df-1', 'name': 'Alpha DF'},
                {'id': 'df-2', 'name': 'Beta DF'},
            ]
        }

        dfid, dfname = pl._resolve_dataflow_id('ws-1', 'Beta DF')

        assert dfid == 'df-2'
        assert dfname == 'Beta DF'

    @patch('msdev_kit.fabric.pipeline.Dataflow')
    def test_resolves_by_name_case_insensitive(self, MockDataflow, pl):
        mock_df = MockDataflow.return_value
        mock_df.get_dataflow_name.return_value = ''
        mock_df.list_dataflows.return_value = {
            'message': 'Success',
            'content': [{'id': 'df-1', 'name': 'My Dataflow'}]
        }

        dfid, dfname = pl._resolve_dataflow_id('ws-1', 'my dataflow')

        assert dfid == 'df-1'
        assert dfname == 'My Dataflow'

    @patch('msdev_kit.fabric.pipeline.Dataflow')
    def test_returns_none_when_not_found(self, MockDataflow, pl):
        mock_df = MockDataflow.return_value
        mock_df.get_dataflow_name.return_value = ''
        mock_df.list_dataflows.return_value = {
            'message': 'Success',
            'content': [{'id': 'df-1', 'name': 'Other DF'}]
        }

        dfid, dfname = pl._resolve_dataflow_id('ws-1', 'Nonexistent')

        assert dfid is None
        assert dfname is None


# ===========================================================================
# get_pipeline_activities — structure, key names, and name resolution
# ===========================================================================

class TestGetPipelineActivities:

    def test_missing_workspace_id(self, pl):
        result = pl.get_pipeline_activities('', 'pl-1')
        assert 'Missing workspace id' in result['message']

    def test_missing_pipeline_id_or_name(self, pl):
        result = pl.get_pipeline_activities('ws-1', '')
        assert 'Missing pipeline id or name' in result['message']

    @patch('msdev_kit.fabric.pipeline.Dataset')
    @patch('msdev_kit.fabric.pipeline.Notebook')
    @patch('msdev_kit.fabric.pipeline.Dataflow')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_returns_correct_keys(self, mock_req, MockDataflow, MockNotebook, MockDataset, pl):
        """Each activity should have pipeline_id, pipeline_name, activity_name, activity_type, typeProperties."""
        definition = _make_pipeline_definition([
            {'name': 'Wait 1', 'type': 'Wait', 'typeProperties': {'waitTimeInSeconds': 5}}
        ])

        # _resolve_pipeline -> get_pipeline
        pipeline_resp = _make_response(200, {'id': 'pl-1', 'displayName': 'Test Pipeline'})
        # get_pipeline_definition
        definition_resp = _make_response(200, definition)
        mock_req.side_effect = [pipeline_resp, definition_resp]

        result = pl.get_pipeline_activities('ws-1', 'pl-1')

        assert result['message'] == 'Success'
        activity = result['content'][0]
        assert activity['pipeline_id'] == 'pl-1'
        assert activity['pipeline_name'] == 'Test Pipeline'
        assert activity['activity_name'] == 'Wait 1'
        assert activity['activity_type'] == 'Wait'
        assert 'typeProperties' in activity

    @patch('msdev_kit.fabric.pipeline.Dataset')
    @patch('msdev_kit.fabric.pipeline.Notebook')
    @patch('msdev_kit.fabric.pipeline.Dataflow')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_accepts_pipeline_name(self, mock_req, MockDataflow, MockNotebook, MockDataset, pl):
        """Should resolve a pipeline by display name when ID lookup fails."""
        definition = _make_pipeline_definition([
            {'name': 'Wait 1', 'type': 'Wait', 'typeProperties': {'waitTimeInSeconds': 5}}
        ])

        # _resolve_pipeline -> get_pipeline (fails, not an ID)
        not_found = _make_response(404, {'error': {'message': 'Not found'}})
        # _resolve_pipeline -> list_pipelines
        list_resp = _make_response(200, {
            'value': [{'id': 'pl-1', 'displayName': 'My Pipeline'}]
        })
        # get_pipeline_definition
        definition_resp = _make_response(200, definition)
        mock_req.side_effect = [not_found, list_resp, definition_resp]

        result = pl.get_pipeline_activities('ws-1', 'My Pipeline')

        assert result['message'] == 'Success'
        assert result['content'][0]['pipeline_id'] == 'pl-1'
        assert result['content'][0]['pipeline_name'] == 'My Pipeline'

    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_pipeline_not_found(self, mock_req, pl):
        """Should return error when pipeline cannot be resolved."""
        not_found = _make_response(404, {'error': {'message': 'Not found'}})
        list_resp = _make_response(200, {'value': []})
        mock_req.side_effect = [not_found, list_resp]

        result = pl.get_pipeline_activities('ws-1', 'Nonexistent')

        assert 'Pipeline not found' in result['message']

    @patch('msdev_kit.fabric.pipeline.Dataset')
    @patch('msdev_kit.fabric.pipeline.Notebook')
    @patch('msdev_kit.fabric.pipeline.Dataflow')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_object_name_in_type_properties_for_dataflow(self, mock_req, MockDataflow, MockNotebook, MockDataset, pl):
        """RefreshDataflow activities should have object_name as first key in typeProperties."""
        definition = _make_pipeline_definition([
            {
                'name': 'Refresh DF',
                'type': 'RefreshDataflow',
                'typeProperties': {'dataflowId': 'df-1', 'workspaceId': 'ws-1'}
            }
        ])

        pipeline_resp = _make_response(200, {'id': 'pl-1', 'displayName': 'Test Pipeline'})
        definition_resp = _make_response(200, definition)
        mock_req.side_effect = [pipeline_resp, definition_resp]

        mock_df_instance = MockDataflow.return_value
        mock_df_instance.get_dataflow_name.return_value = 'Sales Dataflow'

        result = pl.get_pipeline_activities('ws-1', 'pl-1')

        activity = result['content'][0]
        props = activity['typeProperties']
        keys = list(props.keys())
        assert keys[0] == 'object_name'
        assert props['object_name'] == 'Sales Dataflow'
        assert props['dataflowId'] == 'df-1'

    @patch('msdev_kit.fabric.pipeline.Dataset')
    @patch('msdev_kit.fabric.pipeline.Notebook')
    @patch('msdev_kit.fabric.pipeline.Dataflow')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_object_name_for_notebook(self, mock_req, MockDataflow, MockNotebook, MockDataset, pl):
        definition = _make_pipeline_definition([
            {
                'name': 'Run NB',
                'type': 'TridentNotebook',
                'typeProperties': {'notebookId': 'nb-1', 'workspaceId': 'ws-1'}
            }
        ])

        pipeline_resp = _make_response(200, {'id': 'pl-1', 'displayName': 'Test Pipeline'})
        definition_resp = _make_response(200, definition)
        mock_req.side_effect = [pipeline_resp, definition_resp]

        mock_nb_instance = MockNotebook.return_value
        mock_nb_instance.get_notebook.return_value = {
            'message': 'Success',
            'content': {'displayName': 'ETL Notebook'}
        }

        result = pl.get_pipeline_activities('ws-1', 'pl-1')

        assert result['content'][0]['typeProperties']['object_name'] == 'ETL Notebook'

    @patch('msdev_kit.fabric.pipeline.Dataset')
    @patch('msdev_kit.fabric.pipeline.Notebook')
    @patch('msdev_kit.fabric.pipeline.Dataflow')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_object_name_for_invoke_pipeline(self, mock_req, MockDataflow, MockNotebook, MockDataset, pl):
        definition = _make_pipeline_definition([
            {
                'name': 'Call Child',
                'type': 'InvokePipeline',
                'typeProperties': {'pipelineId': 'pl-child', 'workspaceId': 'ws-1'}
            }
        ])

        # _resolve_pipeline for parent
        pipeline_resp = _make_response(200, {'id': 'pl-1', 'displayName': 'Parent Pipeline'})
        # get_pipeline_definition
        definition_resp = _make_response(200, definition)
        # get_pipeline for the child (during name resolution)
        child_resp = _make_response(200, {'id': 'pl-child', 'displayName': 'Child Pipeline'})
        mock_req.side_effect = [pipeline_resp, definition_resp, child_resp]

        result = pl.get_pipeline_activities('ws-1', 'pl-1')

        assert result['content'][0]['typeProperties']['object_name'] == 'Child Pipeline'

    @patch('msdev_kit.fabric.pipeline.Dataset')
    @patch('msdev_kit.fabric.pipeline.Notebook')
    @patch('msdev_kit.fabric.pipeline.Dataflow')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_object_name_for_dataset_refresh(self, mock_req, MockDataflow, MockNotebook, MockDataset, pl):
        definition = _make_pipeline_definition([
            {
                'name': 'Refresh Model',
                'type': 'DatasetRefresh',
                'typeProperties': {'datasetId': 'ds-1', 'workspaceId': 'ws-1'}
            }
        ])

        pipeline_resp = _make_response(200, {'id': 'pl-1', 'displayName': 'Test Pipeline'})
        definition_resp = _make_response(200, definition)
        mock_req.side_effect = [pipeline_resp, definition_resp]

        mock_ds_instance = MockDataset.return_value
        mock_ds_instance.get_dataset_name.return_value = 'Sales Model'

        result = pl.get_pipeline_activities('ws-1', 'pl-1')

        assert result['content'][0]['typeProperties']['object_name'] == 'Sales Model'

    @patch('msdev_kit.fabric.pipeline.Dataset')
    @patch('msdev_kit.fabric.pipeline.Notebook')
    @patch('msdev_kit.fabric.pipeline.Dataflow')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_no_object_name_for_unsupported_type(self, mock_req, MockDataflow, MockNotebook, MockDataset, pl):
        """Activities like Wait should not have object_name in typeProperties."""
        definition = _make_pipeline_definition([
            {'name': 'Wait 1', 'type': 'Wait', 'typeProperties': {'waitTimeInSeconds': 5}}
        ])

        pipeline_resp = _make_response(200, {'id': 'pl-1', 'displayName': 'Test Pipeline'})
        definition_resp = _make_response(200, definition)
        mock_req.side_effect = [pipeline_resp, definition_resp]

        result = pl.get_pipeline_activities('ws-1', 'pl-1')

        assert 'object_name' not in result['content'][0]['typeProperties']

    @patch('msdev_kit.fabric.pipeline.Dataset')
    @patch('msdev_kit.fabric.pipeline.Notebook')
    @patch('msdev_kit.fabric.pipeline.Dataflow')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_multiple_activities_mixed_types(self, mock_req, MockDataflow, MockNotebook, MockDataset, pl):
        definition = _make_pipeline_definition([
            {
                'name': 'Refresh DF',
                'type': 'RefreshDataflow',
                'typeProperties': {'dataflowId': 'df-1', 'workspaceId': 'ws-1'}
            },
            {'name': 'Wait', 'type': 'Wait', 'typeProperties': {'waitTimeInSeconds': 5}},
            {
                'name': 'Run NB',
                'type': 'TridentNotebook',
                'typeProperties': {'notebookId': 'nb-1', 'workspaceId': 'ws-1'}
            },
        ])

        pipeline_resp = _make_response(200, {'id': 'pl-1', 'displayName': 'Mixed Pipeline'})
        definition_resp = _make_response(200, definition)
        mock_req.side_effect = [pipeline_resp, definition_resp]

        mock_df_instance = MockDataflow.return_value
        mock_df_instance.get_dataflow_name.return_value = 'DF Name'
        mock_nb_instance = MockNotebook.return_value
        mock_nb_instance.get_notebook.return_value = {
            'message': 'Success', 'content': {'displayName': 'NB Name'}
        }

        result = pl.get_pipeline_activities('ws-1', 'pl-1')

        assert len(result['content']) == 3
        assert result['content'][0]['typeProperties']['object_name'] == 'DF Name'
        assert 'object_name' not in result['content'][1]['typeProperties']
        assert result['content'][2]['typeProperties']['object_name'] == 'NB Name'

    @patch('msdev_kit.fabric.pipeline.Dataset')
    @patch('msdev_kit.fabric.pipeline.Notebook')
    @patch('msdev_kit.fabric.pipeline.Dataflow')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_deduplicates_resolution_for_same_id(self, mock_req, MockDataflow, MockNotebook, MockDataset, pl):
        """Two activities referencing the same dataflow should only resolve once."""
        definition = _make_pipeline_definition([
            {
                'name': 'Refresh DF 1',
                'type': 'RefreshDataflow',
                'typeProperties': {'dataflowId': 'df-1', 'workspaceId': 'ws-1'}
            },
            {
                'name': 'Refresh DF 2',
                'type': 'RefreshDataflow',
                'typeProperties': {'dataflowId': 'df-1', 'workspaceId': 'ws-1'}
            },
        ])

        pipeline_resp = _make_response(200, {'id': 'pl-1', 'displayName': 'Test Pipeline'})
        definition_resp = _make_response(200, definition)
        mock_req.side_effect = [pipeline_resp, definition_resp]

        mock_df_instance = MockDataflow.return_value
        mock_df_instance.get_dataflow_name.return_value = 'Shared DF'

        result = pl.get_pipeline_activities('ws-1', 'pl-1')

        assert result['content'][0]['typeProperties']['object_name'] == 'Shared DF'
        assert result['content'][1]['typeProperties']['object_name'] == 'Shared DF'
        mock_df_instance.get_dataflow_name.assert_called_once_with('ws-1', 'df-1')

    @patch('msdev_kit.fabric.pipeline.Dataset')
    @patch('msdev_kit.fabric.pipeline.Notebook')
    @patch('msdev_kit.fabric.pipeline.Dataflow')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_uses_activity_workspace_when_different(self, mock_req, MockDataflow, MockNotebook, MockDataset, pl):
        """When typeProperties has a different workspaceId, use that for resolution."""
        definition = _make_pipeline_definition([
            {
                'name': 'Refresh Remote DF',
                'type': 'RefreshDataflow',
                'typeProperties': {'dataflowId': 'df-1', 'workspaceId': 'ws-other'}
            },
        ])

        pipeline_resp = _make_response(200, {'id': 'pl-1', 'displayName': 'Test Pipeline'})
        definition_resp = _make_response(200, definition)
        mock_req.side_effect = [pipeline_resp, definition_resp]

        mock_df_instance = MockDataflow.return_value
        mock_df_instance.get_dataflow_name.return_value = 'Remote DF'

        pl.get_pipeline_activities('ws-1', 'pl-1')

        mock_df_instance.get_dataflow_name.assert_called_once_with('ws-other', 'df-1')

    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_no_pipeline_content_json(self, mock_req, pl):
        """Should handle missing pipeline-content.json gracefully."""
        definition = {'definition': {'parts': [{'path': 'other.json', 'payload': ''}]}}

        pipeline_resp = _make_response(200, {'id': 'pl-1', 'displayName': 'Test Pipeline'})
        definition_resp = _make_response(200, definition)
        mock_req.side_effect = [pipeline_resp, definition_resp]

        result = pl.get_pipeline_activities('ws-1', 'pl-1')

        assert 'No pipeline-content.json' in result['message']


# ===========================================================================
# find_pipelines_by_dataflow — ID and name resolution
# ===========================================================================

class TestFindPipelinesByDataflow:

    def test_missing_workspace_id(self, pl):
        result = pl.find_pipelines_by_dataflow('', 'df-1')
        assert 'Missing workspace id' in result['message']

    def test_missing_dataflow_id_or_name(self, pl):
        result = pl.find_pipelines_by_dataflow('ws-1', '')
        assert 'Missing dataflow id or name' in result['message']

    @patch('msdev_kit.fabric.pipeline.Dataset')
    @patch('msdev_kit.fabric.pipeline.Notebook')
    @patch('msdev_kit.fabric.pipeline.Dataflow')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_finds_matching_pipeline_by_dataflow_id(self, mock_req, MockDataflow, MockNotebook, MockDataset, pl):
        """Should resolve dataflow by ID and find pipelines referencing it."""
        definition = _make_pipeline_definition([
            {
                'name': 'Refresh Target',
                'type': 'RefreshDataflow',
                'typeProperties': {'dataflowId': 'df-target', 'workspaceId': 'ws-1'}
            },
            {
                'name': 'Refresh Other',
                'type': 'RefreshDataflow',
                'typeProperties': {'dataflowId': 'df-other', 'workspaceId': 'ws-1'}
            },
        ])

        # _resolve_dataflow_id uses mocked Dataflow
        mock_df_instance = MockDataflow.return_value
        mock_df_instance.get_dataflow_name.return_value = 'Target DF'

        # list_pipelines
        list_resp = _make_response(200, {
            'value': [{'id': 'pl-1', 'displayName': 'Pipeline A'}]
        })
        # get_pipeline_activities -> _resolve_pipeline -> get_pipeline
        pipeline_meta = _make_response(200, {'id': 'pl-1', 'displayName': 'Pipeline A'})
        # get_pipeline_activities -> get_pipeline_definition
        definition_resp = _make_response(200, definition)
        mock_req.side_effect = [list_resp, pipeline_meta, definition_resp]

        result = pl.find_pipelines_by_dataflow('ws-1', 'df-target')

        assert result['message'] == 'Success'
        assert len(result['content']) == 1
        assert result['content'][0]['pipeline_name'] == 'Pipeline A'
        assert 'Refresh Target' in result['content'][0]['activities']
        assert 'Refresh Other' not in result['content'][0]['activities']

    @patch('msdev_kit.fabric.pipeline.Dataset')
    @patch('msdev_kit.fabric.pipeline.Notebook')
    @patch('msdev_kit.fabric.pipeline.Dataflow')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_finds_matching_pipeline_by_dataflow_name(self, mock_req, MockDataflow, MockNotebook, MockDataset, pl):
        """Should resolve dataflow by display name and find pipelines referencing it."""
        definition = _make_pipeline_definition([
            {
                'name': 'Refresh Target',
                'type': 'RefreshDataflow',
                'typeProperties': {'dataflowId': 'df-target', 'workspaceId': 'ws-1'}
            },
        ])

        # _resolve_dataflow_id: get_dataflow_name returns '' (not an ID)
        mock_df_instance = MockDataflow.return_value
        mock_df_instance.get_dataflow_name.return_value = ''
        # _resolve_dataflow_id: list_dataflows finds the name
        mock_df_instance.list_dataflows.return_value = {
            'message': 'Success',
            'content': [{'id': 'df-target', 'name': 'Target DF'}]
        }

        # list_pipelines
        list_resp = _make_response(200, {
            'value': [{'id': 'pl-1', 'displayName': 'Pipeline A'}]
        })
        # get_pipeline_activities -> _resolve_pipeline -> get_pipeline
        pipeline_meta = _make_response(200, {'id': 'pl-1', 'displayName': 'Pipeline A'})
        # get_pipeline_activities -> get_pipeline_definition
        definition_resp = _make_response(200, definition)
        mock_req.side_effect = [list_resp, pipeline_meta, definition_resp]

        result = pl.find_pipelines_by_dataflow('ws-1', 'Target DF')

        assert result['message'] == 'Success'
        assert len(result['content']) == 1
        assert 'Refresh Target' in result['content'][0]['activities']

    @patch('msdev_kit.fabric.pipeline.Dataflow')
    def test_dataflow_not_found(self, MockDataflow, pl):
        """Should return error when dataflow cannot be resolved."""
        mock_df_instance = MockDataflow.return_value
        mock_df_instance.get_dataflow_name.return_value = ''
        mock_df_instance.list_dataflows.return_value = {
            'message': 'Success',
            'content': []
        }

        result = pl.find_pipelines_by_dataflow('ws-1', 'Nonexistent')

        assert 'Dataflow not found' in result['message']

    @patch('msdev_kit.fabric.pipeline.Dataset')
    @patch('msdev_kit.fabric.pipeline.Notebook')
    @patch('msdev_kit.fabric.pipeline.Dataflow')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_no_matches(self, mock_req, MockDataflow, MockNotebook, MockDataset, pl):
        definition = _make_pipeline_definition([
            {
                'name': 'Refresh Other',
                'type': 'RefreshDataflow',
                'typeProperties': {'dataflowId': 'df-other', 'workspaceId': 'ws-1'}
            },
        ])

        mock_df_instance = MockDataflow.return_value
        mock_df_instance.get_dataflow_name.return_value = 'Target DF'

        list_resp = _make_response(200, {
            'value': [{'id': 'pl-1', 'displayName': 'Pipeline A'}]
        })
        pipeline_meta = _make_response(200, {'id': 'pl-1', 'displayName': 'Pipeline A'})
        definition_resp = _make_response(200, definition)
        mock_req.side_effect = [list_resp, pipeline_meta, definition_resp]

        result = pl.find_pipelines_by_dataflow('ws-1', 'df-target')

        assert result['message'] == 'Success'
        assert len(result['content']) == 0


# ===========================================================================
# _request_with_retry (429 handling)
# ===========================================================================

class TestPipelineRetry:

    @patch('msdev_kit.fabric.pipeline.time.sleep')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_retries_on_429(self, mock_req, mock_sleep, pl):
        rate_limited = _make_response(429, {})
        rate_limited.headers = {'Retry-After': '1'}
        success = _make_response(200, {'id': 'pl-1', 'displayName': 'PL'})
        mock_req.side_effect = [rate_limited, success]

        result = pl.get_pipeline('ws-1', 'pl-1')

        assert result['message'] == 'Success'
        assert mock_sleep.called
        assert mock_req.call_count == 2

    @patch('msdev_kit.fabric.pipeline.time.sleep')
    @patch('msdev_kit.fabric.pipeline.requests.request')
    def test_gives_up_after_max_retries(self, mock_req, mock_sleep, pl):
        rate_limited = _make_response(429, {'error': {'message': 'Too many requests'}})
        rate_limited.headers = {'Retry-After': '1'}
        mock_req.return_value = rate_limited

        result = pl.get_pipeline('ws-1', 'pl-1')

        assert 'error' in result['message']
        # 1 initial + 3 retries = 4 total
        assert mock_req.call_count == 4
