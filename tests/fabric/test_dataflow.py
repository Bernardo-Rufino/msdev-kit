"""
Unit tests for the Dataflow class — get_dataflow_name (PBI + Fabric fallback)
and 429 retry handling.

These tests use mocked API responses and do not call any real API.

Usage:
    pytest tests/test_dataflow.py -v
"""

import base64
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


def _standard_warehouse_definition():
    document = (
        'section Section1;\r\n'
        'shared Orders = let\r\n  Source = #table({"A", "B"}, {})\r\n'
        'in\r\n  Source;\r\n'
        'shared Orders_DataDestination = let\r\n'
        '  Pattern = Fabric.Warehouse(),\r\n'
        '  Workspace = Pattern{[workspaceId = "destination-workspace"]}[Data],\r\n'
        '  Warehouse = Workspace{[warehouseId = "warehouse-id"]}[Data],\r\n'
        '  TableNavigation = Warehouse{[Item = "orders_table", Schema = "sales"]}?[Data]?,\r\n'
        '  Table = NavigationTable.CreateTableOnDemand(TableNavigation, 1)\r\n'
        'in\r\n  Table;\r\n'
        'shared Orders_WriteToDataDestination = let\r\n'
        '  Write = Pipeline.ExecuteAction(ValueAction.WithTransaction('
        '[Target = Orders_DataDestination], (txn) => {'
        'TableAction.DeleteRows(txn[Target]), '
        '() => TableAction.InsertRows(txn[Target], Orders_TransformForWriteToDataDestination)'
        '}))\r\n'
        'in\r\n  Write;\r\n'
        'shared Orders_TransformForWriteToDataDestination = let\r\n'
        '  SourceTable = Table.SelectColumns(Orders, {"A", "B"})\r\n'
        'in\r\n  SourceTable;\r\n'
    )
    return {
        'name': 'Source dataflow',
        'pbi:mashup': {
            'document': document,
            'queriesMetadata': {'Orders': {'queryId': 'query-id'}},
            'connectionOverrides': [{'path': 'Warehouse', 'kind': 'Warehouse'}],
        },
    }


def _mixed_destination_definition():
    source = _standard_warehouse_definition()
    source['pbi:mashup']['document'] += (
        '[BindToDefaultDestination = true]\r\n'
        'shared Returns = let\r\n'
        '  Source = #table({"A"}, {})\r\n'
        'in\r\n'
        '  Source;\r\n'
        'shared DefaultDestination = let\r\n'
        '  Source = Lakehouse.Contents(),\r\n'
        '  Workspace = Source{[workspaceId = "destination-workspace"]}[Data],\r\n'
        '  Lakehouse = Workspace{[lakehouseId = "lakehouse-id"]}[Data]\r\n'
        'in\r\n'
        '  Lakehouse;\r\n'
    )
    source['pbi:mashup']['queriesMetadata']['Returns'] = {'queryId': 'returns-id'}
    source['pbi:mashup']['connectionOverrides'].append({
        'path': 'Lakehouse', 'kind': 'Lakehouse',
    })
    return source


class TestUpgradeDestinationPreservation:
    def test_mixed_default_and_per_query_destinations(self, df):
        source = _mixed_destination_definition()
        definition = df._convert_gen2_to_cicd_definition(source, 'copy')

        source_rows = df._get_data_destinations_standard(source)['content']
        converted_rows = df._get_data_destinations_cicd(definition)['content']
        expected = {
            ('Orders', 'Warehouse', 'warehouse-id'),
            ('Returns', 'Lakehouse', 'lakehouse-id'),
        }
        assert {(r['name'], r['destination_type'], r['item_id'])
                for r in source_rows} == expected
        assert {(r['name'], r['destination_type'], r['item_id'])
                for r in converted_rows} == expected

    def test_upgrade_accepts_mixed_destinations_when_preserved(self, df):
        source = _mixed_destination_definition()
        df.get_dataflow_gen2_definition = MagicMock(return_value={
            'message': 'not native',
        })
        df._get_dataflow_pbi_definition = MagicMock(return_value={
            'message': 'Success', 'content': source,
        })
        df._get_accessible_fabric_connections = MagicMock(return_value={
            'message': 'Success', 'content': [
                {'id': 'warehouse-connection', 'gatewayId': 'gateway',
                 'connectivityType': 'ShareableCloud',
                 'connectionDetails': {'type': 'Warehouse', 'path': 'Warehouse'}},
                {'id': 'lakehouse-connection', 'gatewayId': 'gateway',
                 'connectivityType': 'ShareableCloud',
                 'connectionDetails': {'type': 'Lakehouse', 'path': 'Lakehouse'}},
            ],
        })
        df.create_dataflow_gen2_from_definition = MagicMock(return_value={
            'message': 'Success', 'content': {'id': 'new-id'},
        })

        result = df.upgrade_to_gen2_cicd(
            'source-workspace', 'source-id', source_type='gen2',
            use_accessible_connections=True,
        )

        assert result['message'] == 'Success'
        df.create_dataflow_gen2_from_definition.assert_called_once()

    def test_standard_warehouse_reuses_destination_and_replace_settings(self, df):
        source = _standard_warehouse_definition()
        df.get_dataflow_gen2_definition = MagicMock(return_value={'message': 'not native'})
        df._get_dataflow_pbi_definition = MagicMock(
            return_value={'message': 'Success', 'content': source}
        )
        df._get_dataflow_pbi_datasources = MagicMock(return_value={
            'message': 'Success', 'content': [{
                'datasourceType': 'Extension',
                'connectionDetails': {
                    'extensionDataSourceKind': 'Warehouse',
                    'extensionDataSourcePath': 'Warehouse',
                },
                'gatewayId': 'gateway-id',
                'datasourceId': 'datasource-id',
            }]
        })
        df.create_dataflow_gen2_from_definition = MagicMock(
            return_value={'message': 'Success', 'content': {'id': 'new-id'}}
        )

        result = df.upgrade_to_gen2_cicd(
            'source-workspace', 'source-id', destination_workspace_id='new-workspace',
            source_type='gen2',
        )

        assert result['message'] == 'Success'
        create_args = df.create_dataflow_gen2_from_definition.call_args.args
        assert create_args[0] == 'new-workspace'
        converted = create_args[2]
        destinations = df._get_data_destinations_cicd(converted)['content']
        assert destinations == [{
            'name': 'Orders', 'destination_type': 'Warehouse',
            'workspace_id': 'destination-workspace', 'item_id': 'warehouse-id',
            'sql_schema': 'sales', 'mapping_type': 'Manual',
            'columns': [{'source': 'A', 'destination': 'A'},
                        {'source': 'B', 'destination': 'B'}],
        }]
        mashup = next(part for part in converted['definition']['parts']
                      if part['path'] == 'mashup.pq')
        m_code = base64.b64decode(mashup['payload']).decode('utf-8')
        assert 'UpdateMethod = [Kind = "Replace"]' in m_code
        assert 'Item = "orders_table"' in m_code
        metadata = next(part for part in converted['definition']['parts']
                        if part['path'] == 'queryMetadata.json')
        connections = json.loads(base64.b64decode(metadata['payload']))['connections']
        assert connections == [{
            'path': 'Warehouse', 'kind': 'Warehouse',
            'connectionId': '{"ClusterId":"gateway-id","DatasourceId":"datasource-id"}',
        }]

    def test_blocks_creation_when_destination_changes(self, df):
        source = _standard_warehouse_definition()
        df.get_dataflow_gen2_definition = MagicMock(return_value={'message': 'not native'})
        df._get_dataflow_pbi_definition = MagicMock(
            return_value={'message': 'Success', 'content': source}
        )
        real_convert = df._convert_gen2_to_cicd_definition

        def wrong_destination(content, name, settings):
            converted = real_convert(content, name, settings)
            mashup = next(part for part in converted['definition']['parts']
                          if part['path'] == 'mashup.pq')
            m_code = base64.b64decode(mashup['payload']).decode('utf-8')
            m_code = m_code.replace('warehouse-id', 'wrong-warehouse')
            mashup['payload'] = base64.b64encode(m_code.encode('utf-8')).decode('utf-8')
            return converted

        df._convert_gen2_to_cicd_definition = wrong_destination
        df.create_dataflow_gen2_from_definition = MagicMock()

        result = df.upgrade_to_gen2_cicd('source-workspace', 'source-id', source_type='gen2')

        assert 'destination' in str(result['message']).lower()
        df.create_dataflow_gen2_from_definition.assert_not_called()

    def test_binds_only_source_data_sources_and_drops_unused_overrides(self, df):
        source = _standard_warehouse_definition()
        source['pbi:mashup']['connectionOverrides'].insert(
            0, {'kind': 'SQL', 'path': 'unused-server;unused-database'}
        )
        df.get_dataflow_gen2_definition = MagicMock(return_value={'message': 'not native'})
        df._get_dataflow_pbi_definition = MagicMock(
            return_value={'message': 'Success', 'content': source}
        )
        df._get_dataflow_pbi_datasources = MagicMock(return_value={
            'message': 'Success', 'content': [{
                'datasourceType': 'Extension',
                'connectionDetails': {
                    'extensionDataSourceKind': 'Warehouse',
                    'extensionDataSourcePath': 'Warehouse',
                },
                'gatewayId': 'gateway-id', 'datasourceId': 'datasource-id',
            }]
        })
        df.create_dataflow_gen2_from_definition = MagicMock(
            return_value={'message': 'Success', 'content': {'id': 'new-id'}}
        )

        result = df.upgrade_to_gen2_cicd(
            'source-workspace', 'source-id', source_type='gen2',
            pbi_access_token='pbi-token',
        )

        assert result['message'] == 'Success'
        assert df._get_dataflow_pbi_definition.call_args.kwargs['headers'] == {
            'Authorization': 'Bearer pbi-token',
        }
        assert df._get_dataflow_pbi_datasources.call_args.kwargs['headers'] == {
            'Authorization': 'Bearer pbi-token',
        }
        definition = df.create_dataflow_gen2_from_definition.call_args.args[2]
        metadata = next(part for part in definition['definition']['parts']
                        if part['path'] == 'queryMetadata.json')
        connections = json.loads(base64.b64decode(metadata['payload']))['connections']
        assert len(connections) == 1
        assert connections[0]['kind'] == 'Warehouse'
        assert 'connectionId' in connections[0]

    def test_blocks_creation_when_source_connection_cannot_be_matched(self, df):
        source = _standard_warehouse_definition()
        df.get_dataflow_gen2_definition = MagicMock(return_value={'message': 'not native'})
        df._get_dataflow_pbi_definition = MagicMock(
            return_value={'message': 'Success', 'content': source}
        )
        df._get_dataflow_pbi_datasources = MagicMock(return_value={
            'message': 'Success', 'content': [{
                'datasourceType': 'Extension',
                'connectionDetails': {
                    'extensionDataSourceKind': 'Warehouse',
                    'extensionDataSourcePath': 'different-path',
                },
                'gatewayId': 'gateway-id', 'datasourceId': 'datasource-id',
            }]
        })
        df.create_dataflow_gen2_from_definition = MagicMock()

        result = df.upgrade_to_gen2_cicd('source-workspace', 'source-id', source_type='gen2')

        assert 'unique connection' in str(result['message'])
        df.create_dataflow_gen2_from_definition.assert_not_called()

    def test_binds_sql_server_connection_by_exact_path(self, df):
        source = _standard_warehouse_definition()
        source['pbi:mashup']['connectionOverrides'] = [
            {'kind': 'SQL', 'path': 'server;database'},
            {'kind': 'SQL', 'path': 'server'},
        ]
        definition = df._convert_gen2_to_cicd_definition(source, 'copy')

        df._bind_dataflow_connection_ids(definition, [{
            'datasourceType': 'Sql',
            'connectionDetails': {'server': 'server', 'database': 'database'},
            'gatewayId': 'gateway-id', 'datasourceId': 'datasource-id',
        }])

        metadata = next(part for part in definition['definition']['parts']
                        if part['path'] == 'queryMetadata.json')
        connections = json.loads(base64.b64decode(metadata['payload']))['connections']
        assert connections == [{
            'path': 'server;database', 'kind': 'SQL',
            'connectionId': '{"ClusterId":"gateway-id","DatasourceId":"datasource-id"}',
        }]

    def test_preserves_source_load_enabled_state(self, df):
        source = _standard_warehouse_definition()
        source['pbi:mashup']['queriesMetadata']['Orders']['loadEnabled'] = True
        source['pbi:mashup']['queriesMetadata']['Helper'] = {'queryId': 'helper-id'}

        metadata = df._build_query_metadata(source)

        assert metadata['queriesMetadata']['Orders']['loadEnabled'] is True
        assert metadata['queriesMetadata']['Helper']['loadEnabled'] is False

    def test_binds_exact_accessible_sql_and_warehouse_connections(self, df):
        source = _standard_warehouse_definition()
        document = source['pbi:mashup']['document'].replace(
            '#table({"A", "B"}, {})', 'Sql.Database("server", "database")'
        )
        source['pbi:mashup']['document'] = document
        source['pbi:mashup']['connectionOverrides'].append(
            {'kind': 'SQL', 'path': 'server'}
        )
        definition = df._convert_gen2_to_cicd_definition(source, 'copy')
        available = [
            {'id': 'personal-id', 'gatewayId': 'personal-gateway',
             'connectivityType': 'PersonalCloud',
             'connectionDetails': {'type': 'SQL', 'path': 'server;database'}},
            {'id': 'sql-id', 'gatewayId': 'shared-gateway',
             'connectivityType': 'ShareableCloud',
             'connectionDetails': {'type': 'SQL', 'path': 'server;database'}},
            {'id': 'warehouse-id', 'gatewayId': 'shared-gateway',
             'connectivityType': 'ShareableCloud',
             'connectionDetails': {'type': 'Warehouse', 'path': 'Warehouse'}},
        ]

        df._bind_accessible_connection_ids(definition, document, available)

        part = next(p for p in definition['definition']['parts']
                    if p['path'] == 'queryMetadata.json')
        metadata = json.loads(base64.b64decode(part['payload']))
        bindings = {(c['kind'], c['path']): json.loads(c['connectionId'])
                    for c in metadata['connections']}
        assert bindings == {
            ('SQL', 'server;database'):
                {'ClusterId': 'shared-gateway', 'DatasourceId': 'sql-id'},
            ('Warehouse', 'Warehouse'):
                {'ClusterId': 'shared-gateway', 'DatasourceId': 'warehouse-id'},
        }

    def test_upgrade_uses_accessible_connections_instead_of_source_ids(self, df):
        source = _standard_warehouse_definition()
        df.get_dataflow_gen2_definition = MagicMock(return_value={
            'message': 'not native',
        })
        df._get_dataflow_pbi_definition = MagicMock(return_value={
            'message': 'Success', 'content': source,
        })
        df._get_dataflow_pbi_datasources = MagicMock()
        df._get_accessible_fabric_connections = MagicMock(return_value={
            'message': 'Success', 'content': [{
                'id': 'shared-id', 'gatewayId': 'shared-gateway',
                'connectivityType': 'ShareableCloud',
                'connectionDetails': {'type': 'Warehouse', 'path': 'Warehouse'},
            }],
        })
        df.create_dataflow_gen2_from_definition = MagicMock(return_value={
            'message': 'Success', 'content': {'id': 'new-id'},
        })

        result = df.upgrade_to_gen2_cicd(
            'source-workspace', 'source-id', source_type='gen2',
            use_accessible_connections=True, pbi_access_token='pbi-token',
        )

        assert result['message'] == 'Success'
        assert df._get_dataflow_pbi_definition.call_args.kwargs['headers'] == {
            'Authorization': 'Bearer pbi-token',
        }
        assert df.headers == {'Authorization': 'Bearer fake-token'}
        df._get_dataflow_pbi_datasources.assert_not_called()
        definition = df.create_dataflow_gen2_from_definition.call_args.args[2]
        part = next(p for p in definition['definition']['parts']
                    if p['path'] == 'queryMetadata.json')
        assert json.loads(base64.b64decode(part['payload']))['connections'] == [{
            'kind': 'Warehouse', 'path': 'Warehouse',
            'connectionId': '{"ClusterId":"shared-gateway","DatasourceId":"shared-id"}',
        }]

    def test_rejects_ambiguous_accessible_connection(self, df):
        source = _standard_warehouse_definition()
        definition = df._convert_gen2_to_cicd_definition(source, 'copy')
        available = [
            {'id': id, 'gatewayId': 'shared-gateway',
             'connectivityType': 'ShareableCloud',
             'connectionDetails': {'type': 'Warehouse', 'path': 'Warehouse'}}
            for id in ('first', 'second')
        ]

        with pytest.raises(ValueError, match='found 2'):
            df._bind_accessible_connection_ids(
                definition, source['pbi:mashup']['document'], available
            )

    def test_rejects_dynamic_sql_path(self, df):
        source = _standard_warehouse_definition()
        document = source['pbi:mashup']['document'].replace(
            '#table({"A", "B"}, {})', 'Sql.Database(ServerParameter, DatabaseParameter)'
        )
        definition = df._convert_gen2_to_cicd_definition(source, 'copy')

        with pytest.raises(ValueError, match='dynamic SQL'):
            df._bind_accessible_connection_ids(definition, document, [])

    def test_rejects_sql_database_with_dynamic_suffix(self, df):
        source = _standard_warehouse_definition()
        document = source['pbi:mashup']['document'].replace(
            '#table({"A", "B"}, {})',
            'Sql.Database("server", "database" & Suffix)',
        )
        definition = df._convert_gen2_to_cicd_definition(source, 'copy')

        with pytest.raises(ValueError, match='dynamic SQL'):
            df._bind_accessible_connection_ids(definition, document, [])

    def test_accepts_sql_database_with_literal_options_argument(self, df):
        source = _standard_warehouse_definition()
        document = source['pbi:mashup']['document'].replace(
            '#table({"A", "B"}, {})',
            'Sql.Database("server", "database", [CreateNavigationProperties=false])',
        )
        definition = df._convert_gen2_to_cicd_definition(source, 'copy')
        available = [
            {'id': 'sql-id', 'gatewayId': 'shared-gateway',
             'connectivityType': 'ShareableCloud',
             'connectionDetails': {'type': 'SQL', 'path': 'server;database'}},
            {'id': 'warehouse-id', 'gatewayId': 'shared-gateway',
             'connectivityType': 'ShareableCloud',
             'connectionDetails': {'type': 'Warehouse', 'path': 'Warehouse'}},
        ]

        df._bind_accessible_connection_ids(definition, document, available)

        part = next(p for p in definition['definition']['parts']
                    if p['path'] == 'queryMetadata.json')
        paths = {c['path'] for c in json.loads(base64.b64decode(part['payload']))['connections']}
        assert paths == {'server;database', 'Warehouse'}


class TestUpgradeRefresh:
    def test_refresh_false_does_not_start_job(self, df):
        df._refresh_upgraded_dataflow = MagicMock()
        df.get_dataflow_gen2_definition = MagicMock(return_value={
            'message': 'Success', 'content': {'definition': {'parts': []}}
        })
        df.create_dataflow_gen2_from_definition = MagicMock(return_value={
            'message': 'Success', 'content': {'id': 'new-id'}
        })

        result = df.upgrade_to_gen2_cicd(
            'source-workspace', 'source-id', source_type='gen2'
        )

        assert result['message'] == 'Success'
        df._refresh_upgraded_dataflow.assert_not_called()

    def test_refresh_true_starts_job_for_created_item(self, df):
        df._refresh_upgraded_dataflow = MagicMock(return_value={
            'message': 'Success', 'content': {'id': 'new-id'},
            'refresh': {'status': 'Completed'},
        })
        df.get_dataflow_gen2_definition = MagicMock(return_value={
            'message': 'Success', 'content': {'definition': {'parts': []}}
        })
        df.create_dataflow_gen2_from_definition = MagicMock(return_value={
            'message': 'Success', 'content': {'id': 'new-id'}
        })

        result = df.upgrade_to_gen2_cicd(
            'source-workspace', 'source-id', source_type='gen2', refresh=True,
            refresh_access_token='delegated-token',
        )

        assert result['refresh']['status'] == 'Completed'
        assert df._refresh_upgraded_dataflow.call_args.args[1] == 'source-workspace'
        assert df._refresh_upgraded_dataflow.call_args.args[2] == 'delegated-token'

    def test_refresh_true_waits_for_completed_job(self, df):
        accepted = _make_response(202, {})
        accepted.headers['Location'] = (
            'https://api.fabric.microsoft.com/v1/workspaces/workspace/'
            'items/new-id/jobs/instances/job-id'
        )
        df._request_with_retry = MagicMock(side_effect=[
            accepted,
            _make_response(200, {'id': 'job-id', 'status': 'InProgress'}),
            _make_response(200, {'id': 'job-id', 'status': 'Completed'}),
        ])
        with patch('msdev_kit.fabric.dataflow.time.sleep'):
            result = df._refresh_upgraded_dataflow(
                {'message': 'Success', 'content': {'id': 'new-id'}}, 'workspace'
            )

        assert result['message'] == 'Success'
        assert result['refresh']['status'] == 'Completed'
        method, url = df._request_with_retry.call_args_list[0].args
        assert method == 'POST'
        assert url.endswith('/items/new-id/jobs/Refresh/instances')
        assert df._request_with_retry.call_args_list[0].kwargs['json'] == {
            'executionData': {'executeOption': 'ApplyChangesIfNeeded'}
        }

    def test_refresh_uses_delegated_token_without_changing_client(self, df):
        accepted = _make_response(202, {})
        accepted.headers['Location'] = 'https://api.fabric.microsoft.com/job-id'
        df._request_with_retry = MagicMock(side_effect=[
            accepted, _make_response(200, {'id': 'job-id', 'status': 'Completed'}),
        ])

        with patch('msdev_kit.fabric.dataflow.time.sleep'):
            result = df._refresh_upgraded_dataflow(
                {'message': 'Success', 'content': {'id': 'new-id'}},
                'workspace', 'delegated-token',
            )

        assert result['refresh']['status'] == 'Completed'
        for call in df._request_with_retry.call_args_list:
            assert call.kwargs['headers']['Authorization'] == 'Bearer delegated-token'
        assert df.headers['Authorization'] == 'Bearer fake-token'

    def test_refresh_failure_preserves_new_item_id(self, df):
        accepted = _make_response(202, {})
        accepted.headers['Location'] = 'https://api.fabric.microsoft.com/job-id'
        df._request_with_retry = MagicMock(side_effect=[
            accepted, _make_response(200, {
                'id': 'job-id', 'status': 'Failed',
                'failureReason': {'errorCode': 'CredentialError'},
            })
        ])
        with patch('msdev_kit.fabric.dataflow.time.sleep'):
            result = df._refresh_upgraded_dataflow(
                {'message': 'Success', 'content': {'id': 'new-id'}}, 'workspace'
            )

        assert 'failed' in result['message']['error']
        assert result['content']['id'] == 'new-id'
        assert result['refresh']['failureReason']['errorCode'] == 'CredentialError'

    def test_standard_warehouse_preserves_append_method(self, df):
        source = _standard_warehouse_definition()
        source['pbi:mashup']['document'] = source['pbi:mashup']['document'].replace(
            'TableAction.DeleteRows(txn[Target]), ', ''
        )

        converted = df._convert_gen2_to_cicd_definition(source, 'copy')
        mashup = next(part for part in converted['definition']['parts']
                      if part['path'] == 'mashup.pq')
        m_code = base64.b64decode(mashup['payload']).decode('utf-8')

        assert 'UpdateMethod = [Kind = "Append"]' in m_code
        assert df._get_data_destinations_cicd(converted)['content'][0]['item_id'] == 'warehouse-id'

    def test_blocks_unknown_source_destination_before_create(self, df):
        source = _standard_warehouse_definition()
        source['pbi:mashup']['document'] = source['pbi:mashup']['document'].replace(
            'Fabric.Warehouse()', 'Unknown.Destination()'
        )
        df.get_dataflow_gen2_definition = MagicMock(return_value={'message': 'not native'})
        df._get_dataflow_pbi_definition = MagicMock(
            return_value={'message': 'Success', 'content': source}
        )
        df.create_dataflow_gen2_from_definition = MagicMock()

        result = df.upgrade_to_gen2_cicd('source-workspace', 'source-id', source_type='gen2')

        assert 'destination' in str(result['message']).lower()
        df.create_dataflow_gen2_from_definition.assert_not_called()

    def test_blocks_unresolved_source_destination_id(self, df):
        source = _standard_warehouse_definition()
        source['pbi:mashup']['document'] = source['pbi:mashup']['document'].replace(
            'warehouseId = "warehouse-id"', 'warehouseId = ""'
        )
        df.get_dataflow_gen2_definition = MagicMock(return_value={'message': 'not native'})
        df._get_dataflow_pbi_definition = MagicMock(
            return_value={'message': 'Success', 'content': source}
        )
        df.create_dataflow_gen2_from_definition = MagicMock()

        result = df.upgrade_to_gen2_cicd('source-workspace', 'source-id', source_type='gen2')

        assert 'destination' in str(result['message']).lower()
        df.create_dataflow_gen2_from_definition.assert_not_called()

    def test_blocks_unrecognized_column_transform(self, df):
        source = _standard_warehouse_definition()
        source['pbi:mashup']['document'] = source['pbi:mashup']['document'].replace(
            'Table.SelectColumns(Orders, {"A", "B"})',
            'Table.RenameColumns(Orders, {{"A", "renamed"}})'
        )
        df.get_dataflow_gen2_definition = MagicMock(return_value={'message': 'not native'})
        df._get_dataflow_pbi_definition = MagicMock(
            return_value={'message': 'Success', 'content': source}
        )
        df.create_dataflow_gen2_from_definition = MagicMock()

        result = df.upgrade_to_gen2_cicd('source-workspace', 'source-id', source_type='gen2')

        assert 'mapping' in str(result['message']).lower()
        df.create_dataflow_gen2_from_definition.assert_not_called()

    def test_native_gen2_copy_uses_source_definition_without_retargeting(self, df):
        definition = {'definition': {'parts': [{'path': 'mashup.pq', 'payload': 'unchanged'}]},
                      'displayName': 'native'}
        df.get_dataflow_gen2_definition = MagicMock(
            return_value={'message': 'Success', 'content': definition}
        )
        df.create_dataflow_gen2_from_definition = MagicMock(
            return_value={'message': 'Success', 'content': {'id': 'new-id'}}
        )

        result = df.upgrade_to_gen2_cicd(
            'source-workspace', 'source-id', destination_workspace_id='new-workspace',
            source_type='gen2',
        )

        assert result['message'] == 'Success'
        df.create_dataflow_gen2_from_definition.assert_called_once_with(
            'new-workspace', 'native_cicd', definition
        )
