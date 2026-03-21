"""
Tests for the Dataflow class — destination change, CI/CD conversion, and integration tests.

Offline tests use fixture JSON files and do not call any API.
Integration tests create/update real dataflows and require valid credentials in utils/.env.

Usage:
    pytest tests/test_dataflow.py -v                    # offline tests only (default)
    pytest tests/test_dataflow.py -v --run-integration  # include integration tests
"""

import os
import re
import sys
import json
import base64
import copy
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from fabric_api.dataflow import Dataflow

# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

TESTS_DIR = os.path.dirname(__file__)

FIXTURE_STANDARD_LH = os.path.join(TESTS_DIR, 'dataflow_gen2_example_no_fast_copy_with_combine_queries.json')
FIXTURE_STANDARD_DW = os.path.join(TESTS_DIR, 'dataflow_gen2_example_no_fast_copy_with_combine_queries_dw.json')
FIXTURE_CICD_LH = os.path.join(TESTS_DIR, 'dataflow_gen2_cicd_example_no_fast_copy_with_partitioned_and_query_eval.json')
FIXTURE_CICD_DW = os.path.join(TESTS_DIR, 'dataflow_gen2_cicd_example_no_fast_copy_with_partitioned_and_query_eval_dw.json')

WORKSPACE_ID = '953072b3-e4c7-4551-8886-bbdca85faa2e'
LAKEHOUSE_ID = '1fad6446-6c9c-49cc-bb97-82c824944c53'
WAREHOUSE_ID = 'a40c0f15-2d35-4e95-9008-8b096007f577'

# Dataflow IDs for integration tests
STANDARD_DATAFLOW_ID = '506d7272-f0f3-4851-aed2-43ed47a6a3ed'  # Gen2 standard (non CI/CD)
CICD_DATAFLOW_ID = '06563e9b-07db-4689-b491-80e46d8aea28'      # Gen2 CI/CD


def load_fixture(path):
    if not os.path.exists(path):
        pytest.skip(f'Fixture file not found: {os.path.basename(path)}')
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def decode_cicd_part(definition, part_name):
    """Decode a base64-encoded CI/CD definition part."""
    for part in definition['definition']['parts']:
        if part['path'] == part_name:
            return base64.b64decode(part['payload']).decode('utf-8')
    return None


class MockDataflow(Dataflow):
    """Dataflow instance that skips __init__ I/O (token, dirs, Workspace)."""
    def __init__(self):
        self.main_url = 'https://api.powerbi.com/v1.0/myorg'
        self.fabric_api_base_url = 'https://api.fabric.microsoft.com'


@pytest.fixture(scope='session')
def df():
    return MockDataflow()


@pytest.fixture(scope='session')
def standard_lh():
    return load_fixture(FIXTURE_STANDARD_LH)


@pytest.fixture(scope='session')
def standard_dw():
    return load_fixture(FIXTURE_STANDARD_DW)


@pytest.fixture(scope='session')
def cicd_lh():
    return load_fixture(FIXTURE_CICD_LH)


@pytest.fixture(scope='session')
def cicd_dw():
    return load_fixture(FIXTURE_CICD_DW)


# ---------------------------------------------------------------------------
# Integration-test opt-in flag
# ---------------------------------------------------------------------------

def pytest_addoption(parser):
    parser.addoption('--run-integration', action='store_true', default=False,
                     help='Run integration tests that hit the Fabric/PBI API')


def pytest_configure(config):
    config.addinivalue_line('markers', 'integration: marks tests that call the real API')


def pytest_collection_modifyitems(config, items):
    if not config.getoption('--run-integration'):
        skip = pytest.mark.skip(reason='need --run-integration option to run')
        for item in items:
            if 'integration' in item.keywords:
                item.add_marker(skip)


# ===========================================================================
# Offline tests — _rewrite_data_destination_queries
# ===========================================================================

class TestRewriteDataDestinationQueries:

    def test_lakehouse_to_warehouse(self, df, standard_lh):
        m_code = standard_lh['pbi:mashup']['document']
        result = df._rewrite_data_destination_queries(m_code, 'Warehouse', 'ws-1', 'wh-1')

        assert 'Fabric.Warehouse' in result
        assert 'Lakehouse.Contents' not in result
        assert 'warehouseId = "wh-1"' in result
        assert 'lakehouseId' not in result
        assert 'Schema = "dbo"' in result

    def test_warehouse_to_lakehouse(self, df, standard_dw):
        m_code = standard_dw['pbi:mashup']['document']
        result = df._rewrite_data_destination_queries(m_code, 'Lakehouse', 'ws-1', 'lh-1')

        assert 'Lakehouse.Contents' in result
        assert 'Fabric.Warehouse' not in result
        assert 'lakehouseId = "lh-1"' in result
        assert 'warehouseId' not in result
        assert 'ItemKind = "Table"' in result

    def test_extracts_correct_table_name(self, df, standard_lh):
        m_code = standard_lh['pbi:mashup']['document']
        result = df._rewrite_data_destination_queries(m_code, 'Warehouse', 'ws-1', 'wh-1')

        # Should extract "cost_type_d", not a UUID from workspaceId/lakehouseId
        assert 'Item = "cost_type_d"' in result

    def test_preserves_non_destination_queries(self, df, standard_lh):
        m_code = standard_lh['pbi:mashup']['document']
        result = df._rewrite_data_destination_queries(m_code, 'Warehouse', 'ws-1', 'wh-1')

        assert 'shared cost_type_source = let' in result
        assert 'shared cost_type_d = let' in result
        assert 'shared GH_phanton_cost_type = let' in result

    def test_no_destination_queries_unchanged(self, df):
        m_code = 'section Section1;\r\nshared query1 = let\r\n  Source = 1\r\nin\r\n  Source;\r\n'
        result = df._rewrite_data_destination_queries(m_code, 'Warehouse', 'ws-1', 'wh-1')
        assert result == m_code

    def test_cicd_lakehouse_to_warehouse(self, df, cicd_lh):
        mashup = decode_cicd_part(cicd_lh, 'mashup.pq')
        result = df._rewrite_data_destination_queries(mashup, 'Warehouse', 'ws-1', 'wh-1')

        assert 'Fabric.Warehouse' in result
        assert 'Lakehouse.Contents' not in result
        assert 'warehouseId = "wh-1"' in result

    def test_cicd_warehouse_to_lakehouse(self, df, cicd_dw):
        mashup = decode_cicd_part(cicd_dw, 'mashup.pq')
        result = df._rewrite_data_destination_queries(mashup, 'Lakehouse', 'ws-1', 'lh-1')

        assert 'Lakehouse.Contents' in result
        assert 'Fabric.Warehouse' not in result
        assert 'lakehouseId = "lh-1"' in result


# ===========================================================================
# Offline tests — _update_destination_connections
# ===========================================================================

class TestUpdateDestinationConnections:

    def test_lakehouse_to_warehouse(self, df):
        connections = [
            {'path': 'server;db', 'kind': 'SQL'},
            {'path': 'Lakehouse', 'kind': 'Lakehouse', 'connectionName': '{"kind":"Lakehouse","path":"Lakehouse"}'},
        ]
        result = df._update_destination_connections(connections, 'Warehouse')

        assert result[0]['kind'] == 'SQL'
        assert result[0]['path'] == 'server;db'
        assert result[1]['kind'] == 'Warehouse'
        assert result[1]['path'] == 'Warehouse'
        assert '"kind": "Warehouse"' in result[1]['connectionName']

    def test_warehouse_to_lakehouse(self, df):
        connections = [
            {'path': 'Warehouse', 'kind': 'Warehouse'},
        ]
        result = df._update_destination_connections(connections, 'Lakehouse')
        assert result[0]['kind'] == 'Lakehouse'
        assert result[0]['path'] == 'Lakehouse'

    def test_does_not_mutate_original(self, df):
        original = [{'path': 'Lakehouse', 'kind': 'Lakehouse'}]
        original_copy = copy.deepcopy(original)
        df._update_destination_connections(original, 'Warehouse')
        assert original == original_copy

    def test_preserves_non_destination_connections(self, df):
        connections = [
            {'path': 'server;db', 'kind': 'SQL', 'provider': 'CdsA'},
        ]
        result = df._update_destination_connections(connections, 'Warehouse')
        assert result[0] == connections[0]

    def test_empty_list(self, df):
        assert df._update_destination_connections([], 'Warehouse') == []


# ===========================================================================
# Offline tests — change_data_destination (full method)
# ===========================================================================

class TestChangeDataDestination:

    def test_standard_lakehouse_to_warehouse(self, df, standard_lh):
        result = df.change_data_destination(standard_lh, 'Warehouse', WORKSPACE_ID, WAREHOUSE_ID)

        doc = result['pbi:mashup']['document']
        assert 'Fabric.Warehouse' in doc
        assert 'Lakehouse.Contents' not in doc
        assert 'warehouseId' in doc

        overrides = result['pbi:mashup']['connectionOverrides']
        dw_conns = [c for c in overrides if c['kind'] == 'Warehouse']
        lh_conns = [c for c in overrides if c['kind'] == 'Lakehouse']
        assert len(dw_conns) == 1
        assert len(lh_conns) == 0

        trusted = result['pbi:mashup']['trustedConnections']
        assert all(c['kind'] == 'Warehouse' for c in trusted)

    def test_standard_warehouse_to_lakehouse(self, df, standard_dw):
        result = df.change_data_destination(standard_dw, 'Lakehouse', WORKSPACE_ID, LAKEHOUSE_ID)

        doc = result['pbi:mashup']['document']
        assert 'Lakehouse.Contents' in doc
        assert 'Fabric.Warehouse' not in doc

    def test_cicd_lakehouse_to_warehouse(self, df, cicd_lh):
        result = df.change_data_destination(cicd_lh, 'Warehouse', WORKSPACE_ID, WAREHOUSE_ID)

        mashup = decode_cicd_part(result, 'mashup.pq')
        assert 'Fabric.Warehouse' in mashup
        assert 'Lakehouse.Contents' not in mashup

        meta_raw = decode_cicd_part(result, 'queryMetadata.json')
        meta = json.loads(meta_raw)
        dw_conns = [c for c in meta['connections'] if c['kind'] == 'Warehouse']
        lh_conns = [c for c in meta['connections'] if c['kind'] == 'Lakehouse']
        assert len(dw_conns) == 1
        assert len(lh_conns) == 0

    def test_cicd_warehouse_to_lakehouse(self, df, cicd_dw):
        result = df.change_data_destination(cicd_dw, 'Lakehouse', WORKSPACE_ID, LAKEHOUSE_ID)

        mashup = decode_cicd_part(result, 'mashup.pq')
        assert 'Lakehouse.Contents' in mashup
        assert 'Fabric.Warehouse' not in mashup

    def test_deep_copy_not_mutated(self, df, standard_lh):
        original_doc = standard_lh['pbi:mashup']['document']
        df.change_data_destination(standard_lh, 'Warehouse', 'ws', 'wh')
        assert standard_lh['pbi:mashup']['document'] == original_doc

    def test_invalid_destination_type(self, df, standard_lh):
        result = df.change_data_destination(standard_lh, 'InvalidType', 'ws', 'id')
        assert 'message' in result
        assert 'destination_type' in result['message']

    def test_unrecognized_format(self, df):
        result = df.change_data_destination({'random': 'data'}, 'Warehouse', 'ws', 'id')
        assert 'message' in result
        assert 'Unrecognized' in result['message']

    def test_standard_no_document(self, df):
        definition = {'pbi:mashup': {'document': '', 'connectionOverrides': []}}
        result = df.change_data_destination(definition, 'Warehouse', 'ws', 'id')
        assert 'message' in result
        assert 'No mashup document' in result['message']


# ===========================================================================
# Offline tests — _convert_gen2_to_cicd_definition
# ===========================================================================

class TestConvertGen2ToCicd:

    def test_produces_three_parts(self, df, standard_lh):
        result = df._convert_gen2_to_cicd_definition(standard_lh, 'test_name')
        assert result is not None
        paths = [p['path'] for p in result['definition']['parts']]
        assert set(paths) == {'queryMetadata.json', 'mashup.pq', '.platform'}

    def test_platform_display_name(self, df, standard_lh):
        result = df._convert_gen2_to_cicd_definition(standard_lh, 'my_display_name')
        platform_raw = decode_cicd_part(result, '.platform')
        platform = json.loads(platform_raw)
        assert platform['metadata']['displayName'] == 'my_display_name'

    def test_removes_internal_queries(self, df, standard_lh):
        result = df._convert_gen2_to_cicd_definition(standard_lh, 'test')
        mashup = decode_cicd_part(result, 'mashup.pq')
        assert 'WriteToDataDestination' not in mashup
        assert 'TransformForWriteToDataDestination' not in mashup
        assert 'DefaultStaging' not in mashup

    def test_adds_data_destinations_annotation(self, df, standard_lh):
        result = df._convert_gen2_to_cicd_definition(standard_lh, 'test')
        mashup = decode_cicd_part(result, 'mashup.pq')
        assert '[DataDestinations' in mashup

    def test_removes_staging_annotations(self, df, standard_lh):
        result = df._convert_gen2_to_cicd_definition(standard_lh, 'test')
        mashup = decode_cicd_part(result, 'mashup.pq')
        assert '[Staging = ' not in mashup

    def test_simplifies_data_destination_query(self, df, standard_lh):
        result = df._convert_gen2_to_cicd_definition(standard_lh, 'test')
        mashup = decode_cicd_part(result, 'mashup.pq')
        assert 'NavigationTable.CreateTableOnDemand' not in mashup
        assert 'TableNavigation;' in mashup

    def test_excludes_internal_queries_from_metadata(self, df, standard_lh):
        result = df._convert_gen2_to_cicd_definition(standard_lh, 'test')
        meta_raw = decode_cicd_part(result, 'queryMetadata.json')
        meta = json.loads(meta_raw)
        query_names = list(meta['queriesMetadata'].keys())
        assert not any(n.endswith('_WriteToDataDestination') for n in query_names)
        assert not any(n.endswith('_TransformForWriteToDataDestination') for n in query_names)
        assert 'DefaultStaging' not in query_names

    def test_data_destination_query_is_hidden(self, df, standard_lh):
        result = df._convert_gen2_to_cicd_definition(standard_lh, 'test')
        meta_raw = decode_cicd_part(result, 'queryMetadata.json')
        meta = json.loads(meta_raw)
        dd_queries = {k: v for k, v in meta['queriesMetadata'].items() if k.endswith('_DataDestination')}
        assert len(dd_queries) > 0
        for name, entry in dd_queries.items():
            assert entry.get('isHidden') is True

    def test_returns_none_for_empty_document(self, df):
        content = {'pbi:mashup': {'document': '', 'queriesMetadata': {}, 'connectionOverrides': []}, 'annotations': []}
        result = df._convert_gen2_to_cicd_definition(content, 'test')
        assert result is None


# ===========================================================================
# Offline tests — full pipeline: change destination + convert to CI/CD
# ===========================================================================

class TestFullPipeline:

    def test_standard_lh_to_cicd_with_dw_destination(self, df, standard_lh):
        """Simulate what create_dataflow_with_new_destination does for a standard source."""
        modified = df.change_data_destination(standard_lh, 'Warehouse', WORKSPACE_ID, WAREHOUSE_ID)
        cicd = df._convert_gen2_to_cicd_definition(modified, 'pipeline_test')

        mashup = decode_cicd_part(cicd, 'mashup.pq')
        assert 'Fabric.Warehouse' in mashup
        assert 'Lakehouse.Contents' not in mashup
        assert 'Item = "cost_type_d"' in mashup

        meta = json.loads(decode_cicd_part(cicd, 'queryMetadata.json'))
        kinds = [c['kind'] for c in meta['connections']]
        assert 'Warehouse' in kinds
        assert 'Lakehouse' not in kinds

    def test_cicd_dw_to_cicd_with_lh_destination(self, df, cicd_dw):
        """Simulate what create_dataflow_with_new_destination does for a CI/CD source."""
        modified = df.change_data_destination(cicd_dw, 'Lakehouse', WORKSPACE_ID, LAKEHOUSE_ID)

        mashup = decode_cicd_part(modified, 'mashup.pq')
        assert 'Lakehouse.Contents' in mashup
        assert 'Fabric.Warehouse' not in mashup
        assert 'Id = "cost_type_d"' in mashup

    def test_roundtrip_lh_to_dw_to_lh(self, df, standard_lh):
        """LH -> DW -> LH should reproduce an equivalent DataDestination query."""
        step1 = df.change_data_destination(standard_lh, 'Warehouse', WORKSPACE_ID, WAREHOUSE_ID)
        step2 = df.change_data_destination(step1, 'Lakehouse', WORKSPACE_ID, LAKEHOUSE_ID)

        doc = step2['pbi:mashup']['document']
        assert 'Lakehouse.Contents' in doc
        assert f'lakehouseId = "{LAKEHOUSE_ID}"' in doc
        assert f'workspaceId = "{WORKSPACE_ID}"' in doc
        assert 'Id = "cost_type_d"' in doc


# ===========================================================================
# Integration tests — require --run-integration and valid credentials
# ===========================================================================

@pytest.fixture(scope='session')
def live_dataflow():
    """Authenticated Dataflow instance for integration tests."""
    from dotenv import load_dotenv
    from fabric_api import Auth
    load_dotenv('./utils/.env')
    tenant = os.environ.get('TENANT_ID', '')
    client = os.environ.get('CLIENT_ID', '')
    secret = os.environ.get('CLIENT_SECRET', '')
    if not all([tenant, client, secret]):
        pytest.skip('Missing credentials in utils/.env')
    auth = Auth(tenant, client, secret)
    token = auth.get_token('pbi')
    return Dataflow(token)


@pytest.fixture(scope='session')
def created_dataflow_ids():
    """Collects IDs of dataflows created during integration tests for cleanup."""
    return []


@pytest.mark.integration
class TestIntegrationCreateWithNewDestination:

    def test_standard_to_warehouse(self, live_dataflow, created_dataflow_ids):
        result = live_dataflow.create_dataflow_with_new_destination(
            workspace_id=WORKSPACE_ID,
            dataflow_id=STANDARD_DATAFLOW_ID,
            destination_type='Warehouse',
            destination_workspace_id=WORKSPACE_ID,
            destination_item_id=WAREHOUSE_ID,
            display_name='int_test_standard_to_dw'
        )
        assert result['message'] == 'Success', f"Failed: {result}"
        created_dataflow_ids.append(result['content']['id'])

    def test_cicd_to_warehouse(self, live_dataflow, created_dataflow_ids):
        result = live_dataflow.create_dataflow_with_new_destination(
            workspace_id=WORKSPACE_ID,
            dataflow_id=CICD_DATAFLOW_ID,
            destination_type='Warehouse',
            destination_workspace_id=WORKSPACE_ID,
            destination_item_id=WAREHOUSE_ID,
            display_name='int_test_cicd_to_dw'
        )
        assert result['message'] == 'Success', f"Failed: {result}"
        created_dataflow_ids.append(result['content']['id'])


@pytest.mark.integration
class TestIntegrationReplaceDestination:

    def test_replace_cicd_in_place(self, live_dataflow, created_dataflow_ids):
        """Replace destination on a CI/CD dataflow that was just created."""
        if not created_dataflow_ids:
            pytest.skip('No dataflows created to replace')

        target_id = created_dataflow_ids[-1]
        result = live_dataflow.replace_dataflow_destination(
            workspace_id=WORKSPACE_ID,
            dataflow_id=target_id,
            destination_type='Lakehouse',
            destination_workspace_id=WORKSPACE_ID,
            destination_item_id=LAKEHOUSE_ID
        )
        assert result['message'] == 'Success', f"Failed: {result}"
        assert result['content']['id'] == target_id  # same ID = in-place update


@pytest.mark.integration
class TestIntegrationCleanup:

    def test_cleanup_created_dataflows(self, live_dataflow, created_dataflow_ids):
        """Delete all dataflows created during integration tests."""
        for df_id in created_dataflow_ids:
            result = live_dataflow.delete_dataflow(WORKSPACE_ID, df_id, type='fabric')
            print(f"  Deleted {df_id}: {result['message']}")
