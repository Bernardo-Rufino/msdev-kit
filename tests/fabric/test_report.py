"""
Unit tests for the Report class — measure parsing, script generation,
and report JSON visual extraction.

These tests use synthetic data and do not call any real API.

Usage:
    pytest tests/test_report.py -v
"""

import base64
import json
import pytest
from msdev_kit.fabric.report import Report


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class MockReport(Report):
    """Report instance that skips __init__ I/O (token, dirs)."""
    def __init__(self):
        self.main_url = 'https://api.powerbi.com/v1.0/myorg'
        self.main_fabric_url = 'https://api.fabric.microsoft.com/v1'
        self.token = 'fake-token'
        self.headers = {'Authorization': f'Bearer {self.token}'}


@pytest.fixture
def rpt():
    return MockReport()


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_EXTENSIONS = {
    'entities': [
        {
            'name': 'Sales',
            'measures': [
                {
                    'name': 'Total Revenue',
                    'dataType': 'double',
                    'expression': 'SUM(Sales[Revenue])',
                    'formatString': '#,##0.00',
                    'displayFolder': 'Revenue',
                    'description': 'Sum of all revenue',
                    'references': {
                        'measures': [
                            {'entity': 'Sales', 'name': 'Revenue', 'schema': 'model'},
                        ]
                    }
                },
                {
                    'name': 'YoY Growth',
                    'dataType': 'double',
                    'expression': 'DIVIDE(\n    [Total Revenue] - [PY Revenue],\n    [PY Revenue]\n)',
                    'formatString': '0.0%',
                    'displayFolder': 'Growth',
                    'description': '',
                    'references': {
                        'measures': [
                            {'entity': 'Sales', 'name': 'Total Revenue', 'schema': 'extension'},
                            {'entity': 'Sales', 'name': 'PY Revenue', 'schema': 'model'},
                        ]
                    }
                }
            ]
        },
        {
            'name': 'Inventory',
            'measures': [
                {
                    'name': 'Stock Count',
                    'dataType': 'int64',
                    'expression': 'COUNTROWS(Inventory)',
                    'formatInformation': {'formatString': '#,##0'},
                    'displayFolder': '',
                    'description': 'Total items in stock',
                    'references': {}
                }
            ]
        }
    ]
}

SAMPLE_REPORT_JSON = {
    'config': {
        'sections': [
            {
                'displayName': 'Overview',
                'visualContainers': [
                    {
                        'config': {
                            'name': 'visual-001',
                            'singleVisual': {
                                'visualType': 'barChart',
                                'objects': {
                                    'title': [
                                        {
                                            'properties': {
                                                'text': {
                                                    'expr': {
                                                        'Literal': {'Value': "'Revenue by Region'"}
                                                    }
                                                }
                                            }
                                        }
                                    ]
                                },
                                'vcObjects': {}
                            }
                        }
                    },
                    {
                        'config': {
                            'name': 'visual-002',
                            'singleVisualGroup': {
                                'displayName': 'Filter Panel'
                            }
                        }
                    },
                    {
                        'config': {
                            'name': 'visual-003',
                            'singleVisual': {
                                'visualType': 'card',
                                'objects': {},
                                'vcObjects': {}
                            }
                        }
                    }
                ]
            },
            {
                'displayName': 'Details',
                'visualContainers': [
                    {
                        'config': {
                            'name': 'visual-004',
                            'singleVisual': {
                                'visualType': 'tableEx',
                                'objects': {},
                                'vcObjects': {
                                    'title': [
                                        {
                                            'properties': {
                                                'text': {
                                                    'expr': {
                                                        'Literal': {'Value': "'Sales Detail Table'"}
                                                    }
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        ]
    }
}


def _b64(obj):
    """Encode a dict as InlineBase64 payload (matches Fabric API format)."""
    return base64.b64encode(json.dumps(obj).encode()).decode()


PBIR_PARTS = [
    {
        'path': 'definition/pages/pages.json',
        'payload': _b64({'pageOrder': ['page1', 'page2']}),
        'payloadType': 'InlineBase64',
    },
    {
        'path': 'definition/pages/page1/page.json',
        'payload': _b64({'name': 'page1', 'displayName': 'Overview'}),
        'payloadType': 'InlineBase64',
    },
    {
        'path': 'definition/pages/page1/visuals/vis001/visual.json',
        'payload': _b64({
            'name': 'vis001',
            'visual': {
                'visualType': 'barChart',
                'objects': {
                    'title': [{'properties': {'text': {'expr': {'Literal': {'Value': "'Revenue by Region'"}}}}}]
                },
            },
        }),
        'payloadType': 'InlineBase64',
    },
    {
        'path': 'definition/pages/page1/visuals/vis002/visual.json',
        'payload': _b64({
            'name': 'vis002',
            'visualGroup': {'displayName': 'Filter Panel'},
        }),
        'payloadType': 'InlineBase64',
    },
    {
        'path': 'definition/pages/page1/visuals/vis003/visual.json',
        'payload': _b64({
            'name': 'vis003',
            'visual': {'visualType': 'card', 'objects': {}},
        }),
        'payloadType': 'InlineBase64',
    },
    {
        'path': 'definition/pages/page2/page.json',
        'payload': _b64({'name': 'page2', 'displayName': 'Details'}),
        'payloadType': 'InlineBase64',
    },
    {
        'path': 'definition/pages/page2/visuals/vis004/visual.json',
        'payload': _b64({
            'name': 'vis004',
            'visual': {
                'visualType': 'tableEx',
                'objects': {
                    'title': [{'properties': {'text': {'expr': {'Literal': {'Value': "'Sales Table'"}}}}}]
                },
            },
        }),
        'payloadType': 'InlineBase64',
    },
]


# ===========================================================================
# _parse_report_extensions
# ===========================================================================

class TestParseReportExtensions:

    def test_extracts_all_measures(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        assert len(measures) == 3

    def test_measure_fields(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        revenue = next(m for m in measures if m['name'] == 'Total Revenue')

        assert revenue['entity'] == 'Sales'
        assert revenue['dataType'] == 'double'
        assert revenue['expression'] == 'SUM(Sales[Revenue])'
        assert revenue['formatString'] == '#,##0.00'
        assert revenue['displayFolder'] == 'Revenue'
        assert revenue['description'] == 'Sum of all revenue'

    def test_multiline_expression(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        yoy = next(m for m in measures if m['name'] == 'YoY Growth')

        assert '\n' in yoy['expression']
        assert 'DIVIDE(' in yoy['expression']

    def test_format_from_formatInformation(self, rpt):
        """PBIR-Legacy stores formatString under formatInformation."""
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        stock = next(m for m in measures if m['name'] == 'Stock Count')

        assert stock['formatString'] == '#,##0'

    def test_empty_entities(self, rpt):
        result = rpt._parse_report_extensions({'entities': []})
        assert result == []

    def test_missing_entities_key(self, rpt):
        result = rpt._parse_report_extensions({})
        assert result == []

    def test_carriage_return_stripped(self, rpt):
        """Expressions with \\r\\n should be normalized to \\n."""
        data = {
            'entities': [{
                'name': 'T',
                'measures': [{
                    'name': 'M',
                    'expression': 'SUM(\r\n    T[Col]\r\n)',
                }]
            }]
        }
        measures = rpt._parse_report_extensions(data)
        assert '\r\n' not in measures[0]['expression']
        assert '\n' in measures[0]['expression']


# ===========================================================================
# _get_model_measure_references
# ===========================================================================

class TestGetModelMeasureReferences:

    def test_excludes_extension_schema(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        model_refs = rpt._get_model_measure_references(measures)

        names = [m['name'] for m in model_refs]
        assert 'Revenue' in names
        assert 'PY Revenue' in names
        # extension schema measures should be excluded
        assert 'Total Revenue' not in names

    def test_deduplicates(self, rpt):
        """Same model measure referenced by multiple report measures should appear once."""
        measures = [
            {'references': {'measures': [{'entity': 'A', 'name': 'X', 'schema': 'model'}]}},
            {'references': {'measures': [{'entity': 'A', 'name': 'X', 'schema': 'model'}]}},
        ]
        result = rpt._get_model_measure_references(measures)
        assert len(result) == 1

    def test_sorted_by_name(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        model_refs = rpt._get_model_measure_references(measures)

        names = [m['name'] for m in model_refs]
        assert names == sorted(names)

    def test_empty_references(self, rpt):
        measures = [{'references': {}}]
        result = rpt._get_model_measure_references(measures)
        assert result == []

    def test_no_measures_key(self, rpt):
        measures = [{'references': {'tables': []}}]
        result = rpt._get_model_measure_references(measures)
        assert result == []


# ===========================================================================
# _generate_dax_query_script
# ===========================================================================

class TestGenerateDaxQueryScript:

    def test_contains_define_and_evaluate(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        script = rpt._generate_dax_query_script(measures)

        assert 'DEFINE' in script
        assert 'EVALUATE' in script
        assert 'SUMMARIZECOLUMNS(' in script

    def test_contains_all_measures(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        script = rpt._generate_dax_query_script(measures)

        assert "'Sales'[Total Revenue]" in script
        assert "'Sales'[YoY Growth]" in script
        assert "'Inventory'[Stock Count]" in script

    def test_model_measures_as_comments(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        script = rpt._generate_dax_query_script(measures)

        assert "//  MEASURE 'Sales'[Revenue]" in script
        assert "//  MEASURE 'Sales'[PY Revenue]" in script

    def test_multiline_expression_formatting(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        script = rpt._generate_dax_query_script(measures)

        # First line after = should be on the MEASURE line
        assert "MEASURE 'Sales'[YoY Growth] = DIVIDE(" in script

    def test_single_measure(self, rpt):
        measures = [{'entity': 'T', 'name': 'M', 'expression': '1+1',
                     'references': {}}]
        script = rpt._generate_dax_query_script(measures)

        assert "MEASURE 'T'[M] = 1+1" in script
        assert 'EVALUATE' in script


# ===========================================================================
# _generate_tmdl_script
# ===========================================================================

class TestGenerateTmdlScript:

    def test_contains_createOrReplace(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        script = rpt._generate_tmdl_script(measures)

        assert script.startswith('createOrReplace')

    def test_groups_by_entity(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        script = rpt._generate_tmdl_script(measures)

        assert 'ref table Sales' in script
        assert 'ref table Inventory' in script

    def test_single_line_expression(self, rpt):
        measures = [{'entity': 'T', 'name': 'Simple', 'expression': '1+1',
                     'formatString': '', 'displayFolder': '', 'description': ''}]
        script = rpt._generate_tmdl_script(measures)

        assert "measure 'Simple' = 1+1" in script

    def test_multiline_expression(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        script = rpt._generate_tmdl_script(measures)

        # Multi-line measure should use = on its own line
        assert "measure 'YoY Growth' =" in script

    def test_includes_format_string(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        script = rpt._generate_tmdl_script(measures)

        assert 'formatString: #,##0.00' in script
        assert 'formatString: 0.0%' in script

    def test_includes_display_folder(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        script = rpt._generate_tmdl_script(measures)

        assert 'displayFolder: Revenue' in script
        assert 'displayFolder: Growth' in script

    def test_includes_description_as_comment(self, rpt):
        measures = rpt._parse_report_extensions(SAMPLE_EXTENSIONS)
        script = rpt._generate_tmdl_script(measures)

        assert '/// Sum of all revenue' in script
        assert '/// Total items in stock' in script


# ===========================================================================
# get_legacy_report_pages_and_visuals — visual extraction
# ===========================================================================

class TestGetLegacyReportPagesAndVisuals:

    def test_extracts_visuals(self, rpt):
        # Mock get_report_name since it calls the API
        rpt.get_report_name = lambda ws, rid: 'TestReport'
        rpt.data_dir = '/tmp/test_reports'

        import os
        os.makedirs(f'{rpt.data_dir}/pages_and_visuals', exist_ok=True)

        df = rpt.get_legacy_report_pages_and_visuals(SAMPLE_REPORT_JSON, 'ws-1', 'rpt-1')

        assert len(df) == 4  # 3 visuals on page 1, 1 on page 2

    def test_extracts_page_names(self, rpt):
        rpt.get_report_name = lambda ws, rid: 'TestReport'
        rpt.data_dir = '/tmp/test_reports'

        import os
        os.makedirs(f'{rpt.data_dir}/pages_and_visuals', exist_ok=True)

        df = rpt.get_legacy_report_pages_and_visuals(SAMPLE_REPORT_JSON, 'ws-1', 'rpt-1')

        page_names = df['pageName'].unique().tolist()
        assert 'Overview' in page_names
        assert 'Details' in page_names

    def test_extracts_visual_title(self, rpt):
        rpt.get_report_name = lambda ws, rid: 'TestReport'
        rpt.data_dir = '/tmp/test_reports'

        import os
        os.makedirs(f'{rpt.data_dir}/pages_and_visuals', exist_ok=True)

        df = rpt.get_legacy_report_pages_and_visuals(SAMPLE_REPORT_JSON, 'ws-1', 'rpt-1')

        titles = df['title'].tolist()
        assert 'Revenue by Region' in titles
        assert 'Sales Detail Table' in titles

    def test_visual_group_detected(self, rpt):
        rpt.get_report_name = lambda ws, rid: 'TestReport'
        rpt.data_dir = '/tmp/test_reports'

        import os
        os.makedirs(f'{rpt.data_dir}/pages_and_visuals', exist_ok=True)

        df = rpt.get_legacy_report_pages_and_visuals(SAMPLE_REPORT_JSON, 'ws-1', 'rpt-1')

        group_row = df[df['type'] == 'Visual Group (Container)']
        assert len(group_row) == 1
        assert group_row.iloc[0]['title'] == 'Filter Panel'

    def test_visual_without_title_falls_back_to_type(self, rpt):
        rpt.get_report_name = lambda ws, rid: 'TestReport'
        rpt.data_dir = '/tmp/test_reports'

        import os
        os.makedirs(f'{rpt.data_dir}/pages_and_visuals', exist_ok=True)

        df = rpt.get_legacy_report_pages_and_visuals(SAMPLE_REPORT_JSON, 'ws-1', 'rpt-1')

        card_row = df[df['type'] == 'card']
        assert len(card_row) == 1
        assert card_row.iloc[0]['title'] == 'card'

    def test_invalid_json_returns_empty_df(self, rpt):
        df = rpt.get_legacy_report_pages_and_visuals('not valid json', 'ws-1', 'rpt-1')
        assert len(df) == 0

    def test_visual_container_config_as_string(self, rpt):
        """Real API responses encode each visualContainer's config as a JSON string."""
        rpt.get_report_name = lambda ws, rid: 'TestReport'
        rpt.data_dir = '/tmp/test_reports'

        import os
        os.makedirs(f'{rpt.data_dir}/pages_and_visuals', exist_ok=True)

        json_with_string_configs = {
            'config': {
                'sections': [
                    {
                        'displayName': 'Overview',
                        'visualContainers': [
                            {
                                'config': json.dumps({
                                    'name': 'visual-001',
                                    'singleVisual': {
                                        'visualType': 'barChart',
                                        'objects': {
                                            'title': [{'properties': {'text': {'expr': {'Literal': {'Value': "'Revenue by Region'"}}}}}]
                                        },
                                        'vcObjects': {},
                                    },
                                })
                            }
                        ]
                    }
                ]
            }
        }
        df = rpt.get_legacy_report_pages_and_visuals(json_with_string_configs, 'ws-1', 'rpt-1')
        assert len(df) == 1
        assert df.iloc[0]['visual_id'] == 'visual-001'
        assert df.iloc[0]['title'] == 'Revenue by Region'


# ===========================================================================
# get_pbir_report_pages_and_visuals
# ===========================================================================

class TestGetPbirReportPagesAndVisuals:

    def _setup(self, rpt):
        rpt.get_report_name = lambda ws, rid: 'TestReport'
        rpt.data_dir = '/tmp/test_reports'
        import os
        os.makedirs(f'{rpt.data_dir}/pages_and_visuals', exist_ok=True)

    def test_page_names(self, rpt):
        self._setup(rpt)
        df = rpt.get_pbir_report_pages_and_visuals(PBIR_PARTS, 'ws-1', 'rpt-1')
        assert sorted(df['pageName'].unique()) == ['Details', 'Overview']

    def test_visual_count(self, rpt):
        self._setup(rpt)
        df = rpt.get_pbir_report_pages_and_visuals(PBIR_PARTS, 'ws-1', 'rpt-1')
        assert len(df) == 4  # 3 on page1, 1 on page2

    def test_title_extracted_from_objects(self, rpt):
        self._setup(rpt)
        df = rpt.get_pbir_report_pages_and_visuals(PBIR_PARTS, 'ws-1', 'rpt-1')
        assert 'Revenue by Region' in df['title'].values

    def test_visual_group_type_and_title(self, rpt):
        self._setup(rpt)
        df = rpt.get_pbir_report_pages_and_visuals(PBIR_PARTS, 'ws-1', 'rpt-1')
        group = df[df['type'] == 'Visual Group (Container)']
        assert len(group) == 1
        assert group.iloc[0]['title'] == 'Filter Panel'

    def test_no_title_falls_back_to_type(self, rpt):
        self._setup(rpt)
        df = rpt.get_pbir_report_pages_and_visuals(PBIR_PARTS, 'ws-1', 'rpt-1')
        card = df[df['type'] == 'card']
        assert len(card) == 1
        assert card.iloc[0]['title'] == 'card'

    def test_page_index_follows_page_order(self, rpt):
        self._setup(rpt)
        df = rpt.get_pbir_report_pages_and_visuals(PBIR_PARTS, 'ws-1', 'rpt-1')
        assert df[df['pageName'] == 'Overview']['pageIndex'].iloc[0] == 1
        assert df[df['pageName'] == 'Details']['pageIndex'].iloc[0] == 2

    def test_missing_pages_json_returns_empty_df(self, rpt):
        self._setup(rpt)
        df = rpt.get_pbir_report_pages_and_visuals([], 'ws-1', 'rpt-1')
        assert len(df) == 0


# ===========================================================================
# get_report_pages_and_visuals — dispatcher
# ===========================================================================

class TestGetReportPagesAndVisuals:

    def _setup(self, rpt):
        rpt.get_report_name = lambda ws, rid: 'TestReport'
        rpt.data_dir = '/tmp/test_reports'
        import os
        os.makedirs(f'{rpt.data_dir}/pages_and_visuals', exist_ok=True)

    def test_pbir_format_routes_to_pbir_parser(self, rpt):
        self._setup(rpt)
        rpt.get_report_metadata = lambda ws, rid: {
            'message': 'Success',
            'content': {'format': 'PBIR'},
        }
        rpt.get_report_definition = lambda ws, rid, ops: {
            'message': 'Success',
            'content': {'format': 'PBIR', 'parts': PBIR_PARTS},
        }
        df = rpt.get_report_pages_and_visuals('ws-1', 'rpt-1', None)
        assert len(df) == 4
        assert 'Overview' in df['pageName'].values

    def test_pbir_legacy_format_routes_to_legacy_parser(self, rpt):
        self._setup(rpt)
        report_json_payload = base64.b64encode(
            json.dumps(SAMPLE_REPORT_JSON).encode()
        ).decode()
        rpt.get_report_metadata = lambda ws, rid: {
            'message': 'Success',
            'content': {'format': 'PBIRLegacy'},
        }
        rpt.get_report_definition = lambda ws, rid, ops: {
            'message': 'Success',
            'content': {
                'format': 'PBIR-Legacy',
                'parts': [
                    {'path': 'report.json', 'payload': report_json_payload, 'payloadType': 'InlineBase64'},
                ],
            },
        }
        df = rpt.get_report_pages_and_visuals('ws-1', 'rpt-1', None)
        assert len(df) == 4
        assert 'Overview' in df['pageName'].values

    def test_format_check_is_case_insensitive(self, rpt):
        self._setup(rpt)
        rpt.get_report_metadata = lambda ws, rid: {
            'message': 'Success',
            'content': {'format': 'pbir'},  # all lowercase
        }
        rpt.get_report_definition = lambda ws, rid, ops: {
            'message': 'Success',
            'content': {'format': 'PBIR', 'parts': PBIR_PARTS},
        }
        df = rpt.get_report_pages_and_visuals('ws-1', 'rpt-1', None)
        assert len(df) == 4

    def test_metadata_failure_returns_empty_df(self, rpt):
        rpt.get_report_metadata = lambda ws, rid: {
            'message': {'error': 'Not found'},
            'content': '',
        }
        df = rpt.get_report_pages_and_visuals('ws-1', 'rpt-1', None)
        assert len(df) == 0

    def test_definition_failure_returns_empty_df(self, rpt):
        rpt.get_report_metadata = lambda ws, rid: {
            'message': 'Success',
            'content': {'format': 'PBIR'},
        }
        rpt.get_report_definition = lambda ws, rid, ops: {
            'message': {'error': 'Not found'},
            'content': '',
        }
        df = rpt.get_report_pages_and_visuals('ws-1', 'rpt-1', None)
        assert len(df) == 0

    def test_unknown_format_returns_empty_df(self, rpt):
        rpt.get_report_metadata = lambda ws, rid: {
            'message': 'Success',
            'content': {'format': 'unsupported'},
        }
        rpt.get_report_definition = lambda ws, rid, ops: {
            'message': 'Success',
            'content': {'format': 'unsupported', 'parts': []},
        }
        df = rpt.get_report_pages_and_visuals('ws-1', 'rpt-1', None)
        assert len(df) == 0

    def test_pbir_legacy_missing_report_json_returns_empty_df(self, rpt):
        rpt.get_report_metadata = lambda ws, rid: {
            'message': 'Success',
            'content': {'format': 'PBIRLegacy'},
        }
        rpt.get_report_definition = lambda ws, rid, ops: {
            'message': 'Success',
            'content': {'format': 'PBIR-Legacy', 'parts': []},
        }
        df = rpt.get_report_pages_and_visuals('ws-1', 'rpt-1', None)
        assert len(df) == 0
