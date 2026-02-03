import os
import json
import base64
import requests
import pandas as pd
from time import sleep
from operations import Operations
from typing import Dict, List, Any
from utilities import create_directory


class Report:

    def __init__(self, token: str):
        """
        Initialize variables.
        """
        self.main_url = 'https://api.powerbi.com/v1.0/myorg'
        self.main_fabric_url = 'https://api.fabric.microsoft.com/v1'
        self.token = token
        self.headers = {'Authorization': f'Bearer {self.token}'}
        self.data_dir = './data/reports'

        create_directory(self.data_dir)


    def list_reports(
                self,
                workspace_id: str = '') -> Dict:
        """
        List all reports on a specific workspace_id that the user has access to.

        Args:
            workspace_id (str, optional): workspace id to search reports from.

        Returns:
            Dict: status message and content.
        """

        # Main URL
        request_url = f'{self.main_url}/groups/{workspace_id}/reports'

        # If workspace ID was not informed, return error message...
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}

        # If workspace ID was informed...
        else:
            filename = f'reports_{workspace_id}.xlsx'

            # Make the request
            r = requests.get(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content).get('value', '')

            # If success...
            if status == 200:
                # Save to Excel file
                df = pd.DataFrame(response)
                df.to_excel(f'{self.data_dir}/{filename}', index=False)

                return {'message': 'Success', 'content': response}

            else:
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response['error']['message']

                return {'message': {'error': error_message, 'content': response}}


    def get_report_metadata(
                self,
                workspace_id: str = '',
                report_id: str = '') -> Dict:
        """
        Get report metadata for a specific report_id and workspace_id that the user has access to.

        Args:
            workspace_id (str, optional): workspace id where the report is.
            report_id (str, optional): report id to search pages from.

        Returns:
            Dict: status message and content.
        """

        # Main URL
        request_url = f'{self.main_url}/groups/{workspace_id}/reports/{report_id}'

        # If workspace ID was not informed, return error message...
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}

        # If workspace ID was informed...
        else:
            filename = f'report_{report_id}.xlsx'
            filepath = f'{self.data_dir}/pages/{filename}'
            os.makedirs(filepath, exist_ok=True)

            # Make the request
            r = requests.get(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content)

            # If success...
            if status == 200:
                # # Save to Excel file
                # df = pd.DataFrame(response)
                # try:
                #     df.to_excel(filepath, index=False)
                # except PermissionError as error:
                #     print('File is open already, cannot save it. Skipping...')

                return {'message': 'Success', 'content': response}

            else:
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response['error']['message']

                return {'message': {'error': error_message, 'content': response}}


    def get_report_name(self, workspace_id: str, report_id: str) -> str:
        """
        Get Power BI report name.

        Args:
            workspace_id (str): report workspace ID.
            report_id (str): report ID.

        Returns:
            report_name: Power BI report name.
        """
        report_name = self.get_report_metadata(workspace_id, report_id).get('content').get('name')

        return report_name


    def list_report_pages(
                self,
                workspace_id: str = '',
                report_id: str = '') -> Dict:
        """
        List all report pages on a specific report_id and workspace_id that the user has access to.

        Args:
            workspace_id (str, optional): workspace id where the report is.
            report_id (str, optional): report id to search pages from.

        Returns:
            Dict: status message and content.
        """

        # Main URL
        request_url = f'{self.main_url}/groups/{workspace_id}/reports/{report_id}/pages'

        # If workspace ID was not informed, return error message...
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}

        # If workspace ID was informed...
        else:
            filename = f'report_pages_{report_id}.xlsx'
            filepath = f'{self.data_dir}/pages/{filename}'
            os.makedirs(filepath, exist_ok=True)

            # Make the request
            r = requests.get(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content).get('value', '')

            # If success...
            if status == 200:
                # Save to Excel file
                df = pd.DataFrame(response)
                try:
                    df.to_excel(filepath, index=False)
                except PermissionError as error:
                    print('File is open already, cannot save it. Skipping...')

                return {'message': 'Success', 'content': response}

            else:
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response['error']['message']

                return {'message': {'error': error_message, 'content': response}}


    def get_report_json_pages_and_visuals(
                    self,
                    json_data: str,
                    workspace_id: str,
                    report_id: str) -> pd.DataFrame:
        """
        Parses a Power BI report JSON to extract pages and visual details,
        and returns the result as a Pandas DataFrame.

        Args:
            json_data (dict): Power BI report JSON (legacy).
            report_id (str): report id.

        Returns:
            Dict: status message and content.
        """
        def get_nested_value(data: dict, path: List[Any], default: Any = None) -> Any:
            """Safely traverses a nested dictionary/list structure."""
            current = data
            for key in path:
                if isinstance(current, dict):
                    current = current.get(key)
                elif isinstance(current, list) and isinstance(key, int) and len(current) > key:
                    current = current[key]
                else:
                    return default

                if current is None:
                    return default
            return current

        # List to hold flat dictionary records for the final DataFrame
        report_records: List[Dict[str, Any]] = []

        if isinstance(json_data, dict):
            data = json_data
        else:
            try:
                data = json.loads(json_data)
            except json.JSONDecodeError:
                print("Error: Invalid JSON format.")
                return pd.DataFrame()

        sections = get_nested_value(data, ['config', 'sections'])
        if not sections:
            sections = data.get('sections', [])

        for i, section in enumerate(sections):
            page_index = i+1
            page_name = section.get('displayName', 'Untitled Page')
            visual_containers = section.get('visualContainers', [])

            for vc in visual_containers:
                # The 'name' property inside 'config' is the unique Visual ID
                visual_id = get_nested_value(vc, ['config', 'name'], 'No ID')
                visual_type = 'Unknown'
                visual_title = 'No Title'
                vc_config = vc.get('config', {})

                # --- 1. Handle Visual Groups (e.g., Filter Pane) ---
                single_visual_group = vc_config.get('singleVisualGroup')
                if single_visual_group:
                    visual_type = 'Visual Group (Container)'
                    visual_title = single_visual_group.get('displayName', 'Visual Group')

                else:
                    # --- 2. Handle Single Visuals (Charts, Tables, etc.) ---
                    single_visual = vc_config.get('singleVisual', {})
                    if single_visual:
                        visual_type = single_visual.get('visualType', 'Generic Visual')
                        objects = single_visual.get('objects', {})
                        vc_objects = single_visual.get('vcObjects', {})

                        # Define all known paths for static literal title extraction
                        title_paths = [
                            # Path 1: Most common path for user-set title (your suggested path)
                            ['title', 0, 'properties', 'text', 'expr', 'Literal', 'Value'],
                            # Path 2: General Title (e.g., Navigators, some cards/KPIs)
                            ['general', 0, 'properties', 'title', 'expr', 'Literal', 'Value'],
                            # Path 3: Text Visual/Button Label (often the first text object)
                            ['text', 0, 'properties', 'text', 'expr', 'Literal', 'Value'],
                            # Path 4: Text Visual/Button Label (sometimes the second text object)
                            ['text', 1, 'properties', 'text', 'expr', 'Literal', 'Value'],
                            # Path 5: Text Visual/Button Label (sometimes the second text object)
                            ['text', 1, 'properties', 'text', 'expr', 'Literal', 'Value']
                        ]

                        # Check on Objects
                        found_title = 'No Title'
                        for path in title_paths:
                            title_value = get_nested_value(objects, path)

                            if isinstance(title_value, str) and title_value:
                                found_title = title_value.strip("'").replace('\'', "'")
                                break

                        # Check on vcObjects
                        for path in title_paths:
                            title_value = get_nested_value(vc_objects, path)

                            if isinstance(title_value, str) and title_value:
                                found_title = title_value.strip("'").replace('\'', "'")
                                break

                        visual_title = found_title if found_title != 'No Title' else visual_type

                # --- Diagnostic: Check for dynamic title expressions if literal is missing ---
                title_expression = None
                if visual_title == visual_type:
                    # Check for dynamic title expression
                    title_obj = get_nested_value(objects, ['title', 0, 'properties', 'text', 'expr'])
                    if title_obj and not get_nested_value(title_obj, ['Literal', 'Value']):
                        # Capture the full expression structure (DAX)
                        title_expression = str(title_obj)


                # Append the structured record
                record = {
                    'report_id': report_id,
                    'pageIndex': page_index,
                    'pageName': page_name,
                    'visual_id': visual_id,
                    'type': visual_type,
                    'title': visual_title,
                    'title_expression': title_expression # Contains DAX if title is dynamic
                }
                report_records.append(record)

        # Convert the list of records into a Pandas DataFrame
        report_name = self.get_report_name(workspace_id, report_id).replace(' ', '').replace('(', '').replace(')', '').strip()
        df = pd.DataFrame(report_records)
        df.sort_values(by=['pageIndex', 'title'], inplace=True)
        df.to_excel(f'{self.data_dir}/pages_and_visuals/{report_name}.xlsx', index=False)

        return df


    def get_legacy_report_json(
                self,
                workspace_id: str = '',
                report_id: str = '',
                operations: Operations = None) -> Dict:
        """
        Get a specific report_id definition.

        Args:
            workspace_id (str): workspace id where the report is.
            report_id (str): report id to search pages from.
            Operations (Operations): Operations class.

        Returns:
            Dict: status message and content.
        """
        def _decode_nested_json(value):
            """
            Recursively decodes nested JSON strings inside dicts and lists.
            """
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    return _decode_nested_json(parsed)
                except json.JSONDecodeError:
                    return value
            elif isinstance(value, dict):
                return {k: _decode_nested_json(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [_decode_nested_json(v) for v in value]
            return value

        def _decode_base64_json_to_file(encoded_str: str, workspace_id: str, report_id: str) -> None:
            """
            Decodes a Base64-encoded JSON string, fixes escaped JSON fields,
            and dumps the result into a formatted JSON file.

            Args:
                encoded_str (str): The Base64-encoded JSON string.
                output_path (str): Path where the final JSON file will be saved.
            """
            decoded_bytes = base64.b64decode(encoded_str)
            decoded_str = decoded_bytes.decode('utf-8')
            raw_data = json.loads(decoded_str)
            cleaned_data = _decode_nested_json(raw_data)

            report_name = self.get_report_name(workspace_id, report_id).replace(' ', '').replace('(', '').replace(')', '').strip()
            output_path = f'{self.data_dir}/definitions/{report_name}.json'

            with open(output_path, 'w', encoding='utf-8') as file:
                json.dump(cleaned_data, file, indent=4, ensure_ascii=False)

            return cleaned_data


        # Main URL
        request_url = f'{self.main_fabric_url}/workspaces/{workspace_id}/reports/{report_id}/getDefinition'

        # If workspace ID or report ID was not informed, return error message...
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}
        if report_id == '':
            return {'message': 'Missing report id, please check.', 'content': ''}

        # Continue...
        else:

            # Make the request
            r = requests.post(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content)
            print(f'status_code={r.status_code}')

            # If success...
            if status == 202:
                operation_id = r.headers.get('x-ms-operation-id')
                print(f'operation_id={operation_id}')

                while True:
                    active_operation_state = operations.get_operation_state(operation_id)
                    print('Operation state:', active_operation_state)
                    if active_operation_state:
                        operation_state = active_operation_state.get('operation_state', '')
                        if operation_state in ('Succeeded', 'Failed'):
                            sleep(1)
                            break

                    sleep(1)

                print('Getting operation result. This might take several minutes...')
                report_content = operations.get_operation_result(operation_id).get('content', '')

                print('Parsing report content...')
                report_definition = report_content.get('definition')
                report_format = report_definition.get('format')
                report_parts = report_definition.get('parts')

                if report_format.lower() == 'pbir-legacy':
                    for part in report_parts:
                        if part.get('path') == 'report.json':
                            report_json_byte_string = part.get('payload')
                            break

                    report_json = _decode_base64_json_to_file(report_json_byte_string, workspace_id, report_id)

                    return {'message': 'Success', 'content': report_json}

                # Report not on Legacy format.
                else:
                    return {'message': {'error': 'Report format invalid, only PBIR-Legacy is supported.', 'format': report_format}, 'content': report_definition}
            else:
                # If any error happens, return message.
                error_message = response['error']['message']

                return {'message': {'error': error_message}, 'content': response}


    def export_report(
                self,
                workspace_id: str = '',
                workspace_name: str = '',
                report_id: str = '',
                report_name: str = '',
                dataset_name: str = '',
                replace_existing: bool = False) -> Dict:
        """
        Export a specific report to a .pbix file.

        Args:
            workspace_id (str, optional): workspace id to search datasets from.
            workspace_name (str, optional): workspace name to be associated with the report.
            report_id (str, optional): report id to be exported.
            report_name (str, optional): report name to be saved.
            dataset_name (str, optional): dataset name to be associated with the report.
            replace_existing (bool, optional): if True, replace existing file with the same name.

        Returns:
            Dict: status message and content.
        """
        filename = f'{report_name}.pbix'
        file_path = f'{self.data_dir}/exports/{dataset_name}/{workspace_name}'

        file_exists = os.path.exists(f'{file_path}/{filename}')

        if (not replace_existing) and (file_exists):
            return {'message': f'File {filename} already exists.', 'content': ''}

        print(f'Exporting {report_name}')

        # Main URL
        request_url = f'{self.main_url}/groups/{workspace_id}/reports/{report_id}/Export/?DownloadType=LiveConnect'

        # If workspace ID was not informed, return error message...
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}
        if report_id == '':
            return {'message': 'Missing report id, please check.', 'content': ''}

        # If workspace ID and report ID were informed...
        else:

            create_directory(file_path)

            # Make the request
            r = requests.get(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code

            # If success...
            if status == 200:
                # Save to PBIX file
                with open(f'{file_path}/{filename}', 'wb') as f:
                    f.write(r.content)

                return {'message': 'Success', 'content': 'File downloaded successfully.'}

            else:
                print(f'Error exporting {report_name}:\n{r.content}')

                return {'message': {'error': f'Error with status code {status}'}, 'content': ''}


    def get_report_measures(
                self,
                workspace_id: str = '',
                report_id: str = '',
                operations: Operations = None) -> Dict:
        """
        Extract report-level measures from a report via the Fabric API
        and generate a DAX Query View script (.txt).

        Supports both PBIR and PBIR-Legacy formats. For PBIR, parses the
        reportExtensions.json part. For PBIR-Legacy, decodes report.json
        and extracts measures from config.modelExtensions.

        Args:
            workspace_id (str): workspace id where the report is.
            report_id (str): report id.
            operations (Operations): Operations class instance.

        Returns:
            Dict: status message and content with keys:
                - measures (list): extracted measure definitions.
                - model_measures (list): referenced semantic-model measures.
                - dax_script (str): generated DAX Query View script.
                - dax_script_path (str): path where the .txt was saved.
                - measures_json_path (str): path where the .json was saved.
        """

        # Main URL
        request_url = f'{self.main_fabric_url}/workspaces/{workspace_id}/reports/{report_id}/getDefinition'

        # If workspace ID or report ID was not informed, return error message...
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}
        if report_id == '':
            return {'message': 'Missing report id, please check.', 'content': ''}

        # Make the request
        r = requests.post(url=request_url, headers=self.headers)

        # Get HTTP status and content
        status = r.status_code
        response = json.loads(r.content)
        print(f'status_code={status}')

        if status != 202:
            error_message = response.get('error', {}).get('message', 'Unknown error')
            return {'message': {'error': error_message}, 'content': response}

        # Poll for operation completion
        operation_id = r.headers.get('x-ms-operation-id')
        print(f'operation_id={operation_id}')

        while True:
            active_operation_state = operations.get_operation_state(operation_id)
            print('Operation state:', active_operation_state)
            if active_operation_state:
                operation_state = active_operation_state.get('operation_state', '')
                if operation_state in ('Succeeded', 'Failed'):
                    sleep(1)
                    break

            sleep(1)

        # Get operation result
        print('Getting operation result...')
        report_content = operations.get_operation_result(operation_id).get('content', '')

        print('Parsing report content...')
        report_definition = report_content.get('definition')
        report_format = report_definition.get('format')
        report_parts = report_definition.get('parts')

        # Extract measures based on report format
        print(f'Report format: {report_format}')

        if report_format.lower() == 'pbir':
            # PBIR: measures live in reportExtensions.json
            extensions_payload = None
            for part in report_parts:
                if part.get('path') == 'definition/reportExtensions.json':
                    extensions_payload = part.get('payload')
                    break

            if extensions_payload is None:
                return {
                    'message': 'No reportExtensions.json found. This report has no report-level measures.',
                    'content': ''
                }

            decoded_bytes = base64.b64decode(extensions_payload)
            decoded_str = decoded_bytes.decode('utf-8')
            extensions_data = json.loads(decoded_str)
            measures = self._parse_report_extensions(extensions_data)

        elif report_format.lower() == 'pbir-legacy':
            # PBIR-Legacy: measures live in report.json -> config.modelExtensions
            report_payload = None
            for part in report_parts:
                if part.get('path') == 'report.json':
                    report_payload = part.get('payload')
                    break

            if report_payload is None:
                return {
                    'message': 'No report.json found in report definition.',
                    'content': ''
                }

            decoded_bytes = base64.b64decode(report_payload)
            decoded_str = decoded_bytes.decode('utf-8')
            report_json = json.loads(decoded_str)

            config = report_json.get('config', {})
            if isinstance(config, str):
                config = json.loads(config)
            model_extensions = config.get('modelExtensions', [])

            if not model_extensions:
                return {
                    'message': 'No modelExtensions found. This report has no report-level measures.',
                    'content': ''
                }

            # modelExtensions has the same entities[].measures[] structure
            extensions_data = {'entities': []}
            for ext in model_extensions:
                for entity in ext.get('entities', []):
                    extensions_data['entities'].append(entity)

            measures = self._parse_report_extensions(extensions_data)

        else:
            return {
                'message': {
                    'error': f'Unsupported report format: {report_format}',
                    'format': report_format
                },
                'content': ''
            }

        if not measures:
            return {'message': 'No report-level measures found.', 'content': ''}

        # Identify model-level dependencies
        model_measures = self._get_model_measure_references(measures)

        # Generate DAX Query View script
        dax_script = self._generate_dax_query_script(measures)

        # Save files
        report_name = self.get_report_name(workspace_id, report_id).replace(' ', '').replace('(', '').replace(')', '').strip()
        output_dir = f'{self.data_dir}/measures'
        os.makedirs(output_dir, exist_ok=True)

        # Save DAX script (.txt)
        dax_path = f'{output_dir}/{report_name}_measures.txt'
        with open(dax_path, 'w', encoding='utf-8') as f:
            f.write(dax_script)

        # Save measures list (.json)
        json_path = f'{output_dir}/{report_name}_measures.json'
        measures_export = [
            {k: v for k, v in m.items() if k != 'references'}
            for m in measures
        ]
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(measures_export, f, indent=2, ensure_ascii=False)

        print(f'Measures extracted: {len(measures)}')
        print(f'Model dependencies: {len(model_measures)}')
        print(f'DAX script saved to: {dax_path}')
        print(f'Measures JSON saved to: {json_path}')

        return {
            'message': 'Success',
            'content': {
                'measures': measures,
                'model_measures': model_measures,
                'dax_script': dax_script,
                'dax_script_path': dax_path,
                'measures_json_path': json_path
            }
        }


    def _parse_report_extensions(self, data: dict) -> List[Dict[str, Any]]:
        """
        Parse report extensions content and extract measure definitions.
        Works with both PBIR (reportExtensions.json) and PBIR-Legacy
        (config.modelExtensions) structures.

        Args:
            data (dict): parsed extensions content with 'entities' key.

        Returns:
            List[Dict]: list of measure dicts with keys:
                entity, name, dataType, expression, formatString,
                displayFolder, description, references.
        """
        measures = []

        for entity in data.get('entities', []):
            entity_name = entity.get('name', '')

            for m in entity.get('measures', []):
                expression = m.get('expression', '')
                expression = expression.replace('\r\n', '\n').strip()

                # formatString: PBIR uses top-level key,
                # PBIR-Legacy nests it under formatInformation
                format_string = m.get('formatString', '')
                if not format_string:
                    format_info = m.get('formatInformation', {})
                    if format_info:
                        format_string = format_info.get('formatString', '')

                measures.append({
                    'entity': entity_name,
                    'name': m.get('name', ''),
                    'dataType': m.get('dataType', ''),
                    'expression': expression,
                    'formatString': format_string,
                    'displayFolder': m.get('displayFolder', ''),
                    'description': m.get('description', ''),
                    'references': m.get('references', {}),
                })

        return measures


    def _get_model_measure_references(
                self,
                measures: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """
        Collect unique semantic-model measure references (not report-level).

        Model measures are those referenced WITHOUT "schema": "extension",
        meaning they live in the connected semantic model.

        Args:
            measures (list): parsed measure list from _parse_report_extensions.

        Returns:
            List[Dict]: sorted list of {'entity': ..., 'name': ...} dicts.
        """
        seen = set()
        model_measures = []

        for m in measures:
            for ref in m.get('references', {}).get('measures', []):
                if ref.get('schema') != 'extension':
                    entity = ref.get('entity', 'Calculations')
                    name = ref.get('name', '')
                    key = (entity, name)
                    if key not in seen:
                        seen.add(key)
                        model_measures.append({'entity': entity, 'name': name})

        model_measures.sort(key=lambda x: x['name'])
        return model_measures


    def _generate_dax_query_script(self, measures: List[Dict[str, Any]]) -> str:
        """
        Generate a DAX Query View script from report-level measures.

        The output follows the standard DAX Query View / DAX Studio format:
            - Commented model measure references at the top.
            - DEFINE block with all MEASURE definitions.
            - EVALUATE SUMMARIZECOLUMNS(...) block to validate.

        Args:
            measures (list): parsed measure list from _parse_report_extensions.

        Returns:
            str: complete DAX query script.
        """
        model_measures = self._get_model_measure_references(measures)

        lines = []

        # -- Commented model measure references -- #
        for mm in model_measures:
            lines.append(f"//  MEASURE '{mm['entity']}'[{mm['name']}]")

        # -- DEFINE block -- #
        lines.append('DEFINE')

        for m in measures:
            entity = m['entity']
            name = m['name']
            expr_lines = m['expression'].split('\n')
            first_line = expr_lines[0] if expr_lines else ''

            lines.append(f"    MEASURE '{entity}'[{name}] = {first_line}")

            for eline in expr_lines[1:]:
                lines.append(eline)

        # -- EVALUATE block -- #
        lines.append('')
        lines.append('EVALUATE')
        lines.append('    SUMMARIZECOLUMNS(')

        eval_items = []
        for mm in model_measures:
            eval_items.append(f'        "{mm["name"]}", [{mm["name"]}]')
        for m in measures:
            eval_items.append(f'        "{m["name"]}", [{m["name"]}]')

        for i, item in enumerate(eval_items):
            if i < len(eval_items) - 1:
                lines.append(item + ',')
            else:
                lines.append(item)

        lines.append('    )')

        return '\n'.join(lines) + '\n'


