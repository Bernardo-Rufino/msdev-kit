import os
import json
import base64
import requests
import pandas as pd
from time import sleep
from operations import Operations
from typing import Dict
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

        def _decode_base64_json_to_file(encoded_str: str, output_path: str) -> None:
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

            # If success...
            if status == 202:
                operation_id = r.headers.get('x-ms-operation-id')
            
                while True:
                    active_operation_state = operations.get_operation_state(operation_id)
                    print('Operation state:', active_operation_state)
                    if active_operation_state:
                        operation_state = active_operation_state.get('operation_state', '')
                        if operation_state in ('Succeeded', 'Failed'):
                            break

                    sleep(1)

                report_content = operations.get_operation_result(operation_id).get('content', '')
                report_definition = report_content.get('definition')
                report_format = report_definition.get('format')
                report_parts = report_definition.get('parts')

                if report_format == 'PBIR-Legacy':
                    for part in report_parts:
                        if part.get('path') == 'report.json':
                            report_json_byte_string = part.get('payload')
                            break
                    
                    report_json = _decode_base64_json_to_file(report_json_byte_string, 'decoded_clean.json')
                    report_json_content = report_json.get('content')
                    
                    return {'message': 'Success', 'content': report_json_content}

                # Report not on Legacy format.
                else:
                    return {'message': {'error': 'Report not on legacy format.', 'format': report_format}, 'content': report_definition}
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