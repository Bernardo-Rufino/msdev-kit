import os
import json
import requests
import pandas as pd
from . import workspace
from . import report
from typing import Dict
from .utilities import create_directory
from concurrent.futures import ThreadPoolExecutor


class Dataset:

    def __init__(self, token: str):
        """
        Initialize variables.
        """
        self.main_url = 'https://api.powerbi.com/v1.0/myorg'
        self.token = token
        self.headers = {'Authorization': f'Bearer {self.token}'}
        self.data_dir = './data/datasets'

        create_directory(self.data_dir)


    def get_dataset_details(
                self, 
                workspace_id: str = '',
                dataset_id: str = '') -> Dict:
        """
        Get details of a specific dataset.

        Args:
            workspace_id (str, optional): workspace id to search datasets from.
            dataset_id (str, optional): dataset id to search details from.

        Returns:
            Dict: status message and content.
        """

        # Main URL
        request_url = f'{self.main_url}/groups/{workspace_id}/datasets/{dataset_id}'

        # If workspace ID was not informed, return error message...
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}
        if dataset_id == '':
            return {'message': 'Missing dataset id, please check.', 'content': ''}

        # If workspace ID was informed...
        else: 
            filename = f'datasets_{workspace_id}_{dataset_id}.xlsx'

            # Make the request
            r = requests.get(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content)

            # If success...
            if status == 200:
                # Save to Excel file
                df = pd.DataFrame([response])
                df.to_excel(f'{self.data_dir}/{filename}', index=False)
                
                return {'message': 'Success', 'content': response}

            else:                
                # If any error happens, return message.
                response = json.loads(r.content)
                try:
                    error_message = response['error']['message']
                except KeyError as e:
                    error_message = response['error']['pbi.error']

                return {'message': {'error': error_message, 'content': response}}


    def list_datasets(
                self, 
                workspace_id: str = '') -> Dict:
        """
        List all datasets on a specific workspace_id that the user has access to.

        Args:
            workspace_id (str, optional): workspace id to search datasets from.

        Returns:
            Dict: status message and content.
        """

        # Main URL
        request_url = f'{self.main_url}/groups/{workspace_id}/datasets'

        # If workspace ID was not informed, return error message...
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}

        # If workspace ID was informed...
        else: 
            filename = f'datasets_{workspace_id}.xlsx'

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


    def _post_query(self, workspace_id: str, dataset_id: str, query: str) -> requests.Response:
        """
        Send a DAX query to the Power BI executeQueries API.

        Args:
            workspace_id (str): workspace id.
            dataset_id (str): dataset id.
            query (str): DAX query string.

        Returns:
            requests.Response: raw response from the API.
        """
        request_url = self.main_url + f'/groups/{workspace_id}/datasets/{dataset_id}/executeQueries'
        headers = {'Authorization': f'Bearer {self.token}'}
        data = {
            "queries": [{"query": query}],
            "serializerSettings": {"includeNulls": 'true'}
        }
        return requests.post(url=request_url, headers=headers, json=data)

    @staticmethod
    def _extract_table_expression(query: str) -> str:
        """
        Extract the table expression from a DAX EVALUATE query.

        Args:
            query (str): full DAX query starting with EVALUATE.

        Returns:
            str: the table expression after EVALUATE.
        """
        import re
        match = re.search(r'\bEVALUATE\b\s+(.*)', query, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
        return ''

    def execute_query(
                self,
                workspace_id: str = '',
                dataset_id: str = '',
                query: str = '',
                impersonated_username: str = '') -> Dict:
        """
        Execute a DAX query against a dataset.

        Before running the actual query, a COUNTROWS pre-check is performed to
        detect whether the API row/value limits would truncate the result.

        API limits:
            - Max 100,000 rows per query.
            - Max 1,000,000 values (rows x columns) per query.
            - Max 15 MB of data per query.
            - Whichever limit is hit first applies.

        Args:
            workspace_id (str): workspace id.
            dataset_id (str): dataset id.
            query (str): DAX query (must start with EVALUATE).
            impersonated_username (str, optional): effective username for RLS.

        Returns:
            Dict: status message, content (parsed rows), and truncation metadata.
        """

        if (query == '') or (workspace_id == '') or (dataset_id == ''):
            return {'message': 'Missing parameters, please check.'}

        # --- Step 1: COUNTROWS pre-check ---
        table_expression = self._extract_table_expression(query)
        total_rows = None

        if table_expression:
            count_query = f'EVALUATE ROW("_count", COUNTROWS({table_expression}))'
            count_response = self._post_query(workspace_id, dataset_id, count_query)

            if count_response.status_code == 200:
                count_result = json.loads(count_response.content)
                try:
                    total_rows = count_result['results'][0]['tables'][0]['rows'][0]['[_count]']
                except (KeyError, IndexError):
                    total_rows = None

        # --- Step 2: Execute the actual query ---
        r = self._post_query(workspace_id, dataset_id, query)
        status = r.status_code

        if status == 200:
            result = json.loads(r.content)

            try:
                rows = result['results'][0]['tables'][0]['rows']
            except (KeyError, IndexError):
                rows = []

            # Determine column count from the first row
            num_columns = len(rows[0]) if rows else 0
            rows_returned = len(rows)

            # Calculate the effective row limit based on the 1M values cap
            if num_columns > 0:
                max_rows = min(100_000, 1_000_000 // num_columns)
            else:
                max_rows = 100_000

            # Determine if data was truncated
            truncated = False
            if total_rows is not None and total_rows > max_rows:
                truncated = True
            elif rows_returned >= max_rows:
                truncated = True

            return {
                'message': 'Success',
                'content': rows,
                'total_rows': total_rows,
                'rows_returned': rows_returned,
                'num_columns': num_columns,
                'max_rows_allowed': max_rows,
                'truncated': truncated
            }

        else:
            return {'message': 'Error', 'content': r.content}

    
    def list_users(
                self, 
                workspace_id: str = '',
                dataset_id: str = '') -> Dict:
        """
        List all datasets on a specific workspace_id that the user has access to.

        Args:
            workspace_id (str, optional): workspace id to search datasets from.

        Returns:
            Dict: status message and content.
        """

        # Main URL
        request_url = f'{self.main_url}/groups/{workspace_id}/datasets/{dataset_id}/users'

        # If workspace ID was not informed, return error message...
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}

        # If workspace ID was informed...
        else: 
            filename = f'datasets_{workspace_id}.xlsx'

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


    def add_user(
                self, 
                user_principal_name: str = '', 
                workspace_id: str = '', 
                dataset_id: str = '',
                access_right: str = 'Read',
                user_type: str = 'User') -> Dict:
        """
        Grants an user access to a specific dataset.

        Args:
            user_principal_name (str): user e-mail or identifier of service principal.
            workspace_id (str): workspace id to add the user to.
            dataset_id (str): dataset id to grant access to.
            access_right (str, optional): access right type. Defaults to 'Member'.
            user_type (str, optional): user type, 'SP' for service accounts. Defaults to 'user'.

        Returns:
            Dict: status message.
        """

        # If both, user, workspace and dataset are provided...
        if (user_principal_name != '') & (workspace_id != '') & (dataset_id != ''):

            request_url = self.main_url + f'/groups/{workspace_id}/datasets/{dataset_id}/users'

            headers = {'Authorization': f'Bearer {self.token}'}

            # Add user to dataset with the specified access right.
            # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/post-dataset-user-in-group
            data = {
                "identifier": user_principal_name,
                "principalType": user_type,
                "datasetUserAccessRight": access_right
            }

            # Make the request
            r = requests.post(url=request_url, headers=headers, json=data)

            # Get HTTP status and content
            status = r.status_code

            # If success...
            if status == 200:
                return {'message': 'Success'}
            
            else:                
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response['error']['details']['message']

                return {'message': {'error': error_message, 'content': response}}

        else:
            return {'message': 'Missing parameters, please check.', 'content': ''}


    def update_user(
                self, 
                user_principal_name: str = '', 
                workspace_id: str = '',
                dataset_id: str = '',
                access_right: str = 'Read',
                user_type: str = 'User') -> Dict:
        """
        Update an user access to a specific dataset.

        Args:
            user_principal_name (str): user e-mail or identifier of service principal.
            workspace_id (str): workspace id to add the user to.
            dataset_id (str): dataset id to grant access to.
            access_right (str, optional): access right type. Defaults to 'Member'.
            user_type (str, optional): user type, 'SP' for service accounts. Defaults to 'user'.

        Returns:
            Dict: status message.
        """

        # If both, user, workspace and dataset are provided...
        if (user_principal_name != '') & (workspace_id != '') & (dataset_id != ''):

            request_url = self.main_url + f'/groups/{workspace_id}/datasets/{dataset_id}/users'

            headers = {'Authorization': f'Bearer {self.token}'}

            # Add user to dataset with the specified access right.
            # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/post-dataset-user-in-group
            data = {
                "identifier": user_principal_name,
                "principalType": user_type,
                "datasetUserAccessRight": access_right
            }

            # Make the request
            r = requests.put(url=request_url, headers=headers, json=data)

            # Get HTTP status and content
            status = r.status_code

            # If success...
            if status == 200:
                return {'message': 'Success'}
            
            else:                
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response['error']['code']

                return {'message': {'error': {'status': status, 'description': error_message}, 'content': response}}

        else:
            return {'message': 'Missing parameters, please check.', 'content': ''}


    def remove_user(
                self, 
                user_principal_name: str = '', 
                workspace_id: str = '',
                dataset_id: str = '',
                user_type: str = 'User') -> Dict:
        """
        Removes an user access to a specific dataset.

        Args:
            user_principal_name (str): user e-mail or identifier of service principal.
            workspace_id (str): workspace id to add the user to.
            dataset_id (str): dataset id to grant access to.
            access_right (str, optional): access right type. Defaults to 'Member'.
            user_type (str, optional): user type, 'SP' for service accounts. Defaults to 'user'.

        Returns:
            Dict: status message.
        """

        # If both, user, workspace and dataset are provided...
        if (user_principal_name != '') & (workspace_id != '') & (dataset_id != ''):

            request_url = self.main_url + f'/groups/{workspace_id}/datasets/{dataset_id}/users'

            headers = {'Authorization': f'Bearer {self.token}'}

            # Add user to dataset with the specified access right.
            # https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/post-dataset-user-in-group
            data = {
                "identifier": user_principal_name,
                "principalType": user_type,
                "datasetUserAccessRight": "None"
            }

            # Make the request
            r = requests.put(url=request_url, headers=headers, json=data)

            # Get HTTP status and content
            status = r.status_code

            # If success...
            if status == 200:
                return {'message': 'Success'}
            
            # Too many requests
            elif status == 429:
                return {'message': {'error': {'status': 429, 'description': 'too many requests'}, 'content': ''}}
            
            # Cannot change admin access
            elif status == 401:
                return {'message': {'error': {'status': 401, 'description': 'not authorized'}, 'content': ''}}
            else:                
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response['error']['code']
                return {'message': {'error': {'status': status, 'description': error_message}, 'content': ''}}                    

        else:
            return {'message': 'Missing parameters, please check.', 'content': ''}


    def list_dataset_related_reports(
                self, 
                workspace_id: str = '',
                dataset_id: str = '',
                workspace: workspace.Workspace = None) -> Dict:
        """
        List all reports related to a specific dataset.

        Args:
            workspace_id (str): workspace id where dataset is published.
            dataset_id (str): dataset id to search reports from.
            workspace (Workspace): workspace object to list reports.

        Returns:
            Dict: status message and content.
        """

        dataset_reports = []
        filename = f'dataset_reports_{dataset_id}.xlsx'
        file_path = f'{self.data_dir}/reports'

        os.makedirs(file_path, exist_ok=True)
        
        try:

            workspace_reports = workspace.list_reports(workspace_id=workspace_id)

            for report in workspace_reports['content']:
                if report['datasetId'] == dataset_id:
                    dataset_reports.append(report)

            # Save to Excel file
            df = pd.DataFrame(dataset_reports)
            df.to_excel(f'{self.data_dir}/reports/{filename}', index=False)

            return {'message': 'Success', 'content': dataset_reports}

        except Exception as error_message:
            return {'message': {'error': error_message}, 'content': dataset_reports}


    def export_dataset_related_reports(
                self, 
                workspace_id: str = '',
                dataset_id: str = '',
                replace_existing: bool = False,
                workspace: workspace.Workspace = None,
                report: report.Report = None) -> Dict:
        """
        Export all reports related to a specific dataset.

        Args:
            workspace_id (str): workspace id where dataset is published.
            dataset_id (str): dataset id to search reports from.
            replace_existing (bool, optional): replace existing files. Defaults to False.
            workspace (Workspace): workspace object to list reports.

        Returns:
            Dict: status message and content.
        """

        print('Getting workspace name...')
        workspace_details = workspace.get_workspace_details(workspace_id=workspace_id)
        workspace_name = workspace_details.get('content', []).get('name', 'unknown workspace')

        print('Getting dataset name...')
        dataset_details = self.get_dataset_details(workspace_id=workspace_id, dataset_id=dataset_id)
        dataset_name = dataset_details.get('content', []).get('name', 'unknown dataset')

        print(f'Getting {dataset_name} reports list on {workspace_name}...')
        dataset_reports_list = self.list_dataset_related_reports(workspace_id=workspace_id, dataset_id=dataset_id, workspace=workspace)

        if 'error' in dataset_reports_list['message']:
            return {'message': {'error': dataset_reports_list}}

        reports_to_export = []
        for report_data in dataset_reports_list['content']:
            if report_data['name'] != dataset_name:
                reports_to_export.append(report_data)

        print(f'Downloading reports connected to {dataset_name}.\n\nWorkspace: {workspace_name}\nTotal reports: {len(reports_to_export)}\n')

        def _export(report_data):
            return report.export_report(
                workspace_id=workspace_id,
                workspace_name=workspace_name,
                dataset_name=dataset_name,
                report_id=report_data['id'],
                report_name=report_data['name'],
                replace_existing=replace_existing)

        with ThreadPoolExecutor() as executor:
            list(executor.map(_export, reports_to_export))

        return {'message': 'Success', 'content': dataset_reports_list['content']}