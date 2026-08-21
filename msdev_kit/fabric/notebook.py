import time
import requests
from typing import Dict
from msdev_kit.http import request_with_retry


class Notebook:

    def __init__(self, token: str):
        """
        Initialize variables.
        """
        self.fabric_api_base_url = 'https://api.fabric.microsoft.com'
        self.token = token
        self.headers = {'Authorization': f'Bearer {self.token}'}


    def _request_with_retry(self, method: str, url: str, max_retries: int = 3, **kwargs) -> requests.Response:
        """
        Compatibility wrapper around the shared HTTP retry helper.
        """
        return request_with_retry(
            method,
            url,
            max_retries=max_retries,
            request_func=requests.request,
            sleep=time.sleep,
            **kwargs,
        )


    def list_notebooks(self, workspace_id: str) -> Dict:
        """
        Lists all notebooks in a workspace.

        Args:
            workspace_id (str): The ID of the workspace.

        Returns:
            Dict: 'message' and 'content' (list of notebook dicts with id, displayName, description).
        """
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}

        api_url = f'{self.fabric_api_base_url}/v1/workspaces/{workspace_id}/notebooks'
        notebooks = []

        while api_url:
            response = self._request_with_retry('GET', api_url, headers=self.headers)
            if response.status_code != 200:
                error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else {}
                error_message = error_data.get('message', error_data.get('error', {}).get('message', response.text))
                return {'message': {'error': error_message, 'status_code': response.status_code}}

            data = response.json()
            notebooks.extend(data.get('value', []))
            api_url = data.get('continuationUri', None)

        return {'message': 'Success', 'content': notebooks}


    def get_notebook(self, workspace_id: str, notebook_id: str) -> Dict:
        """
        Gets the metadata of a specific notebook.

        Args:
            workspace_id (str): The ID of the workspace.
            notebook_id (str): The ID of the notebook.

        Returns:
            Dict: 'message' and 'content' (notebook dict with id, displayName, description, etc.).
        """
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}
        if notebook_id == '':
            return {'message': 'Missing notebook id, please check.', 'content': ''}

        api_url = f'{self.fabric_api_base_url}/v1/workspaces/{workspace_id}/notebooks/{notebook_id}'
        response = self._request_with_retry('GET', api_url, headers=self.headers)

        if response.status_code == 200:
            return {'message': 'Success', 'content': response.json()}
        else:
            error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else {}
            error_message = error_data.get('message', error_data.get('error', {}).get('message', response.text))
            return {'message': {'error': error_message, 'status_code': response.status_code}}
