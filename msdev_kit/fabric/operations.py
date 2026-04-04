import json
import requests
from typing import Dict
from .utilities import create_directory


class Operations:

    def __init__(self, token: str):
        """
        Initialize variables.
        """
        self.main_url = 'https://api.fabric.microsoft.com/v1'
        self.token = token
        self.headers = {'Authorization': f'Bearer {self.token}'}
        self.data_dir = './data/operations'

        create_directory(self.data_dir)


    def get_operation_state(
                self, 
                operation_id: str = '') -> str:
        """
        Get the operation state for a long running operation.

        Args:
            operation_id (str, optional): operation id to check state.

        Returns:
            Dict: message and operation state.
        """

        DEFAULT_STATE = 'unknown'

        # Main URL
        request_url = f'{self.main_url}/operations/{operation_id}'

        # If operation ID was not informed, return error message...
        if operation_id == '':
            return {'message': 'Missing operation id, please check.', 'operation_state': DEFAULT_STATE}

        # If workspace ID was informed...
        else:

            # Make the request
            r = requests.get(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content)

            # If success...
            if status == 200:
                # Get the state
                operation_state = response.get('status', DEFAULT_STATE)
                
                return {'message': 'Success', 'operation_state': operation_state}

            else:                
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response['error']['message']

                return {'message': {'error': error_message}, 'operation_state': DEFAULT_STATE}


    def get_operation_result(
                self, 
                operation_id: str = '') -> Dict:
        """
        Get the operation result for a long running operation.

        Args:
            operation_id (str, optional): operation id to get result from.

        Returns:
            Dict: json with it's contents.
        """

        # Main URL
        request_url = f'{self.main_url}/operations/{operation_id}/result'

        # If operation ID was not informed, return error message...
        if operation_id == '':
            return {'message': 'Missing operation id, please check.', 'content': ''}

        # If workspace ID was informed...
        else:

            # Make the request
            r = requests.get(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content)

            # If success...
            if status == 200:
                
                return {'message': 'Success', 'content': response}

            else:                
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response['error']['message']

                return {'message': {'error': error_message}, 'content': ''}