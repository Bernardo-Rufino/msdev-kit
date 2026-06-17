import os
import json
import requests
import pandas as pd
from typing import Dict
from .utilities import create_directory
from .workspace import Workspace


class Capacity:

    def __init__(self, pbi_token: str, fabric_token: str = None, azure_token: str = None):
        """
        Initialize variables.
        """
        # Power BI Capacity parameters
        self.main_url = 'https://api.powerbi.com/v1.0/myorg'

        # Fabric Capacity parameters
        self.fabric_api_base_url = 'https://management.azure.com'
        self.azure_subscription_id = None
        self.azure_resource_group = None

        # General parameters
        self.token = pbi_token
        self.headers = {'Authorization': f'Bearer {self.token}'}
        self.workspace = Workspace(self.token)

        # Directories
        self.capacities_dir = './data/capacities'
        self.directories = [self.capacities_dir]

        for dir in self.directories:
            create_directory(dir)


    def list_powerbi_capacities(self) -> Dict:
        """
        List all Power BI capacities that the user has access to.

        Args:
            None.

        Returns:
            Dict: status message and content.
        """
        
        # Main URL
        request_url = f'{self.main_url}/capacities'

        filename = f'capacities_powerbi.xlsx'

        # Make the request
        r = requests.get(url=request_url, headers=self.headers)

        # Get HTTP status and content
        status = r.status_code
        response = json.loads(r.content).get('value', '')

        # If success...
        if status == 200:
            if type == 'fabric':
                df = pd.json_normalize(response)
                df['name'] = df['displayName']
                df.drop(columns=['displayName'], inplace=True)
            else:
                df = pd.DataFrame(response)
            df.to_excel(f'{self.capacities_dir}/{filename}', index=False)
            result = json.loads(df.to_json(orient='records'))

            return {'message': 'Success', 'content': result}

        else:                
            # If any error happens, return message.
            response = json.loads(r.content)
            error_message = response['error']['message']

            return {'message': {'error': error_message, 'content': response}}


    def list_fabric_capacities(self, azure_subscription_id: str, azure_resource_group: str = None) -> Dict:
        """
        List all Fabric capacities that the user has access to, to a given subscription.
        
        If a resource group is provided, only capacities within that resource group will be listed.

        Args:
            azure_subscription_id (str): Azure subscription ID.
            azure_resource_group (str, optional): Azure resource group. Defaults to None.

        Returns:
            Dict: status message and content.
        """
        
        # Main URL
        if azure_resource_group:
            request_url = f'{self.fabric_api_base_url}/subscriptions/{azure_subscription_id}/resourceGroups/{azure_resource_group}/providers/Microsoft.Fabric/capacities?api-version=2023-11-01'
        else:
            request_url = f'{self.fabric_api_base_url}/subscriptions/{azure_subscription_id}/providers/Microsoft.Fabric/capacities?api-version=2023-11-01'

        filename = f'capacities_powerbi.xlsx'

        # Make the request
        r = requests.get(url=request_url, headers=self.headers)

        # Get HTTP status and content
        status = r.status_code
        response = json.loads(r.content).get('value', '')

        # If success...
        if status == 200:
            if type == 'fabric':
                df = pd.json_normalize(response)
                df['name'] = df['displayName']
                df.drop(columns=['displayName'], inplace=True)
            else:
                df = pd.DataFrame(response)
            df.to_excel(f'{self.capacities_dir}/{filename}', index=False)
            result = json.loads(df.to_json(orient='records'))

            return {'message': 'Success', 'content': result}

        else:                
            # If any error happens, return message.
            response = json.loads(r.content)
            error_message = response['error']['message']

            return {'message': {'error': error_message, 'content': response}}


    def assign_workspace_to_capacity(
                self, 
                workspace_id: str = '',
                capacity_id: str = '') -> Dict:
        """
        Assign a workspace to a specific capacity.

        Args:
            workspace_id (str): workspace id to add the user to.
            capacity_id (str): capacity id to assign the workspace to.

        Returns:
            Dict: status message.
        """

        # If both workspace and capacity were provided...
        if (workspace_id != '') & (capacity_id != ''):

            ws = self.workspace.get_worspace_details(workspace_id)
            workspace_name = ws.get('content', {}).get('name', None)
            current_capacity_id = ws.get('content', {}).get('capacityId', None)

            if current_capacity_id.lower() == capacity_id.lower():
                return {'message': 'Workspace is already assigned to the specified capacity.'}

            request_url = self.main_url + f'/groups/{workspace_id}/AssignToCapacity'

            headers = {'Authorization': f'Bearer {self.token}'}

            # https://learn.microsoft.com/en-us/rest/api/power-bi/capacities/groups-assign-to-capacity
            data = {
                "capacityId": capacity_id
            }

            # Make the request
            r = requests.post(url=request_url, headers=headers, json=data)

            # Get HTTP status and content
            status = r.status_code

            # If success...
            if status == 200:
                return {'message': 'Success'}
            
            elif status == 401:
                return {'message': 'Unauthorized. Please check your access level. Workspace administration rights are required to perform this action.'}
            
            else:                
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response['error']['code']

                return {'message': {'error': {'status': status, 'description': ''}, 'content': response.content}}

        else:
            return {'message': 'Missing parameters, please check.', 'content': ''}
