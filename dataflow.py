import os
import uuid
import json
import requests
import pandas as pd
from pandas.core.frame import DataFrame
from typing import Dict, List
from utilities import create_directory
from workspace import Workspace


class Dataflow:

    def __init__(self, token: str):
        """
        Initialize variables.
        """
        self.main_url = 'https://api.powerbi.com/v1.0/myorg'
        self.token = token
        self.headers = {'Authorization': f'Bearer {self.token}'}

        # Directories
        self.dataflows_dir = './data/dataflows'
        self.directories = [self.dataflows_dir]

        for dir in self.directories:
            create_directory(dir)


    def list_dataflows(self, workspace_id: str = '') -> Dict:
        """
        List all dataflows in a workspace_id that the user has access to.

        Args:
            workspace_id (str, optional): workspace id to search for.

        Returns:
            Dict: status message and content.
        """
        # Main URL
        request_url = self.main_url + '/groups'

        # If workspace ID was not informed, return error message...
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}

        # If workspace ID was informed...
        else:
            request_url = f'{request_url}/{workspace_id}/dataflows'
            filename = f'dataflows_{workspace_id}.xlsx'

            # Make the request
            r = requests.get(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content).get('value', '')

            # If success...
            if status == 200:
                # Save to Excel file
                df = pd.DataFrame(response)
                # df.to_excel(f'{self.dataflows_dir}/{filename}', index=False)
                
                return {'message': 'Success', 'content': response}

            else:                
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response['error']['message']

                return {'message': {'error': error_message, 'content': response}}
            

    def get_dataflow_details(self, workspace_id: str = '', dataflow_id: str = '', folder_name: str = '') -> Dict:
        """
        Get all details from a specific dataflows in a workspace_id that the user has access to.

        Args:
            workspace_id (str, optional): workspace id to search for.
            dataflow_id (str, optional): dataflow id to get the details.

        Returns:
            Dict: status message and content.
        """
        # Main URL
        request_url = self.main_url + '/groups'

        # If workspace ID was not informed, return error message...
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}

        # If workspace ID was informed...
        else:
            request_url = f'{request_url}/{workspace_id}/dataflows/{dataflow_id}'

            # Make the request
            r = requests.get(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content)

            # If success...
            if status == 200:
                
                workspace = Workspace(self.token)
                if folder_name == '':
                    folder_name = workspace.get_workspace_details(workspace_id).get('content', {}).get('name', 'notFound')
                
                # Save to json file
                filepath = f'{self.dataflows_dir}/{folder_name}/dataflows'
                filename = f'{filepath}/{response.get("name", "")}.json'
                os.makedirs(filepath, exist_ok=True)

                with open(filename, mode='w', encoding='utf-8-sig') as f:
                    json.dump(response, f, ensure_ascii=True, indent=4)
                
                return {'message': 'Success', 'content': response}

            else:                
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response['error']['message']

                return {'message': {'error': error_message, 'content': response}}


    def create_dataflow(
                self, 
                workspace_id: str = '', 
                dataflow_content: Dict = '') -> Dict:
        """
        Add an user to a workspace.

        Args:
            workspace_id (str): workspace id where dataflow will be created.
            dataflow_content (Dict): dataflow json with all details from it.

        Returns:
            Dict: status message.
        """

        # If both, user and workspace if are provided...
        if (dataflow_content != '') & (workspace_id != ''):

            request_url = self.main_url + f'/groups/{workspace_id}/imports?datasetDisplayName=model.json&nameConflit=Ignore'

            body = {
                'value': json.dumps(dataflow_content, ensure_ascii=True)
            }
            
            # Make the request
            r = requests.post(url=request_url, headers=self.headers, files=body)

            # Get HTTP status and content
            status = r.status_code

            # If success...
            if status in (200, 202):
                return {'message': 'Success'}
            
            else:                
                
                try:
                    # If any error happens, return message.
                    response = json.loads(r.content)
                    error_message = response['error']

                except:
                    return {'message': 'Error reading JSON response'}
                
                return {'message': {'error': error_message, 'content': response}}

        else:
            return {'message': 'Missing parameters, please check.'}


    def delete_dataflow(
                    self, 
                    workspace_id: str = '', 
                    dataflow_id: str = '') -> Dict:
            """
            Add an user to a workspace.

            Args:
                workspace_id (str): workspace id where dataflow will be created.
                dataflow_content (Dict): dataflow json with all details from it.

            Returns:
                Dict: status message.
            """

            # If both, user and workspace if are provided...
            if (dataflow_id != '') & (workspace_id != ''):

                request_url = self.main_url + f'/groups/{workspace_id}/dataflows/{dataflow_id}'
               
                # Make the request
                r = requests.delete(url=request_url, headers=self.headers)

                # Get HTTP status and content
                status = r.status_code

                # If success...
                if status in (200, 202):
                    return {'message': 'Success'}
                
                else:                
                    
                    try:
                        # If any error happens, return message.
                        response = json.loads(r.content)
                        error_message = response['error']

                    except:
                        return {'message': 'Error reading JSON response'}
                    
                    return {'message': {'error': error_message, 'content': response}}

            else:
                return {'message': 'Missing parameters, please check.'}
            

    def export_dataflow_json(self, workspace_id: str = '', dataflow_id: str = '', dataflow_name: str = '') -> Dict:
        """
        Get all details from a specific dataflows in a workspace_id that the user has access to.

        Args:
            workspace_id (str, optional): workspace id to search for.
            dataflow_id (str, optional): dataflow id to get the details.

        Returns:
            Dict: status message and content.
        """
        # Main URL
        request_url = self.main_url + '/groups'

        # If workspace ID was not informed, return error message...
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}

        # If workspace ID was informed...
        else:
            request_url = f'{request_url}/{workspace_id}/dataflows/{dataflow_id}'

            # Make the request
            r = requests.get(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content)
            response['pbi:mashup']['allowNativeQueries'] = False

            # If success...
            if status == 200:
                # Save to json file
                filename = f'{self.dataflows_dir}/prod backup/{dataflow_name}.json'

                with open(filename, mode='w', encoding='utf-8-sig') as f:
                    json.dump(response, f, ensure_ascii=True, indent=4)
                
                return {'message': 'Success', 'content': response}

            else:                
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response['error']['message']

                return {'message': {'error': error_message, 'content': response}}