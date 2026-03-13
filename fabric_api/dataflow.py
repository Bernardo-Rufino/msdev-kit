import os
import json
import requests
import pandas as pd
from typing import Dict
from .utilities import create_directory
from .workspace import Workspace


class Dataflow:

    def __init__(self, token: str):
        """
        Initialize variables.
        """
        self.main_url = 'https://api.powerbi.com/v1.0/myorg'
        self.fabric_api_base_url = 'https://api.fabric.microsoft.com'
        self.token = token
        self.headers = {'Authorization': f'Bearer {self.token}'}
        self.workspace = Workspace(self.token)

        # Directories
        self.dataflows_dir = './data/dataflows'
        self.directories = [self.dataflows_dir]

        for dir in self.directories:
            create_directory(dir)


    def list_dataflows(self, workspace_id: str = '', type: str = 'pbi') -> Dict:
        """
        List all dataflows in a workspace_id that the user has access to.

        Args:
            workspace_id (str, optional): workspace id to search for.

        Returns:
            Dict: status message and content.
        """
        # If workspace ID was not informed, return error message...
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}
        
        if type not in ('pbi', 'fabric'):
            return {'message': 'Type must be "pbi" or "fabric".', 'content': ''}
        
        # Main URL
        elif type == 'pbi':
            request_url = f'{self.main_url}/groups/{workspace_id}/dataflows'
        elif type == 'fabric':
            request_url = f'{self.fabric_api_base_url}/v1/workspaces/{workspace_id}/dataflows'
        else:
            return {'message': 'Type must be "pbi" or "fabric".', 'content': ''} # Just as fallback, it won't reach here.

        workspace_name = self.workspace.get_workspace_details(workspace_id).get('content', {}).get('name', 'notFound')
        filename = f'dataflows_{workspace_name}.xlsx'

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
            df.to_excel(f'{self.dataflows_dir}/{filename}', index=False)
            result = json.loads(df.to_json(orient='records'))

            return {'message': 'Success', 'content': result}

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
                response = json.loads(r.content)
                error_message = response['error']['message']

                return {'message': {'error': error_message, 'content': response}}


    def create_dataflow(
                self, 
                workspace_id: str = '', 
                dataflow_content: Dict = '') -> Dict:
        """
        Creates a new Power BI dataflow in a specified workspace.

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
                    dataflow_id: str = '',
                    type: str = 'pbi') -> Dict:
            """
            Deletes a Power BI dataflow from a specified workspace.

            Args:
                workspace_id (str): workspace id where dataflow will be deleted.
                dataflow_id (str): dataflow id to be deleted.

            Returns:
                Dict: status message.
            """
            # If workspace ID was not informed, return error message...
            if workspace_id == '':
                return {'message': 'Missing workspace id, please check.', 'content': ''}
            
            # If dataflow ID was not informed, return error message...
            if dataflow_id == '':
                return {'message': 'Missing dataflow id, please check.', 'content': ''}
            

            if type not in ('pbi', 'fabric'):
                return {'message': 'Type must be "pbi" or "fabric".', 'content': ''}
            
            # Main URL
            elif type == 'pbi':
                request_url = f'{self.main_url}/groups/{workspace_id}/dataflows/{dataflow_id}'
            elif type == 'fabric':
                request_url = f'{self.fabric_api_base_url}/v1/workspaces/{workspace_id}/dataflows/{dataflow_id}'
            else:
                return {'message': 'Type must be "pbi" or "fabric".', 'content': ''} # Just as fallback, it won't reach here.

            
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
                    print(r.text)
                    response = json.loads(r.content)
                    error_message = response['error']

                except:
                    return {'message': 'Error reading JSON response'}
                
                return {'message': {'error': error_message, 'content': response}}
            

    def export_dataflow_json(self, workspace_id: str = '', dataflow_id: str = '', dataflow_name: str = '') -> Dict:
        """
        Exports the JSON definition of a Power BI dataflow.

        Args:
            workspace_id (str, optional): workspace id where the dataflow resides.
            dataflow_id (str, optional): dataflow id to get the details.
            dataflow_name (str, optional): name to save the exported JSON file.

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

    def get_dataflow_gen2_definition(self, workspace_id: str, dataflow_id: str) -> Dict:
        """
        Gets the definition of a Dataflow Gen2 (CI/CD) from a specified workspace.
        Only Dataflow Gen2 (CI/CD / native Fabric) items support definition export.
        Standard Dataflow Gen2 items are not supported.

        Args:
            workspace_id (str): The ID of the workspace where the Dataflow Gen2 resides.
            dataflow_id (str): The ID of the Dataflow Gen2 to retrieve the definition for.

        Returns:
            Dict: A dictionary containing the status ('Success' or error) and the Dataflow Gen2 definition content.
        """
        api_url = f'{self.fabric_api_base_url}/v1/workspaces/{workspace_id}/dataflows/{dataflow_id}/getDefinition'

        print(f"Extracting definition for dataflow {dataflow_id} from workspace {workspace_id}...")
        response = requests.post(api_url, headers=self.headers)

        if response.status_code == 200:
            definition = response.json()
            print("Successfully extracted Dataflow Gen2 definition.")
            return {'message': 'Success', 'content': definition}
        else:
            # getDefinition only works for Dataflow Gen2 (CI/CD / native Fabric).
            # Standard Dataflow Gen2 items return an error here.
            error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else {}
            error_code = error_data.get('errorCode', '')

            if response.status_code == 400 or error_code == 'UnknownError':
                return {
                    'message': {
                        'error': 'This dataflow does not support definition export. '
                                 'Only Dataflow Gen2 (CI/CD) items created via the native Fabric experience support this operation. '
                                 'Standard Dataflow Gen2 items are not supported.',
                        'status_code': response.status_code
                    }
                }

            error_message = response.text
            print(f"Error getting Dataflow Gen2 definition: {response.status_code} - {error_message}")
            return {'message': {'error': error_message, 'status_code': response.status_code}}
        

    def create_dataflow_gen2_from_definition(self, workspace_id: str, display_name: str, definition: Dict) -> Dict:
        """
        Creates a new Dataflow Gen2 in a specified workspace from a given definition.

        Args:
            workspace_id (str): The ID of the target workspace where the Dataflow Gen2 will be created.
            display_name (str): The display name for the new Dataflow Gen2.
            definition (Dict): The complete JSON definition of the Dataflow Gen2 (obtained from get_dataflow_gen2_definition).

        Returns:
            Dict: A dictionary containing the status ('Success' or error) and the details of the newly created Dataflow Gen2.
        """
        api_url = f'{self.fabric_api_base_url}/v1/workspaces/{workspace_id}/dataflows'
        
        payload = {
            "displayName": display_name,
            "description": "",
            "definition": definition['definition']
        }
        
        print(f"Creating Dataflow Gen2 '{display_name}' in workspace {workspace_id}...")
        response = requests.post(api_url, headers=self.headers, json=payload)
        
        if response.status_code == 201:
            new_item = response.json()
            print(f"Successfully created Dataflow Gen2. New Item ID: {new_item['id']}")
            return {'message': 'Success', 'content': new_item}
        elif response.status_code == 400 and 'ItemDisplayNameAlreadyInUse' in response.text:
            error_message = response.text
            print(f"Error creating Dataflow Gen2: {response.status_code} - {error_message}")
            print('Use update method instead.')
            return {'message': {'error': error_message, 'status_code': response.status_code}}
        else:
            error_message = response.text
            print(f"Error creating Dataflow Gen2: {response.status_code} - {error_message}")
            return {'message': {'error': error_message, 'status_code': response.status_code}}
        

    def update_dataflow_gen2_from_definition(self, workspace_id: str, dataflow_id: str, display_name: str, definition: Dict) -> Dict:
        """
        Updates an existing Dataflow Gen2 in a specified workspace with a new definition.

        Args:
            workspace_id (str): The ID of the workspace where the Dataflow Gen2 resides.
            dataflow_id (str): The ID of the Dataflow Gen2 to update.
            display_name (str): The new display name for the Dataflow Gen2 (can be the same as current).
            definition (Dict): The complete JSON definition of the Dataflow Gen2 (obtained from get_dataflow_gen2_definition).

        Returns:
            Dict: A dictionary containing the status ('Success' or error) and the details of the updated Dataflow Gen2.
        """
        api_url = f'{self.fabric_api_base_url}/v1/workspaces/{workspace_id}/dataflows/{dataflow_id}/updateDefinition?updateMetadata=true'
        
        payload = {
            "definition": definition['definition']
        }
        
        print(f"Updating Dataflow Gen2 '{display_name}' (ID: {dataflow_id}) in workspace {workspace_id}...")
        response = requests.patch(api_url, headers=self.headers, json=payload)
        
        if response.status_code == 200:
            updated_item = response.json()
            print(f"Successfully updated Dataflow Gen2. Item ID: {updated_item['id']}")
            return {'message': 'Success', 'content': updated_item}
        else:
            error_message = response.text
            print(f"Error updating Dataflow Gen2: {response.status_code} - {error_message}")
            return {'message': {'error': error_message, 'status_code': response.status_code}}

