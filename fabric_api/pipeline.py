import json
import base64
import requests
from typing import Dict, List


class Pipeline:

    def __init__(self, token: str):
        """
        Initialize variables.
        """
        self.fabric_api_base_url = 'https://api.fabric.microsoft.com'
        self.token = token
        self.headers = {'Authorization': f'Bearer {self.token}'}


    def get_pipeline_definition(self, workspace_id: str, pipeline_id: str) -> Dict:
        """
        Gets the definition of a Fabric Data Pipeline from a specified workspace.

        Args:
            workspace_id (str): The ID of the workspace where the pipeline resides.
            pipeline_id (str): The ID of the pipeline to retrieve the definition for.

        Returns:
            Dict: A dictionary containing the status ('Success' or error) and the pipeline definition content.
        """
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}
        if pipeline_id == '':
            return {'message': 'Missing pipeline id, please check.', 'content': ''}

        api_url = f'{self.fabric_api_base_url}/v1/workspaces/{workspace_id}/dataPipelines/{pipeline_id}/getDefinition'

        print(f"Extracting definition for pipeline {pipeline_id} from workspace {workspace_id}...")
        response = requests.post(api_url, headers=self.headers)

        if response.status_code == 200:
            definition = response.json()
            print("Successfully extracted pipeline definition.")
            return {'message': 'Success', 'content': definition}
        else:
            error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else {}
            error_message = error_data.get('message', error_data.get('error', {}).get('message', response.text))
            print(f"Error getting pipeline definition: {response.status_code} - {error_message}")
            return {'message': {'error': error_message, 'status_code': response.status_code}}


    def get_pipeline_activities(self, workspace_id: str, pipeline_id: str) -> Dict:
        """
        Gets the list of activities from a Fabric Data Pipeline definition,
        extracting name, type, and typeProperties for each activity.

        Supported activity types and their key typeProperties:
        - RefreshDataflow: dataflowId, workspaceId, dataflowType
        - TridentNotebook: notebookId, workspaceId, sessionTag
        - InvokePipeline: pipelineId, workspaceId, waitOnCompletion, operationType, parameters

        Args:
            workspace_id (str): The ID of the workspace where the pipeline resides.
            pipeline_id (str): The ID of the pipeline.

        Returns:
            Dict: A dictionary with 'message' and 'content' (list of activity dicts with name, type, typeProperties).
        """
        result = self.get_pipeline_definition(workspace_id, pipeline_id)

        if result.get('message') != 'Success':
            return result

        definition = result['content']

        # Extract the pipeline-content.json part from the definition
        pipeline_content = None
        parts = definition.get('definition', {}).get('parts', [])
        for part in parts:
            if part.get('path') == 'pipeline-content.json':
                payload = base64.b64decode(part['payload']).decode('utf-8')
                pipeline_content = json.loads(payload)
                break

        if not pipeline_content:
            return {'message': 'No pipeline-content.json found in definition.', 'content': ''}

        # Extract activities
        raw_activities = pipeline_content.get('properties', {}).get('activities', [])
        activities = []

        for activity in raw_activities:
            activities.append({
                'name': activity.get('name', ''),
                'type': activity.get('type', ''),
                'typeProperties': activity.get('typeProperties', {})
            })

        return {'message': 'Success', 'content': activities}
