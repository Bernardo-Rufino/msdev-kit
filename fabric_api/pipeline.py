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


    def list_pipelines(self, workspace_id: str) -> Dict:
        """
        Lists all Fabric Data Pipelines in a workspace.

        Args:
            workspace_id (str): The ID of the workspace.

        Returns:
            Dict: 'message' and 'content' (list of pipeline dicts with id, displayName, description).
        """
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}

        api_url = f'{self.fabric_api_base_url}/v1/workspaces/{workspace_id}/dataPipelines'
        pipelines = []

        while api_url:
            response = requests.get(api_url, headers=self.headers)
            if response.status_code != 200:
                error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else {}
                error_message = error_data.get('message', error_data.get('error', {}).get('message', response.text))
                return {'message': {'error': error_message, 'status_code': response.status_code}}

            data = response.json()
            pipelines.extend(data.get('value', []))
            api_url = data.get('continuationUri', None)

        return {'message': 'Success', 'content': pipelines}


    def find_pipelines_by_dataflow(self, workspace_id: str, dataflow_id: str) -> Dict:
        """
        Finds all pipelines in a workspace that reference a specific dataflow.

        Lists all pipelines, fetches their activities, and checks which ones contain
        a RefreshDataflow activity targeting the given dataflow ID.

        Args:
            workspace_id (str): The workspace ID to search pipelines in.
            dataflow_id (str): The dataflow ID to search for.

        Returns:
            Dict: 'message' and 'content' (list of dicts with pipeline_id, pipeline_name,
                and activities — the matching activity names within that pipeline).
        """
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}
        if dataflow_id == '':
            return {'message': 'Missing dataflow id, please check.', 'content': ''}

        # List all pipelines
        pipelines_result = self.list_pipelines(workspace_id)
        if pipelines_result.get('message') != 'Success':
            return pipelines_result

        pipelines = pipelines_result['content']
        print(f"Found {len(pipelines)} pipelines. Scanning for dataflow {dataflow_id}...")

        matches = []
        for p in pipelines:
            pipeline_id = p.get('id', '')
            pipeline_name = p.get('displayName', '')

            activities_result = self.get_pipeline_activities(workspace_id, pipeline_id)
            if activities_result.get('message') != 'Success':
                print(f"  Skipping '{pipeline_name}' — could not fetch activities.")
                continue

            matching_activities = []
            for activity in activities_result['content']:
                if activity['type'] != 'RefreshDataflow':
                    continue
                props = activity.get('typeProperties', {})
                activity_dataflow_id = props.get('dataflowId', '')
                if activity_dataflow_id == dataflow_id:
                    matching_activities.append(activity['name'])

            if matching_activities:
                matches.append({
                    'pipeline_id': pipeline_id,
                    'pipeline_name': pipeline_name,
                    'activities': matching_activities
                })

        print(f"Found {len(matches)} pipeline(s) referencing dataflow {dataflow_id}.")
        return {'message': 'Success', 'content': matches}


    def update_pipeline_definition(self, workspace_id: str, pipeline_id: str, definition: Dict) -> Dict:
        """
        Updates an existing Fabric Data Pipeline definition.

        Args:
            workspace_id (str): The ID of the workspace where the pipeline resides.
            pipeline_id (str): The ID of the pipeline to update.
            definition (Dict): The full pipeline definition (as returned by get_pipeline_definition).

        Returns:
            Dict: 'message' and 'content' with the update result.
        """
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}
        if pipeline_id == '':
            return {'message': 'Missing pipeline id, please check.', 'content': ''}

        api_url = f'{self.fabric_api_base_url}/v1/workspaces/{workspace_id}/dataPipelines/{pipeline_id}/updateDefinition'

        payload = {"definition": definition['definition']}

        print(f"Updating pipeline {pipeline_id} in workspace {workspace_id}...")
        response = requests.post(api_url, headers=self.headers, json=payload)

        if response.status_code in (200, 202):
            content = response.json() if response.content else {'id': pipeline_id}
            print(f"Successfully updated pipeline {pipeline_id}.")
            return {'message': 'Success', 'content': content}
        else:
            error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else {}
            error_message = error_data.get('message', error_data.get('error', {}).get('message', response.text))
            print(f"Error updating pipeline: {response.status_code} - {error_message}")
            return {'message': {'error': error_message, 'status_code': response.status_code}}


    def replace_dataflow_id_in_pipeline(self, workspace_id: str, pipeline_id: str,
                                         old_dataflow_id: str, new_dataflow_id: str) -> Dict:
        """
        Replaces a dataflow ID in all RefreshDataflow activities of a pipeline.

        Fetches the pipeline definition, finds all RefreshDataflow activities that reference
        the old dataflow ID, updates them to point to the new dataflow ID, and saves the
        modified definition back to Fabric.

        Args:
            workspace_id (str): The workspace ID where the pipeline resides.
            pipeline_id (str): The pipeline ID to update.
            old_dataflow_id (str): The current dataflow ID to replace.
            new_dataflow_id (str): The new dataflow ID to set.

        Returns:
            Dict: 'message' and 'content' with the update result, including how many activities were updated.
        """
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}
        if pipeline_id == '':
            return {'message': 'Missing pipeline id, please check.', 'content': ''}

        # Fetch definition
        result = self.get_pipeline_definition(workspace_id, pipeline_id)
        if result.get('message') != 'Success':
            return result

        definition = result['content']

        # Find and decode pipeline-content.json
        parts = definition.get('definition', {}).get('parts', [])
        content_part = None
        pipeline_content = None
        for part in parts:
            if part.get('path') == 'pipeline-content.json':
                content_part = part
                pipeline_content = json.loads(base64.b64decode(part['payload']).decode('utf-8'))
                break

        if not pipeline_content:
            return {'message': 'No pipeline-content.json found in definition.', 'content': ''}

        # Replace dataflow ID in matching activities
        activities = pipeline_content.get('properties', {}).get('activities', [])
        updated_count = 0
        updated_names = []

        for activity in activities:
            if activity.get('type') != 'RefreshDataflow':
                continue
            props = activity.get('typeProperties', {})
            if props.get('dataflowId') == old_dataflow_id:
                props['dataflowId'] = new_dataflow_id
                updated_count += 1
                updated_names.append(activity.get('name', ''))

        if updated_count == 0:
            return {
                'message': f'No RefreshDataflow activities found with dataflow ID {old_dataflow_id}.',
                'content': ''
            }

        # Encode back and update
        content_part['payload'] = base64.b64encode(
            json.dumps(pipeline_content).encode('utf-8')
        ).decode('utf-8')

        print(f"Replacing dataflow ID in {updated_count} activity(ies): {', '.join(updated_names)}")
        update_result = self.update_pipeline_definition(workspace_id, pipeline_id, definition)

        if update_result.get('message') == 'Success':
            update_result['content'] = {
                'pipeline_id': pipeline_id,
                'activities_updated': updated_count,
                'activity_names': updated_names
            }

        return update_result


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
