import json
import time
import base64
import requests
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from .dataflow import Dataflow
from .notebook import Notebook
from .dataset import Dataset


class Pipeline:

    def __init__(self, token: str):
        """
        Initialize variables.
        """
        self.fabric_api_base_url = 'https://api.fabric.microsoft.com'
        self.token = token
        self.headers = {'Authorization': f'Bearer {self.token}'}


    def _request_with_retry(self, method: str, url: str, max_retries: int = 3, **kwargs) -> requests.Response:
        """
        Makes an HTTP request with automatic retry on 429 (Too Many Requests).
        Respects the Retry-After header when present.
        """
        for attempt in range(max_retries + 1):
            response = requests.request(method, url, **kwargs)
            if response.status_code != 429:
                return response

            retry_after = int(response.headers.get('Retry-After', 5))
            print(f"  Rate limited (429). Retrying in {retry_after}s... (attempt {attempt + 1}/{max_retries})")
            time.sleep(retry_after)

        return response


    def _resolve_pipeline(self, workspace_id: str, pipeline_id_or_name: str) -> tuple:
        """
        Resolves a pipeline identifier that can be either an ID or a display name.
        Tries as ID first via get_pipeline, falls back to listing all pipelines
        and matching by display name (case-insensitive).

        Args:
            workspace_id (str): The workspace ID.
            pipeline_id_or_name (str): The pipeline ID or display name.

        Returns:
            tuple: (pipeline_id, pipeline_name) or (None, None) if not found.
        """
        # Try as ID first
        result = self.get_pipeline(workspace_id, pipeline_id_or_name)
        if result.get('message') == 'Success':
            content = result['content']
            return content.get('id', pipeline_id_or_name), content.get('displayName', '')

        # Fall back to name search
        result = self.list_pipelines(workspace_id)
        if result.get('message') != 'Success':
            return None, None

        for p in result['content']:
            if p.get('displayName', '').lower() == pipeline_id_or_name.lower():
                return p['id'], p['displayName']

        return None, None


    def _resolve_dataflow_id(self, workspace_id: str, dataflow_id_or_name: str) -> tuple:
        """
        Resolves a dataflow identifier that can be either an ID or a display name.
        Tries as ID first via Dataflow.get_dataflow_name, falls back to listing
        all dataflows and matching by name (case-insensitive).

        Args:
            workspace_id (str): The workspace ID.
            dataflow_id_or_name (str): The dataflow ID or display name.

        Returns:
            tuple: (dataflow_id, dataflow_name) or (None, None) if not found.
        """
        dataflow = Dataflow(self.token)

        # Try as ID first
        name = dataflow.get_dataflow_name(workspace_id, dataflow_id_or_name)
        if name:
            return dataflow_id_or_name, name

        # Fall back to name search
        result = dataflow.list_dataflows(workspace_id)
        if result.get('message') != 'Success':
            return None, None

        for df in result['content']:
            if (df.get('name') or '').lower() == dataflow_id_or_name.lower():
                return df.get('id', ''), df.get('name', '')

        return None, None


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
            response = self._request_with_retry('GET', api_url, headers=self.headers)
            if response.status_code != 200:
                error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else {}
                error_message = error_data.get('message', error_data.get('error', {}).get('message', response.text))
                return {'message': {'error': error_message, 'status_code': response.status_code}}

            data = response.json()
            pipelines.extend(data.get('value', []))
            api_url = data.get('continuationUri', None)

        return {'message': 'Success', 'content': pipelines}


    def find_pipelines_by_dataflow(self, workspace_id: str, dataflow_id_or_name: str, max_workers: int = 5) -> Dict:
        """
        Finds all pipelines in a workspace that reference a specific dataflow.

        Accepts either a dataflow ID or display name. Lists all pipelines, fetches
        their activities concurrently, and checks which ones contain a RefreshDataflow
        activity targeting the resolved dataflow ID.

        Args:
            workspace_id (str): The workspace ID to search pipelines in.
            dataflow_id_or_name (str): The dataflow ID or display name to search for.
            max_workers (int): Maximum number of concurrent requests. Defaults to 5.

        Returns:
            Dict: 'message' and 'content' (list of dicts with pipeline_id, pipeline_name,
                and activities — the matching activity names within that pipeline).
        """
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}
        if dataflow_id_or_name == '':
            return {'message': 'Missing dataflow id or name, please check.', 'content': ''}

        # Resolve dataflow ID
        dataflow_id, dataflow_name = self._resolve_dataflow_id(workspace_id, dataflow_id_or_name)
        if not dataflow_id:
            return {'message': f'Dataflow not found: {dataflow_id_or_name}', 'content': ''}

        print(f"Resolved dataflow: {dataflow_name} ({dataflow_id})")

        # List all pipelines
        pipelines_result = self.list_pipelines(workspace_id)
        if pipelines_result.get('message') != 'Success':
            return pipelines_result

        pipelines = pipelines_result['content']
        print(f"Found {len(pipelines)} pipelines. Scanning for dataflow {dataflow_id}...")

        def _check_pipeline(p):
            pipeline_id = p.get('id', '')
            pipeline_name = p.get('displayName', '')

            activities_result = self.get_pipeline_activities(workspace_id, pipeline_id)
            if activities_result.get('message') != 'Success':
                return None

            matching_activities = []
            for activity in activities_result['content']:
                if activity['activity_type'] != 'RefreshDataflow':
                    continue
                props = activity.get('typeProperties', {})
                if props.get('dataflowId') == dataflow_id:
                    matching_activities.append(activity['activity_name'])

            if matching_activities:
                return {
                    'pipeline_id': pipeline_id,
                    'pipeline_name': pipeline_name,
                    'activities': matching_activities
                }
            return None

        matches = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_check_pipeline, p): p for p in pipelines}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    matches.append(result)

        matches.sort(key=lambda m: m['pipeline_name'].lower())
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
        response = self._request_with_retry('POST', api_url, headers=self.headers, json=payload)

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

        # print(f"Extracting definition for pipeline {pipeline_id} from workspace {workspace_id}...")
        response = self._request_with_retry('POST', api_url, headers=self.headers)

        if response.status_code == 200:
            definition = response.json()
            # print("Successfully extracted pipeline definition.")
            return {'message': 'Success', 'content': definition}
        else:
            error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else {}
            error_message = error_data.get('message', error_data.get('error', {}).get('message', response.text))
            print(f"Error getting pipeline definition: {response.status_code} - {error_message}")
            return {'message': {'error': error_message, 'status_code': response.status_code}}


    def get_pipeline(self, workspace_id: str, pipeline_id: str) -> Dict:
        """
        Gets the metadata of a specific pipeline (not its definition).

        Args:
            workspace_id (str): The ID of the workspace.
            pipeline_id (str): The ID of the pipeline.

        Returns:
            Dict: 'message' and 'content' (pipeline dict with id, displayName, description, etc.).
        """
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}
        if pipeline_id == '':
            return {'message': 'Missing pipeline id, please check.', 'content': ''}

        api_url = f'{self.fabric_api_base_url}/v1/workspaces/{workspace_id}/dataPipelines/{pipeline_id}'
        response = self._request_with_retry('GET', api_url, headers=self.headers)

        if response.status_code == 200:
            return {'message': 'Success', 'content': response.json()}
        else:
            error_data = response.json() if response.headers.get('content-type', '').startswith('application/json') else {}
            error_message = error_data.get('message', error_data.get('error', {}).get('message', response.text))
            return {'message': {'error': error_message, 'status_code': response.status_code}}


    def get_pipeline_activities(self, workspace_id: str, pipeline_id_or_name: str, max_workers: int = 5) -> Dict:
        """
        Gets the list of activities from a Fabric Data Pipeline definition,
        extracting name, type, typeProperties, and the resolved object name
        for supported activity types.

        Accepts either a pipeline ID or display name. For activities of type
        RefreshDataflow, TridentNotebook, InvokePipeline, or DatasetRefresh,
        the referenced object's display name is resolved using the appropriate class.

        Args:
            workspace_id (str): The ID of the workspace where the pipeline resides.
            pipeline_id_or_name (str): The pipeline ID or display name.
            max_workers (int): Maximum number of concurrent requests for name resolution. Defaults to 5.

        Returns:
            Dict: A dictionary with 'message' and 'content' (list of activity dicts with
                pipeline_id, pipeline_name, activity_name, activity_type, typeProperties,
                and object_name inside typeProperties for supported types).
        """
        if workspace_id == '':
            return {'message': 'Missing workspace id, please check.', 'content': ''}
        if pipeline_id_or_name == '':
            return {'message': 'Missing pipeline id or name, please check.', 'content': ''}

        # Resolve pipeline ID and name
        pipeline_id, pipeline_name = self._resolve_pipeline(workspace_id, pipeline_id_or_name)
        if not pipeline_id:
            return {'message': f'Pipeline not found: {pipeline_id_or_name}', 'content': ''}

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

        # Map activity types to their object ID property key
        activity_object_map = {
            'RefreshDataflow': 'dataflowId',
            'TridentNotebook': 'notebookId',
            'InvokePipeline': 'pipelineId',
            'DatasetRefresh': 'datasetId',
        }

        # Extract activities and collect unique items to resolve, grouped by type
        raw_activities = pipeline_content.get('properties', {}).get('activities', [])
        items_to_resolve = {}  # {object_id: (target_workspace_id, activity_type)}

        for activity in raw_activities:
            activity_type = activity.get('type', '')
            if activity_type in activity_object_map:
                props = activity.get('typeProperties', {})
                id_key = activity_object_map[activity_type]
                object_id = props.get(id_key, '')
                target_ws = props.get('workspaceId', workspace_id)
                if object_id and object_id not in items_to_resolve:
                    items_to_resolve[object_id] = (target_ws, activity_type)

        # Resolve object names concurrently using the appropriate class per type
        resolved_names = {}
        if items_to_resolve:
            dataflow = Dataflow(self.token)
            notebook = Notebook(self.token)
            dataset = Dataset(self.token)

            def _resolve_name(object_id, target_ws, activity_type):
                if activity_type == 'RefreshDataflow':
                    return object_id, dataflow.get_dataflow_name(target_ws, object_id)
                elif activity_type == 'TridentNotebook':
                    result = notebook.get_notebook(target_ws, object_id)
                    name = result['content'].get('displayName', '') if result.get('message') == 'Success' else ''
                    return object_id, name
                elif activity_type == 'InvokePipeline':
                    result = self.get_pipeline(target_ws, object_id)
                    name = result['content'].get('displayName', '') if result.get('message') == 'Success' else ''
                    return object_id, name
                elif activity_type == 'DatasetRefresh':
                    return object_id, dataset.get_dataset_name(target_ws, object_id)
                return object_id, ''

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(_resolve_name, obj_id, ws_id, a_type): obj_id
                    for obj_id, (ws_id, a_type) in items_to_resolve.items()
                }
                for future in as_completed(futures):
                    obj_id, name = future.result()
                    resolved_names[obj_id] = name

        # Build activity list with object_name inside typeProperties as the first key
        activities = []
        for activity in raw_activities:
            activity_type = activity.get('type', '')
            type_props = activity.get('typeProperties', {})

            if activity_type in activity_object_map:
                object_id = type_props.get(activity_object_map[activity_type], '')
                object_name = resolved_names.get(object_id, '')
                type_props = {'object_name': object_name, **type_props}

            entry = {
                'pipeline_id': pipeline_id,
                'pipeline_name': pipeline_name,
                'activity_name': activity.get('name', ''),
                'activity_type': activity_type,
                'typeProperties': type_props,
            }

            activities.append(entry)

        return {'message': 'Success', 'content': activities}
