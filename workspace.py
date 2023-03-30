import json
import requests
import pandas as pd
from pandas.core.frame import DataFrame
from typing import Dict, List
from utilities import create_directory


class Workspace:

    def __init__(self, token: str):
        """
        Initialize variables.
        """
        self.main_url = 'https://api.powerbi.com/v1.0/myorg'
        self.token = token
        self.headers = {'Authorization': f'Bearer {self.token}'}

        # Directories
        self.workspace_dir = './data/workspaces'
        self.users_dir = './data/users'
        self.reports_dir = './data/reports'
        self.directories = [self.workspace_dir, self.users_dir]

        for dir in self.directories:
            create_directory(dir)


    def list_workspaces(
                self, 
                workspace_id: str = '', 
                workspace_name: str = '', 
                filters: str = '') -> Dict:
        """
        List all workspaces that the user has access to.

        Args:
            workspace_id (str, optional): workspace id to search for.
            workspace_name (str, optional): workspace name to search for.
            filters (str, optional): filters to be applied.
                - filters example:
                    - filters=f"contains(name,'{workspace_to_search}')%20or%20name%20eq%20'Dataflows'")

        Returns:
            Dict: status message and content.
        """
        # Main URL
        request_url = self.main_url + '/groups'

        # If no parameter, list all workspaces with access to...
        if (workspace_id == '') & (workspace_name == '') & (filters == ''):
            filename = 'workspaces_all.xlsx'

        # If workspace ID was informed...
        elif workspace_id != '':
            request_url = f"{request_url}/{workspace_id}"
            filename = f'{workspace_id}.xlsx'

        # If workspace name was informed...
        elif workspace_name != '':
            request_url = f"{request_url}/?$filter=name%20eq%20'{workspace_name}'"
            filename = f"{workspace_name.replace(' ', '_').upper()}.xlsx"
        
        # If any custom (OData) filters were informed...
        # Example: passing -> filters="contains(name,'Databrew')"
        # Filters for workspaces that contain Databrew on it's name.
        elif filters != '':
            request_url = f'{request_url}/{workspace_id}?$filter={filters}'
            filename = 'workspaces_filtered.xlsx'
        else: 
            return {'message': 'Missing parameters, please check.', 'content': ''}

        # Make the request
        r = requests.get(url=request_url, headers=self.headers)

        # Get HTTP status and content
        status = r.status_code
        response = json.loads(r.content).get('value', '')

        # If success...
        if status == 200:
            # Save to Excel file
            df = pd.DataFrame(response)
            df.to_excel(f'{self.workspace_dir}/{filename}', index=False)
            
            return {'message': 'Success', 'content': response}

        else:                
            # If any error happens, return message.
            response = json.loads(r.content)
            error_message = response['error']['message']

            return {'message': {'error': error_message, 'content': response}}


    def list_users(self, workspace_id: str = '') -> Dict:
        """
        List all users in a workspace_id that the user has access to.

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
            request_url = f'{request_url}/{workspace_id}/users'
            filename = f'users_{workspace_id}.xlsx'

            # Make the request
            r = requests.get(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content).get('value', '')

            # If success...
            if status == 200:
                # Save to Excel file
                df = pd.DataFrame(response)
                df.to_excel(f'{self.users_dir}/{filename}', index=False)
                
                return {'message': 'Success', 'content': response}

            else:                
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response['error']['message']

                return {'message': {'error': error_message, 'content': response}}

        
    def list_reports(self, workspace_id: str = '') -> Dict:
        """
        List all users in a workspace_id that the user has access to.

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
            request_url = f'{request_url}/{workspace_id}/reports'
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
                df.to_excel(f'{self.reports_dir}/{filename}', index=False)
                
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
                access_right: str = 'Viewer',
                user_type: str = 'user') -> Dict:
        """
        Add an user to a workspace.

        Args:
            user_principal_name (str): user e-mail or identifier of service principal.
            workspace_id (str): workspace id to add the user to.
            access_right (str, optional): access right type. Defaults to 'Viewer'.
            user_type (str, optional): user type, 'SP' for service accounts. Defaults to 'user'.

        Returns:
            Dict: status message.
        """

        # If both, user and workspace if are provided...
        if (user_principal_name != '') & (workspace_id != ''):

            request_url = self.main_url + f'/groups/{workspace_id}/users'

            headers = {'Authorization': f'Bearer {self.token}'}

            # Add user to workspace with the specified access right.
            # https://learn.microsoft.com/en-us/rest/api/power-bi/groups/add-group-user#groupuseraccessright

            # If service principal account
            if user_type == 'SP':
                data = {
                    "identifier": user_principal_name,
                    "groupUserAccessRight": access_right
                }

            else:

                data = {
                    "emailAddress": user_principal_name,
                    "groupUserAccessRight": access_right
                }

            # Make the request
            r = requests.post(url=request_url, headers=headers, json=data)

            # Get HTTP status and content
            status = r.status_code

            # If success...
            if status == 200:
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


    def update_user(
                self, 
                user_principal_name: str = '', 
                workspace_id: str = '', 
                access_right: str = 'Member') -> Dict:
        """
        Update an user on a workspace.

        Args:
            user_principal_name (str, optional): user e-mail or identifier of service principal.
            workspace_id (str, optional): workspace id to add the user to.
            access_right (str, optional): access right type. Defaults to 'Member'.

        Returns:
            Dict: status message.
        """

        # If both, user and workspace if are provided...
        if (user_principal_name != '') & (workspace_id != ''):

            request_url = self.main_url + f'/groups/{workspace_id}/users'

            headers = {'Authorization': f'Bearer {self.token}'}

            # Add user to workspace with the specified access right.
            # https://learn.microsoft.com/en-us/rest/api/power-bi/groups/update-group-user
            data = {
                "identifier": user_principal_name,
                "groupUserAccessRight": access_right,
                "principalType": "User"
            }

            # Make the request
            r = requests.put(url=request_url, headers=headers, json=data)

            # Get HTTP status and content
            status = r.status_code

            # If success...
            if status == 200:
                return {'message': 'Success'}
            
            else:
                
                response = json.loads(r.content)                
                # If any error happens, return message.
                error_message = response['error']['code']

                return {'message': {'error': error_message, 'content': response}}

        else:
            return {'message': 'Missing parameters, please check.'}


    def remove_user(self, user_principal_name: str = '', workspace_id: str = '') -> Dict:
        """
        Remove an user from a workspace.

        Args:
            user_principal_name (str, optional): user e-mail or identifier of service principal.
            workspace_id (str, optional): workspace id to add the user to.

        Returns:
            Dict: status message
        """

        # If both, user and workspace if are provided...
        if (user_principal_name != '') & (workspace_id != ''):

            request_url = self.main_url + f'/groups/{workspace_id}/users/{user_principal_name}'

            headers = {'Authorization': f'Bearer {self.token}'}

            # Make the request
            r = requests.delete(url=request_url, headers=headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content)

            # If success...
            if status == 200:
                return {'message': 'Success'}

            else:                
                # If any error happens, return message.
                error_message = response['error']['message']

                return {'message': {'error': error_message, 'content': response}}

        else:
            return {'message': 'Missing parameters, please check.'}


    def batch_update_user(self, user: str = '', workspaces_list: List[str] = []) -> DataFrame:
        """
        Batch update an user on a list of workspaces.

        Args:
            user (str): user e-mail or identifier of service principal.
            workspaces_list (List[str]): list of workspaces to update an user.

        Returns:
            DataFrame: table with workspaces and status of the update.
        """

        responses = []

        # If user and list of workspaces were informed...
        if (user != '') & (workspaces_list != []):


            for workspace in workspaces_list:
                id = workspace.get('id', '')
                name = workspace.get('name', '')
                response = self.update_user(user_principal_name=user, workspace_id=id, access_right='Admin')

                # Try to update the user
                try:
                    responses.append((id, name, 'Error', response['message']['content']))
                except:
                    responses.append((id, name, 'Success', ''))

            # Create a dataframe with responses
            df1 = pd.DataFrame(responses, columns=['id', 'name', 'status', 'error_message'])

            # Serialize json from error message as a new dataframe
            df2 = pd.json_normalize(df1['error_message'])

            # Drop error message column and merge both dataframes
            df1.drop(labels='error_message', axis='columns', inplace=True)
            df = pd.merge(left=df1, right=df2, left_index=True, right_index=True)
            df = df.fillna('')

            # Save to an Excel file with user name
            df.to_excel(f"./data/workspaces_{user.split('@')[0]}.xlsx", index=False)

            return df

        else:

            return pd.DataFrame([], columns=['id', 'name', 'status', 'error_message'])