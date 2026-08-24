import json
import requests
import pandas as pd
from pandas.core.frame import DataFrame
from typing import Dict, List
from .utilities import create_directory
from concurrent.futures import ThreadPoolExecutor, as_completed


class Workspace:

    def __init__(self, token: str):
        """
        Initialize variables.
        """
        self.main_url = "https://api.powerbi.com/v1.0/myorg"
        self.token = token
        self.headers = {"Authorization": f"Bearer {self.token}"}

        # Directories
        self.workspace_dir = "./data/workspaces"
        self.dataflows_dir = "./data/dataflows"
        self.datasets_dir = "./data/datasets"
        self.reports_dir = "./data/reports"
        self.users_dir = "./data/users"
        self.directories = [
            self.workspace_dir,
            self.dataflows_dir,
            self.datasets_dir,
            self.reports_dir,
            self.users_dir,
        ]

        for dir in self.directories:
            create_directory(dir)

    def list_workspaces_for_user(
        self,
        workspace_id: str = "",
        workspace_name: str = "",
        identifier: str = "",
        principal_type: str = "App",
        filters: str = "",
    ) -> Dict:
        """
        List all workspaces that the user has access to.

        Args:
            workspace_id (str, optional): workspace id to search for.
            workspace_name (str, optional): workspace name to search for.
            identifier (str, optional): identifier of the service principal.
            principal_type (str, optional): principal type, 'App' for service accounts, 'Users' for usual users. Defaults to 'App'.
            filters (str, optional): filters to be applied.
                - filters example:
                    - filters=f"contains(name,'{workspace_to_search}')%20or%20name%20eq%20'Dataflows'")

        Returns:
            Dict: status message and content.
        """
        # Main URL
        request_url = self.main_url + "/groups"

        # If no parameter, list all workspaces with access to...
        if (workspace_id == "") & (workspace_name == "") & (filters == ""):
            filename = "workspaces_all.xlsx"

        # If workspace ID was informed...
        elif workspace_id != "":
            request_url = f"{request_url}/{workspace_id}"
            filename = f"{workspace_id}.xlsx"

        # If workspace name was informed...
        elif workspace_name != "":
            request_url = f"{request_url}/?$filter=name%20eq%20'{workspace_name}'"
            filename = f"{workspace_name.replace(' ', '_').upper()}.xlsx"

        # If any custom (OData) filters were informed...
        # Example: passing -> filters="contains(name,'Databrew')"
        # Filters for workspaces that contain Databrew on it's name.
        elif filters != "":
            request_url = f"{request_url}/{workspace_id}?$filter={filters}"
            filename = "workspaces_filtered.xlsx"
        else:
            return {"message": "Missing parameters, please check.", "content": ""}

        # Make the request
        r = requests.get(url=request_url, headers=self.headers)

        # Get HTTP status and content
        status = r.status_code
        response = json.loads(r.content).get("value", "")

        # If success...

        if status == 200:
            df = pd.DataFrame(response)

            if not df.empty and "id" in df.columns and identifier != "":
                # If identifier was informed, get the role of the app in the workspace
                app_access_rights = [""] * len(df["id"])

                def _fetch_role(args):
                    index, ws_id = args
                    role_url = f"{request_url}/{ws_id}/users"
                    role_response = requests.get(url=role_url, headers=self.headers)
                    if role_response.status_code == 200:
                        users_data = role_response.json().get("value", [])
                        role = next(
                            (
                                user["groupUserAccessRight"]
                                for user in users_data
                                if user.get("identifier") == identifier
                                and user.get("principalType") == principal_type
                            ),
                            "",
                        )
                    else:
                        role = ""
                    return index, role

                with ThreadPoolExecutor() as executor:
                    futures = {
                        executor.submit(_fetch_role, (i, ws_id)): i
                        for i, ws_id in enumerate(df["id"])
                    }
                    for future in as_completed(futures):
                        index, role = future.result()
                        app_access_rights[index] = role
                        response[index]["workspaceRole"] = role

                df["workspaceRole"] = app_access_rights

            df.to_excel(f"{self.workspace_dir}/{filename}", index=False)
            return {"message": "Success", "content": response}
        else:
            response = json.loads(r.content)
            error_message = response["error"]["message"]
            return {"message": {"error": error_message, "content": response}}

    def list_workspaces(self, admin: bool = False, top: int = 5000) -> Dict:
        """
        List all workspaces the authenticated SPN has access to.

        Args:
            admin (bool, optional): use the admin API to list all tenant workspaces.
                Requires tenant-level admin or read-all permissions. Defaults to False.
            top (int, optional): max results per page (admin only). Defaults to 5000.

        Returns:
            Dict: status message and content.
        """
        if admin:
            all_workspaces = []
            request_url = self.main_url + f"/admin/groups?$top={top}"
            while request_url:
                r = requests.get(url=request_url, headers=self.headers)
                if r.status_code != 200:
                    body = json.loads(r.content)
                    return {
                        "message": {"error": body["error"]["message"], "content": body}
                    }
                body = json.loads(r.content)
                all_workspaces.extend(body.get("value", []))
                request_url = body.get("odata.nextLink", None)
            response = all_workspaces
        else:
            r = requests.get(url=self.main_url + "/groups", headers=self.headers)
            if r.status_code != 200:
                body = json.loads(r.content)
                return {"message": {"error": body["error"]["message"], "content": body}}
            response = json.loads(r.content).get("value", [])

        df = pd.DataFrame(response)
        df.to_excel(f"{self.workspace_dir}/workspaces_all.xlsx", index=False)

        return {"message": "Success", "content": response}

    def get_workspace_details(self, workspace_id: str = "") -> Dict:
        """
        Get details for a specific workspace that the user has access to.

        Args:
            workspace_id (str, optional): workspace id to search for.

        Returns:
            Dict: status message and content.
        """
        # Main URL
        request_url = self.main_url + "/groups"

        # If no workspace ID was informed...
        if workspace_id == "":
            return {"message": "Missing parameters, please check.", "content": ""}

        # If workspace ID was informed...
        elif workspace_id != "":
            request_url = f"{request_url}/{workspace_id}"

        # Make the request
        r = requests.get(url=request_url, headers=self.headers)

        # Get HTTP status and content
        status = r.status_code
        response = json.loads(r.content)

        # If success...
        if status == 200:
            return {"message": "Success", "content": response}

        else:
            # If any error happens, return message.
            response = json.loads(r.content)
            error_message = response["error"]["message"]

            return {"message": {"error": error_message, "content": response}}

    def list_users(self, workspace_id: str = "") -> Dict:
        """
        List all users in a workspace_id that the user has access to.

        Args:
            workspace_id (str, optional): workspace id to search for.

        Returns:
            Dict: status message and content.
        """
        # Main URL
        request_url = self.main_url + "/groups"

        # If workspace ID was not informed, return error message...
        if workspace_id == "":
            return {"message": "Missing workspace id, please check.", "content": ""}

        # If workspace ID was informed...
        else:
            request_url = f"{request_url}/{workspace_id}/users"
            filename = f"users_{workspace_id}.xlsx"

            # Make the request
            r = requests.get(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content).get("value", "")

            # If success...
            if status == 200:
                # Save to Excel file
                df = pd.DataFrame(response)
                df.to_excel(f"{self.users_dir}/{filename}", index=False)

                return {"message": "Success", "content": response}

            else:
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response["error"]["message"]

                return {"message": {"error": error_message, "content": response}}

    def list_reports(self, workspace_id: str = "") -> Dict:
        """
        List all reports for a specific workspace.
        Args:
            workspace_id (str, optional): workspace id to search for.

        Returns:
            Dict: status message and content.
        """
        # Main URL
        request_url = self.main_url + "/groups"

        # If workspace ID was not informed, return error message...
        if workspace_id == "":
            return {"message": "Missing workspace id, please check.", "content": ""}

        # If workspace ID was informed...
        else:
            request_url = f"{request_url}/{workspace_id}/reports"
            filename = f"reports_{workspace_id}.xlsx"

            # Make the request
            r = requests.get(url=request_url, headers=self.headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content).get("value", "")

            # If success...
            if status == 200:
                # Save to Excel file
                df = pd.DataFrame(response)
                df.to_excel(f"{self.reports_dir}/{filename}", index=False)

                return {"message": "Success", "content": response}

            else:
                # If any error happens, return message.
                response = json.loads(r.content)
                error_message = response["error"]["message"]

                return {"message": {"error": error_message, "content": response}}

    @staticmethod
    def _workspace_user_payload(
        user_principal_name: str, access_right: str, user_type: str
    ) -> Dict:
        """Build the documented request body for a User, Group, or App principal."""
        return {
            "identifier": user_principal_name,
            "groupUserAccessRight": access_right,
            "principalType": user_type,
        }

    def add_user(
        self,
        user_principal_name: str = "",
        workspace_id: str = "",
        access_right: str = "Viewer",
        user_type: str = "User",
    ) -> Dict:
        """
        Add or update a principal in a workspace.

        Args:
            user_principal_name (str): identifier of the user, security group, or app.
            workspace_id (str): workspace id to add the principal to.
            access_right (str, optional): access right type. Defaults to 'Viewer'.
            user_type (str, optional): principal type, User, Group, or App.
                Defaults to 'User'.

        Returns:
            Dict: status message
        """
        if user_principal_name == "" or workspace_id == "":
            return {"message": "Missing parameters, please check."}

        if user_type not in {"User", "Group", "App"}:
            return {"message": "Invalid user_type. Expected User, Group, or App."}

        print(
            f"Adding principal {user_principal_name} to workspace {workspace_id} "
            f"as {access_right}..."
        )

        # Check if user already exists
        current_users = self.list_users(workspace_id=workspace_id)
        if current_users.get("message") != "Success":
            return {
                "message": "Failed to fetch current users.",
                "content": current_users,
            }

        existing_user = None
        for user in current_users["content"]:
            if user.get("principalType") != user_type:
                continue

            identifiers = [user.get("identifier", "")]
            if user_type == "User":
                identifiers.append(user.get("emailAddress", ""))

            if any(
                identifier.lower() == user_principal_name.lower()
                for identifier in identifiers
                if identifier
            ):
                existing_user = user
                break

        if existing_user is not None:
            current_role = existing_user.get("groupUserAccessRight", "")
            print(
                f"Principal {user_principal_name} already exists with role ({current_role}). Updating to {access_right}."
            )
            return self.update_user(
                user_principal_name=user_principal_name,
                workspace_id=workspace_id,
                access_right=access_right,
                user_type=user_type,
            )

        request_url = self.main_url + f"/groups/{workspace_id}/users"
        headers = {"Authorization": f"Bearer {self.token}"}

        # https://learn.microsoft.com/en-us/rest/api/power-bi/groups/add-group-user
        data = self._workspace_user_payload(
            user_principal_name, access_right, user_type
        )

        r = requests.post(url=request_url, headers=headers, json=data)
        status = r.status_code

        if status == 200:
            print(
                f"Principal {user_principal_name} added successfully to workspace "
                f"{workspace_id} as {access_right}."
            )
            return {"message": "Success"}
        else:
            try:
                response = json.loads(r.content.decode("utf-8"))
                error_message = response["error"]
            except Exception:
                return {"message": "Error reading JSON response"}
            return {"message": {"error": error_message, "content": response}}

    def update_user(
        self,
        user_principal_name: str = "",
        workspace_id: str = "",
        access_right: str = "Member",
        user_type: str = "User",
    ) -> Dict:
        """
        Update a user, security group, or app on a workspace.

        Args:
            user_principal_name (str, optional): identifier of the principal.
            workspace_id (str, optional): workspace id to update the principal in.
            access_right (str, optional): access right type. Defaults to 'Member'.
            user_type (str, optional): principal type, User, Group, or App.
                Defaults to 'User'.

        Returns:
            Dict: status message.
        """

        if user_principal_name == "" or workspace_id == "":
            return {"message": "Missing parameters, please check."}

        if user_type not in {"User", "Group", "App"}:
            return {"message": "Invalid user_type. Expected User, Group, or App."}

        request_url = self.main_url + f"/groups/{workspace_id}/users"

        headers = {"Authorization": f"Bearer {self.token}"}

        # https://learn.microsoft.com/en-us/rest/api/power-bi/groups/update-group-user
        data = self._workspace_user_payload(
            user_principal_name, access_right, user_type
        )

        r = requests.put(url=request_url, headers=headers, json=data)
        status = r.status_code

        if status == 200:
            return {"message": "Success"}

        if status == 401:
            return {"message": "Not enough privileges to update user."}

        if status == 404:
            return {"message": "User was not found in workspace."}

        response = json.loads(r.content)
        print(f"status={status}, response={response}")
        error_message = (
            f"Error for workspace_id={workspace_id}: "
            f"{response.get('error', {}).get('code', 'Unknown')}"
        )

        return {
            "message": {
                "error_status": r.status_code,
                "error": error_message,
                "content": r.content,
            }
        }

    def remove_user(
        self, user_principal_name: str = "", workspace_id: str = ""
    ) -> Dict:
        """
        Remove an user from a workspace.

        Args:
            user_principal_name (str, optional): user e-mail or identifier of service principal.
            workspace_id (str, optional): workspace id to add the user to.

        Returns:
            Dict: status message
        """

        # If both, user and workspace if are provided...
        if (user_principal_name != "") & (workspace_id != ""):

            request_url = (
                self.main_url + f"/groups/{workspace_id}/users/{user_principal_name}"
            )

            headers = {"Authorization": f"Bearer {self.token}"}

            # Make the request
            r = requests.delete(url=request_url, headers=headers)

            # Get HTTP status and content
            status = r.status_code
            response = json.loads(r.content)

            # If success...
            if status == 200:
                return {"message": "Success"}

            elif status == 401:
                return {"message": "Not enough privileges to remove user."}

            elif status == 404:
                return {"message": "User was not found in workspace."}

            else:
                # If any error happens, return message.
                print(f"status={status}, response={response}")
                error_message = response["error"]["message"]

                return {"message": {"error": error_message, "content": response}}

        else:
            return {"message": "Missing parameters, please check."}

    def batch_update_user(
        self, user: str = "", workspaces_list: List[str] = []
    ) -> DataFrame:
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
        if (user != "") & (workspaces_list != []):

            def _update_workspace(workspace):
                id = workspace.get("id", "")
                name = workspace.get("name", "")
                response = self.update_user(
                    user_principal_name=user, workspace_id=id, access_right="Admin"
                )
                try:
                    return (id, name, "Error", response["message"]["content"])
                except Exception:
                    return (id, name, "Success", "")

            with ThreadPoolExecutor() as executor:
                responses = list(executor.map(_update_workspace, workspaces_list))

            # Create a dataframe with responses
            df1 = pd.DataFrame(
                responses, columns=["id", "name", "status", "error_message"]
            )

            # Serialize json from error message as a new dataframe
            df2 = pd.json_normalize(df1["error_message"])

            # Drop error message column and merge both dataframes
            df1.drop(labels="error_message", axis="columns", inplace=True)
            df = pd.merge(left=df1, right=df2, left_index=True, right_index=True)
            df = df.fillna("")

            # Save to an Excel file with user name
            df.to_excel(f"./data/workspaces_{user.split('@')[0]}.xlsx", index=False)

            return df

        else:

            return pd.DataFrame([], columns=["id", "name", "status", "error_message"])
