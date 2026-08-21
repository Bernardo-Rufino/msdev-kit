"""Workspace examples.

Running this module only lists the workspaces visible to the configured app.
The bulk membership helpers write to every supplied workspace, so call them
only after reviewing the list and setting real values in your own script.
"""

import pandas as pd

from examples._setup import build_powerbi_clients


def list_workspaces(workspace):
    """Return the workspaces visible to the configured service principal."""
    result = workspace.list_workspaces()
    if result.get("message") != "Success":
        raise RuntimeError(f"Unable to list workspaces: {result.get('message')}")

    workspaces = result.get("content", [])
    dataframe = pd.DataFrame(workspaces)
    if not dataframe.empty and "name" in dataframe:
        dataframe = dataframe.sort_values(by="name").reset_index(drop=True)
    print(f"Found {len(dataframe)} workspaces")
    return workspaces, dataframe


def add_user_to_all(workspace, workspaces, user_principal_name, role):
    """Write example. Add or update one user in every supplied workspace."""
    for item in workspaces:
        try:
            response = workspace.add_user(
                user_principal_name=user_principal_name,
                access_right=role,
                workspace_id=item["id"],
            )
            if response.get("message") != "Success":
                workspace.update_user(
                    user_principal_name=user_principal_name,
                    access_right=role,
                    workspace_id=item["id"],
                )
        except Exception as error:
            print(f"Error on workspace {item['name']} ({item['id']}): {error}")


def remove_user_from_all(workspace, workspaces, user_principal_name):
    """Write example. Remove one user from every supplied workspace."""
    for item in workspaces:
        try:
            workspace.remove_user(
                user_principal_name=user_principal_name, workspace_id=item["id"]
            )
            print(f"Removed {user_principal_name} from {item['name']}")
        except Exception as error:
            print(f"Error on workspace {item['name']} ({item['id']}): {error}")


if __name__ == "__main__":
    clients = build_powerbi_clients()
    _, dataframe = list_workspaces(clients["workspace"])
    print(dataframe.head(20).to_string(index=False))
