"""Workspace examples: list workspaces, bulk-add and bulk-remove users."""

import pandas as pd

from examples._setup import build_clients


def list_workspaces(workspace):
    workspaces = workspace.list_workspaces()
    workspaces_list = workspaces.get("content", [])

    df = pd.DataFrame(workspaces_list).sort_values(by="name").reset_index(drop=True)
    print(f"Found {len(df)} workspaces")
    return workspaces_list, df


def add_user_to_all(workspace, workspaces_list, user_principal_name, role):
    for w in workspaces_list:
        try:
            response = workspace.add_user(
                user_principal_name=user_principal_name,
                access_right=role,
                workspace_id=w["id"],
            )
            if response["message"] != "Success":
                workspace.update_user(
                    user_principal_name=user_principal_name,
                    access_right=role,
                    workspace_id=w["id"],
                )
        except Exception as e:
            print(f"Error on workspace {w['name']} ({w['id']}): {e}")


def remove_user_from_all(workspace, workspaces_list, user_principal_name):
    for w in workspaces_list:
        try:
            workspace.remove_user(
                user_principal_name=user_principal_name, workspace_id=w["id"]
            )
            print(f"Removed {user_principal_name} from {w['name']}")
        except Exception as e:
            print(f"Error on workspace {w['name']} ({w['id']}): {e}")


if __name__ == "__main__":
    clients = build_clients()
    workspace = clients["workspace"]

    workspaces_list, _ = list_workspaces(workspace)

    add_user_to_all(workspace, workspaces_list, "test@test.com", "Member")
    remove_user_from_all(workspace, workspaces_list, "test@test.com")
