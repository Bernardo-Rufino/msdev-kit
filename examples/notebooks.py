"""Read-only notebook examples."""

import pandas as pd

from examples._setup import build_fabric_clients, require_configured_value


def list_notebooks(notebook, workspace_id):
    result = notebook.list_notebooks(workspace_id=workspace_id)
    if result.get("message") != "Success":
        print(f"Error: {result.get('message')}")
        return None
    dataframe = pd.DataFrame(result.get("content", []))
    print(f"Found {len(dataframe)} notebooks")
    return dataframe


def get_notebook(notebook, workspace_id, notebook_id):
    result = notebook.get_notebook(workspace_id=workspace_id, notebook_id=notebook_id)
    if result.get("message") != "Success":
        print(f"Error: {result.get('message')}")
        return None
    return result.get("content")


if __name__ == "__main__":
    workspace_id = require_configured_value("<workspace-id>", "workspace_id")
    clients = build_fabric_clients()
    dataframe = list_notebooks(clients["notebook"], workspace_id=workspace_id)
    if dataframe is not None:
        print(dataframe.head())
