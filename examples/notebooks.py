"""Notebook examples: list notebooks in a workspace, fetch a single notebook."""

import pandas as pd

from examples._setup import build_clients


def list_notebooks(notebook, workspace_id):
    result = notebook.list_notebooks(workspace_id=workspace_id)
    if result["message"] != "Success":
        print(f"Error: {result['message']}")
        return None
    df = pd.DataFrame(result["content"])
    print(f"Found {len(df)} notebooks")
    return df


def get_notebook(notebook, workspace_id, notebook_id):
    result = notebook.get_notebook(workspace_id=workspace_id, notebook_id=notebook_id)
    if result["message"] != "Success":
        print(f"Error: {result['message']}")
        return None
    return result["content"]


if __name__ == "__main__":
    clients = build_clients()
    df = list_notebooks(clients["notebook"], workspace_id="your-workspace-id")
    if df is not None:
        print(df.head())

    body = get_notebook(
        clients["notebook"],
        workspace_id="your-workspace-id",
        notebook_id="your-notebook-id",
    )
    if body is not None:
        print(body)
