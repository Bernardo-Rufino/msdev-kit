"""Dataset examples: list users with access and run a DAX query."""

import pandas as pd

from examples._setup import build_powerbi_clients, require_configured_value


def list_dataset_users(workspace, dataset, workspace_name, dataset_name):
    workspaces = workspace.list_workspaces().get("content", [])
    workspace_id = next((item["id"] for item in workspaces if item["name"] == workspace_name), "")
    if not workspace_id:
        print(f"Workspace not found: {workspace_name}")
        return None

    datasets = dataset.list_datasets(workspace_id=workspace_id).get("content", [])
    dataset_id = next((item["id"] for item in datasets if item["name"] == dataset_name), "")
    if not dataset_id:
        print(f"Dataset not found: {dataset_name}")
        return None

    users = dataset.list_users(workspace_id=workspace_id, dataset_id=dataset_id)
    dataframe = pd.DataFrame(users.get("content", []))
    dataframe["workspace"] = workspace_name
    dataframe["dataset"] = dataset_name
    return dataframe


def run_dax(dataset, workspace_id, dataset_id, query):
    result = dataset.execute_query(
        workspace_id=workspace_id, dataset_id=dataset_id, query=query
    )
    if result.get("message") != "Success":
        print(f"Error: {result.get('message')}")
        return None

    print(f"Rows returned: {result['rows_returned']}")
    print(f"Total rows in source: {result['total_rows']}")
    print(f"Max rows allowed: {result['max_rows_allowed']} (cols={result['num_columns']})")
    print(f"Truncated: {result['truncated']}")
    return pd.DataFrame(result["content"])


if __name__ == "__main__":
    workspace_name = require_configured_value("<workspace-name>", "workspace_name")
    dataset_name = require_configured_value("<semantic-model-name>", "dataset_name")
    clients = build_powerbi_clients()
    dataframe = list_dataset_users(
        clients["workspace"], clients["dataset"], workspace_name, dataset_name
    )
    if dataframe is not None:
        print(dataframe.head())
