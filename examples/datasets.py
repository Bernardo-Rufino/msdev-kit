"""Dataset examples: list users with access, run a DAX query."""

import pandas as pd

from examples._setup import build_clients


def list_dataset_users(workspace, dataset, workspace_name, dataset_name):
    workspaces = workspace.list_workspaces().get("content", [])
    workspace_id = next((w["id"] for w in workspaces if w["name"] == workspace_name), "")
    if not workspace_id:
        print(f"Workspace not found: {workspace_name}")
        return None

    datasets = dataset.list_datasets(workspace_id=workspace_id).get("content", [])
    dataset_id = next((d["id"] for d in datasets if d["name"] == dataset_name), "")
    if not dataset_id:
        print(f"Dataset not found: {dataset_name}")
        return None

    users = dataset.list_users(workspace_id=workspace_id, dataset_id=dataset_id)
    df = pd.DataFrame(users["content"])
    df["workspace"] = workspace_name
    df["dataset"] = dataset_name
    return df


def run_dax(dataset, workspace_id, dataset_id, query):
    result = dataset.execute_query(
        workspace_id=workspace_id, dataset_id=dataset_id, query=query
    )
    if result["message"] != "Success":
        print(f"Error: {result}")
        return None

    print(f"Rows returned: {result['rows_returned']}")
    print(f"Total rows in source: {result['total_rows']}")
    print(f"Max rows allowed: {result['max_rows_allowed']} (cols={result['num_columns']})")
    print(f"Truncated: {result['truncated']}")
    return pd.DataFrame(result["content"])


if __name__ == "__main__":
    clients = build_clients()
    workspace = clients["workspace"]
    dataset = clients["dataset"]

    df_users = list_dataset_users(workspace, dataset, "Test workspace", "Test dataset")
    if df_users is not None:
        print(df_users.head())

    df_query = run_dax(
        dataset,
        workspace_id="your-workspace-id",
        dataset_id="your-dataset-id",
        query="EVALUATE SUMMARIZECOLUMNS('Table'[Column1], 'Table'[Column2])",
    )
    if df_query is not None:
        print(df_query.head())
