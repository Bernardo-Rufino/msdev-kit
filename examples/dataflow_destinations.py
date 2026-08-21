"""Inventory Dataflow Gen2 destinations in one workspace.

The library writes the flattened Excel file under ``data/dataflows``. This
example also returns the same destination-only rows as a DataFrame for use in
an interactive session.
"""

from pathlib import Path

import pandas as pd

from examples._setup import build_powerbi_clients


WORKSPACE_ID = "<workspace-id>"


def destination_dataframe(workspace_dataflow_destinations: dict) -> pd.DataFrame:
    """Flatten only dataflows that have at least one destination table."""
    records = [
        item
        for item in workspace_dataflow_destinations.get("content", [])
        if item.get("tables")
    ]
    if not records:
        return pd.DataFrame()

    dataframe = pd.json_normalize(
        records,
        record_path=["tables"],
        meta=[
            ["dataflow", "id"],
            ["dataflow", "name"],
            ["dataflow", "configuredBy"],
            ["dataflow", "generation"],
            ["dataflow", "source"],
        ],
        record_prefix="table_",
        errors="ignore",
    )
    dataframe.columns = [column.replace(".", "_") for column in dataframe.columns]
    dataflow_columns = [
        column for column in dataframe.columns if column.startswith("dataflow_")
    ]
    table_columns = [
        column for column in dataframe.columns if not column.startswith("dataflow_")
    ]
    return dataframe[dataflow_columns + table_columns]


def inventory_workspace(dataflow, workspace_id: str, max_workers: int = 4) -> pd.DataFrame:
    """Fetch destinations, save the workbook, and return destination-only rows."""
    result = dataflow.get_workspace_data_destinations(
        workspace_id=workspace_id,
        max_workers=max_workers,
    )
    if result.get("message") not in {"Success", "Partial success"}:
        raise RuntimeError(f"Destination inventory failed: {result.get('message')}")

    dataframe = destination_dataframe(result)
    output = Path(dataflow.dataflows_dir) / f"workspace_dataflow_destinations_{workspace_id}.xlsx"
    print(f"Saved {len(dataframe)} destination table(s) to {output}")
    return dataframe


if __name__ == "__main__":
    if WORKSPACE_ID.startswith("<"):
        raise SystemExit("Set WORKSPACE_ID to a real workspace ID before running this example.")

    clients = build_powerbi_clients()
    dataframe = inventory_workspace(clients["dataflow"], WORKSPACE_ID)
    print(dataframe.head())
