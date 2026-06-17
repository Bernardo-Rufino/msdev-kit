"""Dataflow examples: copy a dataflow to another workspace, change its data
destination, and update downstream pipelines that referenced the old ID.
"""

import pandas as pd

from examples._setup import build_clients


def copy_dataflows(dataflow, source_workspace_id, destination_workspace_id, names):
    dataflows = dataflow.list_dataflows(workspace_id=source_workspace_id).get("content", [])
    to_copy = [d for d in dataflows if d.get("name") in names]

    for d in to_copy:
        details = dataflow.get_dataflow_details(
            workspace_id=source_workspace_id, dataflow_id=d["objectId"]
        ).get("content", "")
        if not details:
            continue

        details["entities"][0].pop("partitions", None)
        details["pbi:mashup"]["allowNativeQueries"] = False

        result = dataflow.create_dataflow(
            workspace_id=destination_workspace_id, dataflow_content=details
        )
        print(f"Copied {d['name']}: {result}")


def replace_destination_and_fix_pipelines(
    dataflow,
    pipeline,
    workspace_id,
    old_dataflow_id,
    destination_workspace_id,
    destination_item_id,
):
    # Step 1: change destination (replace mode produces a new dataflow ID)
    result = dataflow.change_data_destination(
        workspace_id=workspace_id,
        dataflow_id=old_dataflow_id,
        destination_type="Warehouse",
        destination_workspace_id=destination_workspace_id,
        destination_item_id=destination_item_id,
        mode="replace",
    )
    if result["message"] != "Success":
        print(f"Error: {result}")
        return
    new_dataflow_id = result["content"]["id"]
    print(f"New dataflow ID: {new_dataflow_id}")

    # Step 2: find pipelines still referencing the old dataflow
    matches = pipeline.find_pipelines_by_dataflow(
        workspace_id=workspace_id, dataflow_id=old_dataflow_id
    )
    if matches["message"] != "Success":
        print(f"Error: {matches}")
        return
    print(f"Found {len(matches['content'])} pipeline(s) to update")

    # Step 3: rewrite each pipeline
    for m in matches["content"]:
        r = pipeline.replace_dataflow_id_in_pipeline(
            workspace_id=workspace_id,
            pipeline_id=m["pipeline_id"],
            old_dataflow_id=old_dataflow_id,
            new_dataflow_id=new_dataflow_id,
        )
        print(f"  {m['pipeline_name']}: {r['message']}")


def list_destinations(dataflow, workspace_id, dataflow_id):
    result = dataflow.get_data_destinations(workspace_id=workspace_id, dataflow_id=dataflow_id)
    if result["message"] != "Success":
        print(f"Error: {result}")
        return None
    return pd.DataFrame(result["content"])


if __name__ == "__main__":
    clients = build_clients()
    copy_dataflows(
        clients["dataflow"],
        source_workspace_id="123",
        destination_workspace_id="456",
        names=["dataflow1", "dataflow2"],
    )
