"""Dataflow write examples.

This module defines copy and destination replacement helpers. It deliberately
performs no write when run directly. Use ``dataflow_destinations.py`` for a
read-only workspace destination inventory.
"""


def copy_dataflows(dataflow, source_workspace_id, destination_workspace_id, names):
    """Copy named dataflows to another workspace. This creates new items."""
    result = dataflow.list_dataflows(workspace_id=source_workspace_id)
    if result.get("message") != "Success":
        raise RuntimeError(f"Unable to list dataflows: {result.get('message')}")

    to_copy = [item for item in result.get("content", []) if item.get("name") in names]
    for item in to_copy:
        dataflow_id = item.get("id") or item.get("objectId")
        if not dataflow_id:
            print(f"Skipped {item.get('name', '<unnamed>')}: missing dataflow ID")
            continue

        details = dataflow.get_dataflow_details(
            workspace_id=source_workspace_id, dataflow_id=dataflow_id
        ).get("content", "")
        if not details:
            print(f"Skipped {item['name']}: definition was not returned")
            continue

        details["entities"][0].pop("partitions", None)
        details["pbi:mashup"]["allowNativeQueries"] = False
        created = dataflow.create_dataflow(
            workspace_id=destination_workspace_id, dataflow_content=details
        )
        print(f"Copied {item['name']}: {created.get('message')}")


def replace_destination_and_fix_pipelines(
    dataflow,
    pipeline,
    workspace_id,
    old_dataflow_id,
    destination_workspace_id,
    destination_item_id,
):
    """Replace a destination, then update pipeline references to the new ID."""
    result = dataflow.change_data_destination(
        workspace_id=workspace_id,
        dataflow_id=old_dataflow_id,
        destination_type="Warehouse",
        destination_workspace_id=destination_workspace_id,
        destination_item_id=destination_item_id,
        mode="replace",
    )
    if result.get("message") != "Success":
        raise RuntimeError(f"Destination replacement failed: {result}")

    new_dataflow_id = result["content"]["id"]
    matches = pipeline.find_pipelines_by_dataflow(
        workspace_id=workspace_id, dataflow_id=old_dataflow_id
    )
    if matches.get("message") != "Success":
        raise RuntimeError(f"Pipeline lookup failed: {matches}")

    for item in matches.get("content", []):
        updated = pipeline.replace_dataflow_id_in_pipeline(
            workspace_id=workspace_id,
            pipeline_id=item["pipeline_id"],
            old_dataflow_id=old_dataflow_id,
            new_dataflow_id=new_dataflow_id,
        )
        print(f"{item['pipeline_name']}: {updated.get('message')}")


if __name__ == "__main__":
    print("No write operation was run. Edit this file and call a helper with real IDs.")
