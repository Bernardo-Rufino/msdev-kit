"""Read-only Fabric Data Pipeline examples."""

import pandas as pd

from examples._setup import build_fabric_clients, require_configured_value


def get_activities(pipeline, workspace_id, pipeline_id_or_name):
    result = pipeline.get_pipeline_activities(
        workspace_id=workspace_id, pipeline_id_or_name=pipeline_id_or_name
    )
    if result.get("message") != "Success":
        print(f"Error: {result.get('message')}")
        return None
    dataframe = pd.DataFrame(result.get("content", []))
    print(f"Found {len(dataframe)} activities")
    return dataframe


def find_by_dataflow(pipeline, workspace_id, dataflow_id):
    result = pipeline.find_pipelines_by_dataflow(
        workspace_id=workspace_id, dataflow_id=dataflow_id
    )
    if result.get("message") != "Success":
        print(f"Error: {result.get('message')}")
        return []
    for item in result.get("content", []):
        print(f"{item['pipeline_name']} ({item['pipeline_id']})")
        print(f"  activities: {', '.join(item['activities'])}")
    return result.get("content", [])


if __name__ == "__main__":
    workspace_id = require_configured_value("<workspace-id>", "workspace_id")
    pipeline_id_or_name = require_configured_value(
        "<pipeline-id-or-name>", "pipeline_id_or_name"
    )
    clients = build_fabric_clients()
    dataframe = get_activities(
        clients["pipeline"], workspace_id, pipeline_id_or_name
    )
    if dataframe is not None:
        print(dataframe.head(20))
