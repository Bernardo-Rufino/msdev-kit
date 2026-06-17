"""Pipeline examples: read activities, locate pipelines that reference a
given dataflow.
"""

import pandas as pd

from examples._setup import build_clients


def get_activities(pipeline, workspace_id, pipeline_id):
    result = pipeline.get_pipeline_activities(workspace_id=workspace_id, pipeline_id=pipeline_id)
    if result["message"] != "Success":
        print(f"Error: {result['message']}")
        return None
    df = pd.DataFrame(result["content"])
    print(f"Found {len(df)} activities")
    return df


def find_by_dataflow(pipeline, workspace_id, dataflow_id):
    result = pipeline.find_pipelines_by_dataflow(
        workspace_id=workspace_id, dataflow_id=dataflow_id
    )
    if result["message"] != "Success":
        print(f"Error: {result['message']}")
        return []
    for m in result["content"]:
        print(f"{m['pipeline_name']} ({m['pipeline_id']})")
        print(f"  activities: {', '.join(m['activities'])}")
    return result["content"]


if __name__ == "__main__":
    clients = build_clients()
    df_activities = get_activities(
        clients["pipeline"], workspace_id="your-workspace-id", pipeline_id="your-pipeline-id"
    )
    if df_activities is not None:
        print(df_activities[["name", "type"]].head(20))

    find_by_dataflow(
        clients["pipeline"],
        workspace_id="your-workspace-id",
        dataflow_id="your-dataflow-id",
    )
