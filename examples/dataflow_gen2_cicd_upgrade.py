"""Upgrade a standard Dataflow Gen2 using shared connections and user refresh.

This creates a new Fabric item and refreshes its configured data destination.
Nothing is written unless ``--execute`` is supplied.
"""

from __future__ import annotations

import argparse
from uuid import UUID

from examples._setup import build_auth
from msdev_kit.fabric import Dataflow


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--dataflow-id", required=True, help="Standard Gen2 source item ID")
    parser.add_argument("--name", default="", help="New item name, defaults to source name plus _cicd")
    parser.add_argument("--execute", action="store_true", help="Create and refresh the new dataflow")
    args = parser.parse_args()

    if not args.execute:
        print("No change made. Add --execute to create and refresh the new dataflow.")
        return

    for label, value in (("workspace ID", args.workspace_id),
                         ("dataflow ID", args.dataflow_id)):
        try:
            UUID(value)
        except ValueError:
            parser.error(f"{label} must be a real UUID before using --execute")

    auth = build_auth()
    # A Fabric service principal can create the item, but it cannot run its
    # Dataflow Gen2 CI/CD refresh job. Acquire a delegated user token separately.
    delegated_token = auth.get_token_for_user("fabric")
    dataflow = Dataflow(auth.get_token("fabric"))
    pbi_token = auth.get_token("pbi")
    result = dataflow.upgrade_to_gen2_cicd(
        workspace_id=args.workspace_id,
        dataflow_id=args.dataflow_id,
        display_name=args.name,
        source_type="gen2",
        use_accessible_connections=True,
        refresh=True,
        refresh_access_token=delegated_token,
        pbi_access_token=pbi_token,
    )

    item_id = (result.get("content") or {}).get("id")
    if item_id:
        print(f"New dataflow ID: {item_id}")
    refresh = result.get("refresh") or {}
    if refresh:
        print(f"Refresh job: {refresh.get('id')}, status: {refresh.get('status')}")
    if result.get("message") != "Success":
        raise SystemExit(f"Upgrade or refresh failed: {result.get('message')}")
    print("Upgrade and refresh completed.")


if __name__ == "__main__":
    main()
