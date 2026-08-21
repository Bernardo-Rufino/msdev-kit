"""Microsoft Graph examples.

The default run only performs lookups and lists members. ``add_remove`` changes
group membership and must be called explicitly with reviewed IDs.
"""

import pandas as pd

from examples._setup import build_graph_client, require_configured_value


def lookup(graph, email, group_name):
    user_id = graph.get_user_id(email)
    group_id = graph.get_group_id(group_name)
    print(f"User ID: {user_id}")
    print(f"Group ID: {group_id}")
    return user_id, group_id


def list_members(graph, group_id):
    members = graph.list_group_members(group_id)
    dataframe = pd.DataFrame(members)
    print(f"Found {len(dataframe)} members")
    return dataframe


def add_remove(graph, group_id, user_id):
    """Write example. Add a user, then optionally remove it in a separate call."""
    graph.add_group_member(group_id, user_id)
    print(f"Added {user_id} to group {group_id}")


if __name__ == "__main__":
    email = require_configured_value("<user@contoso.com>", "email")
    group_name = require_configured_value("<security-group-name>", "group_name")
    graph = build_graph_client()
    _, group_id = lookup(graph, email, group_name)
    if group_id:
        print(list_members(graph, group_id).head())
