"""Graph examples: look up users and groups, add and remove group members."""

import pandas as pd

from examples._setup import build_clients


def lookup(graph, email, group_name):
    user_id = graph.get_user_id(email)
    print(f"User ID: {user_id}")

    group_id = graph.get_group_id(group_name)
    print(f"Group ID: {group_id}")
    return user_id, group_id


def list_members(graph, group_id):
    members = graph.list_group_members(group_id)
    df = pd.DataFrame(members)
    print(f"Found {len(df)} members")
    return df


def add_remove(graph, group_id, user_id):
    graph.add_group_member(group_id, user_id)
    print(f"Added {user_id} to group {group_id}")
    # graph.remove_group_member(group_id, user_id)


if __name__ == "__main__":
    clients = build_clients()
    user_id, group_id = lookup(clients["graph"], "user@contoso.com", "My Security Group")
    if group_id:
        list_members(clients["graph"], group_id)
        if user_id:
            add_remove(clients["graph"], group_id, user_id)
