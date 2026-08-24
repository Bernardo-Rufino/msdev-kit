"""Unit tests for workspace principal access management."""

import json
from unittest.mock import MagicMock, patch

import pytest

from msdev_kit.fabric.workspace import Workspace


class MockWorkspace(Workspace):
    """Workspace instance that skips token and directory initialization."""

    def __init__(self):
        self.main_url = "https://api.powerbi.com/v1.0/myorg"
        self.token = "fake-token"
        self.headers = {"Authorization": f"Bearer {self.token}"}


@pytest.fixture
def workspace():
    return MockWorkspace()


def _response(status_code=200, body=None):
    response = MagicMock()
    response.status_code = status_code
    response.content = json.dumps(body or {}).encode("utf-8")
    return response


@pytest.mark.parametrize(
    ("user_type", "principal_name", "principal_type"),
    [
        ("User", "person@example.com", "User"),
        ("Group", "security-group-id", "Group"),
        ("App", "service-principal-id", "App"),
    ],
)
def test_add_user_posts_supported_principal_type(
    workspace, user_type, principal_name, principal_type
):
    with (
        patch.object(
            workspace, "list_users", return_value={"message": "Success", "content": []}
        ),
        patch("msdev_kit.fabric.workspace.requests.post", return_value=_response()) as post,
    ):
        result = workspace.add_user(
            user_principal_name=principal_name,
            workspace_id="workspace-id",
            access_right="Contributor",
            user_type=user_type,
        )

    assert result == {"message": "Success"}
    post.assert_called_once_with(
        url="https://api.powerbi.com/v1.0/myorg/groups/workspace-id/users",
        headers={"Authorization": "Bearer fake-token"},
        json={
            "identifier": principal_name,
            "groupUserAccessRight": "Contributor",
            "principalType": principal_type,
        },
    )


@pytest.mark.parametrize(
    ("user_type", "principal_name", "existing_user", "principal_type"),
    [
        (
            "User",
            "person@example.com",
            {
                "emailAddress": "PERSON@example.com",
                "principalType": "User",
                "groupUserAccessRight": "Admin",
            },
            "User",
        ),
        (
            "Group",
            "security-group-id",
            {
                "identifier": "SECURITY-GROUP-ID",
                "principalType": "Group",
                "groupUserAccessRight": "Viewer",
            },
            "Group",
        ),
        (
            "App",
            "service-principal-id",
            {
                "identifier": "SERVICE-PRINCIPAL-ID",
                "principalType": "App",
                "groupUserAccessRight": "Member",
            },
            "App",
        ),
    ],
)
def test_add_user_updates_existing_supported_principal(
    workspace, user_type, principal_name, existing_user, principal_type
):
    with (
        patch.object(
            workspace,
            "list_users",
            return_value={"message": "Success", "content": [existing_user]},
        ),
        patch("msdev_kit.fabric.workspace.requests.post") as post,
        patch("msdev_kit.fabric.workspace.requests.put", return_value=_response()) as put,
    ):
        result = workspace.add_user(
            user_principal_name=principal_name,
            workspace_id="workspace-id",
            access_right="Viewer",
            user_type=user_type,
        )

    assert result == {"message": "Success"}
    post.assert_not_called()
    put.assert_called_once_with(
        url="https://api.powerbi.com/v1.0/myorg/groups/workspace-id/users",
        headers={"Authorization": "Bearer fake-token"},
        json={
            "identifier": principal_name,
            "groupUserAccessRight": "Viewer",
            "principalType": principal_type,
        },
    )


@pytest.mark.parametrize(
    ("user_type", "principal_type"),
    [("User", "User"), ("Group", "Group"), ("App", "App")],
)
def test_update_user_puts_supported_principal_type(
    workspace, user_type, principal_type
):
    with patch(
        "msdev_kit.fabric.workspace.requests.put", return_value=_response()
    ) as put:
        result = workspace.update_user(
            user_principal_name="principal-id",
            workspace_id="workspace-id",
            access_right="Admin",
            user_type=user_type,
        )

    assert result == {"message": "Success"}
    put.assert_called_once_with(
        url="https://api.powerbi.com/v1.0/myorg/groups/workspace-id/users",
        headers={"Authorization": "Bearer fake-token"},
        json={
            "identifier": "principal-id",
            "groupUserAccessRight": "Admin",
            "principalType": principal_type,
        },
    )


@pytest.mark.parametrize("method_name", ["add_user", "update_user"])
@pytest.mark.parametrize("user_type", ["organization", "user"])
def test_workspace_user_methods_reject_unsupported_principal_type(
    workspace, method_name, user_type
):
    with (
        patch.object(workspace, "list_users") as list_users,
        patch("msdev_kit.fabric.workspace.requests.post") as post,
        patch("msdev_kit.fabric.workspace.requests.put") as put,
    ):
        result = getattr(workspace, method_name)(
            user_principal_name="principal-id",
            workspace_id="workspace-id",
            user_type=user_type,
        )

    assert result == {"message": "Invalid user_type. Expected User, Group, or App."}
    list_users.assert_not_called()
    post.assert_not_called()
    put.assert_not_called()
