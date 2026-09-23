"""Safety and authentication flow for the runnable Dataflow upgrade example."""

import sys
from unittest.mock import MagicMock

import pytest

from examples import dataflow_gen2_cicd_upgrade as example


def test_example_does_not_authenticate_without_execute(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", [
        "dataflow_gen2_cicd_upgrade", "--workspace-id", "<workspace-id>",
        "--dataflow-id", "<dataflow-id>",
    ])
    auth = MagicMock()
    monkeypatch.setattr(example, "build_auth", auth)

    example.main()

    auth.assert_not_called()
    assert "No change made" in capsys.readouterr().out


def test_example_rejects_placeholders_before_authentication(monkeypatch):
    monkeypatch.setattr(sys, "argv", [
        "dataflow_gen2_cicd_upgrade", "--workspace-id", "<workspace-id>",
        "--dataflow-id", "<dataflow-id>", "--execute",
    ])
    auth = MagicMock()
    monkeypatch.setattr(example, "build_auth", auth)

    with pytest.raises(SystemExit):
        example.main()

    auth.assert_not_called()


def test_example_uses_spn_for_creation_and_user_for_refresh(monkeypatch, capsys):
    workspace_id = "11111111-1111-4111-8111-111111111111"
    source_id = "22222222-2222-4222-8222-222222222222"
    monkeypatch.setattr(sys, "argv", [
        "dataflow_gen2_cicd_upgrade", "--workspace-id", workspace_id,
        "--dataflow-id", source_id, "--name", "copy", "--execute",
    ])
    auth = MagicMock()
    auth.get_token.return_value = "spn-token"
    auth.get_token_for_user.return_value = "user-token"
    monkeypatch.setattr(example, "build_auth", lambda: auth)
    client = MagicMock()
    client.upgrade_to_gen2_cicd.return_value = {
        "message": "Success", "content": {"id": "new-id"},
        "refresh": {"id": "job-id", "status": "Completed"},
    }
    client_type = MagicMock(return_value=client)
    monkeypatch.setattr(example, "Dataflow", client_type)

    example.main()

    auth.get_token.assert_called_once_with("fabric")
    auth.get_token_for_user.assert_called_once_with("fabric")
    client_type.assert_called_once_with("spn-token")
    client.upgrade_to_gen2_cicd.assert_called_once_with(
        workspace_id=workspace_id,
        dataflow_id=source_id,
        display_name="copy",
        source_type="gen2",
        use_accessible_connections=True,
        refresh=True,
        refresh_access_token="user-token",
    )
    output = capsys.readouterr().out
    assert "Completed" in output
    assert "user-token" not in output
    assert "spn-token" not in output
