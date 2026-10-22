"""Credential environment used by the runnable examples."""

import os
from unittest.mock import patch

import pytest

from examples import _setup


def test_build_auth_uses_fabric_prefixed_credentials():
    values = {
        "FABRIC_TENANT_ID": "fabric-tenant",
        "FABRIC_CLIENT_ID": "fabric-client",
        "FABRIC_CLIENT_SECRET": "fabric-secret",
        "TENANT_ID": "legacy-tenant",
        "CLIENT_ID": "legacy-client",
        "CLIENT_SECRET": "legacy-secret",
    }
    with (
        patch.dict(os.environ, values, clear=True),
        patch.object(_setup, "Auth") as auth,
    ):
        _setup.build_auth()

    auth.assert_called_once_with("fabric-tenant", "fabric-client", "fabric-secret")


def test_build_auth_does_not_fall_back_to_legacy_credentials():
    values = {
        "TENANT_ID": "legacy-tenant",
        "CLIENT_ID": "legacy-client",
        "CLIENT_SECRET": "legacy-secret",
    }
    with patch.dict(os.environ, values, clear=True):
        with pytest.raises(RuntimeError, match="FABRIC_TENANT_ID"):
            _setup.build_auth()


def test_build_database_client_uses_fabric_prefixed_credentials():
    values = {
        "FABRIC_SQL_ENDPOINT": "server.example.com",
        "FABRIC_DATABASE": "database",
        "FABRIC_CLIENT_ID": "fabric-client",
        "FABRIC_CLIENT_SECRET": "fabric-secret",
        "CLIENT_ID": "legacy-client",
        "CLIENT_SECRET": "legacy-secret",
    }
    with (
        patch.dict(os.environ, values, clear=True),
        patch.object(_setup, "Database") as database,
    ):
        _setup.build_database_client()

    database.assert_called_once_with(
        "server.example.com", "database", "fabric-client", "fabric-secret"
    )


def test_build_database_client_does_not_fall_back_to_legacy_credentials():
    values = {
        "FABRIC_SQL_ENDPOINT": "server.example.com",
        "FABRIC_DATABASE": "database",
        "CLIENT_ID": "legacy-client",
        "CLIENT_SECRET": "legacy-secret",
    }
    with patch.dict(os.environ, values, clear=True):
        with pytest.raises(RuntimeError, match="FABRIC_CLIENT_ID"):
            _setup.build_database_client()
