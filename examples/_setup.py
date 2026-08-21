"""Small, explicit client factories shared by the runnable examples.

The examples read ``.env`` from the repository root. ``utils/.env`` remains a
temporary compatibility fallback for existing local setups. Copy
``.env.example`` to ``.env`` and fill only the variables required by the
example you intend to run.
"""

from __future__ import annotations

from os import environ
from pathlib import Path

from dotenv import load_dotenv

from msdev_kit import Auth
from msdev_kit.fabric import (
    Admin,
    Capacity,
    Dataflow,
    Dataset,
    Notebook,
    Operations,
    Pipeline,
    Report,
    Workspace,
)
from msdev_kit.fabric.database import Database
from msdev_kit.graph import GraphClient
from msdev_kit.sharepoint import SharePointClient


def _load_environment() -> None:
    root_env = Path(".env")
    legacy_env = Path("utils/.env")

    if root_env.exists():
        load_dotenv(root_env)
    elif legacy_env.exists():
        load_dotenv(legacy_env)


_load_environment()


def require_environment(*names: str) -> tuple[str, ...]:
    """Return required variables or raise an actionable configuration error."""
    missing = [name for name in names if not environ.get(name)]
    if missing:
        variables = ", ".join(missing)
        raise RuntimeError(
            f"Missing environment variable(s): {variables}. "
            "Copy .env.example to .env and set the values needed by this example."
        )
    return tuple(environ[name] for name in names)


def require_configured_value(value: str, name: str) -> str:
    """Reject empty values and the angle-bracket placeholders used by examples."""
    if not value or value.strip().startswith("<"):
        raise SystemExit(f"Set {name} to a real value before running this example.")
    return value


def build_auth() -> Auth:
    """Build the shared Azure credential used by Fabric, Graph, and SharePoint."""
    tenant_id, client_id, client_secret = require_environment(
        "TENANT_ID", "CLIENT_ID", "CLIENT_SECRET"
    )
    return Auth(tenant_id, client_id, client_secret)


def build_powerbi_clients() -> dict[str, object]:
    """Build clients that use the Power BI REST API audience."""
    auth = build_auth()
    pbi_token = auth.get_token("pbi")

    return {
        "auth": auth,
        "workspace": Workspace(pbi_token),
        "dataset": Dataset(pbi_token),
        "report": Report(pbi_token),
        "dataflow": Dataflow(pbi_token),
        "capacity": Capacity(pbi_token),
        "admin": Admin(pbi_token),
    }


def build_fabric_clients() -> dict[str, object]:
    """Build clients that use the Fabric REST API audience."""
    auth = build_auth()
    fabric_token = auth.get_token("fabric")

    return {
        "auth": auth,
        "operations": Operations(fabric_token),
        "pipeline": Pipeline(fabric_token),
        "notebook": Notebook(fabric_token),
    }


def build_graph_client() -> GraphClient:
    """Build a Microsoft Graph client."""
    return GraphClient(build_auth())


def build_sharepoint_client() -> SharePointClient:
    """Build a SharePoint client using the configured site location."""
    hostname, site_path = require_environment("SP_HOSTNAME", "SP_SITE_PATH")
    return SharePointClient(build_auth(), hostname, site_path)


def build_database_client() -> Database:
    """Build a Fabric SQL endpoint client without opening a connection."""
    endpoint, database, client_id, client_secret = require_environment(
        "FABRIC_SQL_ENDPOINT", "FABRIC_DATABASE", "CLIENT_ID", "CLIENT_SECRET"
    )
    return Database(endpoint, database, client_id, client_secret)
