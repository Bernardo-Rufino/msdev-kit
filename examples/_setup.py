"""Shared setup for the example scripts.

Loads credentials from environment variables (optionally via ./utils/.env) and
constructs the Auth + service clients used by the other examples.
"""

from os import environ

from dotenv import load_dotenv

from msdev_kit import Auth
from msdev_kit.fabric import (
    Admin,
    Capacity,
    Database,
    Dataflow,
    Dataset,
    KQLDatabase,
    Notebook,
    Operations,
    Pipeline,
    Report,
    Workspace,
)
from msdev_kit.graph import GraphClient
from msdev_kit.sharepoint import SharePointClient


_REQUIRED_VARS = ["TENANT_ID", "CLIENT_ID", "CLIENT_SECRET"]
if not all(environ.get(v) for v in _REQUIRED_VARS):
    load_dotenv("./utils/.env")

TENANT_ID = environ.get("TENANT_ID", "")
CLIENT_ID = environ.get("CLIENT_ID", "")
CLIENT_SECRET = environ.get("CLIENT_SECRET", "")
FABRIC_SQL_ENDPOINT = environ.get("FABRIC_SQL_ENDPOINT", "")
FABRIC_DATABASE = environ.get("FABRIC_DATABASE", "")
SP_HOSTNAME = environ.get("SP_HOSTNAME", "")
SP_SITE_PATH = environ.get("SP_SITE_PATH", "")


def build_clients():
    auth = Auth(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    pbi_token = auth.get_token("pbi")
    fabric_token = auth.get_token("fabric")

    return {
        "auth": auth,
        "workspace": Workspace(pbi_token),
        "dataset": Dataset(pbi_token),
        "report": Report(pbi_token),
        "dataflow": Dataflow(pbi_token),
        "capacity": Capacity(pbi_token),
        "admin": Admin(pbi_token),
        "operations": Operations(fabric_token),
        "pipeline": Pipeline(fabric_token),
        "notebook": Notebook(fabric_token),
        "kql": KQLDatabase,
        "db": Database(FABRIC_SQL_ENDPOINT, FABRIC_DATABASE, CLIENT_ID, CLIENT_SECRET),
        "graph": GraphClient(auth),
        "sharepoint": SharePointClient(auth, SP_HOSTNAME, SP_SITE_PATH),
    }
