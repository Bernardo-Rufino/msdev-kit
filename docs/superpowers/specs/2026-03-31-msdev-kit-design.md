# msdev-kit — Design Spec

**Date:** 2026-03-31
**Status:** Approved

## Overview

Rename and restructure `fabric-api` into `msdev-kit`: a broader Microsoft developer toolkit covering Fabric/Power BI, MS Graph (Entra), and SharePoint. The package grows from a single flat module into a sub-package architecture, with a shared `Auth` class as the credential foundation.

---

## Package identity

| Field | Old | New |
|---|---|---|
| Distribution name | `fabric-api` | `msdev-kit` |
| Python package | `fabric_api` | `msdev_kit` |
| Version | `1.1.0` | `0.1.0` |
| Description | Power BI and Fabric REST API wrapper | Microsoft developer toolkit: Fabric, Graph, SharePoint |

---

## Package structure

```
msdev_kit/
├── __init__.py          ← exports Auth only
├── auth.py              ← shared Auth class
├── fabric/
│   ├── __init__.py      ← exports all Fabric/PBI classes
│   ├── workspace.py
│   ├── dataset.py
│   ├── report.py
│   ├── dataflow.py
│   ├── capacity.py
│   ├── admin.py
│   ├── operations.py
│   ├── kql.py
│   ├── database.py
│   ├── pipeline.py
│   └── notebook.py
├── graph/
│   ├── __init__.py      ← exports GraphClient
│   └── client.py
└── sharepoint/
    ├── __init__.py      ← exports SharePointClient
    └── client.py

tests/
├── fabric/              ← existing tests moved here
├── graph/
└── sharepoint/
```

---

## Auth

`Auth` is the single credential object. Users instantiate it once per SPN and pass it into any class. Different sub-packages can use different `Auth` instances (different SPNs) without duplicating credential logic.

**Token caching:** `Auth` maintains a scope-keyed dict `dict[scope_url, (token, expiry)]`. A cached token is returned if it has more than 60 seconds remaining. This replaces the ad-hoc caching previously inside `GraphClient`.

**Supported scopes** (via `get_token(service)`):

| `service` | Scope URL |
|---|---|
| `pbi` | `https://analysis.windows.net/powerbi/api/.default` |
| `fabric` | `https://api.fabric.microsoft.com/.default` |
| `azure` | `https://management.azure.com/.default` |
| `graph` | `https://graph.microsoft.com/.default` |

Interactive auth (`get_token_for_user`) remains unchanged.

**Usage:**

```python
from msdev_kit import Auth

fabric_auth = Auth(tenant_id="...", client_id="spn-a", client_secret="...")
graph_auth  = Auth(tenant_id="...", client_id="spn-b", client_secret="...")
```

---

## `msdev_kit.fabric`

All existing Fabric/PBI classes move here with no logic changes — only internal imports update. Each class continues to accept an `Auth` instance as its first argument.

```python
from msdev_kit.fabric import Workspace, Report, Dataset, Pipeline  # etc.

ws = Workspace(auth=fabric_auth)
```

Classes: `Workspace`, `Dataset`, `Report`, `Dataflow`, `Capacity`, `Admin`, `Operations`, `KQLDatabase`, `Database`, `Pipeline`, `Notebook`.

---

## `msdev_kit.graph`

`GraphClient` wraps the MS Graph REST API (`https://graph.microsoft.com/v1.0`). It takes `Auth` and calls `auth.get_token("graph")` for every request — no credentials stored on the client itself.

**Methods (v0.1.0):**

| Method | Description |
|---|---|
| `get_user_id(email)` | Resolve user object ID by UPN/email, with mail fallback |
| `get_group_id(group_name)` | Resolve Entra group object ID by display name |
| `list_group_members(group_id)` | Paginated member list (id, displayName, mail, UPN) |
| `add_group_member(group_id, user_id)` | Add user; silently ignores already-member errors |
| `remove_group_member(group_id, user_id)` | Remove user; silently ignores 404/403 |

```python
from msdev_kit.graph import GraphClient

graph = GraphClient(auth=graph_auth)
user_id = graph.get_user_id("user@company.com")
```

---

## `msdev_kit.sharepoint`

`SharePointClient` handles SharePoint file and folder operations via MS Graph (no `Office365-REST-Python-Client`). It takes `Auth` directly — `GraphClient` is not a constructor dependency. Internally it calls `auth.get_token("graph")` for HTTP requests.

Hostname and site path inputs are normalized (accept short names, full FQDNs, with or without `https://`, with or without `sites/` prefix).

**Adaptations from the reference implementation:**

| Before (reference) | After (msdev-kit) |
|---|---|
| `__init__(self, graph: GraphClient, ...)` | `__init__(self, auth: Auth, ...)` |
| `self._graph._GRAPH_BASE` | class constant `_GRAPH_BASE = 'https://graph.microsoft.com/v1.0'` |
| `self._graph._headers()` | `{'Authorization': f'Bearer {self._auth.get_token("graph")}', 'Content-Type': 'application/json'}` built inline |
| `self._graph._get_token()` | `self._auth.get_token("graph")` |
| `self._graph._GRAPH_BASE` in `_get_site_id` | `self._GRAPH_BASE` |

`upload_file` accepts `source: str | bytes` — a local file path (string) or raw bytes — unchanged from the reference.

**Methods (v0.1.0):**

| Method | Signature | Description |
|---|---|---|
| `download_file` | `(file_path, local_dir) -> str` | Download file from default document library |
| `upload_file` | `(remote_path, source, content_type?)` | Upload/overwrite a file; source is path or bytes |
| `create_folder` | `(folder_path)` | Create folder and all intermediate folders |

```python
from msdev_kit.sharepoint import SharePointClient

sp = SharePointClient(auth=graph_auth, sp_hostname="company", sp_site_path="sites/DataTeam")
sp.download_file("/Reports/monthly.xlsx", local_dir="./downloads")
sp.upload_file("/Reports/updated.xlsx", source="./local/updated.xlsx")
```

---

## Dependencies

| Package | Change |
|---|---|
| `Office365-REST-Python-Client` | **Removed** (ACS deprecated) |
| `requests>=2.33.0` | **Added** (was transitive, now explicit — used by Graph and SharePoint) |
| All others | Unchanged |

---

## Breaking changes

Import paths change. This is a new package identity (`msdev-kit` replaces `fabric-api`), so users install the new package and update imports. No backwards-compatibility shim.

```python
# Before
from fabric_api import Workspace

# After
from msdev_kit.fabric import Workspace
```

---

## Out of scope (v0.1.0)

- SharePoint list read/write operations (can be added in a follow-up)
- Large file upload (chunked/resumable upload via Graph upload sessions)
- Additional Graph endpoints beyond groups and users
