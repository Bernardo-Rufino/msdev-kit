# msdev-kit

Microsoft developer toolkit for Python: Fabric/Power BI, MS Graph (Entra), and SharePoint.

[![PyPI version](https://img.shields.io/pypi/v/msdev-kit)](https://pypi.org/project/msdev-kit/)
[![Python](https://img.shields.io/pypi/pyversions/msdev-kit)](https://pypi.org/project/msdev-kit/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## Installation

```shell
pip install msdev-kit
```

Or install from GitHub:

```shell
pip install git+https://github.com/Bernardo-Rufino/msdev-kit.git
```

For local development:

```shell
git clone https://github.com/Bernardo-Rufino/msdev-kit.git
cd msdev-kit
pip install -e .
```

**Requirements:** Python >= 3.10, an Azure app registration with a client ID and client secret.

---

## Quick Start

```python
from msdev_kit import Auth
from msdev_kit.fabric import Workspace
from msdev_kit.graph import GraphClient
from msdev_kit.sharepoint import SharePointClient

# authenticate
auth = Auth(tenant_id="...", client_id="...", client_secret="...")

# fabric: list workspaces
ws = Workspace(auth.get_token('fabric'))
workspaces = ws.list_workspaces_for_user()

# graph: look up a user
graph = GraphClient(auth)
user_id = graph.get_user_id('user@company.com')

# sharepoint: download a file
sp = SharePointClient(auth, sp_hostname='company', sp_site_path='sites/DataTeam')
sp.download_file('/Reports/monthly.xlsx', local_dir='./downloads')
```

---

## Authentication

All classes use a shared `Auth` object. You can use different service principals for different services — instantiate one `Auth` per SPN:

```python
from msdev_kit import Auth

# service principal auth
fabric_auth = Auth(tenant_id="...", client_id="spn-a", client_secret="...")
graph_auth  = Auth(tenant_id="...", client_id="spn-b", client_secret="...")
```

### Supported scopes

| Service | Scope | Usage |
|---|---|---|
| `pbi` (default) | Power BI API | `auth.get_token()` or `auth.get_token('pbi')` |
| `fabric` | Fabric API | `auth.get_token('fabric')` |
| `graph` | MS Graph API | `auth.get_token('graph')` |
| `azure` | Azure Management API | `auth.get_token('azure')` |

### Interactive user auth

For scenarios requiring user context (e.g., RLS-enabled datasets):

```python
token = auth.get_token_for_user('pbi')     # opens browser for login
token = auth.get_token_for_user('fabric')
```

### Credentials

Set up credentials via environment variables or a `.env` file:

```shell
TENANT_ID='<YOUR_TENANT_ID>'
CLIENT_ID='<YOUR_CLIENT_ID>'
CLIENT_SECRET='<YOUR_CLIENT_SECRET>'
```

---

## Fabric & Power BI

```python
from msdev_kit import Auth
from msdev_kit.fabric import Workspace, Dataset, Report, Dataflow, Pipeline

auth = Auth(tenant_id, client_id, client_secret)
```

Fabric classes take a token string — call `auth.get_token('fabric')` or `auth.get_token('pbi')` depending on the API.

- [Workspace](#workspace) — workspaces, users, permissions
- [Dataset](#dataset) — semantic models, DAX queries, permissions
- [Report](#report) — metadata, definitions, visuals, measures
- [Dataflow](#dataflow) — Gen1, Gen2, Gen2 CI/CD management
- [Pipeline](#pipeline) — Data Pipeline management
- [Other modules](#other-modules) — Capacity, Admin, KQL, Notebook, Database

### Workspace

Manage Power BI workspaces, users, and permissions.

```python
ws = Workspace(auth.get_token('pbi'))
workspaces = ws.list_workspaces_for_user()
ws.add_user('user@company.com', workspace_id, 'Member', 'User')
```

| Method | Description |
|---|---|
| `list_workspaces_for_user(...)` | List all workspaces the user has access to, with optional filters. |
| `get_workspace_details(workspace_id)` | Get details for a specific workspace. |
| `list_users(workspace_id)` | List all users in a workspace. |
| `list_reports(workspace_id)` | List all reports in a workspace. |
| `add_user(user_principal_name, workspace_id, access_right, user_type)` | Add a user or service principal to a workspace. |
| `update_user(user_principal_name, workspace_id, access_right)` | Update a user's role on a workspace. |
| `remove_user(user_principal_name, workspace_id)` | Remove a user from a workspace. |
| `batch_update_user(user, workspaces_list)` | Batch update a user across multiple workspaces. |

### Dataset

Manage datasets (semantic models), permissions, and execute DAX queries.

```python
ds = Dataset(auth.get_token('pbi'))
result = ds.execute_query(workspace_id, dataset_id, "EVALUATE Sales")
```

| Method | Description |
|---|---|
| `list_datasets(workspace_id)` | List all datasets in a workspace. |
| `get_dataset_details(workspace_id, dataset_id)` | Get details of a specific dataset. |
| `get_dataset_name(workspace_id, dataset_id)` | Resolve the display name of a dataset. Tries PBI API first, falls back to Fabric semantic models API. |
| `execute_query(workspace_id, dataset_id, query)` | Execute a DAX query. Runs a COUNTROWS pre-check to detect truncation. |
| `list_users(workspace_id, dataset_id)` | List users with access to a dataset. |
| `add_user(user_principal_name, workspace_id, dataset_id, access_right)` | Grant a user access to a dataset. |
| `update_user(user_principal_name, workspace_id, dataset_id, access_right)` | Update a user's access to a dataset. |
| `remove_user(user_principal_name, workspace_id, dataset_id)` | Remove a user's access to a dataset. |
| `list_dataset_related_reports(workspace_id, dataset_id)` | List all reports linked to a dataset. |
| `export_dataset_related_reports(workspace_id, dataset_id)` | Export all reports linked to a dataset as `.pbix` files. |

### Report

Retrieve report metadata, definitions, visuals, and report-level measures.

```python
rpt = Report(auth.get_token('pbi'))
pages = rpt.list_report_pages(workspace_id, report_id)
```

| Method | Description |
|---|---|
| `list_reports(workspace_id)` | List all reports in a workspace. |
| `get_report_metadata(workspace_id, report_id)` | Get metadata for a specific report. |
| `get_report_name(workspace_id, report_id)` | Get a report's display name. |
| `list_report_pages(workspace_id, report_id)` | List all pages in a report. |
| `get_report_json_pages_and_visuals(json_data, workspace_id, report_id)` | Parse a PBIR-Legacy report JSON and extract pages/visuals into a DataFrame. |
| `get_legacy_report_json(workspace_id, report_id, operations)` | Get and decode the full report definition for PBIR-Legacy reports. |
| `export_report(workspace_id, report_id, ...)` | Export a report as a `.pbix` file. |
| `get_report_measures(workspace_id, report_id, operations)` | Extract report-level measures and generate a DAX Query View script. |
| `rebind_report(workspace_id, report_id, new_dataset_id, ...)` | Rebind a report to a new dataset and migrate Read access. |

### Dataflow

Manage Power BI and Fabric dataflows, including Gen1, Gen2, and Gen2 CI/CD.

```python
df = Dataflow(auth.get_token('fabric'))

# upgrade Gen1 to Gen2 CI/CD
result = df.upgrade_to_gen2_cicd(
    workspace_id='<workspace_id>',
    dataflow_id='<gen1_dataflow_id>',
    display_name='my_dataflow_cicd',
    source_type='gen1'
)
```

| Method | Description |
|---|---|
| `list_dataflows(workspace_id)` | List all dataflows (Gen1, Gen2, Gen2 CI/CD), merged and deduplicated. |
| `get_dataflow_details(workspace_id, dataflow_id)` | Get details of a specific dataflow. |
| `get_dataflow_name(workspace_id, dataflow_id)` | Resolve the display name of a dataflow. |
| `create_dataflow(workspace_id, dataflow_content)` | Create a new Power BI dataflow. |
| `delete_dataflow(workspace_id, dataflow_id, type='pbi')` | Delete a dataflow. Use `type='fabric'` for Fabric API. |
| `export_dataflow_json(workspace_id, dataflow_id, dataflow_name)` | Export a dataflow definition as JSON. |
| `get_dataflow_gen2_definition(workspace_id, dataflow_id)` | Get the definition of a Dataflow Gen2 CI/CD item. |
| `create_dataflow_gen2_from_definition(workspace_id, display_name, definition)` | Create a Dataflow Gen2 CI/CD from a definition. |
| `update_dataflow_gen2_from_definition(workspace_id, dataflow_id, display_name, definition)` | Update an existing Dataflow Gen2 CI/CD definition. |
| `get_data_destinations(workspace_id, dataflow_id)` | Get data destination details for each table in a dataflow. |
| `change_data_destination(workspace_id, dataflow_id, destination_type, ...)` | Change data destination (Lakehouse/Warehouse). Modes: `preview`, `replace`, `create`. |
| `create_dataflow_with_new_destination(workspace_id, dataflow_id, ...)` | Create a new Gen2 CI/CD dataflow with a different data destination. |
| `upgrade_to_gen2_cicd(...)` | Upgrade a Gen1 or Gen2 (standard) dataflow to Gen2 CI/CD. |

### Pipeline

Manage Fabric Data Pipelines.

```python
pipe = Pipeline(auth.get_token('fabric'))
activities = pipe.get_pipeline_activities(workspace_id, 'My Pipeline')
```

| Method | Description |
|---|---|
| `list_pipelines(workspace_id)` | List all Fabric Data Pipelines in a workspace. |
| `get_pipeline(workspace_id, pipeline_id)` | Get the metadata of a specific pipeline. |
| `get_pipeline_definition(workspace_id, pipeline_id)` | Get the full definition of a pipeline. |
| `update_pipeline_definition(workspace_id, pipeline_id, definition)` | Update an existing pipeline definition. |
| `get_pipeline_activities(workspace_id, pipeline_id_or_name)` | Get activities from a pipeline. Accepts ID or display name. |
| `find_pipelines_by_dataflow(workspace_id, dataflow_id_or_name)` | Find pipelines that reference a specific dataflow. |
| `replace_dataflow_id_in_pipeline(workspace_id, pipeline_id, old_id, new_id)` | Replace a dataflow ID in all RefreshDataflow activities. |

<details>
<summary><strong>Example: replacing a dataflow destination and updating pipelines</strong></summary>

When `change_data_destination(mode='replace')` is used on a standard Gen2 dataflow, the original is deleted and a new CI/CD dataflow is created with a new ID. Pipelines referencing the old ID must be updated:

```python
from msdev_kit import Auth
from msdev_kit.fabric import Dataflow, Pipeline

auth = Auth(tenant_id, client_id, client_secret)
dataflow = Dataflow(auth.get_token('pbi'))
pipeline = Pipeline(auth.get_token('fabric'))

workspace_id = '<workspace_id>'
old_dataflow_id = '<dataflow_id>'

# 1. replace dataflow destination (creates new CI/CD, deletes original)
result = dataflow.change_data_destination(
    workspace_id=workspace_id,
    dataflow_id=old_dataflow_id,
    destination_type='Warehouse',
    destination_workspace_id=workspace_id,
    destination_item_id='<warehouse_id>',
    mode='replace'
)
new_dataflow_id = result['content']['id']

# 2. find pipelines referencing the old dataflow ID
matches = pipeline.find_pipelines_by_dataflow(workspace_id, old_dataflow_id)

# 3. update each pipeline to use the new ID
for m in matches['content']:
    pipeline.replace_dataflow_id_in_pipeline(
        workspace_id, m['pipeline_id'], old_dataflow_id, new_dataflow_id
    )
```

</details>

### Other modules

| Module | Class | Description |
|---|---|---|
| Capacity | `Capacity` | Monitor and manage Power BI and Fabric capacities. |
| Operations | `Operations` | Track long-running Fabric API operations. |
| Admin | `Admin` | Power BI Admin API operations. |
| KQL | `KQLDatabase` | Query Kusto (KQL) databases in Microsoft Fabric. |
| Notebook | `Notebook` | Manage Fabric notebooks (list, get metadata). |
| Database | `Database` | Query and write to SQL databases (Lakehouse, Warehouse) via ODBC. |

---

## MS Graph (Entra)

Manage Entra ID (Azure AD) users and groups via the MS Graph API.

```python
from msdev_kit import Auth
from msdev_kit.graph import GraphClient

auth = Auth(tenant_id, client_id, client_secret)
graph = GraphClient(auth)

# look up a user and add them to a group
user_id = graph.get_user_id('user@company.com')
group_id = graph.get_group_id('Data Team')
graph.add_group_member(group_id, user_id)

# list all members of a group
members = graph.list_group_members(group_id)
```

| Method | Description |
|---|---|
| `get_user_id(email)` | Resolve user object ID by UPN/email, with mail fallback. |
| `get_group_id(group_name)` | Resolve Entra group object ID by display name. |
| `list_group_members(group_id)` | Paginated member list (id, displayName, mail, UPN). |
| `add_group_member(group_id, user_id)` | Add user to group. Silently ignores already-member errors. |
| `remove_group_member(group_id, user_id)` | Remove user from group. Silently ignores 404/403. |

---

## SharePoint

Manage SharePoint files and folders via MS Graph API (no ACS/Office365 dependency).

```python
from msdev_kit import Auth
from msdev_kit.sharepoint import SharePointClient

auth = Auth(tenant_id, client_id, client_secret)
sp = SharePointClient(auth, sp_hostname='company', sp_site_path='sites/DataTeam')

# download a file
sp.download_file('/Reports/monthly.xlsx', local_dir='./downloads')

# upload a file (from path or bytes)
sp.upload_file('/Reports/updated.xlsx', source='./local/updated.xlsx')
sp.upload_file('/Reports/data.csv', source=csv_bytes, content_type='text/csv')

# create nested folders
sp.create_folder('/Reports/2026/Q1')
```

| Method | Description |
|---|---|
| `download_file(file_path, local_dir)` | Download a file from the default document library. Returns local file path. |
| `upload_file(remote_path, source, content_type?)` | Upload/overwrite a file. `source` is a local file path (str) or raw bytes. |
| `create_folder(folder_path)` | Create a folder and all intermediate folders. |

Hostname and site path inputs are normalized automatically:

| Input | Normalized to |
|---|---|
| `company` | `company.sharepoint.com` |
| `company.sharepoint.com` | `company.sharepoint.com` |
| `https://company.sharepoint.com` | `company.sharepoint.com` |
| `DataTeam` | `sites/DataTeam` |
| `sites/DataTeam` | `sites/DataTeam` |
| `/sites/DataTeam` | `sites/DataTeam` |

---

## Limitations

- The Power BI REST API has a **200 requests per hour** rate limit.
- Not all users can be updated via the API. See Microsoft docs: [Dataset permissions](https://learn.microsoft.com/en-us/power-bi/developer/embedded/datasets-permissions#get-and-update-dataset-permissions-with-apis).
- **Dataset query limits** (executeQueries API):
  - Max **100,000 rows** or **1,000,000 values** (rows x columns) per query, whichever is hit first.
  - Max **15 MB** of data per query.
  - **120 query requests per minute** per user.
  - Only **DAX** queries are supported (no MDX, INFO functions, or DMV).
  - Datasets hosted in Azure Analysis Services or with a live connection to on-premises AAS are not supported.
  - Service Principals are not supported for datasets with RLS or SSO enabled.
