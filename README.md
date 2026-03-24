# Power BI & Fabric API Wrapper

Python wrapper for the Power BI REST API and Microsoft Fabric API, designed to automate workspace management, dataset operations, report handling, dataflows, and more.

## Installation

### Install from PyPI (recommended)

```shell
pip install fabric-api
```

To install a specific version:

```shell
pip install fabric-api==1.0.3
```

### Install from GitHub

```shell
pip install git+https://github.com/Bernardo-Rufino/fabric-api.git
```

To install a specific version/tag:

```shell
pip install git+https://github.com/Bernardo-Rufino/fabric-api.git@v1.0.0
```

### Install for local development

```shell
git clone https://github.com/Bernardo-Rufino/fabric-api.git
cd powerbi-api
pip install -e .
```

---

## Prerequisites

- Python >= 3.10
- An Azure app registration with a client ID and client secret

## Setting Up

1. **Install the package** (see above)

2. **Set up credentials** — choose one of the two options:

    **Option A – `.env` file** (local development): create a `utils/.env` file with your credentials. If you cloned the repo, you can copy the example:

    ```shell
    cp utils/.env.example utils/.env
    ```

    Otherwise, create `utils/.env` manually:

    Then edit `utils/.env`:

    ```shell
    TENANT_ID='<YOUR_TENANT_ID>'
    CLIENT_ID='<YOUR_CLIENT_ID>'
    CLIENT_SECRET='<YOUR_CLIENT_SECRET>'
    CLIENT_ID_SHAREPOINT='<YOUR_SHAREPOINT_CLIENT_ID>'
    CLIENT_SECRET_SHAREPOINT='<YOUR_SHAREPOINT_CLIENT_SECRET>'
    AZURE_SUBSCRIPTION_ID='<YOUR_AZURE_SUBSCRIPTION_ID>'
    AZURE_RESOURCE_GROUP_ID='<YOUR_AZURE_RESOURCE_GROUP_ID>'
    FABRIC_SQL_ENDPOINT='<YOUR_FABRIC_SQL_ENDPOINT>'
    FABRIC_DATABASE='<YOUR_FABRIC_DATABASE>'
    ```

    **Option B – environment variables** (CI/CD, Docker, etc.): if the variables are already set in the environment, the `.env` file is skipped automatically — no extra setup needed.

3. **Authenticate**:

    ```python
    from fabric_api import Auth

    auth = Auth(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    token = auth.get_token()            # Power BI API
    fabric_token = auth.get_token('fabric')  # Fabric API
    ```

## Modules

### Auth

Authentication via Azure service principal or interactive user login.

| Method | Description |
|---|---|
| `get_token(service='pbi')` | Get a bearer token for Power BI (`pbi`) or Fabric (`fabric`) APIs using a service principal. |
| `get_token_for_user(service='pbi')` | Get a bearer token via interactive user login. Supports `pbi`, `fabric`, and `azure` services. |

### Workspace

Manage Power BI workspaces, users, and permissions.

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

Manage datasets, permissions, and execute DAX queries.

| Method | Description |
|---|---|
| `list_datasets(workspace_id)` | List all datasets in a workspace. |
| `get_dataset_details(workspace_id, dataset_id)` | Get details of a specific dataset. |
| `execute_query(workspace_id, dataset_id, query)` | Execute a DAX query against a dataset. Runs a COUNTROWS pre-check to detect if API row/value limits would truncate the result and returns truncation metadata. |
| `list_users(workspace_id, dataset_id)` | List users with access to a dataset. |
| `add_user(user_principal_name, workspace_id, dataset_id, access_right)` | Grant a user access to a dataset. |
| `update_user(user_principal_name, workspace_id, dataset_id, access_right)` | Update a user's access to a dataset. |
| `remove_user(user_principal_name, workspace_id, dataset_id)` | Remove a user's access to a dataset. |
| `list_dataset_related_reports(workspace_id, dataset_id)` | List all reports linked to a dataset. |
| `export_dataset_related_reports(workspace_id, dataset_id)` | Export all reports linked to a dataset as `.pbix` files. |

### Report

Retrieve report metadata, definitions, visuals, and report-level measures.

| Method | Description |
|---|---|
| `list_reports(workspace_id)` | List all reports in a workspace. |
| `get_report_metadata(workspace_id, report_id)` | Get metadata for a specific report. |
| `get_report_name(workspace_id, report_id)` | Get a report's display name. |
| `list_report_pages(workspace_id, report_id)` | List all pages in a report. |
| `get_report_json_pages_and_visuals(json_data, workspace_id, report_id)` | Parse a PBIR-Legacy report JSON and extract pages and visual details into a DataFrame. |
| `get_legacy_report_json(workspace_id, report_id, operations)` | Get and decode the full report definition for PBIR-Legacy reports. |
| `export_report(workspace_id, report_id, ...)` | Export a report as a `.pbix` file. |
| `get_report_measures(workspace_id, report_id, operations)` | Extract report-level measures and generate a DAX Query View script. Supports both PBIR and PBIR-Legacy formats. |
| `rebind_report(workspace_id, report_id, new_dataset_id, new_dataset_workspace_id, admin, dataset)` | Rebind a report to a new dataset/semantic model and migrate Read access to the new dataset. |

### Dataflow

Manage Power BI and Fabric dataflows, including Gen1, Gen2, and Gen2 CI/CD.

| Method | Description |
|---|---|
| `list_dataflows(workspace_id)` | List all dataflows in a workspace (Gen1, Gen2 standard, and Gen2 CI/CD). Results are merged and deduplicated with a `source` column. |
| `get_dataflow_details(workspace_id, dataflow_id)` | Get details of a specific dataflow. |
| `create_dataflow(workspace_id, dataflow_content)` | Create a new Power BI dataflow. |
| `delete_dataflow(workspace_id, dataflow_id, type='pbi')` | Delete a dataflow. Use `type='fabric'` for Fabric API. |
| `export_dataflow_json(workspace_id, dataflow_id, dataflow_name)` | Export a dataflow definition as JSON. |
| `get_dataflow_gen2_definition(workspace_id, dataflow_id)` | Get the definition of a Dataflow Gen2 CI/CD item. |
| `create_dataflow_gen2_from_definition(workspace_id, display_name, definition)` | Create a Dataflow Gen2 CI/CD from a definition. |
| `update_dataflow_gen2_from_definition(workspace_id, dataflow_id, display_name, definition)` | Update an existing Dataflow Gen2 CI/CD definition. |
| `change_data_destination(workspace_id, dataflow_id, destination_type, destination_workspace_id, destination_item_id, mode='preview', ...)` | Fetch the dataflow definition and change its data destination (Lakehouse ↔ Warehouse). `mode='preview'` returns the modified definition without saving, `mode='replace'` updates in-place (CI/CD) or deletes+recreates (standard), `mode='create'` creates a new dataflow with `_cicd` suffix keeping the original. |
| `create_dataflow_with_new_destination(workspace_id, dataflow_id, destination_type, destination_workspace_id, destination_item_id, ...)` | Create a new Gen2 CI/CD dataflow from an existing one with a different data destination. Supports custom display name and target workspace. |
| `upgrade_to_gen2_cicd(...)` | Upgrade a Gen1 or Gen2 (standard) dataflow to Gen2 CI/CD. See details below. |

#### `upgrade_to_gen2_cicd`

Converts a Gen1 or Gen2 (standard) dataflow into a Gen2 CI/CD (native Fabric) artifact.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `workspace_id` | `str` | *required* | The workspace ID where the source dataflow resides. |
| `dataflow_id` | `str` | *required* | The ID of the source dataflow to upgrade. |
| `display_name` | `str` | `''` | Display name for the new artifact. If empty, Gen1 auto-generates a name (e.g. `original_copy1`); Gen2 appends `_cicd` to the original name. |
| `description` | `str` | `''` | Description for the new artifact (Gen1 only). If empty, copies from the source. |
| `destination_workspace_id` | `str` | `''` | Target workspace for the new artifact. If empty, creates in the same workspace as the source. |
| `include_schedule` | `bool` | `False` | Whether to migrate the refresh schedule from the source (Gen1 only). The schedule is copied in a disabled state. |
| `compute_engine_settings` | `Dict` | `None` | Compute engine settings for the new CI/CD dataflow (Gen2 only). See supported keys below. If not provided, `allowFastCopy` is derived from the source dataflow. |
| `source_type` | `str` | `'gen1'` | Type of source dataflow: `'gen1'` or `'gen2'`. |

**`compute_engine_settings` keys (Gen2 only):**

| Key | Type | Description |
|---|---|---|
| `allowFastCopy` | `bool` | Enable/disable fast copy (staging). Auto-derived from source if not provided. |
| `allowPartitionedCompute` | `bool` | Enable/disable partitioned compute. Defaults to `false` in Fabric. |
| `allowModernEvaluationEngine` | `bool` | Enable/disable query evaluation (modern evaluation engine). Defaults to `false` in Fabric. |

> **Note:** The "Allow combining data from multiple sources" privacy setting (`pbi:mashup.fastCombine`) is not part of the CI/CD definition format. It must be configured manually via the Fabric portal after creation.

**Gen1 → Gen2 CI/CD:**
Uses the Power BI [`saveAsNativeArtifact`](https://learn.microsoft.com/en-us/rest/api/power-bi/dataflows/save-dataflow-gen-one-as-dataflow-gen-two) API (preview). This handles connection format updates, sensitivity labels, and optionally migrates refresh schedules. Non-fatal warnings (e.g. `FailedToCopySchedule`, `ConnectionsUpdateFailed`) are returned in the `warnings` field without failing the operation.

**Gen2 (standard) → Gen2 CI/CD:**
Fetches the dataflow definition via the PBI API and converts it to the CI/CD format (`mashup.pq`, `queryMetadata.json`, `.platform`). The conversion transforms the M document (removes internal pipeline queries, adds `[StagingDefinition]` and `[DataDestinations]` annotations, simplifies `DataDestination` queries), builds the query metadata from `pbi:mashup` fields, and creates the artifact via the Fabric API.

If the dataflow is already Gen2 CI/CD, it re-creates a copy with the given display name.

**Example:**

```python
from fabric_api import Auth, Dataflow

auth = Auth(TENANT_ID, CLIENT_ID, CLIENT_SECRET)

# Gen1 → Gen2 CI/CD
df = Dataflow(auth.get_token('fabric'))
result = df.upgrade_to_gen2_cicd(
    workspace_id='<workspace_id>',
    dataflow_id='<gen1_dataflow_id>',
    display_name='my_dataflow_cicd',
    include_schedule=True,
    source_type='gen1'
)

# Gen2 (standard) → Gen2 CI/CD with compute settings
result = df.upgrade_to_gen2_cicd(
    workspace_id='<source_workspace_id>',
    dataflow_id='<gen2_dataflow_id>',
    destination_workspace_id='<target_workspace_id>',
    source_type='gen2',
    compute_engine_settings={
        'allowFastCopy': False,
        'allowPartitionedCompute': True,
        'allowModernEvaluationEngine': True
    }
)
```

### Capacity

Monitor and manage Power BI and Fabric capacities.

| Method | Description |
|---|---|
| `list_powerbi_capacities()` | List all Power BI capacities the user has access to. |
| `list_fabric_capacities(azure_subscription_id, azure_resource_group)` | List Fabric capacities for a given Azure subscription. |
| `assign_workspace_to_capacity(workspace_id, capacity_id)` | Assign a workspace to a capacity. |

### Operations

Track long-running Fabric API operations.

| Method | Description |
|---|---|
| `get_operation_state(operation_id)` | Get the current state of a long-running operation. |
| `get_operation_result(operation_id)` | Get the result of a completed operation. |

### Admin

Power BI Admin API operations.

| Method | Description |
|---|---|
| `get_report_users_as_admin(report_id)` | List users with access to a report (admin endpoint). |

### KQLDatabase

Query Kusto (KQL) databases in Microsoft Fabric.

| Method | Description |
|---|---|
| `query_kql_database(kql_query, sort_by)` | Execute a KQL query and return results as a DataFrame. |

### Pipeline

Manage Fabric Data Pipelines.

| Method | Description |
|---|---|
| `get_pipeline_definition(workspace_id, pipeline_id)` | Get the full definition of a Fabric Data Pipeline. |
| `get_pipeline_activities(workspace_id, pipeline_id)` | Get the list of activities from a pipeline with name, type, and typeProperties. |

### Database

Query and write to SQL databases (Lakehouse, Warehouse) via ODBC.

| Method | Description |
|---|---|
| `execute_query(query)` | Execute a SQL query against a fabric database (lakehouse, warehouse or sql database). |
| `write_dataframe(df, table_name, schema='dbo', if_exists='append', chunksize=10000)` | Write a pandas DataFrame to a SQL table. `if_exists` supports `'fail'`, `'replace'`, `'append'`, `'delete_rows'`. |

## Limitations

- The Power BI REST API has a **200 requests per hour** rate limit.
- Not all users can be updated via the API. See Microsoft docs: [Dataset permissions](https://learn.microsoft.com/en-us/power-bi/developer/embedded/datasets-permissions#get-and-update-dataset-permissions-with-apis).
- **Dataset query limits** (executeQueries API):
  - Max **100,000 rows** or **1,000,000 values** (rows × columns) per query, whichever is hit first.
  - Max **15 MB** of data per query.
  - **120 query requests per minute** per user.
  - Only **DAX** queries are supported (no MDX, INFO functions, or DMV).
  - Datasets hosted in Azure Analysis Services or with a live connection to on-premises AAS are not supported.
  - Service Principals are not supported for datasets with RLS or SSO enabled.
