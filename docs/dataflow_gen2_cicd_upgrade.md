# Upgrade a standard Dataflow Gen2 to CI/CD

This workflow creates a **new** Dataflow Gen2 CI/CD item. It does not delete the
source. The new item keeps the source's configured Warehouse or Lakehouse data
destination, including the workspace, item, table, and write method. Its first
refresh can write to that same destination, so review the target before running.

## Why two identities are needed

The service principal (SPN) creates the item and lists the Fabric connections it
can access. With `use_accessible_connections=True`, the upgrade matches literal
connector paths in the Power Query document to shared connections, then stores
their IDs in the new definition. It does **not** copy credentials or secrets.
Matching only the source dataflow's exported connection IDs can retain personal
connections that the SPN cannot use.

The SPN needs two audience-specific access tokens. The Fabric token initializes
`Dataflow` for connection listing and item creation. Pass its Power BI token as
`pbi_access_token` to read the standard source definition and bound data sources.
Do not use the delegated user token for either of those steps.

Fabric currently rejects a Dataflow Gen2 CI/CD refresh initiated by an SPN with
`SPNBasedRefreshNotAllowed`. Pass a **delegated Fabric user token** through
`refresh_access_token` when `refresh=True`. The token is used only to submit and
poll the refresh job. The user must have Member or higher workspace access and
access to every connection used by the dataflow. Connection visibility or an
`Online` connection test alone does not prove that refresh will succeed.
The interactive login is requested in the configured `TENANT_ID`, which also
matters when the user is a guest whose home tenant differs from the workspace
tenant.

## Prerequisites

1. Use a standard Gen2 source, not an existing CI/CD dataflow. Verify its output
   destination and the effect of its write method before creating a copy.
2. Give the SPN at least Contributor access to the workspace and access to the
   shared Fabric connections for the source and destination. The connection
   type and path must match the Power Query connector exactly. Personal
   connections are excluded. Missing or multiple shared matches stop creation.
3. Sign in as a user with Member or higher workspace access and access to all
   those connections. Interactive login must be possible on the machine running
   the script. Do not substitute an SPN token for this user token.
4. Set `TENANT_ID`, `CLIENT_ID`, and `CLIENT_SECRET` in `.env`, as described in
   [the examples guide](../examples/README.md). Keep `.env` out of Git.

The exact-match mode currently handles literal `Sql.Database(server, database)`,
`Fabric.Warehouse`, and `Lakehouse.Contents` calls. Dynamic SQL arguments and
other connection kinds fail closed rather than guess a connection. The original
source-ID behavior remains the default when `use_accessible_connections=False`.

## Run the example

From the repository root, review the source dataflow and destination first:

```shell
python -m examples.dataflow_gen2_cicd_upgrade \
  --workspace-id '<workspace-id>' \
  --dataflow-id '<standard-gen2-dataflow-id>'
```

The command above makes no change. Add `--execute` only when ready to create a
new item and refresh its existing destination. Add `--name '<new-name>'` if the
default `<source-name>_cicd` is not desired:

```shell
python -m examples.dataflow_gen2_cicd_upgrade \
  --workspace-id '<workspace-id>' \
  --dataflow-id '<standard-gen2-dataflow-id>' \
  --name '<new-name>' \
  --execute
```

The browser login supplies the delegated user token. The script prints the new
item ID and refresh job status, but never prints either token. A terminal
`Completed` refresh is required before claiming success.

## Use the method in your own code

```python
from msdev_kit import Auth
from msdev_kit.fabric import Dataflow

auth = Auth(tenant_id, client_id, client_secret)  # SPN credentials
user_token = auth.get_token_for_user('fabric')     # interactive user login
dataflow = Dataflow(auth.get_token('fabric'))      # SPN creates the item
result = dataflow.upgrade_to_gen2_cicd(
    workspace_id=workspace_id,
    dataflow_id=standard_gen2_dataflow_id,
    source_type='gen2',
    use_accessible_connections=True,
    refresh=True,
    refresh_access_token=user_token,
    pbi_access_token=auth.get_token('pbi'),         # SPN reads the source
)
if result.get('message') != 'Success':
    # A failed refresh can still leave the new item in place.
    raise RuntimeError(result)
assert result['refresh']['status'] == 'Completed'
```

## If it fails

- `Expected one accessible ... connection`: Check the SPN's shared-connection
  permissions and the exact connector path. Resolve duplicate matches before
  retrying. The method does not create an item in this case.
- `SPNBasedRefreshNotAllowed`: The refresh used the SPN token. Supply a delegated
  Fabric user token through `refresh_access_token`.
- `Challenge` or `Bad credentials`: Inspect the connection bound in the saved
  definition and the detailed refresh failure. Verify the delegated user's
  access and the connection credential configuration. Do not assume `Online`
  proves that the dataflow's query can evaluate.
- A refresh failure after creation leaves the new item ID in `result['content']`.
  Inspect that item and its refresh job before rerunning the upgrade, because
  rerunning creates another item.
