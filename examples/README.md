# Examples

Runnable scripts that mirror the typical usage of `msdev_kit`. Each file is
standalone; the shared `_setup.py` builds the Auth and clients from
environment variables (or `./utils/.env`).

| Script | What it shows |
| --- | --- |
| `workspaces.py` | List workspaces; bulk add/remove users |
| `datasets.py` | List dataset users; run a DAX query |
| `dataflows.py` | Copy a dataflow; replace its destination and fix downstream pipelines |
| `pipelines.py` | Read activities; find pipelines referencing a dataflow |
| `database.py` | Write a pandas DataFrame to a Fabric SQL endpoint |
| `notebooks.py` | List notebooks; fetch a single notebook |
| `graph.py` | Look up users/groups; add/remove group members |
| `sharepoint.py` | Download, create folder, upload |

Required environment variables:

```
TENANT_ID=...
CLIENT_ID=...
CLIENT_SECRET=...
FABRIC_SQL_ENDPOINT=...
FABRIC_DATABASE=...
SP_HOSTNAME=...
SP_SITE_PATH=...
```

Run a script from the repo root, e.g.:

```bash
python -m examples.workspaces
```
