# Examples

Each script is a small, placeholder-based starting point. Run modules from the
repository root so the shared setup can load `.env`.

## Configure once

```shell
cp .env.example .env
```

Set the values required by the script you choose. Never commit `.env`.

| Script | API audience | Default behavior | Extra configuration |
| --- | --- | --- | --- |
| `workspaces.py` | Power BI | Lists visible workspaces | None |
| `datasets.py` | Power BI | Stops until workspace and semantic model names are set | None |
| `dataflow_destinations.py` | Power BI and Fabric dataflow endpoints | Stops until a workspace ID is set | None |
| `dataflows.py` | Power BI and Fabric dataflow endpoints | Does not perform a write | None |
| `pipelines.py` | Fabric | Stops until pipeline values are set | None |
| `notebooks.py` | Fabric | Stops until a workspace ID is set | None |
| `graph.py` | Microsoft Graph | Stops until user and group values are set | None |
| `sharepoint.py` | Microsoft Graph | Does not perform a file operation | `SP_HOSTNAME`, `SP_SITE_PATH` |
| `database.py` | Fabric SQL endpoint | Does not perform a write | `FABRIC_SQL_ENDPOINT`, `FABRIC_DATABASE` |

Any script that reaches an API call requires `TENANT_ID`, `CLIENT_ID`, and
`CLIENT_SECRET`.

## Run

```shell
python -m examples.workspaces
```

For a Dataflow Gen2 destination inventory, set `WORKSPACE_ID` in
`dataflow_destinations.py`, then run:

```shell
python -m examples.dataflow_destinations
```

The inventory writes `data/dataflows/workspace_dataflow_destinations_<workspace-id>.xlsx`.
Its returned DataFrame and workbook contain only tables with data destinations.
The structured result additionally preserves inspected dataflows with no
destination and any inspection failures.

## Write helpers

`dataflows.py`, `workspaces.py`, `graph.py`, `sharepoint.py`, and `database.py`
include helpers that can change remote data. They never invoke a write when run
directly. Copy the helper call into your own reviewed script, replace all
placeholders, and confirm the target IDs before using it.
