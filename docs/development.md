# Development

## Local setup

Use Python 3.10 or newer. Create an isolated environment, install the package,
and install the tools used by the repository checks.

```shell
git clone https://github.com/Bernardo-Rufino/msdev-kit.git
cd msdev-kit
git switch -c feature/<change-name>
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e . pytest pytest-mock build
```

Poetry users can install the declared development group instead:

```shell
poetry install --with dev
```

## Configure examples

The library never reads secrets from disk on its own. The example scripts load
`.env` from the repository root. Create it from the tracked template and keep
it local:

```shell
cp .env.example .env
```

Set `TENANT_ID`, `CLIENT_ID`, and `CLIENT_SECRET` for Fabric, Power BI, Graph,
and SharePoint examples. The database and SharePoint scripts list their extra
variables in `.env.example`. See [Examples](../examples/README.md) for each
script's API audience and safety level.

## Run and validate

Run the same core test directories used by the pull request workflow:

```shell
pytest tests/fabric/ tests/graph/ tests/sharepoint/ -v --tb=short
```

Then compile the package and examples, and build the distribution:

```shell
python -m compileall -q msdev_kit examples
python -m build
```

Run an example from the repository root with module syntax:

```shell
python -m examples.workspaces
python -m examples.dataflow_destinations
```

Most examples use placeholders. A module that can mutate Fabric, Graph,
SharePoint, or a database does not run a write action by default. Review target
IDs and call the specific helper only when you intend to change data.

## Change and release workflow

1. Create a `feature/<name>`, `fix/<name>`, or `docs/<name>` branch.
2. Keep the change focused and update tests when behavior changes.
3. Run the validation commands above before opening a pull request.
4. Release only after the pull request is merged. A main branch change under
   `msdev_kit/**` or `pyproject.toml` triggers publishing. Documentation-only
   changes do not create a package version. The workflow selects the next PyPI
   version, creates the GitHub release, and requires approval for the `pypi`
   environment.

Do not commit `.env`, generated workbooks, or local data files.
