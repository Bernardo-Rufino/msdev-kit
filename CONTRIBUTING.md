# Contributing

Thank you for improving `msdev-kit`. Small, focused pull requests are easier to
review and release.

## Before you start

- Search existing issues and pull requests before opening a new one.
- Open an issue first for a non-trivial change or an API design change.
- Do not include credentials, access tokens, workspace IDs, personal data, or
  customer data in issues, pull requests, examples, or test fixtures.

## Local setup

Follow the [development guide](docs/development.md) to create an environment,
install dependencies, configure examples, and run validation.

Create a branch from current `main`:

```shell
git switch main
git pull --ff-only
git switch -c feature/<short-name>
```

Use `fix/<short-name>` for corrections and `docs/<short-name>` for
documentation-only changes.

## Make a change

- Keep the public API, documentation, examples, and tests aligned.
- Add or update focused tests for behavior changes.
- Use placeholders in examples. Example modules must not write remote data by
  default.
- Do not commit `.env`, generated files under `data/`, build artifacts, or
  locally downloaded files.

Run the repository checks before opening a pull request:

```shell
python -m pytest tests/fabric tests/graph tests/sharepoint -v --tb=short
python -m compileall -q msdev_kit examples
python -m build
```

## Open a pull request

Use the pull request template. Describe the user-visible behavior, validation,
and any required permissions or migration steps. Link the issue when one exists.

A maintainer reviews the change. Do not merge your own pull request unless you
have explicit maintainer authorization. Package publication is maintainer-owned
and is triggered only by qualifying changes merged to `main`.

## Maintainer merge policy

`main` accepts pull requests only. A change needs one code-owner approval, all
review threads resolved, and passing `build`, `test`, and GitGuardian checks.
Use squash merge to keep one focused change per pull request. Auto merge stays
disabled because a qualifying merge can publish a package.
