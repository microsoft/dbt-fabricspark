# Copilot Instructions — dbt-fabricspark

## What this repo is

A **dbt adapter** that lets dbt-core build SQL models against Apache Spark in Microsoft Fabric. 

It connects to Fabric Lakehouses via the Livy API (or a local Livy in the devcontainer for offline work) and ships materializations for table, view, incremental, seed, snapshot, and `materialized_lake_view`.

Auto-detects schema-enabled vs non-schema lakehouses (four-part/three-part vs two-part naming) and supports cross-lakehouse writes via `database` overrides on a single profile.

## Key paths

| Path                                                                 | What lives there                                                                                               |
| -------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `src/dbt/adapters/fabricspark/`                                      | Python adapter — `impl.py`, `connections.py`, `livysession.py`, `credentials.py`, `mlv_api.py`, `shortcuts.py` |
| `src/dbt/include/fabricspark/macros/`                                | Jinja SQL macros (`adapters/`, `materializations/{models,seeds,snapshots}`, `utils/`)                          |
| `src/dbt/include/fabricspark/{dbt_project.yml,profile_template.yml}` | Adapter package + profile prompts                                                                              |
| `tests/unit/`                                                        | Pure pytest unit tests — no Fabric needed                                                                      |
| `tests/functional/`                                                  | Live-Fabric functional tests, `orchestrator.py`, YAML scheduler, `test_config.yaml`                            |
| `tests/fixtures/dbt-jaffle-shop/`                                    | Project used by the local end-to-end test                                                                      |
| `tools/scripts/run.sh`                                               | Single entry point that every Nx target shells out to                                                          |
| `tools/gh-automation/`                                               | TypeScript helpers for managing Copilot-driven PRs                                                             |
| `docker/Compose.sqlserver.metastore.yaml`                            | SQL Server Hive metastore for the local-e2e flow                                                               |
| `.devcontainer/`                                                     | Prebuilt devcontainer image (pinned by digest) used locally and in CI                                          |
| `.github/workflows/ci.yml`                                           | The CI pipeline                                                                                                |
| `project.json` / `nx.json`                                           | Nx target definitions — the source of truth for build/lint/test                                                |
| `pyproject.toml`                                                     | Python deps (`uv`-managed), ruff config, pytest config                                                         |
| `CHANGELOG.md` / `docs/RELEASE_CHECKLIST.md`                         | Manual changelog + release flow                                                                                |

## CI is the contract — run it locally with `npx nx`

CI (`.github/workflows/ci.yml`) runs **inside the prebuilt devcontainer image** and invokes `npx nx affected ... -t {build|lint|test} --configuration=ci`, followed by `npm run fail-on-untracked-files`. Everything Nx runs delegates to `tools/scripts/run.sh <target>`, which auto-creates a `.venv` via `uv` — never call `pip` directly.

The Nx targets defined in `project.json` (run them with `npx nx run dbt-fabricspark:<target>`):

| Nx target           | Command                                      | What it does                                                                                       |
| ------------------- | -------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `lint`              | `npx nx run dbt-fabricspark:lint`            | `ruff check --fix` + `ruff format` + verify clean                                                  |
| `build`             | `npx nx run dbt-fabricspark:build`           | `uv build` wheel + `twine check`                                                                   |
| `test`              | `npx nx run dbt-fabricspark:test`            | Aggregate: `test:unit` → `test:functional` → `test:local-e2e`                                      |
| `test:unit`         | `npx nx run dbt-fabricspark:test:unit`       | `uv run pytest tests/unit -vv`                                                                     |
| `test:functional`   | `npx nx run dbt-fabricspark:test:functional` | YAML-scheduled live-Fabric run (provision → warm sessions → pytest xdist → nuke); needs `test.env` |
| `test:local-e2e`    | `npx nx run dbt-fabricspark:test:local-e2e`  | Full dbt lifecycle on local Livy + jaffle-shop; depends on `build` and `init`                      |
| `init`              | `npx nx run dbt-fabricspark:init`            | Brings up local Livy (depends on `metastore-up`)                                                   |
| `metastore-up/down` | `npx nx run dbt-fabricspark:metastore-up`    | SQL Server Hive metastore via Docker Compose                                                       |
| `clean`             | `npx nx run dbt-fabricspark:clean`           | Tears down Livy/metastore, removes `dist/`, caches, `__pycache__`                                  |
| `publish`           | `npx nx run dbt-fabricspark:publish`         | `twine check` + `uv publish` (no-op without `UV_PUBLISH_TOKEN`)                                    |

To match CI's affected-only mode locally, prefix with the same flags:

```bash
npx nx affected --base=origin/main -t lint --configuration=ci --output-style=stream
npx nx affected --base=origin/main -t build --configuration=ci --output-style=stream
npx nx affected --base=origin/main -t test --configuration=ci --output-style=stream
```

For one-off pytest runs while iterating:

```bash
uv run pytest tests/unit/test_adapter.py::TestSparkAdapter::test_profile_with_database -vv
```

## Robust CI tests are non-negotiable

- **Full Fabric CI suite must run locally** For validation, you must run `npx nx run test`.
  The secrets should already be hydrated locally in the git repo under `test.env` by the human user during [repo setup](../contrib/README.md).
- **Every change must keep `lint`, `build`, `test` (all targets) green.** Do not gate features on flaky assumptions about Fabric availability — wrap external calls with the retry/backoff and timeout knobs already on `FabricSparkCredentials` (`http_timeout`, `session_start_timeout`, `statement_timeout`, `poll_wait`, `poll_statement_wait`, `connect_retries`, `retry_all`).
- **Functional tests run in parallel across two lakehouses** (`no_schema` and `with_schema`) and across multiple Livy sessions via xdist — see `tests/functional/test_config.yaml` and `tests/functional/conftest.py`. New tests must be xdist-safe and must not assume which session they land on. Use the existing fail-fast sentinel (`--fail-fast-sentinel`) instead of inventing new abort mechanisms.
- **Unit tests should cover any branch in `impl.py`/`connections.py`/`livysession.py`/`mlv_api.py` you touch**, including the schema-detection heuristic (parse-time `schema != lakehouse`) and credential masking.
- **`fail-on-untracked-files` will fail CI if you leave a regenerated file uncommitted** — this catches stale `uv.lock` after a dep change. Always commit lockfile updates that result from your edits.
- **Devcontainer image is pinned by digest** in `.devcontainer/devcontainer.json` and `.github/workflows/ci.yml`. Don't hand-edit these; rebuild via the flow in `.devcontainer/README.md`.

## Do not assume — ask first

Adapter behavior is subtle (schema-enabled detection, cross-lakehouse routing, session reuse, MLV semantics, Livy retry policy). Before making non-trivial changes:

- **Confirm scope with the user.** Is this a bug fix, a new materialization option, or a behavior change to an existing one? Should it apply to schema-enabled mode, non-schema mode, or both?
- **Confirm test strategy.** Unit-only, or does it need a functional test against real Fabric? Which Fabric runtime/lakehouse type is in scope?
- **Confirm public-API impact.** Does this change `FabricSparkCredentials` fields, profile YAML keys, or model `config()` keys? If so, the README config tables and `profile_template.yml` need updates too.
- **Confirm changelog placement.** New version section in `CHANGELOG.md`, or append to the unreleased section?

When in doubt, **ask** rather than guess. The maintainers prefer a question to a wrong assumption that ships.

## Do not commit without review

The repo owner reviews all changes before merge — open a PR and wait for review. Don't run `git commit` autonomously on `main`, and follow [Conventional Commits](https://www.conventionalcommits.org/) for PR titles (the PR template will remind you).
