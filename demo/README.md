# Privy demo (dbt-fabricspark)

Proves `method: privy` can run `SELECT 1` against a Fabric notebook over
Azure Relay, using this branch's adapter wheel.

## 1. Start the notebook manually

Auto-start is flaky right now (Fabric session errors), so start it by hand:

1. Open the notebook: value of `PRIVAY_NOTEBOOK_URL` in `test.env`.
2. Run all cells. Wait until the `RelayServer(...).serve_forever()` cell
   shows a running spinner (it never finishes — that's expected).

`privy_auto_start_notebook: false` is already set in `demo/profiles.yml` so
dbt won't try to trigger a run itself.

## 2. Build & install the wheel

```bash
cd /workspaces/dbt-fabricspark
uv build
python3 -m venv demo/.venv
demo/.venv/bin/pip install "$(ls dist/dbt_fabricspark-*-py3-none-any.whl)[privy]"
```

## 3. Run dbt

```bash
cd /workspaces/dbt-fabricspark
set -a; source test.env; set +a
cd demo
../demo/.venv/bin/dbt debug --profiles-dir .
../demo/.venv/bin/dbt run --profiles-dir .
../demo/.venv/bin/dbt show --inline "select 1 as one" --profiles-dir .
```

`dbt debug` should show a successful Privy relay connection (no notebook
trigger, since auto-start is off). `dbt run` builds `models/hello_privy.sql`
(`select 1 as id`) as a view.

**Verified output:**
```
$ dbt show --inline "select 1 as one"
| one |
| --- |
|   1 |

$ dbt run
1 of 1 OK created sql view model dbo.hello_privy ... [OK in 2.45s]
```

`schema: dbo` must be a schema that already exists in whatever lakehouse the
notebook is attached to — it's not related to the `privy_*` settings. If you
get `SCHEMA_NOT_FOUND`, run `SHOW SCHEMAS` in the notebook to find a valid one.

## Known issues

- Notebook auto-start (`privy_auto_start_notebook: true`) currently fails
  server-side after ~15s (`System_Cancelled_Session_Statements_Failed`),
  suspected cause: the notebook's `%pip install --force-reinstall` step
  clobbering packages the Fabric kernel relies on. Manual start avoids it.
- SPN auth may not be able to trigger notebook runs (Fabric API limitation) —
  use CLI auth (`az login`) for now.
