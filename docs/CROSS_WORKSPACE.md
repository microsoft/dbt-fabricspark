# Cross-Workspace 4-Part Naming

For multi-workspace topologies — e.g. dev workspace reading shared marts from a prod workspace, or a single dbt project orchestrating bronze/silver/gold across separate workspaces — set `workspace_name` on a model's `config()`. The adapter renders the relation as a backtick-quoted four-part name so Fabric Spark routes the statement to the correct workspace catalog. **Both reads (federated `SELECT`) and writes (cross-workspace `CREATE TABLE AS SELECT`) are supported** against schema-enabled lakehouses.

If you set this in `dbt_project.yml`, prefer `+meta.workspace_name` to avoid dbt's custom-config deprecation warning:

```yaml
models:
  my_project:
    marts:
      +meta:
        workspace_name: SharedWorkspace
```

## Reads — stub-and-`ref` pattern

```sql
-- models/silver/from_prod_orders.sql
{{ config(
    materialized='view',
    workspace_name='ProdWorkspace',
    database='prod_silver_lh',
    schema='dbo',
    alias='orders'
) }}

-- This stub is never selected for materialization. It exists so other
-- models can `ref('from_prod_orders')` and resolve to the prod relation.
select cast(null as int) as id
```

Other models then read it normally via `ref()`:

```sql
-- models/silver/orders_metrics.sql
{{ config(materialized='table') }}

select id, count(*) as n
from {{ ref('from_prod_orders') }}
group by id
```

dbt renders the cross-workspace reference as:

```
`ProdWorkspace`.`prod_silver_lh`.`dbo`.orders
```

## Writes — cross-workspace CTAS

A model can also be **materialized into another workspace** by setting `workspace_name` directly on the target model. The Spark session stays bound to your profile's workspace; Fabric routes the `CREATE TABLE` against the remote workspace's catalog:

```sql
-- models/marts/shared_orders.sql
{{ config(
    materialized='table',
    file_format='delta',
    workspace_name='SharedWorkspace',
    database='shared_lh',
    schema='marts'
) }}

select * from {{ ref('orders') }}
```

dbt emits:

```
create or replace table `SharedWorkspace`.`shared_lh`.`marts`.shared_orders as
select * from …
```

With `method: livy`, the target schema (`marts` in `shared_lh` of `SharedWorkspace`) is created automatically by the adapter's standard schema pre-create flow. Fabric Livy supports cross-workspace `CREATE DATABASE IF NOT EXISTS \`SharedWorkspace\`.\`shared_lh\`.\`marts\``, so no manual setup is required.

> **Spark Session requires a pre-existing remote schema.** A runtime-provided Fabric Spark session can execute four-part table and metadata operations, but it cannot resolve the three-part workspace namespace used by cross-workspace `CREATE DATABASE` or `DROP DATABASE`. With `method: session` and `workspace_name` set, the adapter skips those schema operations; create the remote schema before running dbt.

> **Use `file_format='delta'` for idempotent re-runs.** The adapter emits `CREATE OR REPLACE TABLE` for delta tables, which re-materializes cleanly. Non-delta cross-workspace writes will fail on the second run with `TABLE_ALREADY_EXISTS` because `adapter.get_relation` is workspace-unaware and cannot detect the existing remote relation to drop it first.

> **Materializations validated end-to-end:** `table` (full CTAS) and `incremental` (initial CTAS + `MERGE INTO` + `--full-refresh`). Other materializations (`view`, `seed`, `snapshot`, `materialized_lake_view`) share the same render and `ensure_database_exists` plumbing and should work cross-workspace, but are not exercised by functional tests in this repo.

> **Schema-enabled lakehouses only.** Cross-workspace 4-part naming requires a schema-enabled lakehouse. Setting `workspace_name` against a non-schema-enabled target raises a parse-time error.

## Profile-level default workspace

Instead of setting `workspace_name` on every model, you can set it once in `profiles.yml` as a target-scoped default. When set, all relations in that target automatically use the workspace as a prefix — without any per-model config.

```yaml
# profiles.yml
my_profile:
  outputs:
    prod:
      type: fabricspark
      workspace_name: "vd-domain-prod"   # default workspace for this target
      lakehouse: silver_lh
      schema: dbo
      ...
    dev:
      type: fabricspark
      workspace_name: "vd-ephemeral-dev"
      ...
```

**Precedence (lowest → highest):**

| Source                             | Example                                      |
| ---------------------------------- | -------------------------------------------- |
| No workspace (current default)     | two- or three-part names                     |
| Profile `workspace_name`           | all relations in target get 4-part names     |
| Model `config(workspace_name=...)` | overrides the profile default for that model |

The profile value is accessible in Jinja as `target.workspace_name`, so you can inspect or branch on it in macros.

> **Schema-enabled lakehouses only.** If `workspace_name` is set in the profile but the target lakehouse does not have schemas enabled, the adapter logs a warning and ignores the value.

## How it works

Your profile binds to **one workspace + lakehouse** — that's where the Livy session lives, and _every_ SQL statement is issued from that session. `workspace_name` is a rendering decoration applied to the relation; Fabric Livy then federates the statement through its metastore so a `SELECT` returns rows from the other workspace and a `CREATE TABLE AS SELECT` writes into the other workspace's lakehouse.

## When to use it

- **Cross-workspace reads** — A dev workspace references shared dimensions, regulatory data, or prod marts without copying via OneLake shortcuts.
- **Cross-workspace writes** — A consolidation pipeline that aggregates data from one workspace into a shared analytics workspace, without spinning up a second dbt project / profile per target workspace.
- **Multi-workspace project topologies** — One dbt project that needs to read from and/or write to several workspaces.

## When _not_ to use it

- **Non-schema-enabled lakehouses** — Use OneLake shortcuts instead; the adapter errors at parse time to surface the constraint.

## Permissions

The principal authenticated by your profile (CLI user or SPN) must have read access on **both** workspaces — the local one (where the Livy session runs) and the remote one whose data you reference.

## Quoting & casing

Each segment is independently backtick-quoted, so workspace names with spaces or mixed case (e.g. `dbt Fabric Spark 1`) round-trip correctly.

## Sources

`{{ source(...) }}` supports cross-workspace 4-part naming too — declare `workspace_name` under a source's (or table's) `config` block in `sources.yml`, just like models use `config(workspace_name=...)`. Source reads, freshness, and generic tests on sources all render the 4-part name against the remote workspace (schema-enabled lakehouses only).

```yaml
# models/sources.yml
sources:
  - name: my_bronze_source
    database: remote_lakehouse
    schema: my_schema
    config:
      workspace_name: RemoteWorkspaceName
    tables:
      - name: my_table
```

`select * from {{ source('my_bronze_source', 'my_table') }}` then resolves to `` `RemoteWorkspaceName`.`remote_lakehouse`.`my_schema`.my_table ``.

---
[Home](../README.md) > [Documentation](./README.md)
