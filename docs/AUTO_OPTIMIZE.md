# Automatic OPTIMIZE

Every write leaves small Parquet files behind inside a Delta table, and small-file
fragmentation is the single biggest drag on downstream `JOIN` performance. To keep
tables compact, the adapter runs `OPTIMIZE` on the target relation after each
`table`, `incremental` and `snapshot` build. This is on by default.

- **Delta only.** Non-Delta relations are skipped — `OPTIMIZE` is a Delta command.
  A relation counts as Delta when `file_format` is unset (the adapter emits no
  `using` clause, so Fabric defaults to Delta), when `file_format: delta` is set, or
  when the existing table is already Delta.
- **Views, ephemeral models, seeds, `materialized_lake_view` and clones are never
  optimized.** Seeds are typically tiny, and `OPTIMIZE` would rewrite the files a
  shallow clone deliberately shares with its source.
- **Failures never fail the model.** `OPTIMIZE` is maintenance, so a transient Livy
  error or a Delta concurrent-modification conflict is logged as a warning and the
  build continues. It is also exempt from the connection retry loop, so under
  `retry_all: true` a failure is skipped immediately rather than stalling the run
  through every backoff. Small Spark clusters — including the local Livy container —
  can hit thread contention inside Delta's parallel compaction; when that happens you
  will see the warning and the model still succeeds.
- `OPTIMIZE` is a cheap no-op when there is nothing to compact.

Turn it off at any of three levels — the first match wins:

```bash
# 1. Environment kill switch — disables it everywhere, no project edits needed
export DBT_FABRICSPARK_SKIP_OPTIMIZE=true
```

```sql
-- 2. Per model
{{ config(materialized='incremental', auto_optimize=false) }}
```

```yaml
# 3. profiles.yml — project-wide default
my_profile:
  outputs:
    dev:
      type: fabricspark
      auto_optimize: false
```

You can also disable it for a subset of models from `dbt_project.yml`:

```yaml
models:
  my_project:
    staging:
      +auto_optimize: false
```

> On a small local Spark install, running many `OPTIMIZE` jobs concurrently (high
> `threads`) can cause resource contention. Lower `threads` or set
> `DBT_FABRICSPARK_SKIP_OPTIMIZE=true` if you hit it.

---
[Home](../README.md) > [Documentation](./README.md)
