# Incremental strategies

`incremental` models accept these `incremental_strategy` values via `config()`:

| Strategy           | `file_format` | `unique_key` | Behavior                                                                                                         |
| ------------------ | ------------- | ------------ | ---------------------------------------------------------------------------------------------------------------- |
| `append` (default) | any           | optional     | Insert all new rows; no updates or deletes.                                                                      |
| `merge`            | `delta`       | optional     | `MERGE INTO` — update matched rows, insert the rest. Supports [advanced merge options](#advanced-merge-options). |
| `insert_overwrite` | `delta`       | —            | Overwrite matched partitions (`partition_by`), or the whole table when unpartitioned.                            |
| `microbatch`       | `delta`       | —            | Per-batch delete (by `partition_by`) then insert; used by dbt's microbatch.                                      |
| `delete+insert`    | `delta`       | **required** | Delete target rows whose `unique_key`(s) appear in the new data, then insert all new rows.                       |

`delete+insert` is a key-based full row-replace: it deletes every target row whose
`unique_key` appears in the incoming set and then inserts all incoming rows. Use it
instead of `merge` when you want matched keys replaced wholesale rather than updated
column-by-column. It requires `file_format: delta` and a `unique_key` (a single column
or a list) — omitting the key raises a compile-time error. Optional
`incremental_predicates` are ANDed into the delete match to scope it to a window.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='id',
    file_format='delta'
) }}
```

## Advanced `merge` options

When `incremental_strategy='merge'` (on `file_format: delta`) you can shape the
generated `MERGE INTO` statement with the following optional `config()` keys. All of
them default to today's behavior, so existing `merge` models are unaffected.

| Config                            | Type   | Default               | Effect                                                                                              |
| --------------------------------- | ------ | --------------------- | --------------------------------------------------------------------------------------------------- |
| `target_alias`                    | string | `DBT_INTERNAL_DEST`   | Alias used for the target relation in the `MERGE` (and in your conditions).                         |
| `source_alias`                    | string | `DBT_INTERNAL_SOURCE` | Alias used for the staged source in the `MERGE` (and in your conditions).                           |
| `matched_condition`               | string | —                     | Extra predicate `AND (…)` on the `WHEN MATCHED … THEN UPDATE` clause.                               |
| `not_matched_condition`           | string | —                     | Extra predicate `AND (…)` on the `WHEN NOT MATCHED … THEN INSERT` clause.                           |
| `skip_matched_step`               | bool   | `false`               | Omit the `WHEN MATCHED` clause entirely (insert-only merge).                                        |
| `skip_not_matched_step`           | bool   | `false`               | Omit the `WHEN NOT MATCHED` clause entirely (update-only merge).                                    |
| `not_matched_by_source_condition` | string | —                     | Extra predicate `AND (…)` on the `WHEN NOT MATCHED BY SOURCE` clause.                               |
| `not_matched_by_source_action`    | string | —                     | Emits `WHEN NOT MATCHED BY SOURCE` when set to `delete` or `update set …` — e.g. propagate deletes. |
| `merge_with_schema_evolution`     | bool   | `false`               | Enable MERGE schema evolution so new source columns are added to the target automatically.          |

`matched_condition`, `not_matched_condition` and `not_matched_by_source_condition`
should reference the target/source using the aliases above (defaulting to
`DBT_INTERNAL_DEST` / `DBT_INTERNAL_SOURCE`). `not_matched_by_source_action` only
produces a clause when it is `delete` or starts with `update`; any other value is
ignored. `merge_with_schema_evolution` sets the standard Delta
`spark.databricks.delta.schema.autoMerge.enabled` session setting before the merge
rather than emitting a proprietary SQL clause, so it works
on Fabric Runtime 1.3 (Spark 3.5 / Delta Lake 3.2) and local Livy alike. These options
require Fabric Runtime 1.3 or newer.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    file_format='delta',
    target_alias='t',
    source_alias='s',
    matched_condition='s.updated_at > t.updated_at',
    not_matched_by_source_condition="t.status <> 'archived'",
    not_matched_by_source_action='delete',
    merge_with_schema_evolution=true
) }}
```

## Dropping columns with `on_schema_change`

`on_schema_change: sync_all_columns` and `sync_remove_only` drop target columns that
are no longer in the model. Delta Lake can only drop columns when
[column mapping](https://docs.delta.io/latest/delta-column-mapping.html) is enabled on
the table (`delta.columnMapping.mode` = `name` or `id`):

- If the table already uses column mapping, dbt drops the columns with `ALTER TABLE ... DROP COLUMNS`.
- If it does not, set `enable_column_mapping_on_drop: true` to let dbt enable it
  (`delta.columnMapping.mode = 'name'`) before the drop. This upgrades the table
  protocol to at least reader version 2 / writer version 5, so older Delta readers can no
  longer read the table; existing higher protocol versions are never downgraded.
- Otherwise dbt raises a compile error that names both options.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    file_format='delta',
    on_schema_change='sync_all_columns',
    enable_column_mapping_on_drop=true
) }}
```

---
[Home](../README.md) > [Documentation](./README.md)
