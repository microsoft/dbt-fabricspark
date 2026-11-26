# Lakehouses

## Lakehouse without Schema

For standard Lakehouses (schema not enabled), use two-part naming. The `schema` field is set to the lakehouse name:

```yaml
fabric-spark-test:
  target: fabricspark-dev
  outputs:
    fabricspark-dev:
        # Connection
        type: fabricspark
        method: livy
        endpoint: https://api.fabric.microsoft.com/v1
        workspaceid: <your-workspace-id>
        lakehouseid: <your-lakehouse-id>
        lakehouse: my_lakehouse
        schema: my_lakehouse
        threads: 1

        # Authentication (CLI for local dev, SPN for CI/CD)
        authentication: CLI
        # client_id: <your-client-id>              # Required for SPN
        # tenant_id: <your-tenant-id>              # Required for SPN
        # client_secret: <your-client-secret>      # Required for SPN

        # Fabric Environment (optional)
        # environmentId: <your-environment-id>

        # Session management
        reuse_session: true
        # session_idle_timeout: "30m"              # Opt-in only. Setting this triggers
                                                   # Fabric to bypass starter pools and
                                                   # cold-start an on-demand cluster.
        # session_id_file: ./livy-session-id.txt   # Default path

        # Timeouts
        connect_retries: 1
        connect_timeout: 10
        http_timeout: 120                          # Seconds per HTTP request
        session_start_timeout: 600                 # Max wait for session start (10 min)
        statement_timeout: 3600                    # Max wait for statement result (1 hour)
        poll_wait: 10                              # Seconds between session start polls
        poll_statement_wait: 5                     # Seconds between statement result polls
        azure_cli_process_timeout: 10              # Subprocess timeout for `az` token refresh
                                                   # (CLI auth). Raise under high concurrency.

        # Retry & Shortcuts
        retry_all: true
        enable_job_retry: true
        job_retry_on_messages: []                  # Empty means no query retries
        job_retry_max_attempts: 3                  # Total attempts
        job_retry_initial_wait_seconds: 30
        job_retry_max_wait_seconds: 300
        # create_shortcuts: false
        # prune_shortcuts: false
        # shortcuts_json_str: '<json-string>'
        # shortcuts_json_file: config/shortcuts.json   # Alternative to shortcuts_json_str

        # Spark configuration (optional)
        # spark_config:
        #   name: "my-spark-session"
        #   spark.executor.memory: "4g"
```

In this mode:

- Tables are referenced as `lakehouse.table_name`
- The `schema` field should match the `lakehouse` name
- All objects are created directly under the lakehouse

## Lakehouse with Schema (Schema-Enabled)

For schema-enabled Lakehouses, you can organize tables into schemas within the lakehouse. The adapter **auto-detects** whether a lakehouse has schemas enabled via the Fabric REST API (`properties.defaultSchema`):

```yaml
fabric-spark-test:
  target: fabricspark-dev
  outputs:
    fabricspark-dev:
        type: fabricspark
        method: livy
        endpoint: https://api.fabric.microsoft.com/v1
        workspaceid: <your-workspace-id>
        lakehouseid: <your-lakehouse-id>
        lakehouse: my_lakehouse
        schema: my_schema                          # Different from lakehouse name
```

In this mode:

- Tables are referenced using three-part naming: `lakehouse.schema.table_name`
- The `schema` field specifies the target schema within the lakehouse
- dbt's `generate_schema_name` and `generate_database_name` macros are lakehouse-aware
- Schemas are created automatically via `CREATE DATABASE IF NOT EXISTS lakehouse.schema` when the connection method supports schema DDL
- Incremental models use persisted staging tables (instead of temp views) to work around Spark's `REQUIRES_SINGLE_PART_NAMESPACE` limitation

## Schema Detection

The adapter detects whether a lakehouse has schemas enabled using three complementary mechanisms:

1. **Runtime detection (Fabric Livy):** During `connection.open()`, the adapter calls the Fabric REST API to fetch lakehouse properties. If the response contains `defaultSchema`, the lakehouse is treated as schema-enabled and three-part naming is used.

2. **Session detection (profile heuristic):** A runtime-provided Spark session cannot call the Fabric lakehouse-properties API, so `method: session` treats `schema != lakehouse` as the schema-enabled signal for the connection.

3. **Parse-time detection (profile heuristic):** During manifest parsing (before any connection is opened), the adapter checks whether `schema` differs from `lakehouse` in your profile. When they differ (e.g., `lakehouse: bronze`, `schema: dbo`), the adapter infers schema-enabled mode. This ensures correct schema resolution at compile time.

> **Important:** For schema-enabled lakehouses, always set `schema` to a value **different** from `lakehouse` in your profile (e.g., `schema: dbo`). If `schema` equals `lakehouse`, the adapter cannot distinguish schema-enabled from non-schema mode at parse time, and the lakehouse name will be used as the schema name instead.

| Lakehouse Type | `lakehouse`    | `schema`       | Naming                        |
| -------------- | -------------- | -------------- | ----------------------------- |
| Without schema | `my_lakehouse` | `my_lakehouse` | `my_lakehouse.table_name`     |
| With schema    | `my_lakehouse` | `dbo`          | `my_lakehouse.dbo.table_name` |

## Cross-Lakehouse Writes

A single profile can write to multiple lakehouses using the `database` config on individual models. The profile's `lakehouse` is the default target; set `database` on a model to redirect writes to a different lakehouse in the same workspace.

```yaml
# profiles.yml — profile targets the "bronze" lakehouse
fabric-spark:
  type: fabricspark
  lakehouse: bronze
  schema: dbo
  # ... other settings
```

```sql
-- models/silver/silver_orders.sql — writes to the "silver" lakehouse
{{ config(
    materialized='table',
    database='silver',
    schema='dbo'
) }}

select * from {{ ref('bronze_orders') }}
```

In this example:

- Seeds and bronze models write to `bronze.dbo.*` (the default lakehouse)
- Silver models write to `silver.dbo.*` via `database='silver'`
- Gold models write to `gold.dbo.*` via `database='gold'`
- All three lakehouses must exist in the same Fabric workspace and have schemas enabled

## Case-sensitive identifiers

By default the adapter renders identifiers **unquoted**, so Fabric Spark folds them to lowercase (`MyTable` → `mytable`). To preserve exact casing, set `quote_identifiers: true` — but **two settings are required together**:

```yaml
# profiles.yml
my_profile:
  outputs:
    prod:
      type: fabricspark
      quote_identifiers: true
      spark_config:
        name: my-session
        conf:
          spark.sql.caseSensitive: "true"
```

- **`quote_identifiers: true`** backtick-quotes every relation's identifier so the emitted SQL preserves casing.
- **`spark.sql.caseSensitive: "true"`** (under `spark_config.conf`) makes Spark _resolve_ the mixed-case names instead of folding them. The adapter warns at connection time if `quote_identifiers` is on without it.

> ⚠️ `spark.sql.caseSensitive` is **session-wide and affects columns too** — it changes column resolution across every model (joins, `select *`, schema comparisons, `MERGE`/incremental matching can break if column casing is inconsistent). Enable only when you need case-sensitive object names.

Both settings default to off, so existing projects render byte-identical SQL.

---
[Home](../README.md) > [Documentation](./README.md)
