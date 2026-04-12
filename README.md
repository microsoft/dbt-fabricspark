<a href="https://github.com/microsoft/dbt-fabricspark/actions/workflows/ci.yml">
  <img src="https://github.com/microsoft/dbt-fabricspark/actions/workflows/ci.yml/badge.svg?branch=main" alt="Tests and Code Checks"/>
</a>
<a href="https://github.com/microsoft/dbt-fabricspark/actions/workflows/integration.yml">
  <img src="https://github.com/microsoft/dbt-fabricspark/actions/workflows/integration.yml/badge.svg?branch=main&event=pull_request" alt="Adapter Integration Tests"/>
</a>
<a href="https://github.com/microsoft/dbt-fabricspark/actions/workflows/release.yml">
  <img src="https://github.com/microsoft/dbt-fabricspark/actions/workflows/release.yml/badge.svg" alt="Release to PyPI"/>
</a>

![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)
![dbt-core](https://img.shields.io/badge/dbt--core-%3E%3D1.8.0-orange)
![License](https://img.shields.io/badge/license-MIT-green)

<br>

[dbt](https://www.getdbt.com/) enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## dbt-fabricspark

The `dbt-fabricspark` package contains all of the code enabling dbt to work with Apache Spark in Microsoft Fabric. This adapter connects to Fabric Lakehouses via Livy endpoints and supports both **schema-enabled** and **non-schema** Lakehouse configurations.

### Key Features

- **Livy session management** with session reuse across dbt runs
- **Lakehouse with schema support** — auto-detects schema-enabled lakehouses and uses three-part naming (`lakehouse.schema.table`)
- **Lakehouse without schema** — standard two-part naming (`lakehouse.table`)
- **Materializations**: table, view, incremental (append, merge, insert_overwrite), seed, snapshot
- **Fabric Environment support** via `environmentId` configuration
- **Security**: credential masking, UUID validation, HTTPS + domain validation, thread-safe token refresh
- **Resilience**: HTTP 5xx retry with exponential backoff, bounded polling with configurable timeouts

## Getting started

- [Install dbt](https://docs.getdbt.com/docs/installation)
- Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

### Installation

```bash
pip install dbt-fabricspark
```

## Configuration

Use a Livy endpoint to connect to Apache Spark in Microsoft Fabric. Configure your `profiles.yml` to connect via Livy endpoints.

### Connection Modes

The adapter supports two connection modes via the `livy_mode` setting:

- **Local mode** (`livy_mode: local`) — Connects to a self-hosted Spark instance running in a Docker container (contributed by @mdrakiburrahman). This mode supports the `reuse_session` flag and does not require Fabric compute, making it ideal for offline development and testing.

- **Fabric mode** (`livy_mode: fabric`, default) — Connects to Apache Spark in Microsoft Fabric via the Fabric Livy API. For development workflows, enable `reuse_session: true` to persist the Livy session ID to a local file (configured via `session_id_file`, defaults to `./livy-session-id.txt`). On subsequent `dbt` runs, the adapter reuses the existing session from the persisted file instead of creating a new one. If the file does not exist or the session has expired, a new session is created automatically.

### Lakehouse without Schema

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
        # client_id: <your-client-id>        # Required for SPN
        # tenant_id: <your-tenant-id>        # Required for SPN
        # client_secret: <your-client-secret> # Required for SPN

        # Fabric Environment (optional)
        # environmentId: <your-environment-id>

        # Session management
        reuse_session: true
        session_idle_timeout: "30m"
        # session_id_file: ./livy-session-id.txt  # Default path

        # Timeouts
        connect_retries: 1
        connect_timeout: 10
        http_timeout: 120                   # Seconds per HTTP request
        session_start_timeout: 600          # Max wait for session start (10 min)
        statement_timeout: 3600             # Max wait for statement result (1 hour)
        poll_wait: 10                       # Seconds between session start polls
        poll_statement_wait: 5              # Seconds between statement result polls

        # Retry & Shortcuts
        retry_all: true
        # create_shortcuts: false
        # shortcuts_json_str: '<json-string>'

        # Spark configuration (optional)
        # spark_config:
        #   name: "my-spark-session"
        #   spark.executor.memory: "4g"
```

In this mode:
- Tables are referenced as `lakehouse.table_name`
- The `schema` field should match the `lakehouse` name
- All objects are created directly under the lakehouse

### Lakehouse with Schema (Schema-Enabled)

For schema-enabled Lakehouses, you can organize tables into schemas within the lakehouse. The adapter **auto-detects** whether a lakehouse has schemas enabled via the Fabric REST API (`properties.defaultSchema`):

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
        schema: my_schema              # Different from lakehouse name
        threads: 1

        # Authentication (CLI for local dev, SPN for CI/CD)
        authentication: CLI
        # client_id: <your-client-id>        # Required for SPN
        # tenant_id: <your-tenant-id>        # Required for SPN
        # client_secret: <your-client-secret> # Required for SPN

        # Fabric Environment (optional)
        # environmentId: <your-environment-id>

        # Session management
        reuse_session: true
        session_idle_timeout: "30m"
        # session_id_file: ./livy-session-id.txt  # Default path

        # Timeouts
        connect_retries: 1
        connect_timeout: 10
        http_timeout: 120                   # Seconds per HTTP request
        session_start_timeout: 600          # Max wait for session start (10 min)
        statement_timeout: 3600             # Max wait for statement result (1 hour)
        poll_wait: 10                       # Seconds between session start polls
        poll_statement_wait: 5              # Seconds between statement result polls

        # Retry & Shortcuts
        retry_all: true
        # create_shortcuts: false
        # shortcuts_json_str: '<json-string>'

        # Spark configuration (optional)
        # spark_config:
        #   name: "my-spark-session"
        #   spark.executor.memory: "4g"
```

In this mode:
- Tables are referenced using three-part naming: `lakehouse.schema.table_name`
- The `schema` field specifies the target schema within the lakehouse
- dbt's `generate_schema_name` and `generate_database_name` macros are lakehouse-aware
- Schemas are created automatically via `CREATE DATABASE IF NOT EXISTS lakehouse.schema`
- Incremental models use persisted staging tables (instead of temp views) to work around Spark's `REQUIRES_SINGLE_PART_NAMESPACE` limitation

### Schema Detection

The adapter detects whether a lakehouse has schemas enabled using two complementary mechanisms:

1. **Runtime detection (Fabric REST API):** During `connection.open()`, the adapter calls the Fabric REST API to fetch lakehouse properties. If the response contains `defaultSchema`, the lakehouse is treated as schema-enabled and three-part naming is used.

2. **Parse-time detection (profile heuristic):** During manifest parsing (before any connection is opened), the adapter checks whether `schema` differs from `lakehouse` in your profile. When they differ (e.g., `lakehouse: bronze`, `schema: dbo`), the adapter infers schema-enabled mode. This ensures correct schema resolution at compile time.

> **Important:** For schema-enabled lakehouses, always set `schema` to a value **different** from `lakehouse` in your profile (e.g., `schema: dbo`). If `schema` equals `lakehouse`, the adapter cannot distinguish schema-enabled from non-schema mode at parse time, and the lakehouse name will be used as the schema name instead.

| Lakehouse Type | `lakehouse` | `schema` | Naming |
|---|---|---|---|
| Without schema | `my_lakehouse` | `my_lakehouse` | `my_lakehouse.table_name` |
| With schema | `my_lakehouse` | `dbo` | `my_lakehouse.dbo.table_name` |

### Cross-Lakehouse Writes

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

### Configuration Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `type` | string | — | Must be `fabricspark` |
| `method` | string | `livy` | Connection method |
| `endpoint` | string | `https://api.fabric.microsoft.com/v1` | Fabric API endpoint URL |
| `workspaceid` | string | — | Fabric workspace UUID |
| `lakehouseid` | string | — | Lakehouse UUID |
| `lakehouse` | string | — | Lakehouse name |
| `schema` | string | — | Schema name. Must equal `lakehouse` for non-schema lakehouses, must differ from `lakehouse` for schema-enabled (e.g., `dbo`) |
| `threads` | int | `1` | Number of threads for parallel execution |
| **Authentication** | | | |
| `authentication` | string | `CLI` | Auth method: `CLI`, `SPN`, or `fabric_notebook` |
| `client_id` | string | — | Service principal client ID (SPN only) |
| `tenant_id` | string | — | Azure AD tenant ID (SPN only) |
| `client_secret` | string | — | Service principal secret (SPN only) |
| `accessToken` | string | — | Direct access token (optional) |
| **Environment** | | | |
| `environmentId` | string | — | Fabric Environment ID for Spark configuration |
| `spark_config` | dict | `{}` | Spark session configuration (must include `name` key) |
| **Session Management** | | | |
| `reuse_session` | bool | `false` | Keep Livy sessions alive for reuse across runs |
| `session_id_file` | string | `./livy-session-id.txt` | Path to file storing session ID for reuse |
| `session_idle_timeout` | string | `30m` | Livy session idle timeout (e.g. `30m`, `1h`) |
| **Timeouts & Polling** | | | |
| `connect_retries` | int | `1` | Number of connection retries |
| `connect_timeout` | int | `10` | Connection timeout in seconds |
| `http_timeout` | int | `120` | Seconds per HTTP request to Fabric API |
| `session_start_timeout` | int | `600` | Max seconds to wait for session start |
| `statement_timeout` | int | `3600` | Max seconds to wait for statement result |
| `poll_wait` | int | `10` | Seconds between session start polls |
| `poll_statement_wait` | int | `5` | Seconds between statement result polls |
| **Other** | | | |
| `retry_all` | bool | `false` | Retry all operations on failure |
| `create_shortcuts` | bool | `false` | Enable Fabric shortcut creation |
| `shortcuts_json_str` | string | — | JSON string defining shortcuts |
| `livy_mode` | string | `fabric` | `fabric` for Fabric cloud, `local` for local Livy |
| `livy_url` | string | `http://localhost:8998` | Local Livy URL (local mode only) |

### Authentication Modes

| Mode | Value | Use Case | Required Fields |
|------|-------|----------|-----------------|
| **Azure CLI** | `CLI` | Local development. Uses `az login` credentials. | None (run `az login` first) |
| **Service Principal** | `SPN` | CI/CD and automation. Uses Azure AD app registration. | `client_id`, `tenant_id`, `client_secret` |
| **Fabric Notebook** | `fabric_notebook` | Running dbt inside a Fabric notebook. Uses `notebookutils.credentials`. | None (runs in Fabric runtime) |

### Materialized Lake Views

[Materialized lake views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/overview-materialized-lake-view) are a Fabric-native construct that materializes a SQL query as a Delta table in your lakehouse, with automatic lineage-based refresh managed by Fabric.

#### Prerequisites

- Schema-enabled lakehouse
- Fabric Runtime 1.3+
- Source tables must be Delta tables

#### Basic Usage

```sql
-- models/silver/silver_cleaned_orders.sql
{{ config(
    materialized='materialized_lake_view',
    database='silver',
    schema='dbo'
) }}

SELECT
    o.order_id,
    o.product_id,
    p.product_name,
    o.quantity,
    p.price,
    o.quantity * p.price AS revenue
FROM {{ ref('bronze_orders') }} o
JOIN {{ ref('bronze_products') }} p
    ON o.product_id = p.product_id
```

#### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `materialized` | string | — | Must be `'materialized_lake_view'` |
| `database` | string | target lakehouse | Target lakehouse for cross-lakehouse writes |
| `schema` | string | target schema | Target schema within the lakehouse |
| `partitioned_by` | list | — | Columns to partition the MLV by |
| `mlv_comment` | string | — | Description stored with the MLV definition |
| `mlv_constraints` | list | `[]` | Data quality constraints (see below) |
| `tblproperties` | dict | — | Key-value metadata properties |
| `enable_cdf` | bool | `true` | Auto-enable Change Data Feed on source tables |
| `mlv_on_demand` | bool | `false` | Trigger immediate refresh after creation |
| `mlv_schedule` | dict | — | Schedule config for periodic refresh (see below) |

#### Data Quality Constraints

```sql
{{ config(
    materialized='materialized_lake_view',
    mlv_constraints=[
        {"name": "valid_quantity", "expression": "quantity > 0", "on_mismatch": "DROP"},
        {"name": "valid_price", "expression": "price >= 0", "on_mismatch": "FAIL"}
    ]
) }}
```

Each constraint has:
- `name` — Constraint identifier
- `expression` — Boolean expression each row must satisfy
- `on_mismatch` — `DROP` (silently remove violating rows) or `FAIL` (stop refresh with error, default)

#### Change Data Feed

The adapter automatically enables [Change Data Feed](https://learn.microsoft.com/en-us/azure/databricks/delta/delta-change-data-feed) (CDF) on all upstream source tables referenced via `ref()` before creating the MLV. This enables [optimal incremental refresh](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/refresh-materialized-lake-view). To disable:

```sql
{{ config(
    materialized='materialized_lake_view',
    enable_cdf=false
) }}
```

#### On-Demand Refresh

Trigger an immediate MLV lineage refresh after creation:

```sql
{{ config(
    materialized='materialized_lake_view',
    mlv_on_demand=true
) }}
```

This calls the Fabric Job Scheduler API:
```
POST /v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}/jobs/RefreshMaterializedLakeViews/instances
```

#### Scheduled Refresh

Create or update a periodic refresh schedule. The adapter uses the [Fabric Job Scheduler API](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/materialized-lake-views-public-api) to manage schedules. Only one active schedule per lakehouse lineage is supported — the adapter automatically updates an existing schedule if one is found.

**Cron schedule** (interval in minutes):

```sql
{{ config(
    materialized='materialized_lake_view',
    mlv_schedule={
        "enabled": true,
        "configuration": {
            "startDateTime": "2026-04-10T00:00:00",
            "endDateTime": "2026-12-31T23:59:59",
            "localTimeZoneId": "Central Standard Time",
            "type": "Cron",
            "interval": 10
        }
    }
) }}
```

**Daily schedule** (specific times):

```sql
{{ config(
    materialized='materialized_lake_view',
    mlv_schedule={
        "enabled": true,
        "configuration": {
            "startDateTime": "2026-04-10T00:00:00",
            "endDateTime": "2026-12-31T23:59:59",
            "localTimeZoneId": "Central Standard Time",
            "type": "Daily",
            "times": ["06:00", "18:00"]
        }
    }
) }}
```

**Weekly schedule** (specific days and times):

```sql
{{ config(
    materialized='materialized_lake_view',
    mlv_schedule={
        "enabled": true,
        "configuration": {
            "startDateTime": "2026-04-10T00:00:00",
            "endDateTime": "2026-12-31T23:59:59",
            "localTimeZoneId": "Central Standard Time",
            "type": "Weekly",
            "weekdays": ["Monday", "Wednesday", "Friday"],
            "times": ["08:00"]
        }
    }
) }}
```

#### Full Example with All Options

```sql
{{ config(
    materialized='materialized_lake_view',
    database='gold',
    schema='dbo',
    partitioned_by=['product_type'],
    mlv_comment='Product sales summary with quality checks',
    mlv_constraints=[
        {"name": "positive_revenue", "expression": "total_revenue >= 0", "on_mismatch": "DROP"}
    ],
    tblproperties={"quality_tier": "gold"},
    enable_cdf=true,
    mlv_on_demand=true
) }}

SELECT
    product_id,
    product_name,
    product_type,
    SUM(quantity) AS total_quantity_sold,
    SUM(revenue) AS total_revenue
FROM {{ ref('silver_order_items') }}
GROUP BY product_id, product_name, product_type
```

Generated SQL:
```sql
CREATE OR REPLACE MATERIALIZED LAKE VIEW gold.dbo.product_sales_summary
(
    CONSTRAINT positive_revenue CHECK (total_revenue >= 0) ON MISMATCH DROP
)
PARTITIONED BY (product_type)
COMMENT 'Product sales summary with quality checks'
TBLPROPERTIES ("quality_tier"="gold")
AS
SELECT ...
```

#### Limitations

- **No ALTER on definition** — Changing the SELECT query, constraints, or partitioning requires drop + recreate. The adapter uses `CREATE OR REPLACE` which handles this automatically.
- **Only RENAME via ALTER** — `ALTER MATERIALIZED LAKE VIEW ... RENAME TO ...` is the only supported ALTER operation.
- **No DML** — `INSERT`, `UPDATE`, `DELETE` are not supported on MLVs.
- **No UDFs** — User-defined functions are not supported in the SELECT query.
- **No time-travel** — `VERSION AS OF` / `TIMESTAMP AS OF` syntax is not supported.
- **No temp views as sources** — The SELECT query can reference tables and other MLVs, but not temporary views.
- **Schedule is per-lakehouse** — One active schedule per lakehouse lineage, not per MLV.

## Reporting bugs and contributing code

- Want to report a bug or request a feature? Let us know on [Slack](http://community.getdbt.com/), or open [an issue](https://github.com/microsoft/dbt-fabricspark/issues/new)
- Want to help us build dbt? Check out the [Contributing Guide](https://github.com/microsoft/dbt-fabricspark/blob/HEAD/CONTRIBUTING.md)

## Join the dbt Community

- Be part of the conversation in the [dbt Community Slack](http://community.getdbt.com/)
- Read more on the [dbt Community Discourse](https://discourse.getdbt.com)

## Code of Conduct

Everyone interacting in the dbt project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [dbt Code of Conduct](https://community.getdbt.com/code-of-conduct).
