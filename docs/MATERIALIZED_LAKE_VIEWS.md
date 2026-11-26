# Materialized Lake Views

[Materialized lake views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/overview-materialized-lake-view) are a Fabric-native construct that materializes a SQL query as a Delta table in your lakehouse, with automatic lineage-based refresh managed by Fabric.

## Prerequisites

- Schema-enabled lakehouse
- Fabric Runtime 1.3+
- Source tables must be Delta tables

## Basic Usage

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

## Configuration Options

| Option            | Type   | Default          | Description                                      |
| ----------------- | ------ | ---------------- | ------------------------------------------------ |
| `materialized`    | string | —                | Must be `'materialized_lake_view'`               |
| `database`        | string | target lakehouse | Target lakehouse for cross-lakehouse writes      |
| `schema`          | string | target schema    | Target schema within the lakehouse               |
| `partitioned_by`  | list   | —                | Columns to partition the MLV by                  |
| `mlv_comment`     | string | —                | Description stored with the MLV definition       |
| `mlv_constraints` | list   | `[]`             | Data quality constraints (see below)             |
| `tblproperties`   | dict   | —                | Key-value metadata properties                    |
| `enable_cdf`      | bool   | `true`           | Auto-enable Change Data Feed on source tables    |
| `mlv_on_demand`   | bool   | `false`          | Trigger immediate refresh after creation         |
| `mlv_schedule`    | dict   | —                | Schedule config for periodic refresh (see below) |

## Data Quality Constraints

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

## Change Data Feed

The adapter automatically enables [Change Data Feed](https://learn.microsoft.com/en-us/azure/databricks/delta/delta-change-data-feed) (CDF) on all upstream source tables referenced via `ref()` before creating the MLV. This enables [optimal incremental refresh](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/refresh-materialized-lake-view). To disable:

```sql
{{ config(
    materialized='materialized_lake_view',
    enable_cdf=false
) }}
```

## On-Demand Refresh

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

## Scheduled Refresh

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

## Full Example with All Options

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

## Limitations

- **No ALTER on definition** — Changing the SELECT query, constraints, or partitioning requires drop + recreate. The adapter uses `CREATE OR REPLACE` which handles this automatically.
- **Only RENAME via ALTER** — `ALTER MATERIALIZED LAKE VIEW ... RENAME TO ...` is the only supported ALTER operation.
- **No DML** — `INSERT`, `UPDATE`, `DELETE` are not supported on MLVs.
- **No UDFs** — User-defined functions are not supported in the SELECT query.
- **No time-travel** — `VERSION AS OF` / `TIMESTAMP AS OF` syntax is not supported.
- **No temp views as sources** — The SELECT query can reference tables and other MLVs, but not temporary views.
- **Schedule is per-lakehouse** — One active schedule per lakehouse lineage, not per MLV.

---
[Home](../README.md) > [Documentation](./README.md)
