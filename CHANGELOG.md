# Changelog

## v1.10.0

### New Features

- **Cross-workspace 4-part naming** — models can now read and write to relations in another Fabric workspace via `workspace.lakehouse.schema.identifier`. Set `workspace_name` in a model's `config()` to read across workspaces (#167) or to materialize `table` and `incremental` models cross-workspace via CTAS and `MERGE INTO` (#168). Requires schema-enabled lakehouses; the adapter auto-creates the remote schema on first run.

### Bug Fixes

- Fixed `dbt run --full-refresh` on incremental Delta models failing with `TABLE_OR_VIEW_ALREADY_EXISTS` when `file_format` was not explicitly set — incremental now always drops the existing relation before recreating on full-refresh (#156)
- Fixed mixed-case schema names being lowercased during relation rendering by quoting `schema` in `FabricSparkQuotePolicy` (#159)
- Fixed Livy session death (HTTP 404) during query execution to trigger transparent reconnect instead of a hard failure (#159)
- Added missing `LivyCursor.fetchmany()` and `LivySessionConnectionWrapper.fetchone()` to complete the DBAPI 2.0 cursor interface, and reset `_fetch_index` on `execute()` so re-used cursors no longer return `None` after the first query (#159)
- Added retry/backoff to `create_session()` for transient HTTP 404/5xx (Livy not yet available right after lakehouse provisioning) and tolerate transient `RequestException`/`JSONDecodeError` in `wait_for_session_start()` polling (#159)
- Fixed retry warning logs that were silently dropping error details due to an invalid `message=` kwarg (#159)
- Added `REFRESH TABLE` before assertions to prevent metastore flake in tests (2772fda)

---

## v1.9.6

### Bug Fixes

- Fixed cross-lakehouse snapshot writes — snapshots now honor the user's `database` config instead of always writing to the default lakehouse (#96)
- Fixed `ApproximateMatchError` on incremental reruns when lakehouse names contain mixed casing (#94)
- Fixed single-quote escaping in seed values (e.g., `Cote d'Ivoire`) that caused Spark parse errors (#95)
- Fixed `dbt docs generate` failing with multiple lakehouses by removing an unnecessary single-database guard (#84)
- Fixed relation type detection for `MATERIALIZED_LAKE_VIEW` in `show table extended` output and corrected the `DROP` SQL generation (#106)

### Improvements

- Moved `azure-cli` to an optional dependency (`pip install dbt-fabricspark[cli]`) to resolve install conflicts with `azure-cosmos>=4.0` in environments like Fabric Managed Airflow (#149)
- Hardened MLV API against capacity throttling — retries `Failed` jobs with throttle error codes and uses adaptive backoff on sustained 429s (#146)
- Increased Livy session creation timeout and added retry/jitter for high-concurrency CI environments (#146)

### Infrastructure

- Added VSCode Devcontainer walkthrough for new developers (#93)
- Parallelized functional test suite (~7× faster) with declarative YAML scheduler and Nx build system (#87)
- Added branch-aware workspace nuke for safe concurrent CI runs (#98)
- Added GitHub automation tools (`sync-main`, `nudge`) for Copilot PR management (#109)
- Consolidated Dependabot dependency bumps (#152)

---

## v1.9.5

### Materialized Lake View Support

#### New materialization: `materialized_lake_view`

dbt-fabricspark now supports [Materialized Lake Views](https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/materialized-lake-views) as a first-class materialization. MLVs are precomputed, incrementally-maintained views in Fabric lakehouses that accelerate queries over Delta tables without manual refresh pipelines.

**Requirements:**

- Fabric Runtime 1.3+ (Apache Spark ≥ 3.5)
- Schema-enabled lakehouse

**Model configuration:**

```sql
{{ config(
    materialized='materialized_lake_view',
    database='my_lakehouse',
    schema='dbo',
    mlv_on_demand=true,
    mlv_schedule={
        "enabled": true,
        "configuration": {
            "startDateTime": "2026-04-10T00:00:00",
            "endDateTime": "2027-04-10T00:00:00",
            "localTimeZoneId": "Central Standard Time",
            "type": "Daily",
            "times": ["06:00"]
        }
    },
    mlv_comment='Customer summary refreshed daily',
    partitioned_by=['region'],
    mlv_constraints=[
        {"name": "amount_positive", "expression": "amount > 0", "on_mismatch": "DROP"}
    ],
    tblproperties={"delta.autoOptimize.optimizeWrite": "true"}
) }}

select * from {{ ref('orders') }}
```

**Config options:**

| Option            | Type   | Required                                          | Description                                                      |
| ----------------- | ------ | ------------------------------------------------- | ---------------------------------------------------------------- |
| `mlv_on_demand`   | bool   | At least one of `mlv_on_demand` or `mlv_schedule` | Trigger an immediate refresh after creation                      |
| `mlv_schedule`    | dict   | At least one of `mlv_on_demand` or `mlv_schedule` | Schedule config for periodic refresh. Must include `endDateTime` |
| `mlv_comment`     | string | No                                                | Description added to the view                                    |
| `partitioned_by`  | list   | No                                                | Partition columns                                                |
| `mlv_constraints` | list   | No                                                | CHECK constraints with optional `on_mismatch` (DROP or FAIL)     |
| `tblproperties`   | dict   | No                                                | Delta table properties                                           |

---

#### Automatic Change Data Feed (CDF) enablement

MLVs require Change Data Feed on all upstream Delta tables. The adapter automatically enables CDF on every source table before creating the view:

```sql
ALTER TABLE <source> SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
```

This is always-on and not user-configurable.

---

#### On-demand refresh with job polling

When `mlv_on_demand: true`, the adapter triggers an immediate refresh via the Fabric Job Scheduler API and polls until the job reaches a terminal status:

1. `POST .../jobs/RefreshMaterializedLakeViews/instances` → 202 Accepted
2. Extract job instance ID from `Location` header
3. Poll `GET .../jobs/instances/{jobInstanceId}` using `poll_statement_wait` interval (default: 5s)
4. Wait up to `statement_timeout` (default: 3600s)
5. Return on `Completed`, raise `MLVApiError` on `Failed`, `Cancelled`, or `Deduped`

Terminal statuses follow the Fabric `ItemJobStatus` enum: `NotStarted`, `InProgress`, `Completed`, `Failed`, `Cancelled`, `Deduped`.

---

#### Schedule management (create / update / delete)

When `mlv_schedule` is provided, the adapter creates or updates a refresh schedule via the Fabric REST API. The operation is idempotent — if a schedule already exists, it is updated in place.

Supported schedule types:

- **Cron** — `interval` in minutes
- **Daily** — list of `times` (e.g., `["06:00", "18:00"]`)
- **Weekly** — `weekdays` and `times`

The `endDateTime` field is mandatory in the schedule configuration. The adapter validates its presence before calling the API and raises a clear error if missing.

---

#### Automatic lakehouse ID resolution

The adapter resolves the lakehouse name (from `database` config or `target.lakehouse`) to a lakehouse ID automatically via `GET /v1/workspaces/{workspaceId}/lakehouses`. Results are cached per workspace for the duration of the run. No manual `mlv_lakehouse_id` configuration is required.

---

#### Preflight validation (connection open)

MLV prerequisites are validated eagerly at connection open time (after Spark version detection). The adapter checks:

1. **Not running in local/Docker mode** — MLV requires Fabric Runtime
2. **Spark version ≥ 3.5** — checked via `SELECT split(version(), ' ')[0]`
3. **Schema-enabled lakehouse** — detected automatically on connection open

If any check fails, a warning is logged immediately and the error is cached. When an MLV model executes, it reads the cached error and fails instantly with a clear message — no wasted time running models that cannot succeed. Non-MLV projects are completely unaffected.

---

#### Delta source validation

At model execution time (before `CREATE OR REPLACE`), the adapter checks that all upstream tables referenced by the MLV are Delta format. Non-Delta sources (e.g., views, CSV tables) cause an immediate model failure with a descriptive error.

---

#### REST API error handling with retries

All Fabric REST API calls use automatic retries with exponential backoff:

- **3 attempts** per operation
- **Exponential backoff:** 2s, 4s, 8s between retries
- **Retryable:** HTTP 429, 500, 502, 503, 504, connection errors, timeouts
- **Non-retryable:** HTTP 4xx client errors (except 429)

Errors surface as `MLVApiError` (extends `DbtRuntimeError`) with the operation name, HTTP status, and parsed Fabric error details. Failed API calls always fail the model.

---

## v1.9.3

### Session Lifecycle & Stability

#### Livy sessions terminated between dbt phases causing failures

**Problem:** During a single `dbt run`, dbt executes multiple phases (e.g., compilation, execution, cleanup). The adapter's `cleanup_all()` method was terminating the Livy session between phases, forcing a new session to be created for subsequent phases. This caused unnecessary session churn and intermittent failures when the new session could not be created in time.

**Fix:** `cleanup_all()` no longer kills the active Livy session between phases. Sessions are only terminated at process exit via an `atexit` handler, ensuring a single session is reused throughout the entire dbt invocation.

---

#### Livy sessions not reusable across dbt runs in Fabric mode

**Problem:** Every `dbt run` in Fabric mode created a brand-new Livy session and destroyed it on exit. In development workflows, this added significant startup overhead (30–90 seconds per run) as each invocation waited for a new Spark session to initialize on the Fabric Starter Pool.

**Fix:** A new `reuse_session` credential flag allows sessions to persist across dbt runs. When enabled, the adapter writes the active session ID to a file and reattaches to it on the next run if the session is still alive. Fabric automatically reclaims idle sessions after the configured timeout.

**Configuration:**

```yaml
# profiles.yml
my_fabric_profile:
  target: dev
  outputs:
    dev:
      type: fabricspark
      method: livy
      # ... other settings ...
      reuse_session: true                          # Keep session alive across runs (default: false)
      session_idle_timeout: "30m"                  # How long Fabric keeps an idle session (default: "30m")
      session_id_file: "/path/to/session-id.txt"   # Custom file path (default: ./livy-session-id.txt)
```

---

#### Infinite polling loops when Livy becomes unresponsive

**Problem:** The adapter polled indefinitely for session startup and statement completion. If Fabric or the Spark cluster became unresponsive, dbt would hang forever without error.

**Fix:** All polling loops are now bounded by configurable deadlines. The adapter raises a clear error when a timeout is exceeded. Statement result polling also handles `error`, `cancelled`, and `cancelling` states explicitly instead of continuing to poll.

**Configuration:**

```yaml
# profiles.yml — timeout tuning
my_fabric_profile:
  target: dev
  outputs:
    dev:
      type: fabricspark
      method: livy
      # ... other settings ...
      http_timeout: 120               # HTTP request timeout in seconds (default: 120)
      session_start_timeout: 600      # Max wait for session to become idle in seconds (default: 600)
      statement_timeout: 3600         # Max wait for a statement to complete in seconds (default: 3600)
      poll_wait: 10                   # Polling interval for session state in seconds (default: 10)
      poll_statement_wait: 5          # Polling interval for statement results in seconds (default: 5)
```

---

#### HTTP 500 errors from Fabric cause immediate failures

**Problem:** Transient HTTP 500 errors from the Fabric Livy API caused the adapter to fail immediately, even for errors that would resolve on retry.

**Fix:** Both `_submitLivyCode` and `_getLivyResult` now retry on HTTP 5xx responses using exponential backoff (3 attempts, backoff intervals of 5s, 10s, 20s). Query execution also retries on known transient error patterns (timeout, throttling, connection reset, etc.) with capped exponential backoff up to 60 seconds.

---

### Security

#### Credentials exposed in logs and error messages

**Problem:** When the adapter logged connection details or raised exceptions, sensitive fields such as `client_secret` and access tokens could appear in plaintext in log files and terminal output.

**Fix:** The `FabricSparkCredentials.__repr__` method now masks `client_secret` and `accessToken` fields, replacing their values with `***` in all log output.

---

#### No validation on workspace and lakehouse identifiers

**Problem:** The `workspaceid` and `lakehouseid` fields accepted arbitrary strings. Malformed or malicious values could result in unexpected API paths being constructed.

**Fix:** Both fields are now validated as proper UUIDs during credential initialization. Invalid values raise an immediate configuration error. The Fabric endpoint is also validated to require HTTPS and must match a known Fabric domain pattern. Unrecognized domains trigger a security warning in logs.

---

#### Race conditions in token refresh under concurrent threads

**Problem:** When multiple threads attempted to refresh the authentication token simultaneously, overlapping refresh calls could cause token corruption or redundant API calls.

**Fix:** Token refresh is now protected by a global `_token_lock`. The lock ensures only one thread refreshes the token while others wait and reuse the refreshed value.

---

### Lakehouse Schema Support

#### Three-part naming fails on non-schema-enabled lakehouses

**Problem:** Lakehouses created without schema support use two-part naming (`schema.table`), while schema-enabled lakehouses require three-part naming (`database.schema.table`). The adapter had no way to detect which mode to use, causing SQL generation errors when the wrong naming convention was applied.

**Fix:** On connection open, the adapter calls the Fabric REST API (`GET /v1/workspaces/{workspaceId}/lakehouses/{lakehouseId}`) and checks for the `properties.defaultSchema` property. If present, the lakehouse is schema-enabled and three-part naming is used. This detection is automatic and requires no user configuration.

The adapter also validates schema configuration:

- **Schema-enabled lakehouse:** The `schema` value must differ from the lakehouse name (e.g., use `dbo`).
- **Non-schema lakehouse:** The `schema` is silently set to the lakehouse name for correct SQL generation.

---

#### Incremental models fail with `REQUIRES_SINGLE_PART_NAMESPACE` on schema-enabled lakehouses

**Problem:** The incremental materialization used temp views (`CREATE TEMPORARY VIEW`) for staging data before merge/insert. On schema-enabled lakehouses, temp views that reference three-part table names (`lakehouse.schema.table`) triggered Spark's `REQUIRES_SINGLE_PART_NAMESPACE` error because the `V2SessionCatalog` re-resolves the underlying tables during DML execution and cannot handle two-part namespaces.

**Fix:** For schema-enabled lakehouses, the incremental materialization now creates a **persisted view** (`CREATE VIEW`) with full three-part naming instead of a temp view. The persisted view's references are resolved at creation time, avoiding the namespace error during DML. The staging view is dropped after the merge/insert completes. Non-schema lakehouses continue to use temp views.

---

#### `CREATE DATABASE` with bare schema name corrupts Spark namespace resolver

**Problem:** `ensure_database_exists` emitted `CREATE DATABASE IF NOT EXISTS <schema>` with a single-part name. On schema-enabled lakehouses, this corrupted Spark's namespace resolver for the remainder of the session, causing cascading failures.

**Fix:** `ensure_database_exists` now accepts an optional `database` parameter. When provided, it prepends the lakehouse name to produce a two-part `CREATE DATABASE IF NOT EXISTS lakehouse.schema` statement. All materializations (table, view, seed, snapshot, incremental) now pass `database=` to this macro.

---

#### Snapshot merge fails on schema-enabled lakehouses

**Problem:** The snapshot materialization created a temp staging table/view with unqualified naming. On schema-enabled lakehouses, the `MERGE INTO` statement could not resolve the staging relation against the fully-qualified target table.

**Fix:** The snapshot staging relation is now created as a persisted view inheriting `database` and `schema` from the target relation, ensuring proper three-part naming. The staging view is dropped after the snapshot merge completes.

---

#### Schema and database name generation not lakehouse-aware

**Problem:** `generate_schema_name` and `generate_database_name` did not account for lakehouse type, potentially generating invalid namespace values.

**Fix:**

- **Non-schema lakehouses:** `generate_schema_name` always returns the lakehouse name (the only valid namespace).
- **Schema-enabled lakehouses:** Uses dbt's standard `generate_schema_name_for_env` logic.
- `generate_database_name` always returns the target lakehouse name.

---

### Fabric Environment Support

#### No way to specify a shared Spark environment for sessions

**Problem:** Users who configured shared Spark environments (with custom libraries, Spark settings, or pool configurations) in Fabric had no way to tell the dbt adapter to use a specific environment when creating Livy sessions.

**Fix:** A new `environmentId` credential field injects the environment identifier into the Livy session's Spark configuration, telling Fabric to launch the session using that environment's settings.

**Configuration:**

```yaml
# profiles.yml
my_fabric_profile:
  target: dev
  outputs:
    dev:
      type: fabricspark
      method: livy
      # ... other settings ...
      environmentId: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"  # Fabric Environment UUID
```

---

### Incremental Materialization

#### `insert_overwrite` strategy fails with `[NON_PARTITION_COLUMN]` error

**Problem:** The `insert_overwrite` incremental strategy emitted a Hive-style `PARTITION (partition_column)` clause in the `INSERT OVERWRITE` SQL. Delta Lake tables on Fabric do not support this syntax, causing a `[NON_PARTITION_COLUMN]` error on every incremental run using this strategy.

**Fix:** Removed the `{{ partition_cols(label="partition") }}` call from `get_insert_overwrite_sql`. The `INSERT OVERWRITE TABLE ... SELECT` statement now executes without the unsupported `PARTITION` clause, which is the correct syntax for Delta tables on Spark.

---

### View Materialization

#### Replacing a table with a view fails without explicit drop

**Problem:** When changing a model's materialization from `table` to `view`, the existing table was not dropped before the `CREATE VIEW` was issued, causing the statement to fail because the relation already existed as a table.

**Fix:** Added a `fabricspark__handle_existing_table` override in the view materialization macro that drops the existing table before creating the view.

---

### Relation Handling

#### Invalid relation types cause adapter crashes

**Problem:** If the adapter received a relation with an unexpected `type` value (e.g., from metadata or a corrupted manifest), it could crash with an unhandled exception during relation construction.

**Fix:** `FabricSparkRelation.from_dict()` now validates relation types against `_VALID_RELATION_TYPES` and sanitizes invalid values to `None` instead of crashing.

---

### Connection Management

#### `delete_session` referenced wrong variable

**Problem:** The `delete_session` method called `response.raise_for_status()` on the `urllib.response` module import rather than the actual HTTP response object, masking real HTTP errors during session cleanup.

**Fix:** Changed to `res.raise_for_status()` to reference the correct HTTP response. Also removed the unused `from urllib import response` import.

---

#### `is_valid_session` crashes on HTTP failure

**Problem:** When the Fabric API returned an HTTP error during session validation, the `is_valid_session` method raised an unhandled exception instead of gracefully returning `False`.

**Fix:** Wrapped the HTTP call in a try/except block. Any exception during session validation now returns `False`, allowing the adapter to proceed with creating a new session.

---

#### `fetchone` had O(n²) performance on large result sets

**Problem:** The `fetchone` method used `list.pop(0)` to retrieve each row, which copies the entire remaining list on every call. For large result sets, this created O(n²) total overhead.

**Fix:** Replaced with an index-based iterator (`_fetch_index`) that advances through the list in O(1) per call.

---

### Dependencies

- Added `requests>=2.28.0` as an explicit dependency (previously relied on transitive installation).

### Testing

- Added runtime schema-enabled lakehouse detection in `conftest.py` via the Fabric REST API, allowing the same test suite to run against both schema-enabled and non-schema lakehouses without configuration changes.
- Test fixtures automatically set `schema` to a unique per-class value (schema-enabled) or the lakehouse name (non-schema) based on the detected lakehouse type.
- Removed standalone `test_livy_dml.py` manual test script with hardcoded workspace/lakehouse IDs.

### CI/CD

- Renamed `main.yml` to `ci.yml` with code quality checks (ruff linting), unit test matrix across Python 3.9–3.13, and build verification.
- Added `integration.yml` workflow with dynamic Lakehouse and Environment provisioning, Starter Pool compute, and OIDC service principal authentication for PR-triggered integration testing.
