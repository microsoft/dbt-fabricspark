# Changelog

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

### CI/CD

- Renamed `main.yml` to `ci.yml` with code quality checks (ruff linting), unit test matrix across Python 3.9–3.13, and build verification.
- Added `integration.yml` workflow with dynamic Lakehouse and Environment provisioning, Starter Pool compute, and OIDC service principal authentication for PR-triggered integration testing.
