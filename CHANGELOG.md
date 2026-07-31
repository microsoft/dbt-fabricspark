# Changelog

## v1.13.0

### Features

- The adapter now runs `OPTIMIZE` on the target relation after every `table`, `incremental` and `snapshot` build, so downstream joins are no longer slowed down by the small files each write leaves behind. `OPTIMIZE` is cheap when there is nothing to compact, and running it inside the materialization means it is scheduled by dbt's own thread pool rather than by a hand-rolled `post_hook`. Only Delta relations are optimized — `view`, `ephemeral`, `seed`, `materialized_lake_view` and shallow `clone` materializations are never touched, and a non-Delta `file_format` is skipped. Because file compaction is maintenance rather than part of the model's contract, a failed `OPTIMIZE` logs a warning and the model still succeeds; it is also exempt from the connection retry loop, so a failure under `retry_all` is skipped immediately instead of stalling the run through every backoff. The feature can be disabled at three levels, highest precedence first: the `DBT_FABRICSPARK_SKIP_OPTIMIZE` environment variable (an unconditional kill switch that needs no project edits), `{{ config(auto_optimize=false) }}` on an individual model, and a new `auto_optimize: false` profile flag. Both the `optimize` and `get_optimize_sql` macros are dispatched, so projects can override the emitted SQL. **This is a behavior change**: existing Delta models gain one extra Spark job per run — set `auto_optimize: false` in `profiles.yml` to keep the previous behavior. ([#254](https://github.com/microsoft/dbt-fabricspark/issues/254))

### Fixes

- Fixed the `merge_with_schema_evolution` incremental option leaking its session state into every later statement. The option is applied by setting the `spark.databricks.delta.schema.autoMerge.enabled` Spark conf, which was switched on before the merge but never switched back. Because Livy sessions are reused across models, across dbt invocations and — with `reuse_session` — across runs, one model using the option silently turned on schema evolution for every subsequent `MERGE` in that session. The worst symptom was silent snapshot corruption: a snapshot merge would absorb dbt's internal `dbt_change_type` and `dbt_unique_key` staging columns into the snapshot table, and the *next* snapshot run would then fail permanently with `[AMBIGUOUS_REFERENCE] Reference \`snapshotted_data\`.\`dbt_unique_key\` is ambiguous`. The conf is now captured before the merge and restored afterwards, and snapshots explicitly pin it off for the duration of their own merge, so a snapshot can no longer be corrupted by an unrelated model. A conf set deliberately through `spark_config.conf` is preserved.

---

## v1.12.12

### Features

- Added an opt-in `quote_identifiers` profile flag (default `false`) for case-sensitive table names. When enabled, the adapter backtick-quotes every relation's identifier so Fabric Spark preserves casing instead of folding to lowercase. Case-sensitive resolution also requires `spark.sql.caseSensitive: "true"` under `spark_config.conf` (session-wide, also affects columns); the adapter warns at connection time if it's missing. Defaults to off, so existing projects render byte-identical SQL. ([#251](https://github.com/microsoft/dbt-fabricspark/issues/251))

- Added advanced `merge` configuration options to the `merge` incremental strategy (`file_format: delta`), giving models full control over the generated `MERGE INTO` statement. Nine new optional `config()` keys are recognized: `target_alias` / `source_alias` (rename the target/source relations in the statement and in your conditions), `matched_condition` / `not_matched_condition` / `not_matched_by_source_condition` (append `AND (<cond>)` to the respective `WHEN` clauses), `skip_matched_step` / `skip_not_matched_step` (omit the `WHEN MATCHED` or `WHEN NOT MATCHED` clause), `not_matched_by_source_action` (emit a `WHEN NOT MATCHED BY SOURCE` clause when set to `delete` or `update set ...` — e.g. to propagate source deletes for CDC), and `merge_with_schema_evolution` (add new source columns to the target automatically). Schema evolution is applied by setting the standard Delta `spark.databricks.delta.schema.autoMerge.enabled` session setting before the merge rather than emitting a proprietary `WITH SCHEMA EVOLUTION` SQL clause, so it works on open-source Delta Lake 3.2 (Fabric Runtime 1.3) and local Livy, which reject that clause. Every option defaults to the previous behavior, so existing `merge` models render byte-identical SQL and are fully backward compatible. The clauses require Fabric Runtime 1.3 (Spark 3.5 / Delta Lake 3.2) or newer. ([#247](https://github.com/microsoft/dbt-fabricspark/issues/247))

### Documentation

- Documented native cross-workspace `source()` resolution: setting `workspace_name` under a source's `config` in `sources.yml` renders `{{ source(...) }}`, freshness, and source tests as a 4-part `` `workspace`.`lakehouse`.`schema`.identifier `` name, mirroring model `config(workspace_name=...)`. No adapter change required — docs and unit tests only. Schema-enabled lakehouses only. ([#250](https://github.com/microsoft/dbt-fabricspark/issues/250))

---

## v1.12.11

### Features

- Added a `delete+insert` incremental strategy for `incremental` models. Set `incremental_strategy='delete+insert'` with a `unique_key` (single column or list) and `file_format: delta` to get a key-based full row-replace: on each incremental run the adapter deletes every target row whose `unique_key`(s) appear in the newly staged data and then inserts all staged rows, so matched keys are replaced wholesale instead of updated column-by-column as `merge` does. Optional `incremental_predicates` are ANDed into the delete match to scope it to a window. Like the `microbatch` strategy, the delete and insert are issued as two separate statements (Fabric Spark cannot run multiple statements per query), and the delete uses `MERGE ... WHEN MATCHED THEN DELETE` against a `SELECT DISTINCT` of the keys because Delta Lake on Fabric rejects subqueries in `DELETE` conditions (the `DISTINCT` also avoids multiple-source-row match errors on duplicate keys). Omitting `unique_key` or using a non-`delta` `file_format` raises a clear compile-time error. ([#240](https://github.com/microsoft/dbt-fabricspark/issues/240))

---

## v1.12.10

### Features

- Made the `AzureCliCredential` subprocess timeout configurable via a new optional `azure_cli_process_timeout` credential/profile field (int, default `10` — no behavior change unless set). Under high-concurrency dbt builds (many threads), the token-refresh storm in the token's last ~5 minutes spawns many concurrent `az account get-access-token` subprocesses. Occasional host CPU contention or auth-relay latency can push one `az` invocation past `azure-identity`'s default 10s subprocess timeout, killing the process and failing a dbt node with "Failed to invoke the Azure CLI". Previously the only remedy was patching the installed package at image-build time. The value is now threaded through to `AzureCliCredential(process_timeout=...)` in `livysession.py`, so operators can raise it (e.g. `azure_cli_process_timeout: 60`) from `profiles.yml`. ([#236](https://github.com/microsoft/dbt-fabricspark/issues/236))

### Bug Fixes

- Fixed `dbt source freshness` failing on Fabric Spark with "Expected a timestamp value ... but received value of type 'str' instead". Fabric's Livy statement-result API returns `timestamp`, `timestamp_ntz` and `date` columns to Python as strings, but dbt-core's freshness path (and any `run_query()` caller) expects native `datetime`/`date` objects — matching how real drivers (pyodbc/pyhive) behave in upstream dbt-spark. Both Livy cursor backends (`HighConcurrencyCursor` and `LivyCursor`) now coerce time-typed columns to native Python `datetime`/`date` using the positional column types from the Livy result `schema.fields`. Only columns Livy explicitly types as time types are touched; `None` and unparseable values are passed through unchanged, so a malformed value can never turn a successful query into a failure. ([#237](https://github.com/microsoft/dbt-fabricspark/issues/237))

---

## v1.12.9

### Bug Fixes

- Fixed schema pre-creation also creating the schema in the **session-bound** lakehouse (in addition to the configured `+database` target) during same-workspace cross-lakehouse runs. dbt's `before_run` collects the required schemas via `Relation.create_from(...).without_identifier()` while the Livy connection is still a `LazyHandle`, so `FabricSparkRelation._schemas_enabled` is still `False`. On a cold start where the profile leaves `schema` at its default (`schema == lakehouse`), `create_from` sees no schema-enabled signal and locks `include_policy.database=False` on the relation. That stale relation then reaches the inherited `create_schema`/`drop_schema`, where `relation.without_identifier()` renders an *unqualified* `create/drop database if not exists <schema>` that resolves against the session-bound lakehouse instead of the model's `+database` target — so the schema was created in both lakehouses (and, symmetrically, an unqualified `drop database ... cascade` risked dropping the wrong lakehouse's schema). `FabricSparkAdapter` now re-includes the database segment on schema-DDL relations using the same `_catalog_requires_database_scoping` rule already applied by `list_relations_without_caching`, so `create_schema`/`drop_schema` always qualify to the intended lakehouse. Cross-workspace (`workspace_name`) routing was already unaffected because a set `workspace` forces database qualification at parse time. ([#234](https://github.com/microsoft/dbt-fabricspark/issues/234))

---

## v1.12.8

### Bug Fixes

- Fixed high-concurrency Livy sessions not staying warm across `dbt` invocations when `reuse_session: true` (and `session_idle_timeout`) were set. With `high_concurrency: true` the per-thread `HighConcurrencySessionManager.disconnect()` and the process-exit `atexit` handler unconditionally issued `DELETE /highConcurrencySessions/{hc_id}`; deleting the last REPL made Fabric tear the shared underlying Livy session down immediately (flipping it from *In progress* to *Succeeded*), so `spark.livy.session.idle.timeout` never applied and every subsequent run paid a multi-minute Spark cold-start. This contradicted the documented behavior (README "High-concurrency Livy") and the singleton backend, which already keeps the session alive when `reuse_session` is set. HC teardown now honors `reuse_session`: with `reuse_session: true` the HC session is left alive so the underlying Livy session stays warm for reuse (Fabric reaps it on `session_idle_timeout`), while `reuse_session: false` keeps the previous prompt-release behavior that frees REPL slots. ([#232](https://github.com/microsoft/dbt-fabricspark/issues/232))

---

## v1.12.7

### Features

- Added `workspace_name` as an optional profile-level field in `profiles.yml`. When set on a target and the lakehouse has schemas enabled, all relations that do not set `config(workspace_name=...)` will automatically use the profile value as a 4-part name prefix (`\`workspace\`.\`lakehouse\`.\`schema\`.table`). Exposes `target.workspace_name` in Jinja for inspection in macros. Model-level `config(workspace_name=...)` still takes precedence. Silently ignored (with a warning) for non-schema-enabled lakehouses. ([#228](https://github.com/microsoft/dbt-fabricspark/issues/228))
- Security updates

---

## v1.12.6

### Bug Fixes

- Fixed a false `Compilation Error … dbt found two resources with the database representation` (`AmbiguousAliasError`) raised at `dbt parse`/`dbt compile` time when two models share the same `schema.alias` but target **different** Fabric lakehouses/workspaces via model-level `+database` / `+workspace_name`. dbt-core's `_check_resource_uniqueness` keys its dedupe map on `str(relation)`, but during parsing no Livy session is open, so `FabricSparkRelation._schemas_enabled` is still `False` and `include_policy.database` defaults to `False` — which dropped both the lakehouse and the workspace from the rendered identity, collapsing e.g. `wks_common.lh_common.dbo.d_company` and `wks_dwh.lh_dwh.dbo.d_company` to the same `` `dbo`.d_company `` key. `FabricSparkRelation.create_from` now forces the database segment into the relation identity whenever a schema-enabled signal applies (mirroring `_catalog_requires_database_scoping`: `_schemas_enabled`, `lakehouse_schemas_enabled`, the parse-time `schema != lakehouse` fallback, or a `workspace_name` set), so cross-lakehouse / cross-workspace models resolve to distinct identities while genuine same-target duplicates on non-schema lakehouses still collide as before. ([#221](https://github.com/microsoft/dbt-fabricspark/issues/221))

## v1.12.5

### Features

- Added opt-in `mlv_allow_schema_evolution` config (default `false`) for the `materialized_lake_view` materialization. When `true`, the macro emits `SET trident.artifact.type = SynapseNotebook` before `CREATE OR REPLACE MATERIALIZED LAKE VIEW` so subsequent `dbt run` cycles can change the projected columns, source entities, partitions, properties, or DQ constraints of an existing MLV without hitting `MLV_SCHEMA_MISMATCH`. ([#216](https://github.com/microsoft/dbt-fabricspark/issues/216))

### Bug Fixes

- Fix `UnexpectedJinjaBlockDeprecation` (D023) emitted by `create_table_as.sql`: a stray `{% do %}` tag inside a C-style `/* … */` comment was being parsed as a top-level Jinja block. Switched the wrapper to a Jinja comment `{# … #}`. Added a unit-test regression that scans every macro file for `unexpected_block` warnings. ([#46](https://github.com/microsoft/dbt-fabricspark/issues/46))
- Pin `dbt-core<2.0` in `tools/scripts/run-local-e2e.sh` so the local end-to-end harness keeps resolving a `fabricspark`-aware dbt-core (avoids the unrelated `2.0.0a1` alpha).
- Fix latent typo in `alter_column_set_constraints` dispatcher macro (`create_table_as.sql`): the body was missing its `{{ … }}` wrap, so the macro emitted its own source as text instead of dispatching. Added unit guards: (a) the dispatcher now renders via `adapter.dispatch` correctly, and (b) `FabricSparkRelation` cannot silently re-introduce the legacy `Cannot set database in spark!` runtime check that v1.9.3 removed to enable schema-enabled lakehouses and cross-lakehouse writes. ([#46](https://github.com/microsoft/dbt-fabricspark/issues/46))

## v1.12.4

### Bug Fixes

- Fixed `dbt docs generate` cross-attributing models across lakehouses with shared schema names. When a project wrote to multiple schema-enabled Fabric lakehouses that each defined the same schema (e.g. both `silver_lh` and `gold_lh` have a `finance` schema), the catalog enumeration tried to `DESCRIBE` silver-layer models against the gold lakehouse, raising `[TABLE_OR_VIEW_NOT_FOUND]` even though the manifest correctly resolved each model. Root cause: `FabricSparkRelation.include_policy.database` is captured at instance creation time from the class-level `_schemas_enabled` flag, which only flips to `True` inside `connections.open` — relations built earlier (during cache pre-population) locked in `database=False`, so the rendered `SHOW TABLE EXTENDED IN <schema> LIKE '*'` ran against the session-bound default catalog and the cache stored the returned tables under the wrong lakehouse key. `FabricSparkAdapter.list_relations_without_caching` now re-includes the database segment whenever the active mode requires three-part naming, and `_get_one_catalog` defensively skips any cached relation whose database does not match the catalog cell being iterated for ([#209](https://github.com/microsoft/dbt-fabricspark/issues/209))

---

## v1.12.3

### Bug Fixes

- Fixed `dbt clone` failures against Fabric in two scenarios — cloning into a target that doesn't yet exist (raised `AttributeError` on a `None` `existing_relation`) and cloning across workspaces (the destination's `workspace_name` was dropped from the rendered DDL). `clone.sql` now reads `file_format` from `config`, branches on the configured materialization, propagates `workspace_name` onto the target relation via `incorporate(workspace=...)`, and drops the destination before `SHALLOW CLONE` to avoid `DELTA_UNSUPPORTED_NON_EMPTY_CLONE` on `--full-refresh`. Re-enabled `TestSparkClonePossible` and added `TestSparkCloneCrossWorkspace` to the functional suite (#207)
- Fixed exception suppression in all six `__exit__` methods across `singleton_livy.py` and `concurrent_livy.py` — they returned `True`, silently swallowing every database error, timeout, `KeyboardInterrupt`, and programming bug raised inside their `with` blocks (so dbt could report success on a failed model and `Ctrl+C` could be ignored). All six now return `False` so exceptions propagate (#193)
- Fixed `re.sub` calls in `singleton_livy._getLivySQL` and `concurrent_livy._strip_block_comments` passing `re.DOTALL` positionally, which silently set `count=16` and capped Livy comment-stripping to 16 replacements per submitted statement. Both now use `flags=re.DOTALL` so all `/* ... */` blocks are removed before submission (#195)
- Fixed the adapter forcing `botocore` and `boto3` loggers to DEBUG at import time, which flooded the dbt log with AWS SDK request/response noise as soon as a user's project transitively imported `boto3`. The adapter has no AWS dependency; both entries are removed from the dependency-logger list (#198)
- Fixed `int_tests` authentication caching the access token with a hardcoded 2028 expiry, which bypassed all refresh checks. Expiry is now derived from the JWT `exp` claim, with a safe fallback that forces immediate refresh if the token can't be parsed (#205)

### Infrastructure

- Consolidated four near-duplicate `_parse_retry_after` implementations (two full copies in `livysession.py` / `mlv_api.py` plus thin wrappers in `singleton_livy.py` / `concurrent_livy.py`) into a single `parse_retry_after` helper in `_http_utils.py`, and replaced the deprecated `datetime.utcnow()` with timezone-aware `datetime.now(timezone.utc)` so the Fabric `until: ... (UTC)` body fallback parses cleanly under Python 3.12+ (#200)
- Removed a dead Thrift exception-handling branch in `FabricSparkConnectionManager.exception_handler`. The adapter talks to Fabric Livy over HTTP and has no Thrift dependency; the branch was a copy-paste from a `dbt-spark` ancestor and was never reachable (#203)

---

## v1.12.2

### Bug Fixes

- Fixed every fresh Livy session being forced onto an on-demand Spark cluster instead of a Fabric starter pool. `session_idle_timeout` no longer defaults to `"30m"`; the adapter now omits `spark.livy.session.idle.timeout` from the session `conf` unless the user explicitly sets a value. Fabric treats that key as session-immutable, so its mere presence (even matching the pool's own default) emitted `FallbackReasons: UserSparkConfigMismatch` and added ~3 min of cold-start per cold session (vs ~40 s on a warm starter pool). Existing profiles that set `session_idle_timeout` explicitly continue to behave as before, with the same starter-pool trade-off (#184)

## v1.12.1

### Bug Fixes

- Fixed `clustered_by` being silently ignored on Delta tables. Setting `clustered_by=['col_a', 'col_b']` (with no `buckets`) on a Delta model now emits `CREATE OR REPLACE TABLE … USING DELTA CLUSTER BY (col_a, col_b) AS SELECT …` so [Fabric Spark liquid clustering](https://learn.microsoft.com/fabric/data-engineering/liquid-clustering-delta-tables) is applied at create time. Hive bucketing (`clustered_by` + `buckets`) and non-Delta `file_format` are unchanged. `clustered_by` + `partition_by` on Delta now raises a compile-time error — the two are mutually exclusive on Delta (#187)

## v1.12.0

### New Features

- **High-concurrency Livy support** for true parallel statement execution. Each dbt thread acquires its own REPL inside one underlying Livy session via [Fabric's HC Livy API](https://learn.microsoft.com/en-us/fabric/data-engineering/high-concurrency-livy) (`/highConcurrencySessions` + `/repls/{replId}/statements`). All threads in a process share a deterministic `sessionTag` derived from `(workspaceid, lakehouseid)` when `reuse_session: true`, so Fabric snap-attaches new REPLs onto the still-warm underlying session across runs — observed **3.6× wall-clock speedup** on the 2nd run of the issue's repro (442s → 122s). Singleton mode remains available via `high_concurrency: false`; the new flag defaults to `true` for Fabric mode and is a no-op in local mode. See the new "High-concurrency Livy" section in the README for the `threads > 5` cross-REPL state table (#185, #186)

### Infrastructure

- Refactored the Livy backend behind a new `LivyBackend` ABC with two implementations — `singleton_livy.py` (existing single-session path) and `concurrent_livy.py` (new HC path) — selected at connect time by the `high_concurrency` credential. Shared auth/header/retry/lakehouse-property helpers remain in `livysession.py`; the existing class names continue to be re-exported from there for backwards compatibility with downstream importers and the test patch surface (#186)

---

## v1.11.0

### New Features

- Added a `token_credential` authentication method that loads any `azure.core.credentials.TokenCredential` implementation by dotted path via new `credential_class` and `credential_kwargs` profile fields. Enables desktop tools with custom OAuth flows, WIF in CI, and broker-vended tokens without a second `az login` or repurposing the SPN config (#177)

### Bug Fixes

- Fixed cross-workspace 4-part naming for `view`, `snapshot`, and `materialized_lake_view` materializations — they previously dropped `workspace_name` and emitted 3-part DDL against a 4-part target, failing with `Artifact not found`. Snapshot staging views (`__dbt_tmp`) are now workspace-qualified so `MERGE INTO` resolves correctly (#172, #182)
- Fixed `dbt docs generate` retry storm on missing source schemas — `[SCHEMA_NOT_FOUND]` and `[TABLE_OR_VIEW_NOT_FOUND]` are now classified as permanent Spark errors and skip the `retry_all` loop, eliminating ~120 s of waste per missing schema. `list_relations_without_caching` also matches the Spark 3.3+ bracket error format alongside the older wording (#180)

### Infrastructure

- Overhauled contribution docs with crisper local-dev guidance and clearer expectations for external vs Microsoft contributors (#181)

---

## v1.10.1

### Bug Fixes

- Fixed `dbt debug` creating multiple Livy sessions on first connect by treating HTTP `202 Accepted` from `POST /sessions` as a successful creation instead of a retryable failure (#171)

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
