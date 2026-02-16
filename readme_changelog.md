# dbt-fabricspark — Stability, Security & Crash Resilience Changelog

## Overview

This document describes the stability, security, crash-resilience, and test-suite improvements made to the `dbt-fabricspark` adapter. These changes address runtime crashes, SSL connection failures, hangs, resource leaks, and security vulnerabilities that occurred when running dbt models against Microsoft Fabric Spark via the Livy API, as well as fix all 26 pre-existing unit test failures.

---

## Root Cause Analysis

### Adapter Crashes

The adapter was crashing on larger models due to several compounding issues:

1. **Infinite polling loops** — Both `wait_for_session_start` and `_getLivyResult` used `while True` with no timeout or maximum iteration cap. If a Livy session or statement entered an unexpected state, the adapter would hang forever.
2. **No HTTP request timeouts** — Every `requests.get/post/delete` call lacked a `timeout` parameter. If the Fabric API became slow or unresponsive, calls would block indefinitely.
3. **Thread-unsafe shared state** — The global `accessToken` and class-level `LivySessionManager.livy_global_session` were mutated without any synchronization. Under dbt's parallel thread execution, this caused race conditions, duplicate session creation, and state corruption.
4. **Missing error-state handling** — The statement polling loop (`_getLivyResult`) never checked for `error` or `cancelled` states, so a failed server-side statement would cause an infinite loop.
5. **Bugs in cleanup code** — `delete_session` referenced an undefined `response.raise_for_status()` (the `urllib.response` module instead of the HTTP response variable), and `is_valid_session` crashed on HTTP failures instead of returning `False`.
6. **Resource leaks** — `release()` was a no-op with a broken signature, `close()` silently swallowed exceptions, and `cleanup_all()` had a `self`/`cls` mismatch.

### SSL EOF Errors on Large Models

Large models (running 5+ minutes) consistently failed with `SSLEOFError: [SSL: UNEXPECTED_EOF_WHILE_READING]`. Root causes:

1. **No connection pooling** — Every HTTP call used bare `requests.get()`/`requests.post()`, creating a new TCP/SSL connection each time. Stale connections were never cleaned up.
2. **No transport-level retry** — When the Fabric API load balancer terminated idle SSL connections, the adapter had no mechanism to transparently reconnect.
3. **No application-level retry on statement submission** — `_submitLivyCode` raised immediately on any connection error with no retry.
4. **Stale connection pool reuse** — Even after adding `requests.Session`, the SSL connection pool held references to dead sockets. The pool needed to be rebuilt (not just retried) after SSL EOF.
5. **Cleanup crash on SSL failure** — `disconnect()` called `is_valid_session()` which made an HTTP GET. When SSL was dead, this crashed and propagated up to dbt's `cleanup_connections()`, masking the real model error.
6. **Wrong package installed** — The dbt project's `.venv` had a different (older) copy of the adapter than the workspace. Fixes in the workspace were never executed.

### Test Suite Failures (26 tests)

The full unit test suite was failing due to:

1. **Missing `mp_context` argument** — `BaseAdapter.__init__()` in dbt-adapters ≥1.7 requires an `mp_context` (multiprocessing context) positional argument that tests were not providing.
2. **Missing `spark_config`** — `FabricSparkCredentials.__post_init__()` requires `spark_config` to contain a `"name"` key, but test profiles and credential fixtures omitted it.
3. **Wrong import path** — `test_adapter.py` imported `DbtRuntimeError` from `dbt.exceptions` instead of `dbt_common.exceptions` (moved in dbt-core ≥1.8).
4. **Wrong mock target** — `test_livy_connection` mocked `LivySessionConnectionWrapper` but the real HTTP call happens in `LivySessionManager.connect`, so the mock didn't prevent a real connection attempt.
5. **Mismatched method names in shortcut tests** — Tests called `check_exists()` but the real method is `check_if_exists_and_delete_shortcut()`. Target body assertions also missed the `"type": "OneLake"` key.
6. **Broken macro template paths** — `test_macros.py` used relative `FileSystemLoader` paths that only resolved if CWD happened to be the source root.
7. **Missing Jinja globals** — Macros in `create_table_as.sql` reference `statement`, `is_incremental`, `local_md5`, and `alter_column_set_constraints` which were undefined in the isolated test Jinja environment.
8. **Stray `unittest.skip()` call** — A bare `unittest.skip("Skipping temporarily")` at module level in `test_macros.py` did nothing (it's a decorator, not a statement).
9. **`file_format_clause` suppressed `using delta`** — The `fabricspark__file_format_clause` macro had a `file_format != 'delta'` guard that prevented emitting `using delta`, contradicting the expected test output.

---

## Changes by File

### `credentials.py`

#### Stability Fields

| Change | Detail |
|--------|--------|
| Added `http_timeout` field | Configurable timeout (seconds) for each HTTP request to the Fabric API. Default: `120` |
| Added `session_start_timeout` field | Maximum seconds to wait for a Livy session to reach `idle` state. Default: `600` (10 min) |
| Added `statement_timeout` field | Maximum seconds to wait for a Livy statement to complete. Default: `3600` (1 hour) |
| Added `poll_wait` field | Seconds between polls when waiting for session start. Default: `10` |
| Added `poll_statement_wait` field | Seconds between polls when waiting for statement result. Default: `5` |

#### Security Hardening

| Change | Detail |
|--------|--------|
| **UUID validation** | Added `_UUID_PATTERN` regex and `_validate_uuid()` method. `workspaceid` and `lakehouseid` are validated in `__post_init__()` to prevent path traversal attacks via crafted GUIDs. |
| **Endpoint validation** | Added `_ALLOWED_FABRIC_DOMAINS` allowlist and `_validate_endpoint()` method. Enforces HTTPS scheme. Warns on unknown domains to prevent bearer token leakage to untrusted hosts. |
| **`__repr__` masking** | Overrides `__repr__` to mask `client_secret` and `accessToken` as `'***'` in logs and tracebacks. |
| **`_connection_keys` tightened** | Intentionally excludes `client_secret`, `accessToken`, and `tenant_id` from connection keys to prevent credential exposure. |

All new fields are optional and backward-compatible.

**Example `profiles.yml` usage:**

```yaml
my_profile:
  target: dev
  outputs:
    dev:
      type: fabricspark
      method: livy
      # ... existing fields ...
      http_timeout: 180          # 3 minutes per HTTP call
      session_start_timeout: 900 # 15 minutes for session startup
      statement_timeout: 7200    # 2 hours for long-running models
```

---

### `livysession.py`

#### Critical Fixes

| Change | Detail |
|--------|--------|
| **Thread-safe token refresh** | Added `threading.Lock` (`_token_lock`) around the global `accessToken` mutation in `get_headers()`. Prevents race conditions when multiple dbt threads refresh the token simultaneously. |
| **Thread-safe session management** | Added `threading.Lock` (`_session_lock`) around `LivySessionManager.connect()` and `disconnect()`. Prevents concurrent threads from corrupting the shared `livy_global_session`. |
| **HTTP timeouts on all requests** | Added `timeout=self.http_timeout` to all 6 `requests.*` call sites: `create_session`, `wait_for_session_start`, `delete_session`, `is_valid_session`, `_submitLivyCode`, `_getLivyResult`. |
| **`wait_for_session_start` — bounded polling** | Added a deadline based on `session_start_timeout`. Raises `FailedToConnectError` if exceeded. Handles `error`/`killed` states explicitly. Sleeps on unknown/transitional states to prevent CPU burn. Catches HTTP errors during polling and retries gracefully. |
| **`_getLivyResult` — bounded polling** | Added a deadline based on `statement_timeout`. Raises `DbtDatabaseError` if exceeded. Handles `error`/`cancelled`/`cancelling` statement states with descriptive error messages. Validates HTTP responses before parsing JSON. |
| **`_submitLivyCode` — response validation** | Added `res.raise_for_status()` after submitting a statement. Fails fast on HTTP errors instead of passing a bad response to the polling loop. |

#### Bug Fixes

| Change | Detail |
|--------|--------|
| **`delete_session` — wrong variable** | Fixed `response.raise_for_status()` → `res.raise_for_status()`. The old code referenced the `urllib.response` module import, not the HTTP response. |
| **`is_valid_session` — crash on HTTP failure** | Wrapped in `try/except`; returns `False` on any HTTP or parsing error instead of crashing. |
| **`fetchone` — O(n²) performance** | Replaced destructive `self._rows.pop(0)` (O(n) per call) with index-based iteration via `self._fetch_index`. Also prevents `fetchone` from interfering with `fetchall`. |
| **Removed `from urllib import response`** | This unused import was the source of the `delete_session` bug. |

#### Session Recovery

| Change | Detail |
|--------|--------|
| **Invalid session re-creation** | When `is_valid_session()` returns `False`, the manager now creates a fresh `LivySession` object (instead of reusing the dead one) and wraps the old session cleanup in a try/except so a failed delete doesn't block recovery. |

---

### `connections.py`

| Change | Detail |
|--------|--------|
| **`release(self)` → `release(cls)`** | Fixed the `@classmethod` signature. `self` in a classmethod is actually the class — renamed to `cls` for correctness and clarity. |
| **`cleanup_all(self)` → `cleanup_all(cls)`** | Same signature fix. Also added per-session error handling so one failed disconnect doesn't prevent cleanup of others. Iterates over `list(cls.connection_managers.keys())` to avoid mutation-during-iteration. |
| **`close()` — error resilience** | On exception, now sets `connection.state = ConnectionState.CLOSED` and logs at `warning` level (was `debug`). Prevents the connection from being left in an ambiguous state. |
| **`_execute_query_with_retry` — exponential backoff** | Replaced the hardcoded `time.sleep(5)` with exponential backoff: `5s → 10s → 20s → 40s → 60s` (capped). |
| **`_execute_query_with_retry` — indentation fix** | Fixed the `try` block indentation for the call to `_execute_query_with_retry` inside `add_query`. |

---

### `create_table_as.sql`

| Change | Detail |
|--------|--------|
| **`fabricspark__file_format_clause` — emit `using delta`** | Removed the `file_format != 'delta'` guard that suppressed emitting `using delta`. The clause now emits `using <file_format>` for all non-null formats including delta. |

---

## Test Suite Fixes

### `test_adapter.py`

| Change | Detail |
|--------|--------|
| **Added `mp_context` argument** | `FabricSparkAdapter(config)` → `FabricSparkAdapter(config, self.mp_context)` using `multiprocessing.get_context("spawn")`. Required by `BaseAdapter.__init__()` in dbt-adapters ≥1.7. |
| **Fixed `DbtRuntimeError` import** | `from dbt.exceptions` → `from dbt_common.exceptions` (moved in dbt-core ≥1.8). |
| **Added `spark_config` to test profiles** | Both `_get_target_livy` and `test_profile_with_database` profile dicts now include `"spark_config": {"name": "test-session"}` to satisfy `FabricSparkCredentials.__post_init__()` validation. |
| **Fixed `test_livy_connection` mock** | Changed mock target from `LivySessionConnectionWrapper` to `LivySessionManager.connect` — the wrapper class was not where the real HTTP call occurs. |

### `test_credentials.py`

| Change | Detail |
|--------|--------|
| **Added `spark_config`** | Added `spark_config={"name": "test-session"}` to the `FabricSparkCredentials` constructor call. |

### `test_macros.py`

| Change | Detail |
|--------|--------|
| **Fixed template paths** | Replaced hardcoded relative paths with `os.path.dirname(__file__)`-based resolution so templates load regardless of CWD. |
| **Removed stray `unittest.skip()`** | Removed the bare `unittest.skip("Skipping temporarily")` call at module level (a decorator applied to nothing). |
| **Added missing Jinja globals** | Registered `statement`, `is_incremental`, `local_md5`, `alter_column_set_constraints`, `alter_table_add_constraints`, `get_assert_columns_equivalent`, `get_select_subquery`, and `create_temporary_view` as mocks/no-ops in `default_context` so the template parses without `UndefinedError`. |

### `test_shortcuts.py`

| Change | Detail |
|--------|--------|
| **Fixed method name mismatch** | Renamed all references from `check_exists` → `check_if_exists_and_delete_shortcut` to match the actual `ShortcutClient` method name. |
| **Fixed target body assertions** | Added `"type": "OneLake"` to expected target dicts to match the actual `Shortcut.get_target_body()` return value. |
| **Added `raise_for_status` mocks** | Added `mock_post.return_value.raise_for_status = mock.Mock()` (and similar for `get`/`delete`) since the real methods call `response.raise_for_status()`. |
| **Mocked `time.sleep` in delete test** | The `delete_shortcut` method sleeps for 30 seconds — mocked to avoid slow tests. |
| **Fixed re-creation assertions** | In mismatch tests, the subsequent `create_shortcut` call now mocks `check_if_exists_and_delete_shortcut` to return `False` so the POST actually fires. |

---

## Backward Compatibility

All changes are **fully backward-compatible**:

- New credential fields have sensible defaults and are optional.
- No breaking changes to the SQL macro layer, relation model, or dbt contract interfaces.
- Existing `profiles.yml` configurations work without modification.
- The shared Livy session architecture is preserved (one session shared across threads), but now properly synchronized.
- The `file_format_clause` change adds `using delta` to DDL statements — this is valid Spark SQL and matches the intended behavior asserted by the existing tests.

## Recommendations

- **Increase `connect_retries`** from the default `1` to `3` in your `profiles.yml` for better resilience against transient Fabric API errors.
- **Tune `statement_timeout`** if you have models that run longer than 1 hour.
- **Consider replacing `azure-cli`** with `azure-identity` in `pyproject.toml` to reduce the install footprint (the adapter only uses it for token acquisition).