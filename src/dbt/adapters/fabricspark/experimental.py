from __future__ import annotations

import math
import time
from typing import Any

import agate
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.connection import ConnectionState

LIFECYCLE_COLUMNS = frozenset(
    {"table_name", "query_id", "run_id", "status", "checkpoint_location", "definition_hash"}
)
SHOW_COLUMNS = LIFECYCLE_COLUMNS | {"is_active", "last_progress", "last_failure"}
RUN_STATUSES = frozenset({"active", "initializing", "rebuilding", "restarted", "stopped"})


def require_experimental_unstable(adapter: Any, feature: str) -> None:
    credentials = getattr(getattr(adapter, "config", None), "credentials", None)
    if getattr(credentials, "enable_experimental_unstable", False) is not True:
        raise DbtRuntimeError(
            f"{feature} is experimental and unstable, and is disabled by default. "
            "To opt in, set enable_experimental_unstable: true (an unquoted boolean) "
            "in the active fabricspark output in profiles.yml."
        )


def _rows(result: Any, columns: frozenset[str], command: str) -> list[dict[str, Any]]:
    if not isinstance(result, agate.Table) or not columns.issubset(result.column_names):
        raise DbtRuntimeError(f"{command} did not return the required result columns")
    return [dict(zip(result.column_names, row)) for row in result.rows]


def _text(row: dict[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise DbtRuntimeError(f"Runtime metadata returned a missing or invalid {key}")
    return value


def _quote_identifier(identifier: str) -> str:
    if not isinstance(identifier, str) or not identifier:
        raise DbtRuntimeError("Streaming tables require a named schema and table")
    return "`" + identifier.replace("`", "``") + "`"


def _active(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in {"true", "false"}:
        return value.lower() == "true"
    raise DbtRuntimeError("SHOW STREAMING TABLES returned a non-boolean is_active")


def _positive_seconds(value: Any, name: str, zero_default: float | None = None) -> float:
    if isinstance(value, bool):
        raise DbtRuntimeError(f"{name} must be a finite positive number")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DbtRuntimeError(f"{name} must be a finite positive number") from exc
    if number == 0 and zero_default is not None:
        return zero_default
    if not math.isfinite(number) or number <= 0:
        raise DbtRuntimeError(f"{name} must be a finite positive number")
    return number


def _poll_settings(adapter: Any) -> tuple[float, float]:
    credentials = adapter.config.credentials
    timeout = _positive_seconds(
        credentials.statement_timeout, "statement_timeout", zero_default=3600.0
    )
    interval = min(
        5.0, max(0.1, _positive_seconds(credentials.poll_statement_wait, "poll_statement_wait"))
    )
    return timeout, interval


def _streaming_namespace(adapter: Any, relation: Any) -> tuple[str, str]:
    if not isinstance(relation, adapter.Relation):
        raise DbtRuntimeError("streaming_table requires a dbt relation")
    if relation.workspace:
        raise DbtRuntimeError("streaming_table does not support Fabric workspace targets")
    namespace = _quote_identifier(relation.schema)
    if relation.include_policy.database:
        namespace = _quote_identifier(relation.database) + "." + namespace
    return namespace, namespace + "." + _quote_identifier(relation.identifier)


def _connection_owner(adapter: Any) -> tuple[Any, Any, Any]:
    connection = adapter.connections.get_thread_connection()
    # Resolving the lazy handle opens the connection before CREATE is submitted.
    handle = connection.handle
    if connection.state != ConnectionState.OPEN or handle is None:
        raise DbtRuntimeError("streaming_table requires an open owning dbt connection")
    return connection, handle, handle.handle._spark_session.sparkContext


def _check_connection(adapter: Any, owner: tuple[Any, Any, Any]) -> None:
    connection, handle, spark_context = owner
    current = adapter.connections.get_thread_connection()
    if (
        current is not connection
        or current.state != ConnectionState.OPEN
        or current.handle is not handle
        or handle.handle._spark_session.sparkContext is not spark_context
    ):
        raise DbtRuntimeError("The dbt connection changed while awaiting the streaming run")


def _execute(adapter: Any, handle: Any, sql: str) -> tuple[Any, agate.Table]:
    sql = adapter.connections._add_query_comment(sql)
    with adapter.connections.exception_handler(sql):
        cursor = handle.cursor()
        cursor.execute(sql)
        return (
            adapter.connections.get_response(cursor),
            adapter.connections.get_result_from_cursor(cursor, None),
        )


def execute_streaming_table(adapter: Any, relation: Any, sql: str) -> tuple[Any, agate.Table]:
    """Submit CREATE once and wait in its owning connection before returning to dbt."""
    require_experimental_unstable(adapter, "streaming_table")
    if getattr(adapter.config.credentials, "method", None) != "session":
        raise DbtRuntimeError("streaming_table requires method: session and a compatible runtime")
    namespace, table = _streaming_namespace(adapter, relation)
    timeout, interval = _poll_settings(adapter)
    if not isinstance(sql, str) or not sql.strip():
        raise DbtRuntimeError("streaming_table requires a nonempty CREATE statement")
    owner = _connection_owner(adapter)
    response, created = _execute(adapter, owner[1], sql)
    _check_connection(adapter, owner)
    _await_streaming_table(adapter, namespace, table, created, owner, timeout, interval)
    return response, created


def _await_streaming_table(
    adapter: Any,
    namespace: str,
    table: str,
    create_result: Any,
    owner: tuple[Any, Any, Any],
    timeout: float,
    interval: float,
) -> dict[str, Any]:
    created_rows = _rows(create_result, LIFECYCLE_COLUMNS, "CREATE STREAMING TABLE")
    if len(created_rows) != 1:
        raise DbtRuntimeError("CREATE STREAMING TABLE must return exactly one run")
    created = created_rows[0]
    status = _text(created, "status")
    if status not in RUN_STATUSES:
        raise DbtRuntimeError(f"CREATE STREAMING TABLE returned unsuccessful status {status!r}")
    expected = {
        key: _text(created, key)
        for key in ("table_name", "query_id", "run_id", "checkpoint_location", "definition_hash")
    }
    if expected["table_name"] != table and not expected["table_name"].endswith("." + table):
        raise DbtRuntimeError("CREATE STREAMING TABLE returned a different target table")
    deadline = time.monotonic() + timeout

    def check_deadline() -> None:
        if time.monotonic() >= deadline:
            raise DbtRuntimeError(
                f"Timed out after {timeout:g}s awaiting {expected['table_name']} "
                f"run {expected['run_id']}; the native query may still be running"
            )

    while True:
        check_deadline()
        _check_connection(adapter, owner)
        _, result = _execute(adapter, owner[1], f"SHOW STREAMING TABLES IN {namespace}")
        check_deadline()
        _check_connection(adapter, owner)
        matches = [
            row
            for row in _rows(result, SHOW_COLUMNS, "SHOW STREAMING TABLES")
            if row["table_name"] == expected["table_name"]
        ]
        if len(matches) != 1:
            raise DbtRuntimeError(
                f"The submitted streaming table {expected['table_name']} disappeared "
                "or its status is ambiguous"
            )
        observed = matches[0]
        failure = observed["last_failure"]
        if failure is not None and (not isinstance(failure, str) or failure.strip()):
            raise DbtRuntimeError(f"Streaming table {expected['table_name']} failed: {failure}")
        status = _text(observed, "status")
        if status not in RUN_STATUSES:
            raise DbtRuntimeError(
                f"Streaming table {expected['table_name']} returned status {status!r}"
            )
        for key, value in expected.items():
            if observed[key] != value:
                raise DbtRuntimeError(
                    f"Streaming table {expected['table_name']} changed {key} "
                    "while awaiting the submitted run"
                )
        active = _active(observed["is_active"])
        if status == "stopped" and not active:
            return observed
        if status == "stopped" or not active:
            raise DbtRuntimeError(
                f"Streaming table {expected['table_name']} returned inconsistent activity"
            )
        time.sleep(min(interval, max(0.0, deadline - time.monotonic())))
