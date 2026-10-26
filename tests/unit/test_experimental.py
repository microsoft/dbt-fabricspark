from unittest.mock import MagicMock, patch

import agate
import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.fabricspark.experimental import _await_streaming_table, _execute


def test_execute_uses_connection_manager_query_path() -> None:
    adapter = MagicMock()
    handle = MagicMock()
    connection = MagicMock()
    cursor = MagicMock()
    response = MagicMock()
    result = MagicMock()
    connection.handle = handle
    adapter.connections._add_query_comment.return_value = "/* dbt */ select 1"
    adapter.connections.add_query.return_value = (connection, cursor)
    adapter.connections.get_response.return_value = response
    adapter.connections.get_result_from_cursor.return_value = result

    assert _execute(adapter, handle, "select 1") == (response, result)

    adapter.connections.add_query.assert_called_once_with("/* dbt */ select 1", auto_begin=False)
    handle.cursor.assert_not_called()


def _metadata(**changes):
    row = {
        "table_name": "`insights_raw`.`dbo`.`st_migration_events`",
        "query_id": "query",
        "run_id": "run",
        "status": "active",
        "checkpoint_location": "/checkpoint",
        "definition_hash": "hash",
        "is_active": True,
        "last_progress": None,
        "last_failure": None,
    }
    row.update(changes)
    return agate.Table([list(row.values())], list(row))


@pytest.mark.parametrize(
    ("identifier", "literal"),
    [
        ("st_migration_events", "st_migration_events"),
        ("st'o\\events", "st\\'o\\\\events"),
    ],
)
def test_streaming_poll_targets_bare_identifier(identifier, literal):
    adapter = MagicMock()
    owner = (MagicMock(), MagicMock(), MagicMock())
    with (
        patch("dbt.adapters.fabricspark.experimental._check_connection"),
        patch("dbt.adapters.fabricspark.experimental.time.sleep"),
        patch(
            "dbt.adapters.fabricspark.experimental._execute",
            side_effect=[
                (None, _metadata()),
                (None, _metadata(status="stopped", is_active=False)),
            ],
        ) as execute,
    ):
        result = _await_streaming_table(
            adapter,
            "`insights_raw`.`dbo`",
            "`insights_raw`.`dbo`.`st_migration_events`",
            identifier,
            _metadata(),
            owner,
            10,
            0.1,
        )
    assert result["status"] == "stopped"
    assert execute.call_count == 2
    for call in execute.call_args_list:
        assert call.args[2] == (f"SHOW STREAMING TABLES IN `insights_raw`.`dbo` LIKE '{literal}'")


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"query_id": "other"}, "changed query_id"),
        ({"run_id": "other"}, "changed run_id"),
        ({"checkpoint_location": "/other"}, "changed checkpoint_location"),
        ({"definition_hash": "other"}, "changed definition_hash"),
        ({"last_failure": "upstream failed"}, "failed: upstream failed"),
        ({"status": "failed"}, "returned status"),
        ({"status": "stopped", "is_active": True}, "inconsistent activity"),
        ({"is_active": False}, "inconsistent activity"),
        ({"table_name": "other"}, "disappeared"),
    ],
)
def test_targeted_streaming_poll_preserves_run_guards(changes, error):
    with (
        patch("dbt.adapters.fabricspark.experimental._check_connection"),
        patch(
            "dbt.adapters.fabricspark.experimental._execute",
            return_value=(None, _metadata(**changes)),
        ),
        pytest.raises(DbtRuntimeError, match=error),
    ):
        _await_streaming_table(
            MagicMock(),
            "`insights_raw`.`dbo`",
            "`insights_raw`.`dbo`.`st_migration_events`",
            "st_migration_events",
            _metadata(),
            (MagicMock(), MagicMock(), MagicMock()),
            10,
            0.1,
        )


CATALOG_TIMEOUT = (
    "Streaming table ownership could not be verified: Failed to get database metadata; "
    "Caused by: java.net.SocketTimeoutException: Read timed out"
)


def _retrying_adapter():
    adapter = MagicMock()
    adapter.config.credentials.enable_job_retry = True
    adapter.config.credentials.job_retry_on_messages = [
        "re:(?s)(?=.*Failed to get database metadata)(?=.*SocketTimeoutException)"
    ]
    adapter.config.credentials.job_retry_max_attempts = 3
    adapter.config.credentials.job_retry_initial_wait_seconds = 30
    adapter.config.credentials.job_retry_max_wait_seconds = 300
    return adapter


def _await(adapter, rows, timeout=1000):
    with (
        patch("dbt.adapters.fabricspark.experimental._check_connection"),
        patch("dbt.adapters.fabricspark.experimental.time.sleep") as sleep,
        patch(
            "dbt.adapters.fabricspark.experimental._execute",
            side_effect=[(None, row) for row in rows],
        ) as execute,
    ):
        result = _await_streaming_table(
            adapter,
            "`insights_raw`.`dbo`",
            "`insights_raw`.`dbo`.`st_migration_events`",
            "st_migration_events",
            _metadata(),
            (MagicMock(), MagicMock(), MagicMock()),
            timeout,
            0.1,
        )
    return result, sleep, execute


def test_transient_metadata_error_keeps_polling_with_backoff():
    unverified = _metadata(
        query_id=None,
        run_id=None,
        status="metadata_error",
        is_active=False,
        last_failure=CATALOG_TIMEOUT,
    )
    result, sleep, execute = _await(
        _retrying_adapter(),
        [
            unverified,
            unverified,
            _metadata(),
            unverified,
            _metadata(status="stopped", is_active=False),
        ],
    )
    assert result["status"] == "stopped"
    assert execute.call_count == 5
    assert [call.args[0] for call in sleep.call_args_list] == [30.0, 60.0, 0.1, 30.0]


def test_unmatched_metadata_error_fails_fast():
    unverified = _metadata(status="metadata_error", last_failure="Target is not a Delta table")
    with pytest.raises(DbtRuntimeError, match="not a Delta table"):
        _await(_retrying_adapter(), [unverified])


def test_transient_metadata_error_is_bounded_by_statement_timeout():
    unverified = _metadata(status="metadata_error", last_failure=CATALOG_TIMEOUT)
    with (
        patch(
            "dbt.adapters.fabricspark.experimental.time.monotonic",
            side_effect=[0.0, 0.0, 0.0, 0.0, 61.0],
        ),
        pytest.raises(DbtRuntimeError, match="Timed out after 60s"),
    ):
        _await(_retrying_adapter(), [unverified], timeout=60)


def test_matching_failure_on_a_run_status_still_fails():
    failed = _metadata(last_failure=CATALOG_TIMEOUT)
    with pytest.raises(DbtRuntimeError, match="failed: Streaming table ownership"):
        _await(_retrying_adapter(), [failed])
