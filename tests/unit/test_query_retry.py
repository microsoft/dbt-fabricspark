from types import SimpleNamespace
from unittest import mock

import pytest
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

from dbt.adapters.fabricspark.connections import FabricSparkConnectionManager
from dbt.adapters.fabricspark.message_retry import MessageRetryPolicy

CATALOG_SERVER_BUSY = """
com.azure.storage.blob.models.BlobStorageException: Status code 503
<Code>ServerBusy</Code>
at com.microsoft.fabric.spark.catalog.metadata.v202405.TableMetadataManagerV202405.alterTable
"""

CATALOG_SERVER_BUSY_PATTERN = (
    r"re:(?s)(?=.*TableMetadataManagerV202405\.alterTable)"
    r"(?=.*BlobStorageException)(?=.*Status code 503)(?=.*ServerBusy)"
)


def _policy() -> MessageRetryPolicy:
    return MessageRetryPolicy.for_job_retry(
        SimpleNamespace(
            enable_job_retry=True,
            job_retry_on_messages=[CATALOG_SERVER_BUSY_PATTERN],
            job_retry_max_attempts=3,
            job_retry_initial_wait_seconds=30.0,
            job_retry_max_wait_seconds=300.0,
        )
    )


def _manager(cursor, *, policy=None, connect_retries=1, retry_all=False):
    manager = FabricSparkConnectionManager.__new__(FabricSparkConnectionManager)
    manager._job_retry_policy = policy or _policy()
    connection = mock.Mock()
    connection.transaction_open = True
    connection.name = "test"
    connection.credentials = mock.Mock(
        connect_retries=connect_retries,
        retry_all=retry_all,
    )
    connection.handle.cursor.return_value = cursor
    manager.get_thread_connection = mock.Mock(return_value=connection)
    return manager


def test_retries_only_the_failed_query() -> None:
    cursor = mock.Mock()
    cursor.execute.side_effect = [RuntimeError(CATALOG_SERVER_BUSY), None]
    manager = _manager(cursor)

    with mock.patch("dbt.adapters.fabricspark.connections.time.sleep") as sleep:
        manager.add_query("create or replace table target as select 1", auto_begin=False)

    assert cursor.execute.call_count == 2
    sleep.assert_called_once_with(30.0)


def test_exhausts_configured_attempts() -> None:
    cursor = mock.Mock()
    cursor.execute.side_effect = RuntimeError(CATALOG_SERVER_BUSY)
    manager = _manager(cursor)

    with mock.patch("dbt.adapters.fabricspark.connections.time.sleep") as sleep:
        with pytest.raises(RuntimeError, match="ServerBusy"):
            manager.add_query("create or replace table target as select 1", auto_begin=False)

    assert cursor.execute.call_count == 3
    assert sleep.call_args_list == [mock.call(30.0), mock.call(60.0)]


@pytest.mark.parametrize(
    "message",
    [
        CATALOG_SERVER_BUSY.replace("TableMetadataManagerV202405.alterTable", ""),
        CATALOG_SERVER_BUSY.replace("BlobStorageException", ""),
        CATALOG_SERVER_BUSY.replace("Status code 503", ""),
        CATALOG_SERVER_BUSY.replace("ServerBusy", ""),
    ],
)
def test_partial_signature_is_not_retried(message) -> None:
    cursor = mock.Mock()
    cursor.execute.side_effect = RuntimeError(message)
    manager = _manager(cursor)

    with mock.patch("dbt.adapters.fabricspark.connections.time.sleep") as sleep:
        with pytest.raises(RuntimeError):
            manager.add_query("create or replace table target as select 1", auto_begin=False)

    cursor.execute.assert_called_once()
    sleep.assert_not_called()


def test_no_retry_scope_disables_configured_policy() -> None:
    cursor = mock.Mock()
    cursor.execute.side_effect = RuntimeError(CATALOG_SERVER_BUSY)
    manager = _manager(cursor)

    with mock.patch("dbt.adapters.fabricspark.connections.time.sleep") as sleep:
        with FabricSparkConnectionManager.no_retry():
            with pytest.raises(RuntimeError):
                manager.add_query(
                    "create or replace table target as select 1",
                    auto_begin=False,
                )

    cursor.execute.assert_called_once()
    sleep.assert_not_called()


def test_job_retry_budget_is_not_multiplied_by_retry_all() -> None:
    cursor = mock.Mock()
    cursor.execute.side_effect = RuntimeError(CATALOG_SERVER_BUSY)
    manager = _manager(cursor, connect_retries=0, retry_all=True)

    with mock.patch("dbt.adapters.fabricspark.connections.time.sleep") as sleep:
        with pytest.raises(RuntimeError, match="ServerBusy"):
            manager.add_query("create or replace table target as select 1", auto_begin=False)

    assert cursor.execute.call_count == 3
    assert sleep.call_args_list == [mock.call(30.0), mock.call(60.0)]


def test_statement_timeout_is_never_retried_by_job_pattern() -> None:
    cursor = mock.Mock()
    cursor.execute.side_effect = DbtDatabaseError(
        "Timeout (43200s) waiting for statement 42 to complete. "
        "Increase `statement_timeout` in profiles.yml."
    )
    policy = MessageRetryPolicy.for_job_retry(
        SimpleNamespace(
            enable_job_retry=True,
            job_retry_on_messages=["Timeout"],
            job_retry_max_attempts=3,
            job_retry_initial_wait_seconds=30.0,
            job_retry_max_wait_seconds=300.0,
        )
    )
    manager = _manager(cursor, policy=policy, connect_retries=0, retry_all=True)

    with mock.patch("dbt.adapters.fabricspark.connections.time.sleep") as sleep:
        with pytest.raises(DbtRuntimeError, match="statement 42"):
            manager.add_query("select 1", auto_begin=False)

    cursor.execute.assert_called_once()
    sleep.assert_not_called()
