from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.fabricspark.connections import FabricSparkConnectionManager


def _make_connection(*, state=ConnectionState.OPEN, handle=None):
    return SimpleNamespace(
        state=state,
        transaction_open=False,
        handle=handle or MagicMock(),
        name="unit-test-connection",
    )


def test_close_disconnects_attached_manager_and_removes_registry_entry():
    manager = MagicMock()
    handle = MagicMock()
    handle._manager = manager
    connection = _make_connection(handle=handle)
    FabricSparkConnectionManager.connection_managers = {"thread-a": manager}

    FabricSparkConnectionManager.close(connection)

    manager.disconnect.assert_called_once()
    handle.close.assert_called_once()
    assert connection.state == ConnectionState.CLOSED
    assert FabricSparkConnectionManager.connection_managers == {}


def test_close_falls_back_to_current_thread_manager_when_handle_has_no_manager():
    manager = MagicMock()
    handle = SimpleNamespace(close=MagicMock())
    connection = _make_connection(handle=handle)
    FabricSparkConnectionManager.connection_managers = {"thread-a": manager}

    with patch.object(
        FabricSparkConnectionManager, "get_thread_identifier", return_value="thread-a"
    ):
        FabricSparkConnectionManager.close(connection)

    manager.disconnect.assert_called_once()
    handle.close.assert_called_once()
    assert FabricSparkConnectionManager.connection_managers == {}


def test_close_noops_for_closed_connection():
    manager = MagicMock()
    handle = MagicMock()
    handle._manager = manager
    connection = _make_connection(state=ConnectionState.CLOSED, handle=handle)
    FabricSparkConnectionManager.connection_managers = {"thread-a": manager}

    FabricSparkConnectionManager.close(connection)

    manager.disconnect.assert_not_called()
    handle.close.assert_not_called()
    assert FabricSparkConnectionManager.connection_managers == {"thread-a": manager}
