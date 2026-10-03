from unittest.mock import MagicMock

from dbt.adapters.fabricspark.experimental import _execute


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

    adapter.connections.add_query.assert_called_once_with(
        "/* dbt */ select 1", auto_begin=False
    )
    handle.cursor.assert_not_called()
