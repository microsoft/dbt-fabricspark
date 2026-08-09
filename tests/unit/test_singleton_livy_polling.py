from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from dbt.adapters.fabricspark.adaptive_polling import PollPlan, PollScheduler
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.singleton_livy import (
    LivyCursor,
    LivySessionConnectionWrapper,
)
from dbt.adapters.fabricspark.throttle import governor_for_credentials, reset_governors


def _credentials(livy_mode: str = "fabric", **overrides) -> FabricSparkCredentials:
    base = dict(
        method="livy",
        livy_mode=livy_mode,
        spark_config={"name": "test-session"},
        statement_timeout=0,
        poll_statement_wait=2,
        poll_wait=0,
    )
    if livy_mode == "fabric":
        base.update(
            authentication="CLI",
            workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
            lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
            lakehouse="tests",
            endpoint="https://api.fabric.microsoft.com/v1",
        )
    else:
        base["livy_url"] = "http://localhost:8998"
    base.update(overrides)
    return FabricSparkCredentials(**base)


def _cursor(credentials: FabricSparkCredentials | None = None) -> LivyCursor:
    credentials = credentials or _credentials()
    livy_session = SimpleNamespace(session_id="42", is_new_session_required=False)
    return LivyCursor(credentials, livy_session)


def _response(status_code: int, json_body=None, text: str = "") -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    response.headers = {}
    if json_body is None:
        response.json.side_effect = ValueError("no body")
    else:
        response.json.return_value = json_body
    return response


def _submit_response(statement_id: int = 7) -> MagicMock:
    return _response(200, {"id": statement_id})


def _waiting(progress=None) -> MagicMock:
    body = {"id": 7, "state": "running"}
    if progress is not None:
        body["progress"] = progress
    return _response(200, body)


def _available() -> MagicMock:
    return _response(
        200,
        {
            "id": 7,
            "state": "available",
            "output": {"status": "ok", "data": {"application/json": {"data": []}}},
        },
    )


def setup_function():
    reset_governors()


def teardown_function():
    reset_governors()


@patch("dbt.adapters.fabricspark.singleton_livy._get_headers", return_value={})
@patch("dbt.adapters.fabricspark.singleton_livy.time.sleep")
@patch("dbt.adapters.fabricspark.singleton_livy.requests.get")
def test_429_poll_waits_before_retrying(mock_get, mock_sleep, _headers):
    """A capacity 429 is deliberately let through the governor at critical
    priority, so this loop must wait on its own or it spends its whole retry
    budget in milliseconds against an already-saturated backend."""
    cursor = _cursor()
    cursor.governor = MagicMock()
    cursor.governor.acquire.return_value = True
    cursor.governor.note_response.return_value = True
    resp = _response(429, text="rate limited")
    resp.headers = {"Retry-After": "7"}
    mock_get.side_effect = [resp, _available()]

    result = cursor._getLivyResult(_submit_response())

    assert result["state"] == "available"
    assert mock_get.call_count == 2
    assert mock_sleep.call_args_list[0].args[0] >= 7.0


def test_local_mode_gets_unlimited_governor():
    governor = governor_for_credentials(_credentials("local"))

    assert governor._unlimited is True


@patch("dbt.adapters.fabricspark.singleton_livy._get_headers", return_value={})
@patch("dbt.adapters.fabricspark.singleton_livy.time.sleep")
@patch("dbt.adapters.fabricspark.singleton_livy.requests.get")
def test_poll_loop_uses_scheduler_interval(mock_get, mock_sleep, _headers):
    cursor = _cursor(_credentials("local"))
    scheduler = MagicMock()
    scheduler.next_interval.return_value = PollPlan(7.0, "unit-test")
    cursor._new_scheduler = MagicMock(return_value=scheduler)
    mock_get.side_effect = [_waiting(), _available()]

    cursor._getLivyResult(_submit_response())

    mock_sleep.assert_called_once_with(7.0)


@patch("dbt.adapters.fabricspark.singleton_livy._get_headers", return_value={})
@patch("dbt.adapters.fabricspark.singleton_livy.time.sleep")
@patch("dbt.adapters.fabricspark.singleton_livy.time.monotonic")
@patch("dbt.adapters.fabricspark.singleton_livy.requests.get")
def test_progress_tightens_poll_interval_near_completion(
    mock_get, mock_monotonic, mock_sleep, _headers
):
    cursor = _cursor(_credentials("local"))
    scheduler = PollScheduler(min_interval=0.25, jitter=lambda _a, _b: 0.0)
    cursor._new_scheduler = MagicMock(return_value=scheduler)
    mock_monotonic.side_effect = [0.0, 10.0, 20.0, 21.0]
    mock_get.side_effect = [_waiting(0.10), _waiting(0.99), _available()]

    cursor._getLivyResult(_submit_response())

    assert mock_sleep.call_args_list[0].args[0] == 0.25
    assert mock_sleep.call_args_list[1].args[0] == 0.25


@patch("dbt.adapters.fabricspark.singleton_livy._get_headers", return_value={})
@patch("dbt.adapters.fabricspark.singleton_livy.time.sleep")
@patch("dbt.adapters.fabricspark.singleton_livy.requests.get")
def test_absent_or_garbage_progress_is_survivable(mock_get, _sleep, _headers):
    cursor = _cursor(_credentials("local"))
    mock_get.side_effect = [
        _waiting(),
        _waiting("nearly done"),
        _available(),
    ]

    result = cursor._getLivyResult(_submit_response())

    assert result["state"] == "available"


@patch("dbt.adapters.fabricspark.singleton_livy._get_headers", return_value={})
@patch("dbt.adapters.fabricspark.singleton_livy.requests.post")
def test_cancel_posts_to_statement_cancel_url_and_swallows_errors(mock_post, _headers):
    cursor = _cursor()
    cursor.active_statement_id = "7"
    mock_post.side_effect = RuntimeError("network down")

    cursor.cancel()

    assert mock_post.call_args.args[0].endswith("/sessions/42/statements/7/cancel")


@patch("dbt.adapters.fabricspark.singleton_livy.get_node_info")
@patch("dbt.adapters.fabricspark.singleton_livy._get_headers", return_value={})
@patch("dbt.adapters.fabricspark.singleton_livy.time.monotonic")
@patch("dbt.adapters.fabricspark.singleton_livy.requests.get")
def test_duration_is_recorded_on_success(mock_get, mock_monotonic, _headers, mock_node_info):
    """The recorded value is the last elapsed at which the statement was still
    running, never the detection time — detection includes the loop's own final
    sleep, so recording it would feed the scheduler's latency back to itself."""
    cursor = _cursor(_credentials("local"))
    cursor._active_sql = "select 1"
    cursor._duration_store = MagicMock()
    cursor._duration_store.estimate.return_value = (None, 0)
    mock_node_info.return_value = {"unique_id": "model.test.example"}
    mock_monotonic.side_effect = [100.0, 104.0, 104.0, 130.0]
    mock_get.side_effect = [_waiting(), _available()]

    cursor._getLivyResult(_submit_response())

    cursor._duration_store.record.assert_any_call("node:model.test.example|select ?", 4.0)
    cursor._duration_store.record.assert_any_call("shape:select ?", 4.0)


def test_connection_wrapper_cancel_delegates_to_cursor():
    wrapper = LivySessionConnectionWrapper(MagicMock())
    wrapper._cursor = MagicMock()

    wrapper.cancel()

    wrapper._cursor.cancel.assert_called_once_with()
