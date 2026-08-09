"""Unit tests for Livy monitor telemetry."""

import json
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.fabricspark.telemetry import _SNAPSHOT_SENTINEL as SENTINEL
from dbt.adapters.fabricspark.telemetry import (
    MAX_MONITOR_FAILURES,
    MonitorTelemetrySource,
    _parse_probe_output,
)
from dbt.adapters.fabricspark.throttle import ThrottleGovernor


class StubTelemetrySource(MonitorTelemetrySource):
    def __init__(self, probe_results=None, *, http_timeout=1):
        super().__init__(
            credential=SimpleNamespace(http_timeout=http_timeout),
            statements_url="https://fabric.example/sessions/1/statements",
            governor=ThrottleGovernor(0),
            headers_factory=lambda: {},
        )
        self.probe_results = list(probe_results or [])
        self.probe_calls: list[list[str]] = []

    def _probe(self, watched):
        self.probe_calls.append(list(watched))
        if self.probe_results:
            result = self.probe_results.pop(0)
            if isinstance(result, BaseException):
                raise result
            return result
        return {}


def body_with_text(text, *, status="ok", evalue="boom"):
    return {"output": {"status": status, "evalue": evalue, "data": {"text/plain": text}}}


def sentinel_line(payload):
    return f"{SENTINEL}{json.dumps(payload)}"


def response(status=200, body=None):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = body or {}
    return resp


def test_parse_probe_output_returns_well_formed_payload():
    parsed = _parse_probe_output(body_with_text(sentinel_line({5: [100, 40, 10, 0, 1, 1, 0]})))
    assert parsed == {"5": [100, 40, 10, 0, 1, 1, 0]}
    assert all(isinstance(value, int) for value in parsed["5"])


def test_parse_probe_output_tolerates_stdout_noise():
    text = "banner\nwarning\n" + sentinel_line({"7": [1, 2, 3, 4, 5, 6, 7]}) + "\ntrailer"
    assert _parse_probe_output(body_with_text(text)) == {"7": [1, 2, 3, 4, 5, 6, 7]}


def test_parse_probe_output_uses_the_last_sentinel_line():
    text = "\n".join(
        [
            sentinel_line({"old": [1, 1, 1, 1, 1, 1, 1]}),
            "noise",
            sentinel_line({"new": [2, 2, 2, 2, 2, 2, 2]}),
        ]
    )
    assert _parse_probe_output(body_with_text(text)) == {"new": [2, 2, 2, 2, 2, 2, 2]}


def test_parse_probe_output_error_status_raises_with_evalue():
    with pytest.raises(RuntimeError, match="spark exploded"):
        _parse_probe_output(body_with_text("", status="error", evalue="spark exploded"))


def test_parse_probe_output_without_sentinel_raises_value_error():
    with pytest.raises(ValueError, match="no telemetry payload"):
        _parse_probe_output(body_with_text("ordinary stdout"))


def test_parse_probe_output_malformed_json_raises_decode_error():
    with pytest.raises(json.JSONDecodeError):
        _parse_probe_output(body_with_text(f"{SENTINEL}{{not json"))


def test_parse_probe_output_drops_invalid_entries_but_keeps_valid_siblings():
    parsed = _parse_probe_output(
        body_with_text(
            sentinel_line(
                {
                    "valid": [1, 2, 3, 4, 5, 6, 7],
                    "not-list": "bad",
                    "too-short": [1, 2, 3],
                    "too-long": [1, 2, 3, 4, 5, 6, 7, 8],
                }
            )
        )
    )
    assert parsed == {"valid": [1, 2, 3, 4, 5, 6, 7]}


def test_parse_probe_output_missing_output_key_raises_runtime_error():
    with pytest.raises(RuntimeError, match="unknown"):
        _parse_probe_output({})


def test_parse_probe_output_missing_data_key_raises_value_error():
    with pytest.raises(ValueError, match="no telemetry payload"):
        _parse_probe_output({"output": {"status": "ok"}})


def test_refresh_maps_probe_values_to_named_snapshot_fields(monkeypatch):
    source = StubTelemetrySource([{"42": [7, 6, 5, 4, 3, 2, 1]}])
    monkeypatch.setattr("dbt.adapters.fabricspark.telemetry.time.monotonic", lambda: 123.456)
    source.watch("42")

    assert source._refresh(["42"]) is True

    snap = source.snapshot("42")
    assert snap.total_tasks == 7
    assert snap.completed_tasks == 6
    assert snap.active_tasks == 5
    assert snap.failed_tasks == 4
    assert snap.known_jobs == 3
    assert snap.active_jobs == 2
    assert snap.failed_jobs == 1
    assert snap.observed_at == 123.456
    source.stop()


def test_snapshot_for_never_watched_id_returns_none():
    assert StubTelemetrySource().snapshot("missing") is None


def test_unwatch_drops_snapshot_and_removes_statement_from_watch_set():
    source = StubTelemetrySource([{"9": [1, 1, 1, 1, 1, 1, 1]}])
    source.watch("9")
    source._refresh(["9"])
    assert source.snapshot("9") is not None

    source.unwatch("9")

    assert source.snapshot("9") is None
    assert "9" not in source._watched
    source.stop()


def test_refresh_discards_probe_result_for_no_longer_watched_statement():
    source = StubTelemetrySource([{"11": [1, 2, 3, 4, 5, 6, 7]}])
    source.watch("11")
    source.unwatch("11")

    assert source._refresh(["11"]) is True

    assert source.snapshot("11") is None
    source.stop()


def test_observed_at_is_populated_from_monotonic_time(monkeypatch):
    source = StubTelemetrySource([{"1": [1, 0, 0, 0, 1, 0, 0]}])
    monkeypatch.setattr("dbt.adapters.fabricspark.telemetry.time.monotonic", lambda: 987.0)
    source.watch("1")
    source._refresh(["1"])
    assert source.snapshot("1").observed_at == 987.0
    source.stop()


def test_watch_when_disabled_is_a_no_op():
    source = StubTelemetrySource()
    source._disabled = True

    source.watch("1")

    assert source._thread is None
    assert source._watched == set()


def test_refresh_keeps_running_until_failure_limit():
    source = StubTelemetrySource([RuntimeError("fail")] * (MAX_MONITOR_FAILURES - 1))

    for _ in range(MAX_MONITOR_FAILURES - 1):
        assert source._refresh(["1"]) is True
        assert source.disabled is False


def test_refresh_disables_on_failure_limit():
    source = StubTelemetrySource([RuntimeError("fail")] * MAX_MONITOR_FAILURES)

    for _ in range(MAX_MONITOR_FAILURES - 1):
        assert source._refresh(["1"]) is True

    assert source._refresh(["1"]) is False
    assert source.disabled is True


def test_success_resets_consecutive_failure_counter():
    source = StubTelemetrySource(
        [
            RuntimeError("fail 1"),
            RuntimeError("fail 2"),
            {},
            RuntimeError("fail 3"),
            RuntimeError("fail 4"),
        ]
    )

    for _ in range(5):
        assert source._refresh(["1"]) is True

    assert source.disabled is False


def test_watch_starts_exactly_one_background_thread_for_multiple_ids():
    source = StubTelemetrySource()
    source.watch("1")
    first = source._thread
    source.watch("2")
    source.watch("3")

    assert source._thread is first
    assert first.name == "fabricspark-telemetry"
    source.stop()


def test_stop_sets_event_and_joins_background_thread():
    source = StubTelemetrySource()
    source.watch("1")
    thread = source._thread
    assert thread.is_alive()

    source.stop()

    deadline = time.monotonic() + 5
    while thread.is_alive() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert source._stop.is_set()
    assert not thread.is_alive()


def test_stop_before_thread_starts_does_not_raise():
    StubTelemetrySource().stop()


def test_background_thread_is_daemon():
    source = StubTelemetrySource()
    source.watch("1")
    assert source._thread.daemon is True
    source.stop()


def test_concurrent_watch_unwatch_snapshot_and_refresh_do_not_raise():
    source = StubTelemetrySource()
    errors: list[BaseException] = []
    barrier = threading.Barrier(6)

    def worker(n):
        try:
            barrier.wait()
            for i in range(50):
                sid = str((n + i) % 7)
                source.watch(sid)
                source.snapshot(sid)
                if i % 2 == 0:
                    source.unwatch(sid)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(5)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for _ in range(20):
        source._probe = lambda watched: {sid: [7, 6, 5, 4, 3, 2, 1] for sid in watched}
        source._refresh([str(i) for i in range(7)])
    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive()

    assert not errors
    with source._lock:
        assert set(source._snapshots).issubset(source._watched)
    source.stop()


def test_probe_submits_statement_and_parses_available_result():
    source = StubTelemetrySource()
    source._probe = MonitorTelemetrySource._probe.__get__(source, StubTelemetrySource)
    submit = response(body={"id": 42})
    poll = response(
        body={
            "state": "available",
            "output": {
                "status": "ok",
                "data": {"text/plain": sentinel_line({"1": [1, 2, 3, 4, 5, 6, 7]})},
            },
        }
    )

    with patch("dbt.adapters.fabricspark.telemetry.requests") as requests:
        requests.post.return_value = submit
        requests.get.return_value = poll
        assert source._probe(["1"]) == {"1": [1, 2, 3, 4, 5, 6, 7]}

    requests.get.assert_called_once()
    assert requests.get.call_args.args[0] == "https://fabric.example/sessions/1/statements/42"


def test_probe_submit_http_error_raises_runtime_error():
    source = StubTelemetrySource()
    source._probe = MonitorTelemetrySource._probe.__get__(source, StubTelemetrySource)
    with patch("dbt.adapters.fabricspark.telemetry.requests") as requests:
        requests.post.return_value = response(status=500)
        with pytest.raises(RuntimeError, match="monitor submit returned HTTP 500"):
            source._probe(["1"])


def test_probe_submit_without_statement_id_raises_runtime_error():
    source = StubTelemetrySource()
    source._probe = MonitorTelemetrySource._probe.__get__(source, StubTelemetrySource)
    with patch("dbt.adapters.fabricspark.telemetry.requests") as requests:
        requests.post.return_value = response(body={})
        with pytest.raises(RuntimeError, match="no statement id"):
            source._probe(["1"])


def test_probe_poll_http_error_raises_runtime_error():
    source = StubTelemetrySource()
    source._probe = MonitorTelemetrySource._probe.__get__(source, StubTelemetrySource)
    with patch("dbt.adapters.fabricspark.telemetry.requests") as requests:
        requests.post.return_value = response(body={"id": 42})
        requests.get.return_value = response(status=503)
        with pytest.raises(RuntimeError, match="monitor poll returned HTTP 503"):
            source._probe(["1"])


@pytest.mark.parametrize("state", ["error", "cancelled"])
def test_probe_terminal_error_states_raise_runtime_error(state):
    source = StubTelemetrySource()
    source._probe = MonitorTelemetrySource._probe.__get__(source, StubTelemetrySource)
    with patch("dbt.adapters.fabricspark.telemetry.requests") as requests:
        requests.post.return_value = response(body={"id": 42})
        requests.get.return_value = response(body={"state": state})
        with pytest.raises(RuntimeError, match=f"state {state}"):
            source._probe(["1"])


def test_probe_loops_through_running_state_until_available(monkeypatch):
    source = StubTelemetrySource()
    source._probe = MonitorTelemetrySource._probe.__get__(source, StubTelemetrySource)
    monkeypatch.setattr("dbt.adapters.fabricspark.telemetry.time.sleep", lambda _: None)
    with patch("dbt.adapters.fabricspark.telemetry.requests") as requests:
        requests.post.return_value = response(body={"id": 42})
        requests.get.side_effect = [
            response(body={"state": "running"}),
            response(body={"state": "running"}),
            response(
                body={
                    "state": "available",
                    "output": {
                        "status": "ok",
                        "data": {"text/plain": sentinel_line({"1": [1, 2, 3, 4, 5, 6, 7]})},
                    },
                }
            ),
        ]
        assert source._probe(["1"]) == {"1": [1, 2, 3, 4, 5, 6, 7]}
    assert requests.get.call_count == 3


def test_probe_submission_body_contains_pyspark_sentinel_and_watched_ids():
    source = StubTelemetrySource()
    source._probe = MonitorTelemetrySource._probe.__get__(source, StubTelemetrySource)
    with patch("dbt.adapters.fabricspark.telemetry.requests") as requests:
        requests.post.return_value = response(body={"id": 42})
        requests.get.return_value = response(
            body={
                "state": "available",
                "output": {
                    "status": "ok",
                    "data": {"text/plain": sentinel_line({"10": [1, 2, 3, 4, 5, 6, 7]})},
                },
            }
        )
        source._probe(["10", "20"])

    payload = json.loads(requests.post.call_args.kwargs["data"])
    assert payload["kind"] == "pyspark"
    assert SENTINEL in payload["code"]
    assert "10" in payload["code"]
    assert "20" in payload["code"]
