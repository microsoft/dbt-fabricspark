"""Regression guards for adaptive polling invariants."""

import importlib
import inspect
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
import requests
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.fabricspark._http_utils import MAX_RETRY_AFTER
from dbt.adapters.fabricspark.adaptive_polling import (
    LENGTHEN_MULTIPLE,
    MIN_SAMPLES_TO_EXTEND,
    DurationStore,
    PollScheduler,
    TelemetrySnapshot,
)
from dbt.adapters.fabricspark.concurrent_livy import (
    _SUBMIT_MARKER_PREFIX,
    HighConcurrencyCursor,
    HighConcurrencySession,
)
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.telemetry import (
    MIN_MONITOR_INTERVAL,
    MonitorTelemetrySource,
)
from dbt.adapters.fabricspark.throttle import (
    PRIORITY_BACKGROUND,
    PRIORITY_CRITICAL,
    PRIORITY_NORMAL,
    ThrottleGovernor,
)


class TestPredictionCannotStall:
    """A wrong-high prediction costs extra sleep, never a stalled statement.

    A model that took 900s under `--full-refresh` and now takes 4s would
    otherwise be polled at the 30s ceiling from its second poll onward.
    """

    def _scheduler(self, predicted, samples):
        scheduler = PollScheduler(
            predicted_duration=predicted,
            jitter=lambda _a, _b: 0.0,
        )
        scheduler.samples = samples
        return scheduler

    def test_stale_large_prediction_does_not_jump_to_the_ceiling(self):
        scheduler = self._scheduler(900.0, MIN_SAMPLES_TO_EXTEND + 5)
        scheduler.next_interval(0.0)

        plan = scheduler.next_interval(0.25)

        assert plan.interval < 5.0, "an early poll must stay responsive"

    def test_a_fast_statement_is_detected_promptly_despite_a_huge_estimate(self):
        scheduler = self._scheduler(1800.0, MIN_SAMPLES_TO_EXTEND + 5)
        elapsed = 0.0
        polls = 0
        while elapsed < 4.0 and polls < 100:
            polls += 1
            elapsed += scheduler.next_interval(elapsed).interval
        assert elapsed < 12.0, f"detected a 4s statement only after {elapsed:.1f}s"

    def test_lengthening_is_bounded_by_the_elapsed_schedule(self):
        scheduler = self._scheduler(10_000.0, MIN_SAMPLES_TO_EXTEND + 5)
        scheduler.next_interval(0.0)

        elapsed = 10.0
        plan = scheduler.next_interval(elapsed)
        unbounded = scheduler._elapsed_based(elapsed)

        assert plan.interval <= unbounded * LENGTHEN_MULTIPLE + 1e-6

    def test_an_uncorroborated_prediction_still_cannot_lengthen(self):
        scheduler = self._scheduler(1800.0, MIN_SAMPLES_TO_EXTEND - 1)
        scheduler.next_interval(0.0)

        plan = scheduler.next_interval(5.0)

        assert plan.interval <= scheduler._elapsed_based(5.0) + 1e-6

    def test_a_short_prediction_still_tightens_polling(self):
        """An imminent finish must shorten the wait so we notice it promptly."""
        scheduler = self._scheduler(61.0, MIN_SAMPLES_TO_EXTEND + 5)
        scheduler.next_interval(0.0)

        plan = scheduler.next_interval(60.0)

        assert plan.interval < scheduler._elapsed_based(60.0)


class TestEstimateDoesNotCompound:
    """The store must not learn its own detection latency.

    Feeding detection time back in makes the estimator's error its own input, so
    one inflated observation is self-sustaining across runs.
    """

    def test_repeated_runs_converge_on_the_true_duration(self):
        store = DurationStore()
        true_duration = 4.0

        for _ in range(6):
            predicted, samples = store.estimate("node:m|select ?", None)
            scheduler = PollScheduler(predicted_duration=predicted, jitter=lambda _a, _b: 0.0)
            scheduler.samples = samples

            elapsed = 0.0
            last_running = 0.0
            while elapsed < true_duration:
                last_running = elapsed
                elapsed += scheduler.next_interval(elapsed).interval
            store.record("node:m|select ?", last_running)

        final, _ = store.estimate("node:m|select ?", None)
        assert final is not None
        assert final <= true_duration + 1e-6, (
            f"estimate {final:.1f}s exceeded the true duration {true_duration}s, "
            f"so detection latency is compounding"
        )


class TestUnlimitedGovernorStillHonoursTheGate:
    """`api_calls_per_minute: 0` disables the limiter, not the 429 backoff.

    Otherwise turning off the limiter turns every 429 into a retry storm.
    """

    def _governor(self, budget):
        slept = []
        clock = {"t": 0.0}

        def sleeper(seconds):
            slept.append(seconds)
            clock["t"] += max(seconds, 0.001)

        return (
            ThrottleGovernor(
                budget,
                clock=lambda: clock["t"],
                sleeper=sleeper,
                jitter=lambda _a, _b: 0.0,
            ),
            slept,
        )

    def test_unlimited_governor_parks_on_the_gate(self):
        governor, slept = self._governor(0)
        governor.penalize(30.0)

        assert governor.acquire(PRIORITY_CRITICAL) is True
        assert sum(slept) >= 30.0

    def test_unlimited_governor_respects_a_deadline(self):
        governor, _ = self._governor(0)
        governor.penalize(600.0)

        assert governor.acquire(PRIORITY_CRITICAL, deadline=5.0) is False

    def test_unlimited_governor_is_free_once_the_gate_lifts(self):
        governor, slept = self._governor(0)

        for _ in range(500):
            assert governor.acquire(PRIORITY_NORMAL) is True

        assert slept == []


class TestMonitorProbeRateFloor:
    """`watch()` may shorten the wait to the floor, never below it.

    Every probe costs a POST plus at least one GET, and `_new_scheduler` calls
    `watch()` for every statement — including sub-second metadata queries.
    """

    def _source(self):
        credential = MagicMock()
        credential.http_timeout = 30
        governor = ThrottleGovernor(0)
        return MonitorTelemetrySource(credential, "http://x/statements", governor, dict)

    def test_rapid_watch_calls_cannot_exceed_the_probe_floor(self):
        source = self._source()
        probes = []
        source._probe = lambda watched: (probes.append(time.monotonic()) or {})

        source.watch("1")
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            source.watch(str(time.monotonic()))
            time.sleep(0.01)
        source.stop()

        allowed = 1.0 / MIN_MONITOR_INTERVAL + 2
        assert len(probes) <= allowed, f"{len(probes)} probes in 1s exceeds the floor"

    def test_consecutive_probes_are_spaced_by_the_floor(self):
        source = self._source()
        probes = []
        source._probe = lambda watched: (probes.append(time.monotonic()) or {})

        source.watch("1")
        time.sleep(MIN_MONITOR_INTERVAL * 1.5)
        source.stop()

        gaps = [b - a for a, b in zip(probes, probes[1:])]
        assert all(gap >= MIN_MONITOR_INTERVAL * 0.9 for gap in gaps), gaps


class TestMonitorAcquisitionRace:
    def test_threads_racing_the_monitor_acquire_all_receive_it(self):
        from dbt.adapters.fabricspark import concurrent_livy as cl

        cl._monitors.clear()
        cl._monitor_sessions.clear()
        cl._monitor_ready.clear()

        credentials = MagicMock()
        credentials.adaptive_polling = True
        credentials.is_local_mode = False
        credentials.session_start_timeout = 30
        credentials.spark_config = {"name": "t"}

        worker = MagicMock()
        worker.session_id = "sess-1"

        sentinel = object()

        class FakeSession:
            def __init__(self, *a, **kw):
                self.session_id = None

            def acquire(self):
                time.sleep(0.5)
                self.session_id = "sess-1"

            def statements_url(self):
                return "http://x/statements"

            def delete(self):
                pass

        results = []
        with (
            patch.object(cl, "HighConcurrencySession", FakeSession),
            patch.object(cl, "MonitorTelemetrySource", lambda *a, **kw: sentinel),
            patch.object(cl, "governor_for_credentials", lambda c: ThrottleGovernor(0)),
            patch.object(cl, "_get_headers", lambda *a, **kw: {}),
        ):

            def call():
                results.append(cl.telemetry_for_session(credentials, worker))

            threads = [threading.Thread(target=call) for _ in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)

        cl._monitors.clear()
        cl._monitor_sessions.clear()
        cl._monitor_ready.clear()

        assert len(results) == 8
        assert all(r is sentinel for r in results), (
            f"{sum(r is None for r in results)} threads lost telemetry to the acquire race"
        )


class TestCapacityThrottleDoesNotBusyLoop:
    def test_hc_poll_waits_between_capacity_429s(self):
        from dbt.adapters.fabricspark.concurrent_livy import (
            HighConcurrencyCursor,
            HighConcurrencySession,
        )
        from dbt.adapters.fabricspark.credentials import FabricSparkCredentials

        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="fabric",
            authentication="CLI",
            workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
            lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
            lakehouse="tests",
            spark_config={"name": "t"},
            statement_timeout=300,
        )
        session = HighConcurrencySession(credentials, credentials.spark_config)
        session.hc_id = "hc-1"
        session.session_id = "sess-1"
        session.repl_id = "repl-1"
        session.is_new_session_required = False
        cursor = HighConcurrencyCursor(credentials, session)

        throttled = MagicMock()
        throttled.status_code = 429
        throttled.text = "capacity"
        throttled.headers = {"Retry-After": "20"}
        throttled.json.return_value = {"errorCode": "CapacityLimitExceeded"}

        available = MagicMock()
        available.status_code = 200
        available.headers = {}
        available.json.return_value = {"state": "available", "output": {"status": "ok"}}

        slept = []
        with (
            patch("dbt.adapters.fabricspark.concurrent_livy._get_headers", return_value={}),
            patch("requests.get", side_effect=[throttled, throttled, available]),
            patch("dbt.adapters.fabricspark.concurrent_livy.time.sleep", side_effect=slept.append),
        ):
            cursor._poll(MagicMock(json=lambda: {"id": 4}))

        assert len(slept) >= 2
        assert all(s >= 1.0 for s in slept[:2]), slept


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


def _reconcile_creds():
    return FabricSparkCredentials(
        method="livy",
        livy_mode="fabric",
        authentication="CLI",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        lakehouse="tests",
        endpoint="https://api.fabric.microsoft.com/v1",
        spark_config={"name": "test-session"},
        session_start_timeout=10,
        statement_timeout=30,
        poll_wait=0,
        poll_statement_wait=0,
    )


def _hc_cursor(creds):
    session = HighConcurrencySession(creds, creds.spark_config)
    session.hc_id = "hc-1"
    session.session_id = "sess-1"
    session.repl_id = "repl-1"
    session.is_new_session_required = False
    return HighConcurrencyCursor(creds, session)


def _resp(status_code, json_body=None, text=""):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.json.return_value = json_body if json_body is not None else {}
    return resp


class TestReconciliationEvidenceAccumulates:
    """A late network blip must not discard earlier conclusive reads.

    Reconciliation only ever runs *because* the network just failed, so
    correlated flakiness on a later attempt is common. Overwriting the outcome
    each attempt turned an ordinary retryable hiccup into a hard build failure.
    """

    def test_three_clean_absent_reads_survive_a_final_failure(self):
        cursor = _hc_cursor(_reconcile_creds())
        calls = 0

        def fake(governor, priority, func, url, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 4:
                raise requests.exceptions.ConnectionError("late blip")
            return _resp(200, {"statements": []})

        with (
            patch("dbt.adapters.fabricspark.concurrent_livy._governed", side_effect=fake),
            patch("dbt.adapters.fabricspark.concurrent_livy._sleep_until"),
        ):
            outcome, stmt = cursor._find_submitted_statement("marker-x")

        assert outcome == "absent"
        assert stmt is None

    def test_unknown_only_when_no_attempt_ever_read_the_list(self):
        cursor = _hc_cursor(_reconcile_creds())

        with (
            patch(
                "dbt.adapters.fabricspark.concurrent_livy._governed",
                side_effect=requests.exceptions.ConnectionError("down"),
            ),
            patch("dbt.adapters.fabricspark.concurrent_livy._sleep_until"),
        ):
            outcome, _ = cursor._find_submitted_statement("marker-x")

        assert outcome == "unknown"

    def test_error_status_alone_is_not_evidence_of_absence(self):
        cursor = _hc_cursor(_reconcile_creds())

        with (
            patch(
                "dbt.adapters.fabricspark.concurrent_livy._governed",
                return_value=_resp(503, text="unavailable"),
            ),
            patch("dbt.adapters.fabricspark.concurrent_livy._sleep_until"),
        ):
            outcome, _ = cursor._find_submitted_statement("marker-x")

        assert outcome == "unknown"


class TestAmbiguous5xxSubmitDoesNotDuplicate:
    """A 502 can be emitted after Livy already started the statement."""

    def test_hc_adopts_running_statement_instead_of_resubmitting(self):
        cursor = _hc_cursor(_reconcile_creds())
        posts = []

        def fake(governor, priority, func, url, **kwargs):
            if func is requests.post:
                posts.append(kwargs["data"])
                return _resp(502, text="bad gateway")
            marker = next(
                token.split("*/")[0].strip()
                for token in posts[-1].split("/*")
                if _SUBMIT_MARKER_PREFIX in token
            )
            return _resp(200, {"statements": [{"id": 55, "code": f"/* {marker} */\ninsert"}]})

        with (
            patch("dbt.adapters.fabricspark.concurrent_livy._governed", side_effect=fake),
            patch("dbt.adapters.fabricspark.concurrent_livy._sleep_until"),
            patch("time.sleep"),
        ):
            res = cursor._submit("insert into audit_log values (1)")

        assert res.json()["id"] == 55
        assert len(posts) == 1, "a 5xx that already landed must not resubmit the statement"

    def test_hc_refuses_to_resubmit_when_list_unreadable_after_5xx(self):
        cursor = _hc_cursor(_reconcile_creds())
        posts = 0

        def fake(governor, priority, func, url, **kwargs):
            nonlocal posts
            if func is requests.post:
                posts += 1
                return _resp(503, text="unavailable")
            raise requests.exceptions.ConnectionError("down")

        with (
            patch("dbt.adapters.fabricspark.concurrent_livy._governed", side_effect=fake),
            patch("dbt.adapters.fabricspark.concurrent_livy._sleep_until"),
            patch("time.sleep"),
            pytest.raises(DbtRuntimeError, match="Refusing to resubmit"),
        ):
            cursor._submit("insert into audit_log values (1)")

        assert posts == 1


class TestSingletonPollBackoffIsBounded:
    """Uncapped exponential backoff sailed past ``statement_timeout``.

    30 unbroken 5xx responses reached a single 536-million-second sleep, so dbt
    hung for years with no output and no error.
    """

    def test_5xx_backoff_never_exceeds_the_ceiling(self):
        from dbt.adapters.fabricspark.singleton_livy import _MAX_RETRY_BACKOFF

        worst = max(min(2 ** (n - 1), _MAX_RETRY_BACKOFF) for n in range(1, 31))
        assert worst == _MAX_RETRY_BACKOFF

    @pytest.mark.parametrize("backend", ["singleton", "hc"])
    def test_poll_loop_sleeps_are_deadline_aware(self, backend):
        import inspect

        if backend == "singleton":
            from dbt.adapters.fabricspark.singleton_livy import LivyCursor

            source = inspect.getsource(LivyCursor._getLivyResult)
        else:
            from dbt.adapters.fabricspark.concurrent_livy import HighConcurrencyCursor as _HC

            source = inspect.getsource(_HC._poll_loop)

        assert "time.sleep(" not in source, (
            "the poll loop must use _sleep_until so retries cannot overshoot statement_timeout"
        )
        assert "_sleep_until(" in source


class TestMonitorReadyAlwaysReleased:
    """A BaseException in the owner left every other thread parked for 600s."""

    def test_event_is_released_even_on_base_exception(self):
        import inspect

        from dbt.adapters.fabricspark import concurrent_livy

        source = inspect.getsource(concurrent_livy.telemetry_for_session)
        assert "finally:" in source and "pending.set()" in source
        tail = source.split("finally:")[-1]
        assert "pending.set()" in tail, "pending.set() must run on every exit path"


class TestReconciliationFailsClosedOnMalformedListings:
    """A body we cannot interpret must never be read as "nothing was submitted".

    The mechanism rests entirely on Livy echoing ``code`` back. If that ever
    stops, reporting "absent" would silently resubmit side-effecting DML with no
    log line and no test failure.
    """

    @pytest.mark.parametrize(
        "body",
        [
            {},
            {"statements": None},
            {"statements": {}},
            {"statements": "notalist"},
            {"statements": 5},
            [],
            {"statements": [{"id": 1, "state": "running"}]},
        ],
    )
    def test_unreadable_listing_reports_unknown(self, body):
        cursor = _hc_cursor(_reconcile_creds())

        with (
            patch(
                "dbt.adapters.fabricspark.concurrent_livy._governed",
                return_value=_resp(200, body),
            ),
            patch("dbt.adapters.fabricspark.concurrent_livy._sleep_until"),
        ):
            outcome, _ = cursor._find_submitted_statement("marker-x")

        assert outcome == "unknown", f"{body!r} must not authorise a resubmit"

    @pytest.mark.parametrize(
        "body",
        [
            {"statements": []},
            {"statements": [{"id": 1, "code": "select 2"}]},
        ],
    )
    def test_well_formed_listing_without_the_marker_reports_absent(self, body):
        cursor = _hc_cursor(_reconcile_creds())

        with (
            patch(
                "dbt.adapters.fabricspark.concurrent_livy._governed",
                return_value=_resp(200, body),
            ),
            patch("dbt.adapters.fabricspark.concurrent_livy._sleep_until"),
        ):
            outcome, _ = cursor._find_submitted_statement("marker-x")

        assert outcome == "absent"


def _progress(total, completed, at):
    return TelemetrySnapshot(
        total_tasks=total, completed_tasks=completed, known_jobs=1, observed_at=at
    )


class TestTelemetryMayLengthenWithoutCorroboration:
    """Live telemetry is the whole reason the monitor REPL exists.

    A learned estimate needs `MIN_SAMPLES_TO_EXTEND` corroborating samples
    before it may stretch the wait, but telemetry observes actual progress, so
    it must be trusted on its first reading. Collapsing the two rules into one
    silently removes the poll reduction telemetry is there to provide.
    """

    def test_telemetry_lengthens_the_wait_with_zero_samples(self):
        scheduler = PollScheduler(jitter=lambda _a, _b: 0.0, base_interval=1.0)
        scheduler.next_interval(0.0)
        baseline = scheduler.next_interval(20.0).interval

        scheduler.observe(_progress(1000, 0, 0.0), 0.0)
        scheduler.observe(_progress(1000, 20, 20.0), 20.0)

        assert scheduler.samples == 0
        plan = scheduler.next_interval(20.0)
        assert plan.reason == "telemetry-eta"
        assert plan.interval > baseline

    def test_a_learned_estimate_alone_may_not_lengthen_the_wait(self):
        scheduler = PollScheduler(
            predicted_duration=980.0, jitter=lambda _a, _b: 0.0, base_interval=1.0
        )
        scheduler.samples = MIN_SAMPLES_TO_EXTEND - 1
        scheduler.next_interval(0.0)

        plan = scheduler.next_interval(20.0)

        assert plan.reason == "elapsed-proportional"


class TestBothBackendsUseTheSampleCount:
    """`estimate()` is the only accessor because dropping the sample count
    pins `samples` at zero, which permanently disables learned lengthening."""

    def test_duration_store_exposes_no_count_dropping_accessor(self):
        assert not hasattr(DurationStore, "predict")

    def test_singleton_scheduler_carries_the_sample_count(self):
        from dbt.adapters.fabricspark.singleton_livy import LivyCursor

        credentials = _reconcile_creds()
        cursor = LivyCursor(credentials, MagicMock(session_id="42"))
        cursor._active_sql = "select 1"
        cursor._duration_store = MagicMock()
        cursor._duration_store.estimate.return_value = (300.0, 7)

        with patch("dbt.adapters.fabricspark.singleton_livy.get_node_info", return_value={}):
            scheduler = cursor._new_scheduler()

        assert scheduler.samples == 7
        assert scheduler.predicted_duration == 300.0


class TestTeardownNeverParksOnTheThrottleGate:
    """Teardown runs under the connection-manager lock and at interpreter exit,
    so it must not wait out a Fabric `Retry-After` that can be hours long."""

    def test_singleton_delete_session_is_bounded(self):
        from dbt.adapters.fabricspark import singleton_livy

        credentials = _reconcile_creds()
        session = singleton_livy.LivySession(credentials)
        session.session_id = "42"

        with patch.object(singleton_livy, "_governed", return_value=_resp(200, {})) as governed:
            session.delete_session()

        _, kwargs = governed.call_args
        assert kwargs["governor_deadline"] is not None
        assert governed.call_args[0][1] == PRIORITY_CRITICAL


class TestThrottledSubmitDoesNotDoubleWait:
    """`governed()` calls `note_response`, which parks the shared gate for the
    full `Retry-After`. A submit retry that also sleeps it duplicates that wait
    uninterruptibly, and burns a final full-length sleep immediately before
    giving up anyway.
    """

    @pytest.mark.parametrize(
        "module,submit",
        [
            ("concurrent_livy", "_submit"),
            ("singleton_livy", "_submitLivyCode"),
        ],
    )
    def test_submit_retry_leaves_the_wait_to_the_governor(self, module, submit):
        source = inspect.getsource(
            getattr(
                getattr(
                    importlib.import_module(f"dbt.adapters.fabricspark.{module}"),
                    "HighConcurrencyCursor" if module == "concurrent_livy" else "LivyCursor",
                ),
                submit,
            )
        )
        throttled = source.split("status_code == 429")[1].split("continue")[0]
        assert "time.sleep" not in throttled
        assert "parse_retry_after" not in throttled

    def test_the_governor_gate_alone_serves_the_retry_after(self):
        governor = ThrottleGovernor(200, clock=lambda: 1000.0, jitter=lambda _a, _b: 0.0)
        response = MagicMock(status_code=429, headers={"Retry-After": "7199"})
        response.json.return_value = {}

        assert governor.note_response(response) is True

        waits = []
        governor._sleeper = waits.append
        governor._clock = lambda: 1000.0 + sum(waits)
        assert governor.acquire(PRIORITY_BACKGROUND, deadline=1000.0 + 8000) is True
        assert max(waits) <= 5.0, "gate must be served in interruptible slices"
        assert sum(waits) == pytest.approx(MAX_RETRY_AFTER, abs=5)

    @pytest.mark.parametrize("budget", [0, 200])
    @pytest.mark.parametrize("body", [{}, {"errorCode": "CapacityLimitExceeded"}])
    def test_every_governor_shape_still_gates_submissions(self, budget, body):
        """Removing the manual sleep is only safe while the gate always holds.

        `budget=0` is the unlimited governor, whose fast path returns before the
        rate window is consulted -- if it skipped the gate too, the retry budget
        would burn instantly and turn a slow submit into a hard failure.
        """
        governor = ThrottleGovernor(budget, clock=lambda: 1000.0, jitter=lambda _a, _b: 0.0)
        response = MagicMock(status_code=429, headers={"Retry-After": "600"})
        response.json.return_value = body
        governor.note_response(response)

        waits = []
        governor._sleeper = waits.append
        governor._clock = lambda: 1000.0 + sum(waits)
        governor.acquire(PRIORITY_BACKGROUND, deadline=1000.0 + 5000)

        assert sum(waits) == pytest.approx(MAX_RETRY_AFTER, abs=5)

    def test_a_hintless_throttle_still_gates(self):
        governor = ThrottleGovernor(0, clock=lambda: 1000.0, jitter=lambda _a, _b: 0.0)
        response = MagicMock(status_code=429, headers={})
        response.json.side_effect = ValueError("not json")
        governor.note_response(response)

        waits = []
        governor._sleeper = waits.append
        governor._clock = lambda: 1000.0 + sum(waits)
        governor.acquire(PRIORITY_BACKGROUND, deadline=1000.0 + 5000)

        assert sum(waits) > 0, "a 429 with no hint must still gate or the retry loop spins"

    def test_a_long_pause_is_reported_above_debug(self):
        governor = ThrottleGovernor(200)
        response = MagicMock(status_code=429, headers={"Retry-After": "7199"})
        response.json.return_value = {}

        with patch("dbt.adapters.fabricspark.throttle.logger") as log:
            governor.note_response(response)

        assert log.warning.called, "an hours-long stall must not hide at debug level"
