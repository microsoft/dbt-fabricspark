"""Unit tests for telemetry-backed adaptive statement polling."""

import json
import os
import threading

import pytest

from dbt.adapters.fabricspark.adaptive_polling import (
    MAX_INTERVAL,
    MIN_INTERVAL,
    DurationStore,
    PollScheduler,
    TelemetrySnapshot,
    duration_store,
    reset_duration_stores,
    sql_shape,
    stats_path_for,
)

NO_JITTER = lambda a, b: 0.0  # noqa: E731


def simulate(runtime, predicted=None, telemetry_at=None, jitter=NO_JITTER):
    """Run the scheduler against a statement of known runtime.

    Returns (poll_count, detection_time). ``telemetry_at`` maps elapsed time to
    a TelemetrySnapshot the monitor would have produced.
    """
    scheduler = PollScheduler(predicted_duration=predicted, jitter=jitter)
    elapsed = 0.0
    polls = 0
    while True:
        polls += 1
        if elapsed >= runtime:
            return polls, elapsed
        if telemetry_at:
            scheduler.observe(telemetry_at(elapsed), elapsed)
        elapsed += scheduler.next_interval(elapsed).interval
        assert polls < 100_000, "scheduler failed to converge"


def simulate_trusted(runtime, predicted, samples, jitter=NO_JITTER):
    scheduler = PollScheduler(predicted_duration=predicted, jitter=jitter)
    scheduler.samples = samples
    elapsed = 0.0
    polls = 0
    while True:
        polls += 1
        if elapsed >= runtime:
            return polls, elapsed
        elapsed += scheduler.next_interval(elapsed).interval
        assert polls < 100_000, "scheduler failed to converge"


def old_fixed_interval_loop(runtime):
    elapsed, polls, interval = 0.0, 0, 0.3
    while True:
        polls += 1
        if elapsed >= runtime:
            return polls, elapsed
        elapsed += interval
        interval = min(interval * 1.5, 1.5)


class TestPollVolume:
    """Adaptive polling should make fewer calls while bounding latency."""

    @pytest.mark.parametrize(
        "runtime,max_polls",
        [
            (0.5, 4),
            (3.0, 8),
            (30.0, 24),
            (300.0, 45),
            (2400.0, 115),
        ],
    )
    def test_poll_count_is_sublinear_in_runtime(self, runtime, max_polls):
        polls, _ = simulate(runtime)
        assert polls <= max_polls, f"{runtime}s statement took {polls} polls"

    @pytest.mark.parametrize(
        "runtime,min_ratio",
        [(300.0, 4.0), (600.0, 7.0), (2400.0, 12.0)],
    )
    def test_long_statements_cost_far_fewer_calls_than_the_old_loop(self, runtime, min_ratio):
        new_polls, _ = simulate(runtime)
        old_polls, _ = old_fixed_interval_loop(runtime)
        assert old_polls / new_polls >= min_ratio

    @pytest.mark.parametrize("runtime", [0.5, 1.0, 3.0, 10.0, 30.0])
    def test_short_statements_do_not_regress_into_more_polling(self, runtime):
        """Most dbt models are short; they must not eat the whole API budget."""
        new_polls, _ = simulate(runtime)
        old_polls, _ = old_fixed_interval_loop(runtime)
        assert new_polls <= old_polls + 2

    @pytest.mark.parametrize("runtime", [1.0, 10.0, 120.0, 2400.0])
    def test_detection_latency_stays_proportional(self, runtime):
        _, detected = simulate(runtime)
        overshoot = detected - runtime
        assert overshoot <= max(runtime * 0.25, MIN_INTERVAL * 2) + MAX_INTERVAL

    @pytest.mark.parametrize("runtime", [3.0, 30.0, 300.0, 2400.0])
    def test_latency_is_no_worse_than_the_old_loop_for_long_statements(self, runtime):
        _, new_detected = simulate(runtime, predicted=runtime)
        assert new_detected - runtime <= max(runtime * 0.02, 1.0)


class TestPredictionSafety:
    """A prediction may tighten polling. It must never stall a statement."""

    def test_accurate_prediction_detects_almost_immediately(self):
        _, detected = simulate(600.0, predicted=600.0)
        assert detected - 600.0 < 2.0

    def test_accurate_prediction_beats_the_blind_schedule_on_latency(self):
        _, blind = simulate(600.0)
        _, informed = simulate(600.0, predicted=600.0)
        assert informed - 600.0 < blind - 600.0

    def test_wildly_high_prediction_does_not_stall_a_fast_statement(self):
        """The dangerous case: a stale estimate from when the table was huge."""
        _, blind = simulate(10.0)
        _, informed = simulate(10.0, predicted=2400.0)
        assert informed <= blind, "a bad prediction must never lengthen polling"

    def test_wildly_low_prediction_degrades_to_the_blind_schedule(self):
        polls, detected = simulate(2400.0, predicted=10.0)
        blind_polls, blind_detected = simulate(2400.0)
        assert detected <= blind_detected * 1.05
        assert polls <= blind_polls * 1.05

    def test_prediction_only_ever_shortens_the_interval(self):
        blind = PollScheduler(jitter=NO_JITTER)
        informed = PollScheduler(predicted_duration=5000.0, jitter=NO_JITTER)
        blind.next_interval(0.0)
        informed.next_interval(0.0)
        for elapsed in (1.0, 5.0, 50.0, 500.0, 4000.0):
            assert (
                informed.next_interval(elapsed).interval
                <= blind.next_interval(elapsed).interval + 1e-9
            )

    def test_a_single_sample_may_not_lengthen_the_wait(self):
        """One observation is not evidence; a table can grow 100x overnight."""
        blind = PollScheduler(jitter=NO_JITTER)
        informed = PollScheduler(predicted_duration=5000.0, jitter=NO_JITTER)
        informed.samples = 1
        blind.next_interval(0.0)
        informed.next_interval(0.0)
        assert informed.next_interval(5.0).interval <= blind.next_interval(5.0).interval + 1e-9

    def test_a_corroborated_prediction_may_lengthen_the_wait(self):
        blind = PollScheduler(jitter=NO_JITTER)
        informed = PollScheduler(predicted_duration=5000.0, jitter=NO_JITTER)
        informed.samples = 5
        blind.next_interval(0.0)
        informed.next_interval(0.0)
        assert informed.next_interval(5.0).interval > blind.next_interval(5.0).interval

    def test_a_corroborated_prediction_is_still_capped(self):
        informed = PollScheduler(predicted_duration=100_000.0, jitter=NO_JITTER)
        informed.samples = 10
        informed.next_interval(0.0)
        assert informed.next_interval(5.0).interval <= MAX_INTERVAL

    def test_a_corroborated_prediction_still_cannot_stall_a_fast_statement(self):
        _, detected = simulate_trusted(10.0, predicted=2400.0, samples=10)
        assert detected - 10.0 <= MAX_INTERVAL + 1.0

    def test_a_corroborated_prediction_reduces_calls_on_long_statements(self):
        blind, _ = simulate(2400.0)
        trusted, _ = simulate_trusted(2400.0, predicted=2400.0, samples=10)
        assert trusted < blind


class TestSchedulerMechanics:
    def test_first_call_is_always_a_cheap_probe(self):
        """Metadata-only statements never register a Spark job."""
        plan = PollScheduler(predicted_duration=3600.0, jitter=NO_JITTER).next_interval(0.0)
        assert plan.reason == "initial-probe"
        assert plan.interval == MIN_INTERVAL

    def test_interval_is_clamped_to_the_ceiling(self):
        scheduler = PollScheduler(jitter=NO_JITTER)
        scheduler.next_interval(0.0)
        assert scheduler.next_interval(10_000.0).interval == MAX_INTERVAL

    def test_interval_never_drops_below_the_floor(self):
        scheduler = PollScheduler(predicted_duration=0.001, jitter=NO_JITTER)
        scheduler.next_interval(0.0)
        assert scheduler.next_interval(0.0001).interval >= MIN_INTERVAL

    def test_jitter_is_applied_and_bounded(self):
        scheduler = PollScheduler(jitter=lambda a, b: b)
        scheduler.next_interval(0.0)
        plan = scheduler.next_interval(100.0)
        assert MIN_INTERVAL <= plan.interval <= MAX_INTERVAL

    def test_intervals_are_monotonically_non_decreasing_without_an_eta(self):
        scheduler = PollScheduler(jitter=NO_JITTER)
        scheduler.next_interval(0.0)
        intervals = [scheduler.next_interval(e).interval for e in (1, 5, 20, 100, 500)]
        assert intervals == sorted(intervals)


def _snapshot(total, completed, at, jobs=1):
    return TelemetrySnapshot(
        total_tasks=total,
        completed_tasks=completed,
        known_jobs=jobs,
        observed_at=at,
    )


class TestTelemetry:
    def test_task_progress_produces_an_eta(self):
        scheduler = PollScheduler(jitter=NO_JITTER)
        scheduler.observe(_snapshot(1000, 0, 0.0), 0.0)
        scheduler.observe(_snapshot(1000, 100, 10.0), 10.0)
        assert scheduler._effective_eta(10.0) == pytest.approx(90.0, rel=0.01)

    def test_telemetry_eta_ages_with_elapsed_time(self):
        """A static ETA would keep claiming the same time remaining forever."""
        scheduler = PollScheduler(jitter=NO_JITTER)
        scheduler.observe(_snapshot(1000, 0, 0.0), 0.0)
        scheduler.observe(_snapshot(1000, 100, 10.0), 10.0)
        assert scheduler._effective_eta(50.0) == pytest.approx(50.0, rel=0.01)
        assert scheduler._effective_eta(100.0) <= 0

    def test_telemetry_eta_tightens_polling_near_completion(self):
        scheduler = PollScheduler(jitter=NO_JITTER)
        scheduler.next_interval(0.0)
        blind = scheduler.next_interval(500.0).interval
        scheduler.observe(_snapshot(1000, 0, 0.0), 0.0)
        scheduler.observe(_snapshot(1000, 999, 500.0), 500.0)
        assert scheduler.next_interval(500.0).interval < blind

    def test_all_known_tasks_done_arms_exactly_one_early_probe(self):
        """A statement runs several jobs in sequence; an empty group is normal.

        Re-arming on every identical quiescent snapshot would pin the loop at
        the floor and burn the whole API budget for the rest of the statement.
        """
        scheduler = PollScheduler(jitter=NO_JITTER)
        scheduler.next_interval(0.0)
        scheduler.observe(_snapshot(100, 50, 0.0), 0.0)
        scheduler.observe(_snapshot(100, 100, 10.0), 10.0)

        plan = scheduler.next_interval(10.0)
        assert plan.reason == "telemetry-quiescent"
        assert plan.interval == pytest.approx(MIN_INTERVAL)

        for elapsed in (11.0, 12.0, 13.0):
            scheduler.observe(_snapshot(100, 100, elapsed), elapsed)
            assert scheduler.next_interval(elapsed).reason != "telemetry-quiescent"

    def test_sustained_quiescence_does_not_saturate_the_poll_rate(self):
        scheduler = PollScheduler(jitter=NO_JITTER)
        elapsed = 0.0
        scheduler.observe(_snapshot(100, 50, 0.0), 0.0)
        intervals = []
        for _ in range(30):
            scheduler.observe(_snapshot(100, 100, elapsed), elapsed)
            interval = scheduler.next_interval(elapsed).interval
            intervals.append(interval)
            elapsed += interval
        assert sum(1 for i in intervals if i <= MIN_INTERVAL + 1e-9) <= 2

    def test_empty_job_group_is_ignored_rather_than_treated_as_progress(self):
        scheduler = PollScheduler(jitter=NO_JITTER)
        scheduler.observe(TelemetrySnapshot(known_jobs=0, observed_at=1.0), 1.0)
        assert scheduler._effective_eta(1.0) is None

    def test_none_snapshot_is_survivable(self):
        scheduler = PollScheduler(jitter=NO_JITTER)
        scheduler.observe(None, 5.0)
        assert scheduler.next_interval(5.0).interval > 0

    def test_aqe_denominator_growth_resets_the_rate_estimate(self):
        """AQE re-plans mid-flight; the old rate measured different work."""
        scheduler = PollScheduler(jitter=NO_JITTER)
        scheduler.observe(_snapshot(100, 0, 0.0), 0.0)
        scheduler.observe(_snapshot(100, 90, 9.0), 9.0)
        assert scheduler._rate_ewma is not None
        scheduler.observe(_snapshot(5000, 90, 10.0), 10.0)
        assert scheduler._rate_ewma is None

    def test_small_denominator_drift_does_not_reset_the_rate(self):
        scheduler = PollScheduler(jitter=NO_JITTER)
        scheduler.observe(_snapshot(100, 0, 0.0), 0.0)
        scheduler.observe(_snapshot(100, 50, 5.0), 5.0)
        scheduler.observe(_snapshot(104, 60, 6.0), 6.0)
        assert scheduler._rate_ewma is not None

    def test_regressing_completed_count_is_ignored(self):
        scheduler = PollScheduler(jitter=NO_JITTER)
        scheduler.observe(_snapshot(100, 50, 5.0), 5.0)
        before = scheduler._rate_ewma
        scheduler.observe(_snapshot(100, 40, 6.0), 6.0)
        assert scheduler._rate_ewma == before

    def test_stale_snapshot_timestamps_are_ignored(self):
        scheduler = PollScheduler(jitter=NO_JITTER)
        scheduler.observe(_snapshot(100, 10, 10.0), 10.0)
        scheduler.observe(_snapshot(100, 20, 9.0), 10.0)
        assert scheduler._rate_ewma is None

    def test_telemetry_driven_run_converges(self):
        runtime = 900.0

        def feed(elapsed):
            return _snapshot(2000, int(2000 * min(elapsed / runtime, 1.0)), elapsed)

        polls, detected = simulate(runtime, telemetry_at=feed)
        assert detected - runtime < 5.0
        assert polls < 120


class TestSqlShape:
    def test_literals_collapse_but_object_names_survive(self):
        a = sql_shape("create table analytics.orders as select * from raw where id = 1")
        b = sql_shape("create table analytics.orders as select * from raw where id = 99")
        c = sql_shape("create table analytics.customers as select * from raw where id = 1")
        assert a == b
        assert a != c

    def test_whitespace_is_normalized(self):
        assert sql_shape("select\n  1\n from t") == sql_shape("select 1 from t")

    def test_case_is_normalized(self):
        assert sql_shape("SELECT * FROM T") == sql_shape("select * from t")

    def test_string_literals_collapse(self):
        assert sql_shape("select * from t where c = 'a'") == sql_shape(
            "select * from t where c = 'zzz'"
        )

    def test_output_is_bounded(self):
        assert len(sql_shape("select " + "x, " * 10_000)) <= 512


class TestDurationStore:
    def test_records_and_predicts(self):
        store = DurationStore()
        store.record("node:a", 12.0)
        assert store.predict("node:a") == 12.0

    def test_unknown_key_predicts_nothing(self):
        assert DurationStore().predict("node:missing") is None

    def test_none_and_invalid_durations_are_ignored(self):
        store = DurationStore()
        store.record(None, 5.0)
        store.record("node:a", 0.0)
        store.record("node:a", float("inf"))
        assert store.predict("node:a") is None

    def test_ewma_moves_toward_recent_observations(self):
        store = DurationStore()
        store.record("node:a", 10.0)
        store.record("node:a", 20.0)
        prediction = store.predict("node:a")
        assert 10.0 < prediction < 20.0

    def test_prefers_the_most_specific_key(self):
        store = DurationStore()
        store.record("node:a", 5.0)
        store.record("shape:x", 500.0)
        assert store.predict("node:a", "shape:x") == 5.0
        assert store.predict("node:missing", "shape:x") == 500.0

    def test_round_trips_through_disk(self, tmp_path):
        path = str(tmp_path / "stats.json")
        store = DurationStore(path)
        store.record("node:a", 42.0)
        store.flush()
        assert DurationStore(path).predict("node:a") == 42.0

    def test_expired_samples_are_dropped_on_load(self, tmp_path):
        path = str(tmp_path / "stats.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(
                {"version": 1, "stats": {"node:a": {"ewma": 9, "samples": 3, "updated_at": 0}}},
                handle,
            )
        assert DurationStore(path).predict("node:a") is None

    def test_corrupt_file_is_survivable(self, tmp_path):
        path = str(tmp_path / "stats.json")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("{not json")
        store = DurationStore(path)
        assert store.predict("node:a") is None
        store.record("node:a", 3.0)
        store.flush()
        assert DurationStore(path).predict("node:a") == 3.0

    def test_malformed_entries_are_skipped_individually(self, tmp_path):
        path = str(tmp_path / "stats.json")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "stats": {
                        "bad": {"ewma": "nope"},
                        "good": {"ewma": 7.0, "samples": 1, "updated_at": 10**10},
                    }
                },
                handle,
            )
        store = DurationStore(path)
        assert store.predict("bad") is None
        assert store.predict("good") == 7.0

    def test_flush_is_atomic_and_leaves_no_temp_files(self, tmp_path):
        path = str(tmp_path / "stats.json")
        store = DurationStore(path)
        store.record("node:a", 1.0)
        store.flush()
        leftovers = [n for n in os.listdir(tmp_path) if n.startswith(".fabricspark-stats-")]
        assert leftovers == []

    def test_flush_without_a_path_is_a_no_op(self):
        store = DurationStore()
        store.record("node:a", 1.0)
        store.flush()

    def test_concurrent_records_do_not_corrupt_state(self):
        store = DurationStore()

        def worker(n):
            for _ in range(50):
                store.record(f"node:{n}", 1.0 + n)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
            assert not t.is_alive()
        for i in range(8):
            assert store.predict(f"node:{i}") == pytest.approx(1.0 + i)


class TestStoreRegistry:
    def setup_method(self):
        reset_duration_stores()

    def teardown_method(self):
        reset_duration_stores()

    def test_same_path_shares_one_store(self):
        assert duration_store("/tmp/x.json") is duration_store("/tmp/x.json")

    def test_different_paths_are_isolated(self):
        assert duration_store("/tmp/a.json") is not duration_store("/tmp/b.json")


class TestStatsPath:
    def test_lives_outside_the_project_so_runs_leave_no_untracked_files(self):
        class Creds:
            workspaceid = "ws-1"
            lakehouseid = "lh-1"

        path = stats_path_for(Creds())
        assert os.path.basename(os.path.dirname(path)) == "dbt-fabricspark"
        assert os.path.isabs(path)
        assert os.path.basename(path).startswith(".fabricspark_poll_stats_")

    def test_honours_xdg_cache_home(self, tmp_path, monkeypatch):
        class Creds:
            workspaceid = "ws-1"

        monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
        assert stats_path_for(Creds()).startswith(str(tmp_path))

    def test_targets_sharing_a_directory_do_not_share_timings(self):
        class Creds:
            workspaceid = "ws-1"
            lakehouseid = "lh-1"

        class OtherCreds(Creds):
            lakehouseid = "lh-2"

        assert stats_path_for(Creds()) != stats_path_for(OtherCreds())

    def test_same_profile_resolves_to_a_stable_path(self):
        class Creds:
            workspaceid = "ws-1"

        assert stats_path_for(Creds()) == stats_path_for(Creds())

    def test_missing_attribute_is_survivable(self):
        assert stats_path_for(object()) is None
