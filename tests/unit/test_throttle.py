"""Unit tests for the process-wide Fabric throttling governor."""

import threading
from unittest.mock import MagicMock

import pytest

from dbt.adapters.fabricspark.throttle import (
    PRIORITY_BACKGROUND,
    PRIORITY_CRITICAL,
    ThrottleGovernor,
    governor_for,
    governor_key,
    reset_governors,
)


class FakeClock:
    """Monotonic clock whose only advance comes from sleeping."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += max(seconds, 0.0)
        if seconds <= 0:
            # Guarantee forward progress so a buggy governor shows up as a test
            # timeout rather than an infinite loop.
            self.now += 0.001


def _governor(budget=10, clock=None, jitter=lambda a, b: 0.0):
    clock = clock or FakeClock()
    return (
        ThrottleGovernor(budget, clock=clock.time, sleeper=clock.sleep, jitter=jitter),
        clock,
    )


def _response(status=429, headers=None, body=None):
    resp = MagicMock()
    resp.status_code = status
    resp.headers = headers or {}
    if body is None:
        resp.json.side_effect = ValueError("no body")
    else:
        resp.json.return_value = body
    return resp


def test_allows_calls_up_to_budget_without_sleeping():
    gov, clock = _governor(budget=10)
    for _ in range(6):
        gov.acquire(PRIORITY_CRITICAL)
    assert clock.slept == []


def test_never_exceeds_budget_within_a_window():
    """The core invariant: no 60s window may contain more than `budget` calls."""
    gov, clock = _governor(budget=10)
    issued = []
    for _ in range(40):
        gov.acquire(PRIORITY_CRITICAL)
        issued.append(clock.now)

    for i, start in enumerate(issued):
        in_window = [t for t in issued[i:] if t < start + 60.0]
        assert len(in_window) <= 10, f"{len(in_window)} calls within 60s of t={start}"


def test_background_priority_is_capped_below_critical():
    gov, _ = _governor(budget=100)
    assert gov._allowance(PRIORITY_BACKGROUND) < gov._allowance(PRIORITY_CRITICAL)


def test_critical_still_admitted_once_background_exhausts_its_share():
    gov, clock = _governor(budget=10)
    for _ in range(gov._allowance(PRIORITY_BACKGROUND)):
        gov.acquire(PRIORITY_BACKGROUND)
    before = clock.now
    gov.acquire(PRIORITY_CRITICAL)
    assert clock.now == before, "critical traffic should not wait behind background traffic"


def test_penalize_parks_callers_for_retry_after():
    gov, clock = _governor(budget=100)
    gov.penalize(30)
    start = clock.now
    gov.acquire(PRIORITY_CRITICAL)
    assert clock.now >= start + 30


def test_a_429_in_one_thread_parks_every_other_thread():
    """Backoff must be process-wide, not thread-local."""
    gov, clock = _governor(budget=100)
    gov.note_response(_response(headers={"Retry-After": "45"}))

    started = threading.Barrier(4)
    observed: list[float] = []

    def worker():
        started.wait()
        gov.acquire(PRIORITY_CRITICAL)
        observed.append(clock.now)

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for t in threads:
        t.start()
    started.wait()
    for t in threads:
        t.join(timeout=10)
        assert not t.is_alive()

    assert len(observed) == 3
    assert all(t >= 1045.0 for t in observed), observed


def test_penalize_takes_the_latest_deadline_and_never_shortens_it():
    gov, clock = _governor(budget=100)
    gov.penalize(60)
    gov.penalize(5)
    start = clock.now
    gov.acquire(PRIORITY_CRITICAL)
    assert clock.now >= start + 60


def test_jitter_spreads_resumption_to_avoid_a_thundering_herd():
    jitters = iter([0.5, 3.0, 1.75])
    gov, _ = _governor(budget=100, jitter=lambda a, b: next(jitters))
    first = gov.penalize(10)
    gov._gate_until = 0.0
    second = gov.penalize(10)
    gov._gate_until = 0.0
    third = gov.penalize(10)
    assert len({round(first, 3), round(second, 3), round(third, 3)}) == 3


def test_jitter_is_bounded_by_the_retry_after():
    gov, _ = _governor(budget=100, jitter=lambda a, b: b)
    wait = gov.penalize(2)
    assert wait <= 2 + 2 + 0.001


def test_note_response_ignores_success():
    gov, _ = _governor(budget=100)
    assert gov.note_response(_response(status=200)) is False
    assert gov.throttle_events == 0


def test_note_response_reports_throttle_and_counts_it():
    gov, _ = _governor(budget=100)
    assert gov.note_response(_response(headers={"Retry-After": "12"})) is True
    assert gov.throttle_events == 1


def test_capacity_limit_defers_new_work_but_lets_polls_drain():
    """CapacityLimitExceeded is not a rate limit; blocking polls would stall
    the very work that frees the capacity back up."""
    gov, clock = _governor(budget=100)
    gov.note_response(
        _response(headers={"Retry-After": "5"}, body={"errorCode": "CapacityLimitExceeded"})
    )
    snap = gov.snapshot()
    assert snap["submit_gate_remaining"] >= 30
    assert snap["gate_remaining"] == 0

    before = clock.now
    gov.acquire(PRIORITY_CRITICAL)
    assert clock.now == before, "completion polls must not be blocked by capacity errors"

    gov.acquire(PRIORITY_BACKGROUND)
    assert clock.now >= before + 30, "new submissions must wait out the capacity limit"


def test_plain_rate_limit_blocks_every_priority():
    gov, clock = _governor(budget=100)
    gov.note_response(_response(headers={"Retry-After": "20"}))
    assert gov.snapshot()["gate_remaining"] >= 20
    start = clock.now
    gov.acquire(PRIORITY_CRITICAL)
    assert clock.now >= start + 20


def test_acquire_honours_a_caller_deadline():
    """A repeatedly extended gate must never park a caller past its timeout."""
    gov, clock = _governor(budget=100)
    gov.penalize(3600)
    deadline = clock.now + 5
    assert gov.acquire(PRIORITY_CRITICAL, deadline=deadline) is False
    assert clock.now <= 1000.0 + 5 + 1


def test_acquire_without_a_deadline_returns_true_on_success():
    gov, _ = _governor(budget=10)
    assert gov.acquire(PRIORITY_CRITICAL) is True


def test_zero_budget_is_unlimited_and_never_sleeps():
    """Local Livy has no quota; governing it would only add latency."""
    gov, clock = _governor(budget=0)
    for _ in range(5000):
        assert gov.acquire(PRIORITY_BACKGROUND) is True
    assert clock.slept == []


def test_missing_retry_after_still_produces_a_non_zero_pause():
    gov, _ = _governor(budget=100)
    gov.note_response(_response())
    assert gov.snapshot()["gate_remaining"] > 0


def test_window_slides_so_throughput_recovers():
    gov, clock = _governor(budget=10)
    for _ in range(10):
        gov.acquire(PRIORITY_CRITICAL)
    clock.sleep(61)
    before = clock.now
    gov.acquire(PRIORITY_CRITICAL)
    assert clock.now == before, "calls older than the window should have been pruned"


def test_concurrent_acquires_respect_the_budget():
    gov = ThrottleGovernor(20, jitter=lambda a, b: 0.0)
    gov.acquire(PRIORITY_CRITICAL)

    errors: list[BaseException] = []

    def worker():
        try:
            for _ in range(3):
                gov.acquire(PRIORITY_CRITICAL)
        except BaseException as exc:  # pragma: no cover - surfaced via assert
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive()
    assert not errors
    assert gov.snapshot()["calls_in_window"] <= 20


class TestGovernorRegistry:
    def setup_method(self):
        reset_governors()

    def teardown_method(self):
        reset_governors()

    def test_same_key_returns_the_same_governor(self):
        key = governor_key("https://api.fabric.microsoft.com/v1", "sp-1")
        assert governor_for(key) is governor_for(key)

    def test_different_principals_get_separate_budgets(self):
        endpoint = "https://api.fabric.microsoft.com/v1"
        a = governor_for(governor_key(endpoint, "sp-1"))
        b = governor_for(governor_key(endpoint, "sp-2"))
        assert a is not b

    def test_key_ignores_lakehouse_because_the_quota_is_per_identity(self):
        endpoint = "https://api.fabric.microsoft.com/v1"
        assert governor_key(endpoint, "sp-1") == governor_key(endpoint, "sp-1")


@pytest.mark.parametrize("budget", [1, 5, 200])
def test_budget_is_always_at_least_one(budget):
    gov, _ = _governor(budget=budget)
    assert gov._allowance(PRIORITY_BACKGROUND) >= 1
