"""Guards for hostile or malformed throttling responses from Fabric."""

from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.fabricspark._http_utils import MAX_RETRY_AFTER, parse_retry_after
from dbt.adapters.fabricspark.throttle import (
    PRIORITY_BACKGROUND,
    ThrottleGovernor,
)


def _throttled(header=None, body=None):
    response = MagicMock(
        status_code=429, headers={} if header is None else {"Retry-After": header}
    )
    if isinstance(body, Exception):
        response.json.side_effect = body
    else:
        response.json.return_value = body if body is not None else {}
    return response


class TestMalformedRetryAfterCannotBreakTheGovernor:
    """`float()` accepts `NaN`, `Infinity` and values that overflow to `inf`.

    Both leak straight into the shared gate. `NaN` fails every comparison in
    `penalize`, so the gate never advances and the submit retry loop hammers an
    endpoint that just asked for a pause; `inf` parks every call in the process
    forever with no timeout and no way out.
    """

    @pytest.mark.parametrize("header", ["NaN", "nan", "Infinity", "inf", "1e309"])
    def test_non_finite_hints_are_discarded(self, header):
        assert parse_retry_after(_throttled(header)) == 0

    @pytest.mark.parametrize("header", ["-5", "0"])
    def test_non_positive_hints_are_discarded(self, header):
        assert parse_retry_after(_throttled(header)) == 0

    @pytest.mark.parametrize("header", ["1e300", "999999999", "7199"])
    def test_absurd_but_finite_hints_are_capped(self, header):
        assert parse_retry_after(_throttled(header)) == MAX_RETRY_AFTER

    def test_a_multi_hour_fabric_hint_is_capped_rather_than_obeyed(self):
        """Fabric asks for as much as 7199s, but the quota usually frees up
        sooner, so the endpoint is re-probed instead of stalling for hours."""
        assert parse_retry_after(_throttled("7199")) == MAX_RETRY_AFTER

    def test_garbage_hints_fall_back_to_the_body(self):
        body = {"message": "throttled until: 4/17/2099 12:22:35 PM (UTC)"}
        assert parse_retry_after(_throttled("not-a-number", body)) > 0

    @pytest.mark.parametrize("header", ["NaN", "Infinity", "1e309", "-5"])
    def test_the_gate_always_advances_by_a_finite_amount(self, header):
        governor = ThrottleGovernor(200, clock=lambda: 1000.0, jitter=lambda _a, _b: 0.0)
        governor.note_response(_throttled(header))

        waits = []
        governor._sleeper = waits.append
        governor._clock = lambda: 1000.0 + sum(waits)
        governor.acquire(PRIORITY_BACKGROUND, deadline=1000.0 + MAX_RETRY_AFTER * 2)

        assert 0 < sum(waits) <= MAX_RETRY_AFTER + 60


class TestThrottledSubmitWaitsBeforeRetrying:
    """End to end: a 429 on submit must delay the retry POST.

    Asserting on the governor alone would still pass if a backend later stopped
    routing submissions through it or sent them at a priority the gate ignores.
    """

    @pytest.mark.parametrize("budget", [0, 200])
    @pytest.mark.parametrize("body", [{}, {"errorCode": "CapacityLimitExceeded"}])
    def test_high_concurrency_submit_retry_is_gated(self, budget, body):
        from dbt.adapters.fabricspark import concurrent_livy as cl

        clock = [1000.0]
        governor = ThrottleGovernor(budget, clock=lambda: clock[0], jitter=lambda _a, _b: 0.0)
        governor._sleeper = lambda seconds: clock.__setitem__(0, clock[0] + seconds)

        posted_at = []
        responses = [_throttled("600", body), MagicMock(status_code=200)]
        responses[1].json.return_value = {"id": 7}

        def post(*_args, **_kwargs):
            posted_at.append(clock[0])
            return responses[len(posted_at) - 1]

        cursor = MagicMock(spec=cl.HighConcurrencyCursor)
        cursor.governor = governor
        cursor.credential = MagicMock(http_timeout=120)
        cursor.hc_session = MagicMock()
        cursor.hc_session.statements_url.return_value = "http://livy/statements"

        with (
            patch.object(cl.requests, "post", side_effect=post),
            patch.object(cl, "_get_headers", return_value={}),
        ):
            cl.HighConcurrencyCursor._submit(cursor, "select 1")

        assert len(posted_at) == 2
        assert posted_at[1] - posted_at[0] == pytest.approx(MAX_RETRY_AFTER, abs=5)


class TestReconciliationStopsAtItsDeadline:
    """Reconciliation decides whether side-effecting SQL is already running.

    Once its deadline passes, the remaining attempts would fire back to back
    with no pause -- extra load on an endpoint that is already throttling us,
    for a lookup that cannot see anything a moment earlier did not.
    """

    @pytest.mark.parametrize("module_name", ["concurrent_livy", "singleton_livy"])
    def test_no_further_lookups_are_issued_after_the_deadline(self, module_name):
        import importlib

        module = importlib.import_module(f"dbt.adapters.fabricspark.{module_name}")
        cls = (
            module.HighConcurrencyCursor if module_name == "concurrent_livy" else module.LivyCursor
        )

        calls = []

        def governed(_governor, _priority, _method, *_args, **kwargs):
            calls.append(kwargs.get("governor_deadline"))
            raise RuntimeError("lookup unavailable")

        cursor = MagicMock(spec=cls)
        cursor.governor = MagicMock()
        cursor.credential = MagicMock(http_timeout=1)
        cursor._statements_url = MagicMock(return_value="http://livy/statements")
        cursor.hc_session = MagicMock()
        cursor.hc_session.statements_url.return_value = "http://livy/statements"

        with (
            patch.object(module, "_governed", side_effect=governed),
            patch.object(module, "_get_headers", return_value={}),
            patch.object(module, "_sleep_until"),
            patch.object(module.time, "monotonic", side_effect=[0.0] + [10_000.0] * 20),
        ):
            outcome, adopted = cls._find_submitted_statement(cursor, "marker")

        assert outcome == "unknown"
        assert adopted is None
        assert len(calls) == 1, "an expired deadline must not buy more attempts"

    @pytest.mark.parametrize("module_name", ["concurrent_livy", "singleton_livy"])
    def test_a_failed_lookup_never_reports_absent(self, module_name):
        import importlib

        module = importlib.import_module(f"dbt.adapters.fabricspark.{module_name}")
        cls = (
            module.HighConcurrencyCursor if module_name == "concurrent_livy" else module.LivyCursor
        )

        cursor = MagicMock(spec=cls)
        cursor.governor = MagicMock()
        cursor.credential = MagicMock(http_timeout=1)
        cursor._statements_url = MagicMock(return_value="http://livy/statements")
        cursor.hc_session = MagicMock()
        cursor.hc_session.statements_url.return_value = "http://livy/statements"

        with (
            patch.object(module, "_governed", side_effect=RuntimeError("boom")),
            patch.object(module, "_get_headers", return_value={}),
            patch.object(module, "_sleep_until"),
        ):
            outcome, _ = cls._find_submitted_statement(cursor, "marker")

        assert outcome == "unknown", "only a successful read may permit a resubmit"
