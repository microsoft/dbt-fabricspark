"""Process-wide throttling governor for Fabric REST traffic.

Fabric enforces its REST quota per identity, not per lakehouse. The shared
governor combines a sliding-window limiter with a process-wide ``Retry-After``
gate so one throttled thread slows the rest.
"""

from __future__ import annotations

import random
import threading
import time
from collections import deque
from types import TracebackType
from typing import Any, Callable, Optional

import requests

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabricspark._http_utils import parse_retry_after

logger = AdapterLogger("Microsoft Fabric-Spark")


# Fabric's published unified quota. The adapter aims below it because other
# processes using the same principal share the bucket.
FABRIC_QUOTA_PER_MINUTE = 200
DEFAULT_BUDGET_PER_MINUTE = 150

PRIORITY_CRITICAL = 0  # cancel, authoritative statement GET
PRIORITY_NORMAL = 1  # session lifecycle, metadata
PRIORITY_BACKGROUND = 2  # statement submit, monitor telemetry

# Reserve headroom for critical polls and cancels.
_PRIORITY_SHARE = {
    PRIORITY_CRITICAL: 1.0,
    PRIORITY_NORMAL: 0.85,
    PRIORITY_BACKGROUND: 0.65,
}

_WINDOW_SECONDS = 60.0
# Re-evaluate promptly if the governor is re-penalised while waiters sleep.
_MAX_SLEEP_SLICE = 5.0


class ThrottleGovernor:
    """Sliding-window limiter plus a shared ``Retry-After`` gate.

    ``clock``/``sleeper``/``jitter`` are injectable for deterministic tests.
    """

    def __init__(
        self,
        budget_per_minute: int = DEFAULT_BUDGET_PER_MINUTE,
        *,
        window: float = _WINDOW_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        jitter: Callable[[float], float] = random.uniform,
    ) -> None:
        self.budget_per_minute = max(int(budget_per_minute), 1)
        self.window = window
        self._clock = clock
        self._sleeper = sleeper
        self._jitter = jitter
        self._lock = threading.Lock()
        self._calls: deque[float] = deque()
        self._gate_until = 0.0
        # Capacity exhaustion parks new work but lets polls and cancels through.
        self._submit_gate_until = 0.0
        self._throttle_events = 0
        self._unlimited = int(budget_per_minute) <= 0

    def _prune(self, now: float) -> None:
        cutoff = now - self.window
        while self._calls and self._calls[0] <= cutoff:
            self._calls.popleft()

    def _allowance(self, priority: int) -> int:
        share = _PRIORITY_SHARE.get(priority, _PRIORITY_SHARE[PRIORITY_BACKGROUND])
        return max(int(self.budget_per_minute * share), 1)

    def _gate_for(self, priority: int) -> float:
        if priority <= PRIORITY_CRITICAL:
            return self._gate_until
        return max(self._gate_until, self._submit_gate_until)

    def acquire(self, priority: int = PRIORITY_NORMAL, deadline: Optional[float] = None) -> bool:
        """Block until this caller may issue one request.

        ``deadline`` is an absolute time on this governor's clock. ``False``
        means it passed before a slot became available.
        """
        while True:
            with self._lock:
                now = self._clock()
                gate = self._gate_for(priority)
                if now >= gate:
                    if self._unlimited:
                        return True
                    self._prune(now)
                    if len(self._calls) < self._allowance(priority):
                        self._calls.append(now)
                        return True
                    wait = (self._calls[0] + self.window) - now
                else:
                    wait = gate - now
                wait += self._jitter(0.0, min(wait * 0.25, 2.0))

            if deadline is not None:
                remaining = deadline - self._clock()
                if remaining <= 0:
                    return False
                wait = min(wait, remaining)
            self._sleeper(max(min(wait, _MAX_SLEEP_SLICE), 0.0))

    def penalize(self, retry_after: float, submissions_only: bool = False) -> float:
        """Park callers until ``retry_after`` (plus jitter) has elapsed.

        With ``submissions_only``, critical polls and cancels still pass so
        in-flight work can drain.
        """
        wait = max(float(retry_after), 1.0)
        with self._lock:
            self._throttle_events += 1
            deadline = self._clock() + wait + self._jitter(0.0, min(wait, 5.0))
            if submissions_only:
                self._submit_gate_until = max(self._submit_gate_until, deadline)
                return self._submit_gate_until - self._clock()
            if deadline > self._gate_until:
                self._gate_until = deadline
            return self._gate_until - self._clock()

    def note_response(self, response: requests.Response) -> bool:
        """Record a response; park callers on ``429``.

        Returns ``True`` when the response was a throttle and the caller should
        retry the *same* request.
        """
        if response.status_code != 429:
            return False
        if _is_capacity_error(response):
            wait = self.penalize(max(parse_retry_after(response), 30.0), submissions_only=True)
            logger.warning(
                f"Fabric capacity limit exceeded; deferring new Fabric work for {wait:.0f}s"
            )
            return True
        wait = self.penalize(parse_retry_after(response) or 10.0)
        logger.debug(f"HTTP 429 from Fabric; pausing all Fabric calls for {wait:.0f}s")
        return True

    @property
    def throttle_events(self) -> int:
        return self._throttle_events

    def snapshot(self) -> dict[str, float]:
        with self._lock:
            now = self._clock()
            self._prune(now)
            return {
                "calls_in_window": len(self._calls),
                "budget_per_minute": self.budget_per_minute,
                "gate_remaining": max(self._gate_until - now, 0.0),
                "submit_gate_remaining": max(self._submit_gate_until - now, 0.0),
                "throttle_events": self._throttle_events,
            }

    def now(self) -> float:
        return self._clock()


def _is_capacity_error(response: requests.Response) -> bool:
    try:
        body = response.json()
    except Exception:
        return False
    if not isinstance(body, dict):
        return False
    return "capacitylimitexceeded" in str(body.get("errorCode", "")).lower()


class _Slot:
    def __init__(self, governor: ThrottleGovernor, priority: int) -> None:
        self._governor = governor
        self._priority = priority

    def __enter__(self) -> ThrottleGovernor:
        self._governor.acquire(self._priority)
        return self._governor

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        return False


_registry_lock = threading.Lock()
_governors: dict[str, ThrottleGovernor] = {}


def governor_for(key: str, budget_per_minute: Optional[int] = None) -> ThrottleGovernor:
    with _registry_lock:
        gov = _governors.get(key)
        if gov is None:
            budget = (
                budget_per_minute if budget_per_minute is not None else DEFAULT_BUDGET_PER_MINUTE
            )
            gov = ThrottleGovernor(budget)
            _governors[key] = gov
        return gov


def reset_governors() -> None:
    with _registry_lock:
        _governors.clear()


def governor_key(endpoint: Optional[str], principal: Optional[str]) -> str:
    return f"{endpoint or 'fabric'}|{principal or 'default'}"


def governor_for_credentials(credentials: Any) -> ThrottleGovernor:
    if getattr(credentials, "is_local_mode", False):
        return _UNLIMITED
    return governor_for(
        governor_key(
            getattr(credentials, "endpoint", None),
            getattr(credentials, "throttle_identity", None),
        ),
        getattr(credentials, "api_calls_per_minute", DEFAULT_BUDGET_PER_MINUTE),
    )


def governed(
    governor: ThrottleGovernor,
    priority: int,
    method: Callable[..., requests.Response],
    *args: Any,
    **kwargs: Any,
) -> requests.Response:
    """Issue one REST call under the process-wide throttle governor.

    ``governor_deadline`` bounds how long the call may be parked.
    """
    deadline = kwargs.pop("governor_deadline", None)
    governor.acquire(priority, deadline)
    response = method(*args, **kwargs)
    governor.note_response(response)
    return response


_UNLIMITED = ThrottleGovernor(0)


def slot(governor: ThrottleGovernor, priority: int = PRIORITY_NORMAL) -> _Slot:
    return _Slot(governor, priority)
