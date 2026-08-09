"""Telemetry-backed scheduling for authoritative Livy statement polls.

Elapsed time, in-memory learned runtimes and optional Spark task counters decide
when to poll next. Telemetry is advisory only; only the statement ``GET`` may
resolve, complete or fail a statement.
"""

from __future__ import annotations

import math
import random
import re
import threading
from dataclasses import dataclass, field
from typing import Callable, Optional, Protocol

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("fabricspark")

MIN_INTERVAL = 0.25

MAX_INTERVAL = 30.0

# Additive term that keeps short statements from burning the API budget.
BASE_INTERVAL = 0.5

ELAPSED_FRACTION = 0.12

CONVERGENCE_FRACTION = 0.85

EWMA_ALPHA = 0.35

MIN_SAMPLES_FOR_TRUST = 1

# Below this a learned estimate can only shorten the wait, which costs more
# polls than having no estimate at all. Over-estimates stay safe regardless:
# lengthening also requires elapsed < predicted * CONVERGENCE_FRACTION and is
# capped by LENGTHEN_MULTIPLE and MAX_INTERVAL.
MIN_SAMPLES_TO_EXTEND = 1

LENGTHEN_MULTIPLE = 4.0

_IDENTIFIER_RE = re.compile(r"'[^']*'|\"[^\"]*\"|\b\d+\b")
_WHITESPACE_RE = re.compile(r"\s+")


def sql_shape(sql: str) -> str:
    """Collapse a statement to a coarse fingerprint.

    Literals and whitespace are stripped so structurally identical statements
    against different partitions share a runtime estimate.
    """
    normalized = _IDENTIFIER_RE.sub("?", sql.lower())
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip()
    return normalized[:512]


@dataclass
class TelemetrySnapshot:
    """Point-in-time Spark counters; ``total_tasks`` is not a fixed denominator."""

    total_tasks: int = 0
    completed_tasks: int = 0
    active_tasks: int = 0
    failed_tasks: int = 0
    known_jobs: int = 0
    active_jobs: int = 0
    failed_jobs: int = 0
    observed_at: float = 0.0

    @property
    def has_work(self) -> bool:
        return self.known_jobs > 0


class TelemetrySource(Protocol):
    def snapshot(self, statement_id: str) -> Optional[TelemetrySnapshot]: ...

    def watch(self, statement_id: str) -> None: ...

    def unwatch(self, statement_id: str) -> None: ...


@dataclass
class _Stat:
    ewma: float
    samples: int


class DurationStore:
    """Thread-safe in-memory EWMA of statement runtimes.

    Keys use dbt node ``unique_id`` where available and fall back to SQL shape.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stats: dict[str, _Stat] = {}

    def record(self, key: Optional[str], duration: float) -> None:
        if not key or duration <= 0 or not math.isfinite(duration):
            return
        with self._lock:
            existing = self._stats.get(key)
            if existing is None:
                self._stats[key] = _Stat(ewma=duration, samples=1)
            else:
                existing.ewma = EWMA_ALPHA * duration + (1 - EWMA_ALPHA) * existing.ewma
                existing.samples += 1

    def estimate(self, *keys: Optional[str]) -> tuple[Optional[float], int]:
        """Return the first trusted key's EWMA and its sample count.

        Callers pass keys most-specific first. The sample count travels with the
        estimate because it gates whether the scheduler may lengthen a wait.
        """
        with self._lock:
            for key in keys:
                if not key:
                    continue
                stat = self._stats.get(key)
                if stat is not None and stat.samples >= MIN_SAMPLES_FOR_TRUST:
                    return stat.ewma, stat.samples
        return None, 0

    def clear(self) -> None:
        with self._lock:
            self._stats.clear()


_store = DurationStore()


def duration_store() -> DurationStore:
    return _store


@dataclass
class PollPlan:
    interval: float
    reason: str


@dataclass
class PollScheduler:
    """Decides how long to wait before the next authoritative statement GET.

    It polls once early so metadata-only statements, which never register Spark
    jobs, are not delayed by a long first sleep.
    """

    predicted_duration: Optional[float] = None
    min_interval: float = MIN_INTERVAL
    max_interval: float = MAX_INTERVAL
    base_interval: float = BASE_INTERVAL
    elapsed_fraction: float = ELAPSED_FRACTION
    jitter: Callable[[float, float], float] = random.uniform
    telemetry: Optional[TelemetrySource] = None

    polls: int = field(default=0, init=False)
    samples: int = field(default=0, init=False)
    _telemetry_deadline: Optional[float] = field(default=None, init=False)
    _quiescent_probe_pending: bool = field(default=False, init=False)
    _last_snapshot: Optional[TelemetrySnapshot] = field(default=None, init=False)
    _rate_ewma: Optional[float] = field(default=None, init=False)
    _last_total_tasks: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        # A large poll_statement_wait can push min_interval above max_interval,
        # which would make _clamp return the *minimum* for every plan.
        self.max_interval = max(self.max_interval, self.min_interval)

    def observe(self, snapshot: Optional[TelemetrySnapshot], elapsed: float) -> None:
        if snapshot is None or not snapshot.has_work:
            # Empty job groups happen before and between a statement's Spark jobs.
            return

        previous = self._last_snapshot
        self._last_snapshot = snapshot

        # AQE can change the task denominator, invalidating the prior rate.
        if snapshot.total_tasks != self._last_total_tasks:
            topology_changed = bool(self._last_total_tasks) and abs(
                snapshot.total_tasks - self._last_total_tasks
            ) > max(1, self._last_total_tasks // 2)
            self._last_total_tasks = snapshot.total_tasks
            if topology_changed:
                self._rate_ewma = None
                self._telemetry_deadline = None
                return

        if previous is None or snapshot.observed_at <= previous.observed_at:
            return

        delta_tasks = snapshot.completed_tasks - previous.completed_tasks
        delta_time = snapshot.observed_at - previous.observed_at
        if delta_tasks < 0 or delta_time <= 0:
            return

        rate = delta_tasks / delta_time
        self._rate_ewma = rate if self._rate_ewma is None else 0.4 * rate + 0.6 * self._rate_ewma

        remaining = snapshot.total_tasks - snapshot.completed_tasks
        if remaining <= 0:
            # All known jobs terminal does not mean statement completion; one SQL
            # statement can run several Spark jobs.
            if previous is None or previous.completed_tasks < previous.total_tasks:
                self._quiescent_probe_pending = True
            self._telemetry_deadline = None
            return
        if self._rate_ewma and self._rate_ewma > 0:
            self._telemetry_deadline = elapsed + remaining / self._rate_ewma
        else:
            self._telemetry_deadline = None

    def next_interval(self, elapsed: float) -> PollPlan:
        """Plan the next authoritative GET.

        Live telemetry may lengthen the wait immediately because it observes
        actual progress; a learned estimate may only do so once corroborated.
        """
        self.polls += 1

        # Metadata-only statements never register a Spark job.
        if self.polls == 1:
            return PollPlan(self.min_interval, "initial-probe")

        if self._quiescent_probe_pending:
            self._quiescent_probe_pending = False
            return PollPlan(self._jittered(self.min_interval), "telemetry-quiescent")

        interval = self._elapsed_based(elapsed)
        reason = "elapsed-proportional"
        eta = self._effective_eta(elapsed)

        if eta is not None:
            from_telemetry = self._telemetry_deadline is not None
            source = "telemetry" if from_telemetry else "learned"
            if eta <= 0:
                reason = "overrun"
            else:
                converged = max(eta * CONVERGENCE_FRACTION, self.min_interval)
                if converged < interval:
                    interval = converged
                    reason = f"{source}-eta"
                elif self._may_lengthen(from_telemetry, elapsed):
                    interval = min(converged, interval * LENGTHEN_MULTIPLE)
                    reason = f"{source}-eta"

        return PollPlan(self._jittered(interval), reason)

    def _may_lengthen(self, from_telemetry: bool, elapsed: float) -> bool:
        if from_telemetry:
            return True
        if self.samples < MIN_SAMPLES_TO_EXTEND or self.predicted_duration is None:
            return False
        return elapsed < self.predicted_duration * CONVERGENCE_FRACTION

    def _jittered(self, interval: float) -> float:
        interval = self._clamp(interval)
        return self._clamp(interval + self.jitter(0.0, min(interval * 0.15, 1.0)))

    def _effective_eta(self, elapsed: float) -> Optional[float]:
        if self._telemetry_deadline is not None:
            return self._telemetry_deadline - elapsed
        if self.predicted_duration is None:
            return None
        return self.predicted_duration - elapsed

    def _elapsed_based(self, elapsed: float) -> float:
        return max(self.base_interval + elapsed * self.elapsed_fraction, self.min_interval)

    def _clamp(self, interval: float) -> float:
        return max(self.min_interval, min(interval, self.max_interval))
