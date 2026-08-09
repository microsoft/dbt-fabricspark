"""Telemetry-backed scheduling for authoritative Livy statement polls.

Elapsed time, learned runtimes and optional Spark task counters decide when to
poll next. Telemetry is advisory only; only the statement ``GET`` may resolve,
complete or fail a statement.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
import re
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Protocol

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("fabricspark")

MIN_INTERVAL = 0.25

# Ceiling on the poll interval and on added detection latency.
MAX_INTERVAL = 30.0

# Additive term that keeps short statements from burning the API budget.
BASE_INTERVAL = 0.5

ELAPSED_FRACTION = 0.12

CONVERGENCE_FRACTION = 0.85

EWMA_ALPHA = 0.35

# Predictions are only trusted once a shape has been seen this many times.
MIN_SAMPLES_FOR_TRUST = 1

# A prediction may only *extend* the poll interval once this many runs agree.
MIN_SAMPLES_TO_EXTEND = 3

# Caps how far a corroborated prediction may stretch the wait.
LENGTHEN_MULTIPLE = 4.0

STATS_TTL_SECONDS = 14 * 24 * 3600

STATS_FILENAME_TEMPLATE = ".fabricspark_poll_stats_{digest}.json"

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

    @property
    def fraction_done(self) -> Optional[float]:
        if self.total_tasks <= 0:
            return None
        return min(self.completed_tasks / self.total_tasks, 1.0)


class TelemetrySource(Protocol):
    def snapshot(self, statement_id: str) -> Optional[TelemetrySnapshot]: ...

    def watch(self, statement_id: str) -> None: ...

    def unwatch(self, statement_id: str) -> None: ...


@dataclass
class _Stat:
    ewma: float
    samples: int
    updated_at: float


class DurationStore:
    """Thread-safe EWMA of statement runtimes, persisted between dbt runs.

    Keys use dbt node ``unique_id`` where available and fall back to SQL shape.
    """

    def __init__(self, path: Optional[str] = None, clock: Callable[[], float] = time.time):
        self._path = path
        self._clock = clock
        self._lock = threading.Lock()
        self._stats: dict[str, _Stat] = {}
        self._dirty = False
        self._loaded = False

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        self._merge_file(self._stats)

    def _merge_file(self, into: dict[str, _Stat]) -> None:
        if not self._path or not os.path.exists(self._path):
            return
        try:
            with open(self._path, encoding="utf-8") as handle:
                raw = json.load(handle)
        except Exception as exc:
            logger.debug(f"Could not read poll stats from {self._path}: {exc}")
            return
        if not isinstance(raw, dict):
            return
        stats = raw.get("stats")
        if not isinstance(stats, dict):
            return
        now = self._clock()
        for key, value in stats.items():
            if not isinstance(value, dict):
                continue
            try:
                updated_at = float(value["updated_at"])
                if now - updated_at > STATS_TTL_SECONDS:
                    continue
                candidate = _Stat(
                    ewma=float(value["ewma"]),
                    samples=int(value["samples"]),
                    updated_at=updated_at,
                )
            except (KeyError, TypeError, ValueError):
                continue
            existing = into.get(key)
            if existing is None or existing.updated_at < candidate.updated_at:
                into[key] = candidate

    def record(self, key: Optional[str], duration: float) -> None:
        if not key or duration <= 0 or not math.isfinite(duration):
            return
        with self._lock:
            self._load()
            existing = self._stats.get(key)
            if existing is None:
                self._stats[key] = _Stat(ewma=duration, samples=1, updated_at=self._clock())
            else:
                existing.ewma = EWMA_ALPHA * duration + (1 - EWMA_ALPHA) * existing.ewma
                existing.samples += 1
                existing.updated_at = self._clock()
            self._dirty = True

    def predict(self, *keys: Optional[str]) -> Optional[float]:
        return self._lookup(*keys)[0]

    def estimate(self, *keys: Optional[str]) -> tuple[Optional[float], int]:
        return self._lookup(*keys)

    def _lookup(self, *keys: Optional[str]) -> tuple[Optional[float], int]:
        with self._lock:
            self._load()
            for key in keys:
                if not key:
                    continue
                stat = self._stats.get(key)
                if stat is not None and stat.samples >= MIN_SAMPLES_FOR_TRUST:
                    return stat.ewma, stat.samples
        return None, 0

    def flush(self) -> None:
        with self._lock:
            if not self._dirty or not self._path:
                return
            # Merge concurrent dbt process writes instead of clobbering them.
            merged = dict(self._stats)
            self._merge_file(merged)
            for key, stat in self._stats.items():
                on_disk = merged.get(key)
                if on_disk is None or on_disk.updated_at <= stat.updated_at:
                    merged[key] = stat
            payload = {
                "version": 1,
                "stats": {
                    key: {
                        "ewma": stat.ewma,
                        "samples": stat.samples,
                        "updated_at": stat.updated_at,
                    }
                    for key, stat in merged.items()
                },
            }
            handle = None
            try:
                os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
                # Concurrent dbt processes must never observe a half-written file.
                handle = tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    dir=os.path.dirname(self._path) or ".",
                    prefix=".fabricspark-stats-",
                    delete=False,
                )
                try:
                    json.dump(payload, handle)
                    handle.flush()
                finally:
                    handle.close()
                os.replace(handle.name, self._path)
                self._stats = merged
                self._dirty = False
            except Exception as exc:
                logger.debug(f"Could not persist poll stats to {self._path}: {exc}")
                if handle is not None:
                    try:
                        os.unlink(handle.name)
                    except OSError:
                        pass


_store_lock = threading.Lock()
_stores: dict[str, DurationStore] = {}


def duration_store(path: Optional[str]) -> DurationStore:
    key = path or ""
    with _store_lock:
        store = _stores.get(key)
        if store is None:
            store = DurationStore(path)
            _stores[key] = store
        return store


def flush_duration_stores() -> None:
    with _store_lock:
        stores = list(_stores.values())
    for store in stores:
        try:
            store.flush()
        except Exception as exc:
            logger.debug(f"Could not flush poll stats: {exc}")


def reset_duration_stores() -> None:
    with _store_lock:
        _stores.clear()


@dataclass
class PollPlan:
    interval: float
    reason: str
    eta: Optional[float] = None


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
    statement_id: Optional[str] = None

    polls: int = field(default=0, init=False)
    samples: int = field(default=0, init=False)
    _telemetry_eta_at: Optional[float] = field(default=None, init=False)
    _telemetry_deadline: Optional[float] = field(default=None, init=False)
    _quiescent_probe_pending: bool = field(default=False, init=False)
    _last_snapshot: Optional[TelemetrySnapshot] = field(default=None, init=False)
    _rate_ewma: Optional[float] = field(default=None, init=False)
    _last_total_tasks: int = field(default=0, init=False)

    @property
    def _telemetry_eta(self) -> Optional[float]:
        if self._telemetry_deadline is None or self._telemetry_eta_at is None:
            return None
        return max(self._telemetry_deadline - self._telemetry_eta_at, 0.0)

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
            self._telemetry_eta_at = elapsed
            self._telemetry_deadline = elapsed + remaining / self._rate_ewma
        else:
            self._telemetry_deadline = None

    def next_interval(self, elapsed: float) -> PollPlan:
        """Return how long to sleep before the next authoritative GET.

        Learned estimates only lengthen the wait after several corroborating
        runs; telemetry may lengthen immediately because it observes live work.
        """
        self.polls += 1

        # Metadata-only statements never register a Spark job.
        if self.polls == 1:
            return PollPlan(self.min_interval, "initial-probe")

        if self._quiescent_probe_pending:
            self._quiescent_probe_pending = False
            return PollPlan(self._jittered(self.min_interval), "telemetry-quiescent", 0.0)

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
                    # Bound the damage from an estimate that is wrong high.
                    interval = min(converged, interval * LENGTHEN_MULTIPLE)
                    reason = f"{source}-eta"

        return PollPlan(self._jittered(interval), reason, eta)

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


def stats_path_for(credentials: Any) -> Optional[str]:
    """Where to persist learned runtimes for this profile.

    The file lives in the user cache, not the project, and is keyed by a digest
    of the profile identity so targets do not share timings.
    """
    identity = "|".join(
        str(getattr(credentials, attr, "") or "")
        for attr in ("workspaceid", "lakehouseid", "schema", "livy_mode", "livy_url")
    )
    if not identity.strip("|"):
        return None
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]
    cache_root = os.environ.get("XDG_CACHE_HOME") or os.path.join(
        os.path.expanduser("~"), ".cache"
    )
    directory = os.path.join(cache_root, "dbt-fabricspark")
    try:
        os.makedirs(directory, exist_ok=True)
    except OSError:
        return None
    return os.path.join(directory, STATS_FILENAME_TEMPLATE.format(digest=digest))
