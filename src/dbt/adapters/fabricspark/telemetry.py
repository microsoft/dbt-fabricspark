"""Live Spark telemetry for in-flight Livy statements.

Fabric's statement GET has no ``progress`` field, but Fabric sets
``spark.jobGroup.id`` to the Livy statement id. A monitor REPL on the same
``SparkContext`` can read task counters for other REPLs' statements.

Telemetry is advisory only. A statement can run several Spark jobs with an empty
job group between them, so only the authoritative statement GET may resolve,
complete or fail it.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Optional

import requests

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabricspark.adaptive_polling import TelemetrySnapshot
from dbt.adapters.fabricspark.throttle import (
    PRIORITY_BACKGROUND,
    ThrottleGovernor,
    governed,
)

logger = AdapterLogger("Microsoft Fabric-Spark")

# Detection granularity for all watched statements, not per model.
MIN_MONITOR_INTERVAL = 2.0
MAX_MONITOR_INTERVAL = 15.0

# Losing telemetry must never fail a model.
MAX_MONITOR_FAILURES = 3

_SNAPSHOT_SENTINEL = "__DBT_FABRICSPARK_TELEMETRY__"

# Runs inside the monitor REPL and prints one JSON line for the adapter to parse.
_PROBE_TEMPLATE = """
import json
_tracker = sc.statusTracker()
_out = {{}}
for _sid in {statement_ids!r}:
    _total = _completed = _active = _failed = 0
    _jobs = _active_jobs = _failed_jobs = 0
    try:
        _job_ids = _tracker.getJobIdsForGroup(_sid)
    except Exception:
        _job_ids = []
    for _jid in _job_ids:
        _jobs += 1
        try:
            _job = _tracker.getJobInfo(_jid)
        except Exception:
            _job = None
        if _job is None:
            continue
        _status = str(getattr(_job, "status", ""))
        if _status == "RUNNING":
            _active_jobs += 1
        elif _status == "FAILED":
            _failed_jobs += 1
        for _stage_id in getattr(_job, "stageIds", []) or []:
            try:
                _stage = _tracker.getStageInfo(_stage_id)
            except Exception:
                _stage = None
            if _stage is None:
                continue
            _total += getattr(_stage, "numTasks", 0) or 0
            _completed += getattr(_stage, "numCompletedTasks", 0) or 0
            _active += getattr(_stage, "numActiveTasks", 0) or 0
            _failed += getattr(_stage, "numFailedTasks", 0) or 0
    _out[_sid] = [_total, _completed, _active, _failed, _jobs, _active_jobs, _failed_jobs]
print("{sentinel}" + json.dumps(_out))
"""


class MonitorTelemetrySource:
    """Reads Spark task counters for watched statements via a monitor REPL.

    One instance per underlying Livy session; workers read cached snapshots
    while a single background thread refreshes counters.
    """

    def __init__(
        self,
        credential: Any,
        statements_url: str,
        governor: ThrottleGovernor,
        headers_factory,
    ) -> None:
        self.credential = credential
        self._statements_url = statements_url
        self._governor = governor
        self._headers = headers_factory
        self._lock = threading.Lock()
        self._watched: set[str] = set()
        self._snapshots: dict[str, TelemetrySnapshot] = {}
        self._failures = 0
        self._disabled = False
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def watch(self, statement_id: str) -> None:
        if self._disabled:
            return
        with self._lock:
            self._watched.add(statement_id)
        self._ensure_thread()
        self._wake.set()

    def unwatch(self, statement_id: str) -> None:
        with self._lock:
            self._watched.discard(statement_id)
            self._snapshots.pop(statement_id, None)

    def snapshot(self, statement_id: str) -> Optional[TelemetrySnapshot]:
        with self._lock:
            return self._snapshots.get(statement_id)

    @property
    def disabled(self) -> bool:
        return self._disabled

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=5)

    def _ensure_thread(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._thread = threading.Thread(
                target=self._run, name="fabricspark-telemetry", daemon=True
            )
            self._thread.start()

    def _run(self) -> None:
        interval = MIN_MONITOR_INTERVAL
        last_probe_at = 0.0
        while not self._stop.is_set():
            # New watches wake the monitor without bypassing the probe floor.
            floor_wait = max(last_probe_at + MIN_MONITOR_INTERVAL - time.monotonic(), 0.0)
            self._wake.wait(max(interval, floor_wait))
            if self._stop.is_set():
                return
            remaining = last_probe_at + MIN_MONITOR_INTERVAL - time.monotonic()
            if remaining > 0:
                self._stop.wait(remaining)
                if self._stop.is_set():
                    return
            self._wake.clear()
            with self._lock:
                watched = sorted(self._watched)
            if not watched:
                interval = MIN_MONITOR_INTERVAL
                continue
            last_probe_at = time.monotonic()
            if not self._refresh(watched):
                return
            interval = min(
                max(MIN_MONITOR_INTERVAL, MIN_MONITOR_INTERVAL * (4.0 / max(len(watched), 1))),
                MAX_MONITOR_INTERVAL,
            )

    def _refresh(self, watched: list[str]) -> bool:
        try:
            counters = self._probe(watched)
        except Exception as exc:
            self._failures += 1
            logger.debug(
                f"Telemetry probe failed ({self._failures}/{MAX_MONITOR_FAILURES}): {exc}"
            )
            if self._failures >= MAX_MONITOR_FAILURES:
                logger.warning(
                    "Disabling adaptive polling telemetry after repeated monitor failures; "
                    "falling back to schedule-based polling"
                )
                self._disabled = True
                return False
            return True

        self._failures = 0
        now = time.monotonic()
        with self._lock:
            for statement_id, values in counters.items():
                if statement_id not in self._watched:
                    continue
                total, completed, active, failed, jobs, active_jobs, failed_jobs = values
                self._snapshots[statement_id] = TelemetrySnapshot(
                    total_tasks=total,
                    completed_tasks=completed,
                    active_tasks=active,
                    failed_tasks=failed,
                    known_jobs=jobs,
                    active_jobs=active_jobs,
                    failed_jobs=failed_jobs,
                    observed_at=now,
                )
        return True

    def _probe(self, watched: list[str]) -> dict[str, list[int]]:
        code = _PROBE_TEMPLATE.format(statement_ids=watched, sentinel=_SNAPSHOT_SENTINEL)
        submit = governed(
            self._governor,
            PRIORITY_BACKGROUND,
            requests.post,
            self._statements_url,
            data=json.dumps({"code": code, "kind": "pyspark"}),
            headers=self._headers(),
            timeout=self.credential.http_timeout,
        )
        if submit.status_code >= 400:
            raise RuntimeError(f"monitor submit returned HTTP {submit.status_code}")
        statement_id = submit.json().get("id")
        if statement_id is None:
            raise RuntimeError("monitor submit returned no statement id")

        url = f"{self._statements_url}/{statement_id}"
        deadline = time.monotonic() + max(self.credential.http_timeout, 30)
        while time.monotonic() < deadline:
            resp = governed(
                self._governor,
                PRIORITY_BACKGROUND,
                requests.get,
                url,
                headers=self._headers(),
                timeout=self.credential.http_timeout,
                governor_deadline=deadline,
            )
            if resp.status_code >= 400:
                raise RuntimeError(f"monitor poll returned HTTP {resp.status_code}")
            body = resp.json()
            state = body.get("state")
            if state == "available":
                return _parse_probe_output(body)
            if state in ("error", "cancelled", "cancelling"):
                raise RuntimeError(f"monitor statement ended in state {state}")
            time.sleep(0.5)
        raise TimeoutError("monitor statement did not complete in time")


def _parse_probe_output(body: dict) -> dict[str, list[int]]:
    output = body.get("output") or {}
    if output.get("status") != "ok":
        raise RuntimeError(f"monitor statement failed: {output.get('evalue', 'unknown')}")
    text = ((output.get("data") or {}).get("text/plain")) or ""
    for line in reversed(text.splitlines()):
        if _SNAPSHOT_SENTINEL in line:
            payload = line.split(_SNAPSHOT_SENTINEL, 1)[1].strip()
            parsed = json.loads(payload)
            return {
                str(key): [int(v) for v in values]
                for key, values in parsed.items()
                if isinstance(values, list) and len(values) == 7
            }
    raise ValueError("monitor statement produced no telemetry payload")
