from __future__ import annotations

import atexit
import datetime as dt
import hashlib
import json
import re
import threading
import time
import uuid
from types import TracebackType
from typing import Any, Optional

import requests
from dbt_common.events.contextvars import get_node_info
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt_common.utils.encoding import DECIMALS

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.fabricspark import livysession as _livy_helpers
from dbt.adapters.fabricspark.adaptive_polling import (
    MIN_INTERVAL,
    PollScheduler,
    TelemetrySource,
    duration_store,
    sql_shape,
)
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.errors import AmbiguousSubmissionError
from dbt.adapters.fabricspark.livy_backend import LivyBackend, coerce_time_columns
from dbt.adapters.fabricspark.shortcuts import ShortcutClient
from dbt.adapters.fabricspark.telemetry import MonitorTelemetrySource
from dbt.adapters.fabricspark.throttle import (
    PRIORITY_BACKGROUND,
    PRIORITY_CRITICAL,
    PRIORITY_NORMAL,
    governor_for_credentials,
    parse_retry_after,
)
from dbt.adapters.fabricspark.throttle import (
    governed as _governed,
)

logger = AdapterLogger("Microsoft Fabric-Spark")
NUMBERS = DECIMALS + (int, float)

# Injected into the submitted SQL as a comment so an ambiguous POST can be
# reconciled against Livy's statement list instead of blindly resubmitted.
_SUBMIT_MARKER_PREFIX = "dbt-fabricspark-submit:"

# Livy may publish an accepted statement after the failed POST returns.
_RECONCILE_ATTEMPTS = 4
_RECONCILE_BACKOFF = 1.5

# Teardown and cancel run on the interpreter-exit path and under dbt's
# connection-manager lock, so they must never park on the throttle gate.
_TEARDOWN_TIMEOUT = 15
_TEARDOWN_GOVERNOR_WAIT = 5.0

# Fabric's REPL packing cap. Defaults to 5 server-side and accepts 2-50.
_HC_MAX_CONF = "spark.highConcurrency.max"
_HC_MAX_DEFAULT = 5
_HC_MAX_CEILING = 50


class _AdoptedSubmission:
    def __init__(self, statement_id: int) -> None:
        self._statement_id = statement_id

    def json(self) -> dict:
        return {"id": self._statement_id}


# HC sessions whose state transitions through these values have not yet
# produced sessionId/replId; keep polling until state leaves the set.
_ACQUIRING_STATES = frozenset({"NotStarted", "starting", "AcquiringHighConcurrencySession"})
_TERMINAL_BAD_STATES = frozenset({"Dead", "Killed", "Failed", "Error"})


_active_sessions_lock = threading.Lock()
# All in-flight HighConcurrencySession instances across every dbt thread.
# On process exit the atexit handler DELETEs each HC id whose credentials do
# NOT set reuse_session, freeing REPL slots promptly instead of waiting for
# Fabric's idle reaper. reuse_session sessions are intentionally left alive so
# the underlying Livy session stays warm for the next invocation.
_active_sessions: "set[HighConcurrencySession]" = set()


_session_tag_lock = threading.Lock()
# Deterministic tag per (workspaceid, lakehouseid) when reuse_session is true,
# uuid per process otherwise. Cached at module scope so every per-thread
# manager generates the same tag and Fabric packs every acquire onto the
# same underlying Livy session.
_session_tags: dict[tuple[str, str, bool], str] = {}


_shortcuts_done_lock = threading.Lock()
# Process-level guard so OneLake shortcuts are created exactly once per
# (workspaceid, lakehouseid) even when multiple threads acquire HC sessions
# in parallel.
_shortcuts_done: "set[tuple[str, str]]" = set()


_MAX_RETRY_BACKOFF = 30.0


def _sleep_until(wait: float, deadline: Optional[float]) -> None:
    if deadline is not None:
        wait = min(wait, max(deadline - time.monotonic(), 0.0))
    if wait > 0:
        time.sleep(wait)


def _get_headers(credentials: FabricSparkCredentials, tokenPrint: bool = False) -> dict[str, str]:
    return _livy_helpers.get_headers(credentials, tokenPrint)


def derive_session_tag(credentials: FabricSparkCredentials) -> str:
    """Return the sessionTag used by all HC acquires from this process.

    When ``reuse_session`` is true: a deterministic hash of
    ``(workspaceid, lakehouseid)`` so successive dbt invocations get packed
    onto the same underlying Livy session while it's still warm. Different
    profiles targeting the same workspace+lakehouse intentionally collide on
    the same tag — they share a Spark cluster, which is the cheapest outcome.

    When ``reuse_session`` is false: a fresh uuid the first time we're asked
    in this process, cached thereafter so every per-thread manager sees the
    same tag.
    """
    key = (credentials.workspaceid or "", credentials.lakehouseid or "", credentials.reuse_session)
    with _session_tag_lock:
        if key in _session_tags:
            return _session_tags[key]
        if credentials.reuse_session:
            material = f"{credentials.workspaceid}|{credentials.lakehouseid}"
            digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]
            tag = f"dbt-fabricspark-{digest}"
        else:
            tag = f"dbt-fabricspark-{uuid.uuid4().hex}"
        _session_tags[key] = tag
        return tag


class HighConcurrencySession:
    """Owns the lifecycle of one HC session (= one REPL).

    One instance per dbt thread. Acquires via ``POST /highConcurrencySessions``,
    polls until Fabric reports ``Idle`` (which means the underlying Livy
    session is up and a REPL has been allocated), then exposes the
    ``sessionId`` (underlying Livy id) and ``replId`` for statement
    submission.
    """

    def __init__(self, credentials: FabricSparkCredentials, spark_config: dict[str, Any]):
        self.credential = credentials
        self.spark_config = spark_config
        self.connect_url = credentials.lakehouse_endpoint
        self.session_tag = derive_session_tag(credentials)
        self.hc_id: Optional[str] = None
        self.session_id: Optional[str] = None
        self.repl_id: Optional[str] = None
        self.is_new_session_required = True
        # Instance-level flag set by retry helpers when a 404 indicates the
        # REPL is gone. Read by HighConcurrencyCursor before submitting the
        # next statement so it can transparently re-acquire.
        self.is_dead = False
        self._lock = threading.Lock()
        self.governor = governor_for_credentials(credentials)

    def __enter__(self) -> HighConcurrencySession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        return False

    # ---- acquire ---------------------------------------------------------

    def acquire(self) -> None:
        """POST /highConcurrencySessions then poll until Idle.

        On success, ``self.hc_id``, ``self.session_id`` and ``self.repl_id``
        are all populated and the REPL is ready for statement submission.
        """
        payload = self._build_acquire_payload()
        url = self.connect_url + "/highConcurrencySessions"
        logger.debug(
            f"Acquiring HC session (sessionTag={self.session_tag}): {json.dumps(payload)}"
        )

        response = None
        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = _governed(
                    self.governor,
                    PRIORITY_NORMAL,
                    requests.post,
                    url,
                    data=json.dumps(payload),
                    headers=_get_headers(self.credential, False),
                    timeout=self.credential.http_timeout,
                )
                if response.status_code in (200, 201, 202):
                    break
                # Fabric returns 404 transiently after a lakehouse is
                # provisioned before the Livy endpoint is fully wired.
                if attempt < max_retries - 1 and (
                    response.status_code == 404 or response.status_code >= 500
                ):
                    wait = 5 * (2**attempt)
                    logger.warning(
                        f"HC acquire returned HTTP {response.status_code}, "
                        f"retrying in {wait}s (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait)
                    continue
                response.raise_for_status()
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as exc:
                if attempt >= max_retries - 1:
                    raise FailedToConnectError(f"HC session acquire failed: {exc}") from exc
                time.sleep(2**attempt)

        if response is None:
            raise FailedToConnectError("HC acquire produced no response")

        try:
            body = response.json()
        except requests.exceptions.JSONDecodeError as exc:
            raise FailedToConnectError(
                f"HC acquire returned non-JSON response: {response.text}"
            ) from exc

        self.hc_id = body.get("id")
        if not self.hc_id:
            raise FailedToConnectError(f"HC acquire response missing 'id': {body}")

        with _active_sessions_lock:
            _active_sessions.add(self)

        self._poll_until_idle()
        self.is_new_session_required = False
        self.is_dead = False
        logger.debug(
            f"HC session ready: hc_id={self.hc_id} sessionId={self.session_id} replId={self.repl_id}"
        )

    def _build_acquire_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = dict(self.spark_config)
        # The HC payload accepts the same conf/numExecutors/etc. as the
        # singleton /sessions POST — we just add the sessionTag, which drives
        # server-side session packing and therefore always wins.
        if "sessionTag" in payload and payload["sessionTag"] != self.session_tag:
            logger.warning(
                f"spark_config.sessionTag={payload['sessionTag']!r} is overridden by the "
                f"adapter-derived sessionTag={self.session_tag!r}, which controls "
                f"high-concurrency session packing."
            )
        payload["sessionTag"] = self.session_tag

        conf = dict(payload.get("conf") or {})
        if self.credential.environmentId:
            conf["spark.fabric.environment.id"] = self.credential.environmentId
        if self.credential.session_idle_timeout:
            conf["spark.livy.session.idle.timeout"] = self.credential.session_idle_timeout
        if self.credential.adaptive_polling and _HC_MAX_CONF not in conf:
            # Fabric silently spills past the REPL cap onto a different SparkContext,
            # so leave room for the telemetry monitor.
            threads = self.credential.dbt_threads or 1
            conf[_HC_MAX_CONF] = str(max(_HC_MAX_DEFAULT, min(threads + 2, _HC_MAX_CEILING)))
            logger.debug(
                f"adaptive_polling: setting {_HC_MAX_CONF}={conf[_HC_MAX_CONF]} "
                f"to reserve a REPL slot for the telemetry monitor"
            )
        if conf:
            payload["conf"] = conf
        return payload

    def _poll_until_idle(self) -> None:
        deadline = time.time() + self.credential.session_start_timeout
        url = self.connect_url + "/highConcurrencySessions/" + self.hc_id

        while True:
            if time.time() > deadline:
                raise FailedToConnectError(
                    f"Timeout ({self.credential.session_start_timeout}s) waiting for HC session "
                    f"{self.hc_id} to become Idle. Increase `session_start_timeout` in profiles.yml."
                )
            try:
                resp = _governed(
                    self.governor,
                    PRIORITY_NORMAL,
                    requests.get,
                    url,
                    headers=_get_headers(self.credential, False),
                    timeout=self.credential.http_timeout,
                )
                body = resp.json()
            except (
                requests.exceptions.RequestException,
                requests.exceptions.JSONDecodeError,
            ) as exc:
                logger.warning(
                    f"Transient error polling HC session {self.hc_id}: {exc}; "
                    f"retrying in {self.credential.poll_wait}s"
                )
                time.sleep(self.credential.poll_wait)
                continue

            state = body.get("state", "")
            session_id = body.get("sessionId")
            repl_id = body.get("replId")

            if state in _TERMINAL_BAD_STATES:
                err = body.get("fabricSessionStateInfo", {}).get("errorMessage") or state
                raise FailedToConnectError(f"HC session {self.hc_id} state={state}: {err}")

            if state == "Idle" and session_id and repl_id:
                self.session_id = session_id
                self.repl_id = repl_id
                return

            if state not in _ACQUIRING_STATES and state != "Idle":
                logger.debug(f"HC session {self.hc_id} in unfamiliar state '{state}', polling on")

            time.sleep(self.credential.poll_wait)

    # ---- statement URLs --------------------------------------------------

    def statements_url(self) -> str:
        return (
            self.connect_url
            + "/highConcurrencySessions/"
            + self.session_id
            + "/repls/"
            + self.repl_id
            + "/statements"
        )

    # ---- release ---------------------------------------------------------

    def delete(self) -> None:
        """DELETE /highConcurrencySessions/{hc_id}; best-effort.

        Deletes only this HC id; the underlying Livy session continues to host
        any other REPLs in the same packing group and is reaped by Fabric on
        idle timeout.
        """
        if not self.hc_id:
            return
        try:
            res = _governed(
                self.governor,
                PRIORITY_CRITICAL,
                requests.delete,
                self.connect_url + "/highConcurrencySessions/" + str(self.hc_id),
                headers=_get_headers(self.credential, False),
                timeout=min(self.credential.http_timeout, _TEARDOWN_TIMEOUT),
                governor_deadline=time.monotonic() + _TEARDOWN_GOVERNOR_WAIT,
            )
            if res.status_code in (200, 202, 204, 404):
                logger.debug(f"Released HC session {self.hc_id} (HTTP {res.status_code})")
            else:
                logger.warning(f"HC session delete returned HTTP {res.status_code}: {res.text}")
        except Exception as ex:
            logger.warning(f"Failed to delete HC session {self.hc_id}: {ex}")
        finally:
            with _active_sessions_lock:
                _active_sessions.discard(self)
            self.hc_id = None
            self.session_id = None
            self.repl_id = None
            self.is_new_session_required = True


class HighConcurrencyCursor:
    """Cursor backed by one HC REPL. Mirrors :class:`LivyCursor`'s surface.

    The HC statement-result payload uses the same JSON envelope as singleton
    Livy (``output.data.application/json.{schema,data}``), so the parsing and
    fetch* helpers are intentionally aligned.
    """

    def __init__(self, credential: FabricSparkCredentials, hc_session: HighConcurrencySession):
        self.credential = credential
        self.connect_url = credential.lakehouse_endpoint
        self.hc_session = hc_session
        self._rows: Optional[list] = None
        self._schema: Optional[list] = None
        self._fetch_index = 0
        self.governor = governor_for_credentials(credential)
        self.active_statement_id: Optional[str] = None
        self._active_sql: Optional[str] = None
        self._duration_store = duration_store()
        self.telemetry: Optional[TelemetrySource] = None

    def __enter__(self) -> HighConcurrencyCursor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return False

    @property
    def description(
        self,
    ) -> list[tuple[str, str, None, None, None, None, bool]]:
        if self._schema is None:
            return []
        return [
            (
                field["name"],
                field["type"],
                None,
                None,
                None,
                None,
                field["nullable"],
            )
            for field in self._schema
        ]

    def close(self) -> None:
        self._rows = None

    def cancel(self) -> None:
        """Best-effort cancel of the statement currently being polled."""
        statement_id = self.active_statement_id
        if not statement_id:
            return
        try:
            # Ctrl-C runs under dbt's connection-manager lock, so it must not
            # park on the throttle gate.
            url = f"{self.hc_session.statements_url()}/{statement_id}/cancel"
            resp = _governed(
                self.governor,
                PRIORITY_CRITICAL,
                requests.post,
                url,
                headers=_get_headers(self.credential, False),
                timeout=min(self.credential.http_timeout, _TEARDOWN_TIMEOUT),
                governor_deadline=time.monotonic() + _TEARDOWN_GOVERNOR_WAIT,
            )
            logger.debug(f"Cancel of statement {statement_id} returned HTTP {resp.status_code}")
        except Exception as exc:
            logger.debug(f"Cancel of statement {statement_id} failed: {exc}")

    # ---- submit + poll ---------------------------------------------------

    def _ensure_repl(self) -> None:
        """Re-acquire this thread's HC session if it was marked dead.

        Called before every statement submit so that 404s on a stale REPL
        recover transparently. Only acts when ``is_dead`` or
        ``is_new_session_required`` is set.
        """
        if self.hc_session.is_dead or self.hc_session.is_new_session_required:
            logger.debug("HC REPL marked stale — re-acquiring")
            self.hc_session.acquire()

    def _find_submitted_statement(self, marker: str) -> tuple[str, Optional[int]]:
        """Look for an already-accepted statement carrying ``marker``.

        Returns ``("found", id)``, ``("absent", None)`` when the list was read
        successfully and the statement is definitely not there, or
        ``("unknown", None)`` when the lookup itself failed. Only ``absent`` is
        safe to resubmit because the original POST may already be running
        side-effecting DDL/DML.
        """
        deadline = time.monotonic() + max(self.credential.http_timeout, 30)
        read_the_list = False
        for attempt in range(_RECONCILE_ATTEMPTS):
            try:
                res = _governed(
                    self.governor,
                    PRIORITY_CRITICAL,
                    requests.get,
                    self.hc_session.statements_url(),
                    headers=_get_headers(self.credential, False),
                    timeout=self.credential.http_timeout,
                    governor_deadline=deadline,
                )
                if res.status_code < 400:
                    body = res.json()
                    listing = body.get("statements") if isinstance(body, dict) else None
                    if isinstance(listing, list):
                        for statement in listing:
                            if not isinstance(statement, dict):
                                continue
                            if marker in (statement.get("code") or ""):
                                statement_id = statement.get("id")
                                if statement_id is None:
                                    return "unknown", None
                                return "found", statement_id
                        # Absence is only provable when every listed statement
                        # carried a `code` we could have matched against. A
                        # missing/null `code`, or a non-dict entry, could be
                        # hiding our just-accepted statement, so the lookup
                        # stays inconclusive rather than resubmitting
                        # side-effecting DDL/DML. An empty listing is still
                        # conclusive: nothing can be hiding in it.
                        if all(isinstance(s, dict) and s.get("code") is not None for s in listing):
                            read_the_list = True
            except Exception as exc:
                logger.debug(f"Could not reconcile ambiguous HC submit: {exc}")
            # Livy may publish the statement late, and a later lookup failure
            # must not discard earlier evidence of absence.
            if time.monotonic() >= deadline:
                # Retrying now would fire the remaining attempts back to back
                # with no pause, and a later look cannot see anything new.
                break
            if attempt < _RECONCILE_ATTEMPTS - 1:
                _sleep_until(_RECONCILE_BACKOFF * (attempt + 1), deadline)
        return ("absent" if read_the_list else "unknown"), None

    def _submit(self, code: str) -> Any:
        self._ensure_repl()
        url = self.hc_session.statements_url()
        max_retries = 5
        res = None
        for attempt in range(max_retries):
            # A fresh marker per attempt keeps two landed submissions distinguishable.
            marker = f"{_SUBMIT_MARKER_PREFIX}{uuid.uuid4().hex}"
            data = {"code": f"/* {marker} */\n{code}", "kind": "sql"}
            logger.debug(f"Submitted: {data} {url}")
            try:
                res = _governed(
                    self.governor,
                    PRIORITY_BACKGROUND,
                    requests.post,
                    url,
                    data=json.dumps(data),
                    headers=_get_headers(self.credential, False),
                    timeout=self.credential.http_timeout,
                )
            except (
                requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as exc:
                outcome, adopted = self._find_submitted_statement(marker)
                if outcome == "found":
                    logger.debug(
                        f"HC submit hit {type(exc).__name__} but statement {adopted} is already "
                        f"running; adopting it instead of resubmitting"
                    )
                    return _AdoptedSubmission(adopted)  # type: ignore[arg-type]
                if outcome == "unknown":
                    raise AmbiguousSubmissionError(
                        f"HC statement submit failed with {type(exc).__name__} and the statement "
                        f"list could not be read, so it is unknown whether the statement is "
                        f"running. Refusing to resubmit, which could execute this statement "
                        f"twice. Original error: {exc}"
                    ) from exc
                if attempt >= max_retries - 1:
                    raise DbtRuntimeError(
                        f"HC statement submit failed after {max_retries} retries: {exc}"
                    )
                wait = 2**attempt
                logger.debug(
                    f"HC statement submit got transient network error "
                    f"({type(exc).__name__}) and did not reach Fabric, retrying in {wait}s"
                )
                time.sleep(wait)
                continue
            if res.status_code == 429:
                # `_governed` has already parked the shared gate for the
                # Retry-After, so sleeping here again would double the wait.
                logger.debug("HC statement submit got HTTP 429, retrying behind the throttle gate")
                continue
            if res.status_code < 500:
                break
            # A 5xx can arrive after Livy accepted the statement, so reconcile
            # before resubmitting side-effecting DDL/DML.
            outcome, adopted = self._find_submitted_statement(marker)
            if outcome == "found":
                logger.debug(
                    f"HC submit returned HTTP {res.status_code} but statement {adopted} is "
                    f"already running; adopting it instead of resubmitting"
                )
                return _AdoptedSubmission(adopted)  # type: ignore[arg-type]
            if outcome == "unknown":
                raise AmbiguousSubmissionError(
                    f"HC statement submit returned HTTP {res.status_code} and the statement "
                    f"list could not be read, so it is unknown whether the statement is "
                    f"running. Refusing to resubmit, which could execute this statement "
                    f"twice. Response: {res.text}"
                )
            if attempt < max_retries - 1:
                wait = 2**attempt
                logger.debug(
                    f"HC statement submit got HTTP {res.status_code}, retrying in {wait}s"
                )
                time.sleep(wait)

        if res.status_code >= 400:
            if res.status_code == 404:
                # The REPL or underlying session is gone — flag this thread's
                # HC session for re-acquisition; the next add_query retry on
                # the dbt side will rebuild it transparently.
                self.hc_session.is_dead = True
                self.hc_session.is_new_session_required = True
                logger.debug("HC statement submit returned 404 — flagging REPL for re-acquire")
            raise DbtRuntimeError(
                f"HC statement submit failed (HTTP {res.status_code}): {res.text}"
            )

        body = res.json()
        if "id" not in body:
            raise DbtRuntimeError(
                f"HC statement submit returned unexpected response (missing 'id'): {body}"
            )
        return res

    def _poll(self, submit_response: Any) -> dict:
        body = submit_response.json()
        statement_id = repr(body["id"])
        self.active_statement_id = statement_id
        scheduler = self._new_scheduler(statement_id)
        try:
            return self._poll_loop(statement_id, scheduler)
        finally:
            self._release_telemetry(scheduler, statement_id)

    def _poll_loop(self, statement_id: str, scheduler: PollScheduler) -> dict:
        url = self.hc_session.statements_url() + "/" + statement_id

        started_at = time.monotonic()
        deadline = (
            (started_at + self.credential.statement_timeout)
            if self.credential.statement_timeout > 0
            else None
        )
        consecutive_failures = 0
        last_running_elapsed = 0.0
        max_poll_retries = 30
        not_found_retries = 0
        max_not_found_retries = 20

        while True:
            if deadline is not None and time.monotonic() > deadline:
                raise DbtDatabaseError(
                    f"Timeout ({self.credential.statement_timeout}s) waiting for HC statement "
                    f"{statement_id}. Increase `statement_timeout` in profiles.yml."
                )
            try:
                resp = _governed(
                    self.governor,
                    PRIORITY_CRITICAL,
                    requests.get,
                    url,
                    headers=_get_headers(self.credential, False),
                    timeout=self.credential.http_timeout,
                    governor_deadline=deadline,
                )
            except (
                requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as exc:
                consecutive_failures += 1
                if consecutive_failures > max_poll_retries:
                    raise DbtRuntimeError(
                        f"HC statement poll failed after {max_poll_retries} retries: {exc}"
                    )
                wait = min(2 ** (consecutive_failures - 1), _MAX_RETRY_BACKOFF)
                logger.debug(f"HC statement poll got transient error, retrying in {wait}s")
                _sleep_until(wait, deadline)
                continue
            if resp.status_code == 429:
                consecutive_failures += 1
                if consecutive_failures > max_poll_retries:
                    raise DbtRuntimeError(
                        f"HC statement poll failed after {max_poll_retries} retries (HTTP 429)"
                    )
                # Critical polls bypass the capacity gate so work can drain, but
                # still sleep to avoid burning the retry budget immediately.
                wait = max(parse_retry_after(resp), 1.0)
                logger.debug(f"HC statement poll got HTTP 429, retrying in {wait:.0f}s")
                _sleep_until(wait, deadline)
                continue
            if resp.status_code >= 500:
                consecutive_failures += 1
                if consecutive_failures <= max_poll_retries:
                    wait = min(2 ** (consecutive_failures - 1), _MAX_RETRY_BACKOFF)
                    logger.debug(
                        f"HC statement poll got HTTP {resp.status_code}, retrying in {wait}s"
                    )
                    _sleep_until(wait, deadline)
                    continue
                raise DbtRuntimeError(
                    f"HC statement poll failed after {max_poll_retries} retries "
                    f"(HTTP {resp.status_code}): {resp.text}"
                )
            if resp.status_code == 404 and not_found_retries < max_not_found_retries:
                not_found_retries += 1
                wait = min(0.3 * (2.0 ** (not_found_retries - 1)), 5.0)
                logger.debug(
                    f"HC statement poll got HTTP 404, retrying in {wait:.2f}s "
                    f"(not-found {not_found_retries}/{max_not_found_retries})"
                )
                _sleep_until(wait, deadline)
                continue
            if resp.status_code >= 400:
                if resp.status_code == 404:
                    self.hc_session.is_dead = True
                    self.hc_session.is_new_session_required = True
                raise DbtRuntimeError(
                    f"HC statement poll failed (HTTP {resp.status_code}): {resp.text}"
                )
            consecutive_failures = 0

            body = resp.json()
            if "state" not in body:
                raise DbtRuntimeError(
                    f"HC statement poll returned unexpected response (missing 'state'): {body}"
                )

            if body["state"] == "available":
                # Record the last known-running elapsed time, not the final
                # detection latency that includes this loop's sleep.
                self._record_duration(last_running_elapsed)
                return body
            if body["state"] in ("error", "cancelled", "cancelling"):
                error_msg = body.get("output", {}).get("evalue", "Unknown error")
                raise DbtDatabaseError(
                    f"Statement {statement_id} failed with state '{body['state']}': {error_msg}"
                )

            elapsed = time.monotonic() - started_at
            last_running_elapsed = elapsed
            self._refresh_telemetry(scheduler, statement_id, elapsed)
            plan = scheduler.next_interval(elapsed)
            if deadline is not None:
                plan_interval = min(plan.interval, max(deadline - time.monotonic(), 0.0))
            else:
                plan_interval = plan.interval
            logger.debug(
                f"Statement {statement_id}: elapsed={elapsed:.1f}s "
                f"next poll in {plan_interval:.2f}s ({plan.reason})"
            )
            _sleep_until(plan_interval, deadline)

    def _statement_keys(self) -> tuple[Optional[str], Optional[str]]:
        """Identity used to look up and record this statement's runtime.

        A dbt node issues several very different statements under one
        ``unique_id`` — the CTAS, a ``describe``, the post-build ``OPTIMIZE`` —
        so the node key alone would blend a 30-minute build with a 200ms
        metadata lookup and stall the fast ones. The shape is therefore part of
        the specific key, with the shape alone as the fallback so an unseen node
        still inherits an estimate from structurally similar statements.
        """
        shape = sql_shape(self._active_sql) if self._active_sql else None
        node_key = None
        try:
            node_info = get_node_info()
        except Exception:
            node_info = None
        if node_info:
            unique_id = node_info.get("unique_id")
            if unique_id:
                node_key = f"node:{unique_id}|{shape or 'unknown'}"
        shape_key = f"shape:{shape}" if shape else None
        return node_key, shape_key

    def _new_scheduler(self, statement_id: str) -> PollScheduler:
        node_key, shape_key = self._statement_keys()
        predicted, samples = self._duration_store.estimate(node_key, shape_key)
        telemetry = self.telemetry if self.credential.adaptive_polling else None
        if telemetry is not None:
            try:
                telemetry.watch(statement_id)
            except Exception as exc:
                logger.debug(f"Telemetry watch failed for statement {statement_id}: {exc}")
                telemetry = None
        scheduler = PollScheduler(
            predicted_duration=predicted,
            min_interval=max(self.credential.poll_statement_wait / 2, MIN_INTERVAL),
            base_interval=max(self.credential.poll_statement_wait, MIN_INTERVAL),
            telemetry=telemetry,
        )
        scheduler.samples = samples
        return scheduler

    def _refresh_telemetry(
        self, scheduler: PollScheduler, statement_id: str, elapsed: float
    ) -> None:
        if scheduler.telemetry is None:
            return
        try:
            scheduler.observe(scheduler.telemetry.snapshot(statement_id), elapsed)
        except Exception as exc:
            logger.debug(f"Telemetry read failed for statement {statement_id}: {exc}")
            scheduler.telemetry = None

    def _record_duration(self, duration: float) -> None:
        node_key, shape_key = self._statement_keys()
        for key in (node_key, shape_key):
            self._duration_store.record(key, duration)

    def _release_telemetry(self, scheduler: Optional[PollScheduler], statement_id: str) -> None:
        if scheduler is None or scheduler.telemetry is None:
            return
        try:
            scheduler.telemetry.unwatch(statement_id)
        except Exception:
            pass

    @staticmethod
    def _strip_block_comments(sql: str) -> str:
        return re.sub(r"\s*/\*(.|\n)*?\*/\s*", "\n", sql, flags=re.DOTALL).strip()

    def execute(self, sql: str, *parameters: Any) -> None:
        if len(parameters) > 0:
            sql = sql % parameters
        self._fetch_index = 0

        code = self._strip_block_comments(sql)
        self._active_sql = code
        try:
            result = self._poll(self._submit(code))
        finally:
            self.active_statement_id = None
        logger.debug(result)

        output = result.get("output", {})
        if output.get("status") == "ok":
            data = output.get("data", {})
            payload = data.get("application/json")
            if isinstance(payload, dict) and "data" in payload:
                self._rows = payload["data"]
                self._schema = payload.get("schema", {}).get("fields", [])
                coerce_time_columns(self._rows, self._schema)
            else:
                # DDL / DML or unexpected envelope — produce an empty result set
                self._rows = []
                self._schema = []
        else:
            self._rows = None
            self._schema = None
            raise DbtDatabaseError(
                "Error while executing query: " + output.get("evalue", "<no evalue>")
            )

    def fetchall(self):
        return self._rows

    def fetchmany(self, size=None):
        if self._rows is None:
            return None
        if size is None:
            return self._rows
        return self._rows[:size]

    def fetchone(self):
        if self._rows is not None and self._fetch_index < len(self._rows):
            row = self._rows[self._fetch_index]
            self._fetch_index += 1
            return row
        return None


class HighConcurrencyConnection:
    """DB-API-shaped connection backed by a single HC REPL."""

    def __init__(self, credentials: FabricSparkCredentials, hc_session: HighConcurrencySession):
        self.credential = credentials
        self.connect_url = credentials.lakehouse_endpoint
        self.hc_session = hc_session
        self._cursor = HighConcurrencyCursor(credentials, hc_session)

    def get_session_id(self) -> Optional[str]:
        return self.hc_session.session_id

    def get_headers(self) -> dict[str, str]:
        return _get_headers(self.credential, False)

    def get_connect_url(self) -> str:
        return self.connect_url

    def cursor(self) -> HighConcurrencyCursor:
        return self._cursor

    def close(self) -> None:
        logger.debug("HC Connection.close()")
        self._cursor.close()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return False


def _maybe_create_shortcuts(credentials: FabricSparkCredentials) -> None:
    """Create OneLake shortcuts once per process per (workspace, lakehouse)."""
    if not credentials.create_shortcuts:
        return
    key = (credentials.workspaceid or "", credentials.lakehouseid or "")
    with _shortcuts_done_lock:
        if key in _shortcuts_done:
            return
        _shortcuts_done.add(key)

    # Force a header build so the module-level accessToken is populated
    # before instantiating ShortcutClient.
    _ = _get_headers(credentials, False)

    try:
        shortcut_client = ShortcutClient(
            _livy_helpers.accessToken.token,
            credentials.workspaceid,
            credentials.lakehouseid,
            credentials.endpoint,
            credentials=credentials,
        )
        shortcut_client.create_shortcuts(credentials.shortcuts_json_str)
    except Exception as ex:
        logger.error(f"Unable to create shortcuts: {ex}")


_monitors_lock = threading.Lock()
_monitors: dict[str, Optional[MonitorTelemetrySource]] = {}
_monitor_sessions: list[HighConcurrencySession] = []
_monitor_ready: dict[str, threading.Event] = {}


def telemetry_for_session(
    credentials: FabricSparkCredentials, worker: HighConcurrencySession
) -> Optional[MonitorTelemetrySource]:
    """Return the shared telemetry source for ``worker``'s Livy session.

    The monitor must land on the same underlying session as the workers because
    Fabric silently spills past the REPL cap to a different ``SparkContext``.
    """
    if not credentials.adaptive_polling or credentials.is_local_mode:
        return None
    session_id = worker.session_id
    if not session_id:
        return None

    with _monitors_lock:
        pending = _monitor_ready.get(session_id)
        if pending is not None:
            owner = False
        else:
            pending = threading.Event()
            _monitor_ready[session_id] = pending
            _monitors[session_id] = None
            owner = True

    if not owner:
        pending.wait(timeout=credentials.session_start_timeout)
        with _monitors_lock:
            return _monitors.get(session_id)

    monitor: Optional[MonitorTelemetrySource] = None
    hc_session: Optional[HighConcurrencySession] = None
    try:
        try:
            # Any conf difference makes Fabric pack the REPL onto another session.
            hc_session = HighConcurrencySession(credentials, credentials.spark_config)
            hc_session.acquire()
            if hc_session.session_id != session_id:
                logger.warning(
                    f"Adaptive polling monitor landed on session {hc_session.session_id} "
                    f"instead of {session_id} (REPL packing cap reached); telemetry disabled "
                    f"for this session. Raise `spark.highConcurrency.max` in spark_config.conf "
                    f"to leave room for it."
                )
                hc_session.delete()
                hc_session = None
            else:
                monitor = MonitorTelemetrySource(
                    credentials,
                    hc_session.statements_url(),
                    governor_for_credentials(credentials),
                    lambda: _get_headers(credentials, False),
                )
                logger.debug(f"Adaptive polling monitor attached to session {session_id}")
        except Exception as exc:
            logger.warning(
                f"Could not start the adaptive polling monitor ({exc}); "
                f"falling back to schedule-based polling"
            )
            if hc_session is not None:
                try:
                    hc_session.delete()
                except Exception:
                    pass
                hc_session = None

        with _monitors_lock:
            _monitors[session_id] = monitor
            if hc_session is not None:
                _monitor_sessions.append(hc_session)
        return monitor
    finally:
        pending.set()


def shutdown_monitors() -> None:
    with _monitors_lock:
        monitors = [m for m in _monitors.values() if m is not None]
        sessions = list(_monitor_sessions)
        pending = list(_monitor_ready.values())
        _monitors.clear()
        _monitor_sessions.clear()
        _monitor_ready.clear()
    for event in pending:
        event.set()
    for monitor in monitors:
        try:
            monitor.stop()
        except Exception as exc:
            logger.debug(f"Telemetry monitor shutdown raised: {exc}")
    for session in sessions:
        try:
            session.delete()
        except Exception as exc:
            logger.debug(f"Telemetry monitor session delete raised: {exc}")


class HighConcurrencySessionManager(LivyBackend):
    """Per-dbt-thread backend. One instance owns one HC session = one REPL.

    Acquires lazily on the first :meth:`connect` call; cleanup happens in
    :meth:`disconnect` (called explicitly by `connections.cleanup_all` or via
    the module-level atexit handler).
    """

    def __init__(self) -> None:
        self._hc_session: Optional[HighConcurrencySession] = None
        self._connection: Optional[HighConcurrencyConnection] = None

    def connect(self, credentials: FabricSparkCredentials) -> HighConcurrencyConnection:  # type: ignore[override]
        if self._hc_session is None or self._hc_session.is_new_session_required:
            self._hc_session = HighConcurrencySession(credentials, credentials.spark_config)
            self._hc_session.acquire()
            _maybe_create_shortcuts(credentials)
            self._connection = HighConcurrencyConnection(credentials, self._hc_session)
            self._connection.cursor().telemetry = telemetry_for_session(
                credentials, self._hc_session
            )
        return self._connection  # type: ignore[return-value]

    def disconnect(self) -> None:  # type: ignore[override]
        """Release this thread's HC id, unless ``reuse_session`` is set.

        With ``reuse_session=False`` (default) the HC session is deleted so the
        REPL slot frees up immediately. With ``reuse_session=True`` the HC
        session is left alive so the underlying Livy session stays warm for the
        next dbt invocation (Fabric reaps it on ``session_idle_timeout``) —
        deleting the last REPL would otherwise make Fabric tear the session
        down straight away, defeating reuse. Mirrors the singleton backend's
        ``_disconnect_impl``.
        """
        if self._hc_session is not None:
            if not self._hc_session.credential.reuse_session:
                self._hc_session.delete()
            else:
                logger.debug(
                    f"Keeping HC session {self._hc_session.hc_id} alive for reuse "
                    f"(sessionId={self._hc_session.session_id})"
                )
            self._hc_session = None
            self._connection = None


class HighConcurrencyConnectionWrapper(object):
    """DB-API connection wrapper used by ``FabricSparkConnectionManager``.

    Surface is intentionally identical to
    :class:`dbt.adapters.fabricspark.singleton_livy.LivySessionConnectionWrapper`
    so the rest of the SQL connection manager doesn't know which backend
    produced the handle.
    """

    def __init__(self, handle: HighConcurrencyConnection):
        self.handle = handle
        self._cursor: Optional[HighConcurrencyCursor] = None

    def cursor(self) -> HighConcurrencyConnectionWrapper:
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        cursor = self._cursor
        if cursor is None:
            return
        cursor.cancel()

    def close(self):
        self.handle.close()

    def rollback(self, *args, **kwargs):
        logger.debug("NotImplemented: rollback")

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchmany(self, size=None):
        return self._cursor.fetchmany(size)

    def fetchone(self):
        return self._cursor.fetchone()

    def execute(self, sql, bindings=None):
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]
        if bindings is None:
            self._cursor.execute(sql)
        else:
            bindings = [self._fix_binding(b) for b in bindings]
            self._cursor.execute(sql, *bindings)

    @property
    def description(self):
        return self._cursor.description

    @classmethod
    def _fix_binding(cls, value) -> float | str:
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        elif value is None:
            return "''"
        else:
            escaped = str(value).replace("'", "\\'")
            return f"'{escaped}'"


def _atexit_cleanup_hc() -> None:
    """DELETE still-active HC sessions on process exit.

    Iterates ``_active_sessions`` rather than relying on
    ``connection_managers`` in ``connections.py``, which can be cleared by
    ``cleanup_all`` before exit. Sessions whose credentials set
    ``reuse_session`` are left alive so the underlying Livy session stays warm
    for the next invocation (Fabric reaps them on ``session_idle_timeout``).
    """
    with _active_sessions_lock:
        sessions = list(_active_sessions)
    for s in sessions:
        if s.credential.reuse_session:
            logger.debug(f"atexit: keeping HC session {s.hc_id} alive for reuse")
            continue
        try:
            s.delete()
        except Exception as ex:
            logger.debug(f"atexit HC delete failed for {s.hc_id}: {ex}")
    shutdown_monitors()


atexit.register(_atexit_cleanup_hc)
