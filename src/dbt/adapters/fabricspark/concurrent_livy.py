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
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt_common.utils.encoding import DECIMALS

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.fabricspark import livysession as _livy_helpers
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.livy_backend import LivyBackend
from dbt.adapters.fabricspark.shortcuts import ShortcutClient

logger = AdapterLogger("Microsoft Fabric-Spark")
NUMBERS = DECIMALS + (int, float)

# HC sessions whose state transitions through these values have not yet
# produced sessionId/replId; keep polling until state leaves the set.
_ACQUIRING_STATES = frozenset({"NotStarted", "starting", "AcquiringHighConcurrencySession"})
_TERMINAL_BAD_STATES = frozenset({"Dead", "Killed", "Failed", "Error"})


_active_sessions_lock = threading.Lock()
# All in-flight HighConcurrencySession instances across every dbt thread.
# Used by the atexit handler to DELETE each HC id on process exit so REPL
# slots free up promptly instead of waiting for Fabric's idle reaper.
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


def _get_headers(credentials: FabricSparkCredentials, tokenPrint: bool = False) -> dict[str, str]:
    return _livy_helpers.get_headers(credentials, tokenPrint)


def _parse_retry_after(response: requests.Response) -> float:
    return _livy_helpers._parse_retry_after(response)


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

    def __enter__(self) -> HighConcurrencySession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        return True

    # ---- acquire ---------------------------------------------------------

    def acquire(self) -> None:
        """POST /highConcurrencySessions then poll until Idle.

        On success, ``self.hc_id``, ``self.session_id`` and ``self.repl_id``
        are all populated and the REPL is ready for statement submission.
        """
        payload = self._build_acquire_payload()
        url = self.connect_url + "/highConcurrencySessions"
        logger.debug(f"Acquiring HC session (sessionTag={self.session_tag})")

        response = None
        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = requests.post(
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
        cfg = dict(self.spark_config)
        # The HC payload accepts the same conf/numExecutors/etc. as the
        # singleton /sessions POST — we just add the sessionTag.
        payload: dict[str, Any] = {"sessionTag": self.session_tag}
        for key in (
            "name",
            "conf",
            "driverMemory",
            "driverCores",
            "executorMemory",
            "executorCores",
            "numExecutors",
            "jars",
            "files",
            "pyFiles",
            "archives",
            "args",
            "className",
            "file",
            "tags",
            "artifactName",
        ):
            if key in cfg:
                payload[key] = cfg[key]

        conf = dict(payload.get("conf") or {})
        if self.credential.environmentId:
            conf["spark.fabric.environment.id"] = self.credential.environmentId
        if self.credential.session_idle_timeout:
            conf["spark.livy.session.idle.timeout"] = self.credential.session_idle_timeout
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
                resp = requests.get(
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
            res = requests.delete(
                self.connect_url + "/highConcurrencySessions/" + self.hc_id,
                headers=_get_headers(self.credential, False),
                timeout=self.credential.http_timeout,
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

    def __enter__(self) -> HighConcurrencyCursor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return True

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

    def _submit(self, code: str) -> requests.Response:
        self._ensure_repl()
        url = self.hc_session.statements_url()
        data = {"code": code, "kind": "sql"}
        logger.debug(f"Submitted: {data} {url}")

        max_retries = 5
        res = None
        for attempt in range(max_retries):
            try:
                res = requests.post(
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
                if attempt >= max_retries - 1:
                    raise DbtRuntimeError(
                        f"HC statement submit failed after {max_retries} retries: {exc}"
                    )
                wait = 2**attempt
                logger.debug(
                    f"HC statement submit got transient network error "
                    f"({type(exc).__name__}), retrying in {wait}s"
                )
                time.sleep(wait)
                continue
            if res.status_code == 429:
                retry_after = _parse_retry_after(res)
                wait = max(retry_after, 2**attempt)
                logger.debug(f"HC statement submit got HTTP 429, retrying in {wait:.0f}s")
                time.sleep(wait)
                continue
            if res.status_code < 500:
                break
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

    def _poll(self, submit_response: requests.Response) -> dict:
        body = submit_response.json()
        statement_id = repr(body["id"])
        url = self.hc_session.statements_url() + "/" + statement_id

        deadline = (
            (time.time() + self.credential.statement_timeout)
            if self.credential.statement_timeout > 0
            else None
        )
        consecutive_failures = 0
        max_poll_retries = 30
        poll_interval = 0.3
        poll_cap = max(self.credential.poll_statement_wait * 3, 1.5)
        not_found_retries = 0
        max_not_found_retries = 20

        while True:
            if deadline is not None and time.time() > deadline:
                raise DbtDatabaseError(
                    f"Timeout ({self.credential.statement_timeout}s) waiting for HC statement "
                    f"{statement_id}. Increase `statement_timeout` in profiles.yml."
                )
            try:
                resp = requests.get(
                    url,
                    headers=_get_headers(self.credential, False),
                    timeout=self.credential.http_timeout,
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
                wait = min(2 ** (consecutive_failures - 1), 30)
                logger.debug(f"HC statement poll got transient error, retrying in {wait}s")
                time.sleep(wait)
                continue
            if resp.status_code == 429:
                consecutive_failures += 1
                retry_after = _parse_retry_after(resp)
                wait = max(retry_after, 2 ** (consecutive_failures - 1))
                logger.debug(f"HC statement poll got HTTP 429, retrying in {wait:.0f}s")
                time.sleep(wait)
                if consecutive_failures > max_poll_retries:
                    raise DbtRuntimeError(
                        f"HC statement poll failed after {max_poll_retries} retries (HTTP 429)"
                    )
                continue
            if resp.status_code >= 500:
                consecutive_failures += 1
                if consecutive_failures <= max_poll_retries:
                    wait = 2 ** (consecutive_failures - 1)
                    logger.debug(
                        f"HC statement poll got HTTP {resp.status_code}, retrying in {wait}s"
                    )
                    time.sleep(wait)
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
                time.sleep(wait)
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
                return body
            if body["state"] in ("error", "cancelled", "cancelling"):
                error_msg = body.get("output", {}).get("evalue", "Unknown error")
                raise DbtDatabaseError(
                    f"Statement {statement_id} failed with state '{body['state']}': {error_msg}"
                )
            time.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, poll_cap)

    @staticmethod
    def _strip_block_comments(sql: str) -> str:
        return re.sub(r"\s*/\*(.|\n)*?\*/\s*", "\n", sql, re.DOTALL).strip()

    def execute(self, sql: str, *parameters: Any) -> None:
        if len(parameters) > 0:
            sql = sql % parameters
        self._fetch_index = 0

        code = self._strip_block_comments(sql)
        result = self._poll(self._submit(code))
        logger.debug(result)

        output = result.get("output", {})
        if output.get("status") == "ok":
            data = output.get("data", {})
            payload = data.get("application/json")
            if isinstance(payload, dict) and "data" in payload:
                self._rows = payload["data"]
                self._schema = payload.get("schema", {}).get("fields", [])
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
        return True


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
        )
        shortcut_client.create_shortcuts(credentials.shortcuts_json_str)
    except Exception as ex:
        logger.error(f"Unable to create shortcuts: {ex}")


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
        return self._connection  # type: ignore[return-value]

    def disconnect(self) -> None:  # type: ignore[override]
        """Release this thread's HC id. The underlying Livy session lives on."""
        if self._hc_session is not None:
            self._hc_session.delete()
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
        logger.debug("NotImplemented: cancel")

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
    """DELETE every still-active HC session on process exit.

    Iterates ``_active_sessions`` rather than relying on
    ``connection_managers`` in ``connections.py``, which can be cleared by
    ``cleanup_all`` before exit.
    """
    with _active_sessions_lock:
        sessions = list(_active_sessions)
    for s in sessions:
        try:
            s.delete()
        except Exception as ex:
            logger.debug(f"atexit HC delete failed for {s.hc_id}: {ex}")


atexit.register(_atexit_cleanup_hc)
