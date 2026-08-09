from __future__ import annotations

import atexit
import datetime as dt
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
from requests.models import Response

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.fabricspark import livysession as _livy_helpers
from dbt.adapters.fabricspark.adaptive_polling import (
    MIN_INTERVAL,
    PollScheduler,
    TelemetrySnapshot,
    duration_store,
    sql_shape,
    stats_path_for,
)
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.livy_backend import LivyBackend, coerce_time_columns
from dbt.adapters.fabricspark.shortcuts import ShortcutClient
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

# Teardown and cancel run under dbt's connection-manager lock and on the
# interpreter-exit path, so they must never park on the throttle gate.
_TEARDOWN_TIMEOUT = 15
_TEARDOWN_GOVERNOR_WAIT = 5.0

# Lets ambiguous POSTs be reconciled against Livy's statement list.
_SUBMIT_MARKER_PREFIX = "dbt-fabricspark-submit:"
_RECONCILE_ATTEMPTS = 4
_RECONCILE_BACKOFF = 1.5

# Cap exponential retry sleeps so they stay within `statement_timeout`.
_MAX_RETRY_BACKOFF = 30.0

NUMBERS = DECIMALS + (int, float)

_session_lock = threading.Lock()


def _sleep_until(wait: float, deadline: Optional[float]) -> None:
    if deadline is not None:
        wait = min(wait, max(deadline - time.monotonic(), 0.0))
    if wait > 0:
        time.sleep(wait)


class _AdoptedSubmission:
    def __init__(self, statement_id: int) -> None:
        self._statement_id = statement_id

    def json(self) -> dict:
        return {"id": self._statement_id}


def _get_headers(credentials: FabricSparkCredentials, tokenPrint: bool = False) -> dict[str, str]:
    return _livy_helpers.get_headers(credentials, tokenPrint)


class LivySession:
    def __init__(self, credentials: FabricSparkCredentials):
        self.credential = credentials
        self.connect_url = credentials.lakehouse_endpoint
        self.session_id = None
        self.is_new_session_required = True
        self.is_local_mode = credentials.is_local_mode
        self.governor = governor_for_credentials(credentials)

    def __enter__(self) -> LivySession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        return False

    def try_reuse_session(self, session_id: str) -> bool:
        """Try to reuse an existing session by ID.

        Checks if the session exists in Livy and is in a usable state.

        Parameters
        ----------
        session_id : str
            The session ID to try to reuse.

        Returns
        -------
        bool
            True if session was successfully reused, False otherwise.
        """
        try:
            logger.debug(f"Attempting to reuse existing session: {session_id}")
            self.session_id = session_id

            res = _governed(
                self.governor,
                PRIORITY_NORMAL,
                requests.get,
                self.connect_url + "/sessions/" + session_id,
                headers=_get_headers(self.credential, False),
                timeout=self.credential.http_timeout,
            )

            if res.status_code != 200:
                logger.debug(f"Session {session_id} not found (status: {res.status_code})")
                self.session_id = None
                return False

            res_json = res.json()

            invalid_states = ["dead", "shutting_down", "killed", "error", "not_found"]

            if self.is_local_mode:
                current_state = res_json.get("state", "dead")
                top_level_state = current_state
            else:
                top_level_state = res_json.get("state", "")
                livy_info = res_json.get("livyInfo", {})
                current_state = livy_info.get("currentState", "")

                if not current_state and top_level_state in ("starting", "not_started"):
                    current_state = top_level_state

            if current_state in invalid_states:
                logger.debug(f"Session {session_id} is in invalid state: {current_state}")
                self.session_id = None
                return False

            if self.is_local_mode:
                if current_state == "idle":
                    logger.info(f"Successfully reusing existing Livy session: {session_id}")
                    self.is_new_session_required = False
                    return True
                elif current_state in ("starting", "not_started", "busy"):
                    logger.debug(f"Session {session_id} is {current_state}, waiting...")
                    self._wait_for_existing_session(session_id)
                    logger.info(f"Successfully reusing existing Livy session: {session_id}")
                    self.is_new_session_required = False
                    return True
            else:
                if current_state == "idle":
                    logger.info(f"Successfully reusing existing Livy session: {session_id}")
                    self.is_new_session_required = False
                    return True
                elif current_state in ("starting", "not_started", "busy") or top_level_state in (
                    "starting",
                    "not_started",
                ):
                    logger.debug(
                        f"Session {session_id} is {current_state} (top: {top_level_state}), waiting..."
                    )
                    self._wait_for_existing_session(session_id)
                    logger.info(f"Successfully reusing existing Livy session: {session_id}")
                    self.is_new_session_required = False
                    return True

            logger.debug(f"Session {session_id} in unexpected state: {current_state}")
            self.session_id = None
            return False

        except requests.exceptions.RequestException as ex:
            logger.debug(f"Error checking session {session_id}: {ex}")
            self.session_id = None
            return False
        except Exception as ex:
            logger.debug(f"Unexpected error reusing session {session_id}: {ex}")
            self.session_id = None
            return False

    def _wait_for_existing_session(self, session_id: str) -> None:
        """Wait for an existing session to become idle."""
        deadline = time.time() + self.credential.session_start_timeout

        while time.time() < deadline:
            res = _governed(
                self.governor,
                PRIORITY_NORMAL,
                requests.get,
                self.connect_url + "/sessions/" + session_id,
                headers=_get_headers(self.credential, False),
                timeout=self.credential.http_timeout,
            ).json()

            if self.is_local_mode:
                state = res.get("state", "")
                if state == "idle":
                    return
                elif state in ("dead", "error", "killed"):
                    raise FailedToConnectError(f"Session {session_id} died while waiting")
                else:
                    logger.debug(f"Session {session_id} is {state}, waiting...")
            else:
                top_level_state = res.get("state", "")
                livy_info = res.get("livyInfo", {})
                livy_state = livy_info.get("currentState", "")

                if livy_state == "idle":
                    return
                elif livy_state in ("dead", "error", "killed") or top_level_state in (
                    "dead",
                    "error",
                    "killed",
                ):
                    raise FailedToConnectError(f"Session {session_id} died while waiting")
                else:
                    logger.debug(
                        f"Session {session_id} state: top={top_level_state}, livy={livy_state}, waiting..."
                    )

            time.sleep(self.credential.poll_wait)

        raise FailedToConnectError(
            f"Timeout ({self.credential.session_start_timeout}s) waiting for session {session_id} to become idle"
        )

    def create_session(self, spark_config) -> str:
        # Fabric Livy returns 404 transiently right after a lakehouse is
        # provisioned, before the Livy feature is fully wired up.
        response = None
        logger.debug("Creating Livy session (this may take a few minutes)")

        if self.is_local_mode:
            session_data = {"kind": "sql", **spark_config}
        else:
            session_data = spark_config

        logger.debug(f"Creating Livy session with payload: {json.dumps(session_data)}")

        max_create_retries = 5
        for attempt in range(max_create_retries):
            try:
                response = _governed(
                    self.governor,
                    PRIORITY_NORMAL,
                    requests.post,
                    self.connect_url + "/sessions",
                    data=json.dumps(session_data),
                    headers=_get_headers(self.credential, False),
                    timeout=self.credential.http_timeout,
                )
                if response.status_code in (200, 201, 202):
                    logger.debug("Initiated Livy Session...")
                    break
                if attempt < max_create_retries - 1 and (
                    response.status_code == 404 or response.status_code >= 500
                ):
                    wait_time = 5 * (2**attempt)
                    logger.warning(
                        f"Livy session create returned HTTP {response.status_code}, "
                        f"retrying in {wait_time}s (attempt {attempt + 1}/{max_create_retries})"
                    )
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()
            except requests.exceptions.ConnectionError as c_err:
                err_detail = c_err.response.json() if c_err.response else str(c_err)
                raise Exception("Connection Error :", err_detail)
            except requests.exceptions.HTTPError as h_err:
                err_detail = h_err.response.json() if h_err.response else str(h_err)
                raise Exception("Http Error: ", err_detail)
            except requests.exceptions.Timeout as t_err:
                err_detail = t_err.response.json() if t_err.response else str(t_err)
                raise Exception("Timeout Error: ", err_detail)
            except requests.exceptions.RequestException as a_err:
                err_detail = a_err.response.json() if a_err.response else str(a_err)
                raise Exception("Authorization Error: ", err_detail)
            except Exception as ex:
                raise Exception(ex) from ex

        if response is None:
            raise Exception("Invalid response from Livy server")

        self.session_id = None
        try:
            self.session_id = str(response.json()["id"])
        except requests.exceptions.JSONDecodeError as json_err:
            raise Exception("Json decode error to get session_id") from json_err

        self.wait_for_session_start()

        logger.debug("Livy session created successfully")
        return self.session_id

    def wait_for_session_start(self) -> None:
        """Wait for the Livy session to reach the 'idle' state."""
        deadline = time.time() + self.credential.session_start_timeout
        while True:
            if time.time() > deadline:
                raise FailedToConnectError(
                    f"Timeout ({self.credential.session_start_timeout}s) waiting for session "
                    f"{self.session_id} to start. Increase `session_start_timeout` in profiles.yml."
                )
            try:
                response = _governed(
                    self.governor,
                    PRIORITY_NORMAL,
                    requests.get,
                    self.connect_url + "/sessions/" + self.session_id,
                    headers=_get_headers(self.credential, False),
                    timeout=self.credential.http_timeout,
                )
                res = response.json()
            except (
                requests.exceptions.RequestException,
                requests.exceptions.JSONDecodeError,
            ) as exc:
                logger.warning(
                    f"Transient error polling session {self.session_id} status: {exc}; "
                    f"will retry in {self.credential.poll_wait}s"
                )
                time.sleep(self.credential.poll_wait)
                continue

            if self.is_local_mode:
                state = res.get("state", "")
                if state in ("starting", "not_started"):
                    time.sleep(self.credential.poll_wait)
                elif state == "idle":
                    logger.debug(f"New livy session id is: {self.session_id}, {res}")
                    self.is_new_session_required = False
                    break
                elif state in ("dead", "error"):
                    logger.error("ERROR, cannot create a livy session")
                    raise FailedToConnectError("failed to connect")
            else:
                top_level_state = res.get("state", "")
                livy_info = res.get("livyInfo", {})
                livy_state = livy_info.get("currentState", "")

                if top_level_state in ("starting", "not_started"):
                    logger.debug(f"Session {self.session_id} is {top_level_state}, waiting...")
                    time.sleep(self.credential.poll_wait)
                elif livy_state == "idle":
                    logger.debug(f"New livy session id is: {self.session_id}, {res}")
                    self.is_new_session_required = False
                    break
                elif livy_state == "dead" or top_level_state == "dead":
                    logger.error("ERROR, cannot create a livy session")
                    raise FailedToConnectError("failed to connect")
                else:
                    logger.debug(
                        f"Session {self.session_id} in state: top={top_level_state}, livy={livy_state}, waiting..."
                    )
                    time.sleep(self.credential.poll_wait)

    def delete_session(self) -> None:
        if self.session_id is None:
            return
        try:
            res = _governed(
                self.governor,
                PRIORITY_NORMAL,
                requests.delete,
                self.connect_url + "/sessions/" + self.session_id,
                headers=_get_headers(self.credential, False),
                timeout=self.credential.http_timeout,
            )
            if res.status_code == 200:
                logger.debug(f"Closed the livy session: {self.session_id}")
            else:
                res.raise_for_status()

        except Exception as ex:
            logger.error(f"Unable to close the livy session {self.session_id}, error: {ex}")

    def is_valid_session(self) -> bool:
        if self.session_id is None:
            logger.error("Session ID is None")
            return False
        try:
            res = _governed(
                self.governor,
                PRIORITY_NORMAL,
                requests.get,
                self.connect_url + "/sessions/" + self.session_id,
                headers=_get_headers(self.credential, False),
                timeout=self.credential.http_timeout,
            ).json()
        except Exception as ex:
            logger.debug(f"is_valid_session HTTP error: {ex}")
            return False

        invalid_states = ["dead", "shutting_down", "killed", "error"]

        if self.is_local_mode:
            current_state = res.get("state", "dead")
        else:
            top_level_state = res.get("state", "")
            livy_info = res.get("livyInfo", {})
            current_state = livy_info.get("currentState", "")

            if not current_state:
                current_state = top_level_state if top_level_state else "dead"

        return current_state not in invalid_states


class LivyCursor:
    """Mock a pyodbc cursor.

    Source: https://github.com/mkleehammer/pyodbc/wiki/Cursor
    """

    def __init__(self, credential, livy_session) -> None:
        self._rows = None
        self._schema = None
        self._fetch_index = 0
        self.credential = credential
        self.connect_url = credential.lakehouse_endpoint
        self.session_id = livy_session.session_id
        self.livy_session = livy_session
        self.is_local_mode = credential.is_local_mode
        self.governor = governor_for_credentials(credential)
        self.active_statement_id: Optional[str] = None
        self._active_sql: Optional[str] = None
        self._duration_store = duration_store(stats_path_for(credential))

    def __enter__(self) -> LivyCursor:
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
            description = list()
        else:
            description = [
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
        return description

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
            url = (
                self.connect_url
                + "/sessions/"
                + str(self.session_id)
                + "/statements/"
                + statement_id
                + "/cancel"
            )
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

    def _statements_url(self) -> str:
        return self.connect_url + "/sessions/" + str(self.session_id) + "/statements"

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
                    self._statements_url(),
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
                        # If Livy stops echoing `code`, lookup cannot safely prove
                        # absence for side-effecting DDL/DML.
                        if not listing or any("code" in s for s in listing):
                            read_the_list = True
            except Exception as exc:
                logger.debug(f"Could not reconcile ambiguous Livy submit: {exc}")
            # Livy may publish the statement late, and a later lookup failure
            # must not discard earlier evidence of absence.
            if attempt < _RECONCILE_ATTEMPTS - 1 and time.monotonic() < deadline:
                _sleep_until(_RECONCILE_BACKOFF * (attempt + 1), deadline)
        return ("absent" if read_the_list else "unknown"), None

    def _submitLivyCode(self, code) -> Response:
        if self.livy_session.is_new_session_required:
            LivySessionManager._connect_impl(self.credential)
            # connect() may swap in a new LivySession; resync our reference.
            self.livy_session = LivySessionManager.livy_global_session
            self.session_id = self.livy_session.session_id

        url = self._statements_url()

        max_retries = 5
        res = None
        for attempt in range(max_retries):
            # A fresh marker keeps two landed submissions distinguishable.
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
                        f"Livy submit hit {type(exc).__name__} but statement {adopted} is "
                        f"already running; adopting it instead of resubmitting"
                    )
                    return _AdoptedSubmission(adopted)  # type: ignore[arg-type,return-value]
                if outcome == "unknown":
                    raise DbtRuntimeError(
                        f"Livy statement submit failed with {type(exc).__name__} and the "
                        f"statement list could not be read, so it is unknown whether the "
                        f"statement is running. Refusing to resubmit, which could execute this "
                        f"statement twice. Original error: {exc}"
                    ) from exc
                if attempt >= max_retries - 1:
                    raise DbtRuntimeError(
                        f"Livy statement submit failed after {max_retries} retries: {exc}"
                    )
                wait_time = 2**attempt * 1
                logger.debug(
                    f"Livy statement submit got transient network error "
                    f"({type(exc).__name__}: {exc}) and did not reach Livy, retrying in "
                    f"{wait_time}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)
                continue
            if res.status_code == 429:
                wait_time = max(parse_retry_after(res), 1.0)
                logger.debug(f"Livy statement submit got HTTP 429, retrying in {wait_time:.0f}s")
                time.sleep(wait_time)
                continue
            if res.status_code < 500:
                break
            # A 5xx can arrive after Livy accepted the statement, so reconcile
            # before resubmitting side-effecting DDL/DML.
            outcome, adopted = self._find_submitted_statement(marker)
            if outcome == "found":
                logger.debug(
                    f"Livy submit returned HTTP {res.status_code} but statement {adopted} is "
                    f"already running; adopting it instead of resubmitting"
                )
                return _AdoptedSubmission(adopted)  # type: ignore[arg-type,return-value]
            if outcome == "unknown":
                raise DbtRuntimeError(
                    f"Livy statement submit returned HTTP {res.status_code} and the statement "
                    f"list could not be read, so it is unknown whether the statement is "
                    f"running. Refusing to resubmit, which could execute this statement "
                    f"twice. Response: {res.text}"
                )
            if attempt < max_retries - 1:
                wait_time = 2**attempt * 1
                logger.debug(
                    f"Livy statement submit got HTTP {res.status_code}, "
                    f"retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)

        if res.status_code >= 400:
            if res.status_code == 404 and LivySessionManager.livy_global_session is not None:
                LivySessionManager.livy_global_session.is_new_session_required = True
                logger.debug("Livy statement submit returned 404 — flagging session for reconnect")
            raise DbtRuntimeError(
                f"Livy statement submit failed (HTTP {res.status_code}): {res.text}"
            )
        json_body = res.json()
        if "id" not in json_body:
            raise DbtRuntimeError(
                f"Livy statement submit returned unexpected response (missing 'id'): {json_body}"
            )
        return res

    def _getLivySQL(self, sql) -> str:
        # The Livy SQL submit path interpolates this string into a code block
        # for the server-side interpreter, so embedded /* ... */ comments are
        # stripped here before submission. Client-side escaping is unnecessary
        # because submission uses POST JSON, not URL encoding.
        code = re.sub(r"\s*/\*(.|\n)*?\*/\s*", "\n", sql, flags=re.DOTALL).strip()
        return code

    def _statement_keys(self) -> tuple[Optional[str], Optional[str]]:
        """Identity used to look up and record this statement's runtime.

        The shape is part of the specific key because one dbt node issues very
        different statements — a CTAS, a ``describe``, the post-build
        ``OPTIMIZE`` — and blending them would stall the fast ones.
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
        return PollScheduler(
            predicted_duration=self._duration_store.predict(node_key, shape_key),
            min_interval=max(self.credential.poll_statement_wait / 2, MIN_INTERVAL),
            statement_id=statement_id,
        )

    def _record_duration(self, duration: float) -> None:
        node_key, shape_key = self._statement_keys()
        for key in (node_key, shape_key):
            self._duration_store.record(key, duration)

    @staticmethod
    def _observe_livy_progress(
        scheduler: PollScheduler, progress: Any, observed_at: float, elapsed: float
    ) -> None:
        if not isinstance(progress, float) or not (0.0 < progress < 1.0):
            return
        total_tasks = 10000
        completed_tasks = max(0, min(int(progress * total_tasks), total_tasks - 1))
        scheduler.observe(
            TelemetrySnapshot(
                total_tasks=total_tasks,
                completed_tasks=completed_tasks,
                known_jobs=1,
                observed_at=observed_at,
            ),
            elapsed,
        )

    def _getLivyResult(self, res_obj) -> dict:
        json_res = res_obj.json()
        statement_id = repr(json_res["id"])
        self.active_statement_id = statement_id
        url = self.connect_url + "/sessions/" + self.session_id + "/statements/" + statement_id
        started_at = time.monotonic()
        deadline = (
            (started_at + self.credential.statement_timeout)
            if self.credential.statement_timeout > 0
            else None
        )
        consecutive_failures = 0
        last_running_elapsed = 0.0
        max_poll_retries = 30
        scheduler = self._new_scheduler(statement_id)
        # 404 can appear transiently right after submit before the statement id
        # is registered, or when the Fabric Livy service briefly loses track of
        # the session/statement. Retry with exponential backoff before giving up.
        not_found_retries = 0
        max_not_found_retries = 20
        while True:
            if deadline is not None and time.monotonic() > deadline:
                raise DbtDatabaseError(
                    f"Timeout ({self.credential.statement_timeout}s) waiting for statement "
                    f"{statement_id} to complete. Increase `statement_timeout` in profiles.yml."
                )
            try:
                poll_res = _governed(
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
                        f"Livy statement poll failed after {max_poll_retries} retries "
                        f"({type(exc).__name__}: {exc})"
                    )
                wait_time = min(2 ** (consecutive_failures - 1), _MAX_RETRY_BACKOFF)
                logger.debug(
                    f"Livy statement poll got transient network error "
                    f"({type(exc).__name__}: {exc}), retrying in {wait_time}s "
                    f"(attempt {consecutive_failures}/{max_poll_retries})"
                )
                _sleep_until(wait_time, deadline)
                continue
            if poll_res.status_code == 429:
                consecutive_failures += 1
                if consecutive_failures > max_poll_retries:
                    raise DbtRuntimeError(
                        f"Livy statement poll failed after {max_poll_retries} retries "
                        f"(HTTP 429): {poll_res.text}"
                    )
                # Critical polls bypass the capacity gate so work can drain, but
                # still sleep to avoid burning the retry budget immediately.
                wait_time = max(parse_retry_after(poll_res), 1.0)
                logger.debug(f"Livy statement poll got HTTP 429, retrying in {wait_time:.0f}s")
                _sleep_until(wait_time, deadline)
                continue
            if poll_res.status_code >= 500:
                consecutive_failures += 1
                if consecutive_failures <= max_poll_retries:
                    wait_time = min(2 ** (consecutive_failures - 1), _MAX_RETRY_BACKOFF)
                    logger.debug(
                        f"Livy statement poll got HTTP {poll_res.status_code}, "
                        f"retrying in {wait_time}s (attempt {consecutive_failures}/{max_poll_retries})"
                    )
                    _sleep_until(wait_time, deadline)
                    continue
                raise DbtRuntimeError(
                    f"Livy statement poll failed after {max_poll_retries} retries "
                    f"(HTTP {poll_res.status_code}): {poll_res.text}"
                )
            if poll_res.status_code == 404 and not_found_retries < max_not_found_retries:
                not_found_retries += 1
                wait_time = min(0.3 * (2.0 ** (not_found_retries - 1)), 5.0)
                logger.debug(
                    f"Livy statement poll got HTTP 404, retrying in {wait_time:.2f}s "
                    f"(not-found attempt {not_found_retries}/{max_not_found_retries})"
                )
                _sleep_until(wait_time, deadline)
                continue
            if poll_res.status_code >= 400:
                if (
                    poll_res.status_code == 404
                    and LivySessionManager.livy_global_session is not None
                ):
                    LivySessionManager.livy_global_session.is_new_session_required = True
                    logger.debug(
                        "Livy statement poll exhausted 404 retries — flagging session for reconnect"
                    )
                raise DbtRuntimeError(
                    f"Livy statement poll failed (HTTP {poll_res.status_code}): {poll_res.text}"
                )
            consecutive_failures = 0
            res = poll_res.json()
            if "state" not in res:
                raise DbtRuntimeError(
                    f"Livy statement poll returned unexpected response (missing 'state'): {res}"
                )

            if res["state"] == "available":
                # Record the last known-running elapsed time, not the final
                # detection latency that includes this loop's sleep.
                self._record_duration(last_running_elapsed)
                return res
            elif res["state"] in ("error", "cancelled", "cancelling"):
                error_msg = res.get("output", {}).get("evalue", "Unknown error")
                raise DbtDatabaseError(
                    f"Statement {statement_id} failed with state '{res['state']}': {error_msg}"
                )
            now = time.monotonic()
            elapsed = now - started_at
            last_running_elapsed = elapsed
            self._observe_livy_progress(scheduler, res.get("progress"), now, elapsed)
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

    def execute(self, sql: str, *parameters: Any) -> None:
        if len(parameters) > 0:
            sql = sql % parameters

        # Reset fetch position for the new query
        self._fetch_index = 0

        code = self._getLivySQL(sql)
        self._active_sql = code
        try:
            res = self._getLivyResult(self._submitLivyCode(code))
        finally:
            self.active_statement_id = None
        logger.debug(res)
        if res["output"]["status"] == "ok":
            if self.is_local_mode:
                output_data = res["output"].get("data", {})
                if "application/json" in output_data:
                    values = output_data["application/json"]
                    if isinstance(values, dict) and "data" in values:
                        self._rows = values["data"]
                        self._schema = values.get("schema", {}).get("fields", [])
                        coerce_time_columns(self._rows, self._schema)
                    elif isinstance(values, list):
                        self._rows = values
                        self._schema = []
                    else:
                        self._rows = []
                        self._schema = []
                elif "text/plain" in output_data:
                    self._rows = []
                    self._schema = []
                else:
                    self._rows = []
                    self._schema = []
            else:
                values = res["output"]["data"]["application/json"]
                if len(values) >= 1:
                    self._rows = values["data"]
                    self._schema = values["schema"]["fields"]
                    coerce_time_columns(self._rows, self._schema)
                else:
                    self._rows = []
                    self._schema = []
        else:
            self._rows = None
            self._schema = None

            raise DbtDatabaseError("Error while executing query: " + res["output"]["evalue"])

    def fetchall(self):
        return self._rows

    def fetchmany(self, size=None):
        """Fabric's Livy statement-result API returns the entire result set in
        one JSON response — there is no server-side cursor or streaming
        primitive. The full result set is therefore already materialised in
        ``self._rows`` before this method is called. Slicing locally is
        faithful to the actual underlying behaviour.
        """
        if self._rows is None:
            return None
        if size is None:
            return self._rows
        return self._rows[:size]

    def fetchone(self):
        if self._rows is not None and self._fetch_index < len(self._rows):
            row = self._rows[self._fetch_index]
            self._fetch_index += 1
        else:
            row = None

        return row


class LivyConnection:
    """Mock a pyodbc connection.

    Source: https://github.com/mkleehammer/pyodbc/wiki/Connection
    """

    def __init__(self, credentials, livy_session) -> None:
        self.credential: FabricSparkCredentials = credentials
        self.connect_url = credentials.lakehouse_endpoint
        self.session_id = livy_session.session_id

        self._cursor = LivyCursor(self.credential, livy_session)

    def get_session_id(self) -> str:
        return self.session_id

    def get_headers(self) -> dict[str, str]:
        return _get_headers(self.credential, False)

    def get_connect_url(self) -> str:
        return self.connect_url

    def cursor(self) -> LivyCursor:
        return self._cursor

    def close(self) -> None:
        logger.debug("Connection.close()")
        self._cursor.close()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return False


def _atexit_cleanup() -> None:
    """Delete the Fabric Livy session on process exit.

    Local-mode sessions are kept alive for reuse across runs.
    """
    LivySessionManager._disconnect_impl()


atexit.register(_atexit_cleanup)


class LivySessionManager(LivyBackend):
    livy_global_session: Optional[LivySession] = None

    @classmethod
    def connect(cls, credentials: FabricSparkCredentials) -> LivyConnection:  # type: ignore[override]
        return cls._connect_impl(credentials)

    @classmethod
    def disconnect(cls) -> None:  # type: ignore[override]
        cls._disconnect_impl()

    @staticmethod
    def _connect_impl(credentials: FabricSparkCredentials) -> LivyConnection:
        """Singleton Livy session — one per process, shared across threads.

        This is the legacy code path preserved verbatim from the pre-HC
        adapter. Statements submitted to one Livy session execute inside its
        single interpreter context and are queued FIFO inside the default
        Spark scheduling pool.
        """
        with _session_lock:
            spark_config = credentials.spark_config

            if credentials.is_local_mode:
                LivySessionManager._connect_local(credentials, spark_config)
            else:
                LivySessionManager._connect_fabric(credentials, spark_config)

            livyConnection = LivyConnection(credentials, LivySessionManager.livy_global_session)
            return livyConnection

    @staticmethod
    def _connect_local(credentials: FabricSparkCredentials, spark_config) -> None:
        """Local mode connection with session-file reuse.

        Strategy:
        1. Reuse the in-memory session if valid and ready.
        2. Read the persisted session ID and try to reattach.
        3. Create a brand-new session and persist its ID.
        """
        session_file_path = credentials.resolved_session_id_file
        session = LivySessionManager.livy_global_session

        if (
            session is not None
            and session.is_valid_session()
            and not session.is_new_session_required
        ):
            logger.debug(f"Reusing session: {session.session_id}")
            return

        if session is None:
            session = LivySession(credentials)
            LivySessionManager.livy_global_session = session

        existing_session_id = _livy_helpers.read_session_id_from_file(session_file_path)
        if existing_session_id and existing_session_id != session.session_id:
            if session.try_reuse_session(existing_session_id):
                logger.debug(f"Reused session from file: {existing_session_id}")
                return

        LivySessionManager._create_and_persist_session(spark_config, session_file_path)

    @staticmethod
    def _connect_fabric(credentials: FabricSparkCredentials, spark_config) -> None:
        if credentials.reuse_session:
            LivySessionManager._connect_fabric_reuse(credentials, spark_config)
        else:
            LivySessionManager._connect_fabric_fresh(credentials, spark_config)

    @staticmethod
    def _connect_fabric_fresh(credentials: FabricSparkCredentials, spark_config) -> None:
        """Always create a new session unless a valid one is already in memory."""
        session = LivySessionManager.livy_global_session
        needs_new_session = (
            session is None or not session.is_valid_session() or session.is_new_session_required
        )

        if not needs_new_session:
            logger.debug(f"Reusing session: {session.session_id}")
            return

        LivySessionManager._create_fabric_session(credentials, spark_config)

    @staticmethod
    def _connect_fabric_reuse(credentials: FabricSparkCredentials, spark_config) -> None:
        """Same strategy as local mode: in-memory > file > create."""
        session_file_path = credentials.resolved_session_id_file
        session = LivySessionManager.livy_global_session

        if (
            session is not None
            and session.is_valid_session()
            and not session.is_new_session_required
        ):
            logger.debug(f"Reusing Fabric session: {session.session_id}")
            return

        if session is None:
            session = LivySession(credentials)
            LivySessionManager.livy_global_session = session

        existing_session_id = _livy_helpers.read_session_id_from_file(session_file_path)
        if existing_session_id and existing_session_id != session.session_id:
            if session.try_reuse_session(existing_session_id):
                logger.info(f"Reused existing Fabric session from file: {existing_session_id}")
                return

        LivySessionManager._create_fabric_session(credentials, spark_config)
        _livy_helpers.write_session_id_to_file(
            session_file_path,
            LivySessionManager.livy_global_session.session_id,
        )

    @staticmethod
    def _create_fabric_session(credentials: FabricSparkCredentials, spark_config) -> None:
        LivySessionManager.livy_global_session = LivySession(credentials)

        if credentials.environmentId:
            spark_config = {
                **spark_config,
                "conf": {
                    **spark_config.get("conf", {}),
                    "spark.fabric.environment.id": credentials.environmentId,
                },
            }
            logger.debug(f"Using Fabric Environment: {credentials.environmentId}")

        if credentials.session_idle_timeout:
            spark_config = {
                **spark_config,
                "conf": {
                    **spark_config.get("conf", {}),
                    "spark.livy.session.idle.timeout": credentials.session_idle_timeout,
                },
            }
            logger.debug(f"Session idle timeout: {credentials.session_idle_timeout}")

        LivySessionManager.livy_global_session.create_session(spark_config)
        LivySessionManager.livy_global_session.is_new_session_required = False

        if credentials.create_shortcuts:
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

    @staticmethod
    def _create_and_persist_session(spark_config, session_file_path: str) -> None:
        LivySessionManager.livy_global_session.create_session(spark_config)
        LivySessionManager.livy_global_session.is_new_session_required = False
        _livy_helpers.write_session_id_to_file(
            session_file_path, LivySessionManager.livy_global_session.session_id
        )

    @staticmethod
    def _disconnect_impl() -> None:
        """Disconnect from the session manager.

        - Local mode: keeps the Livy session alive for reuse.
        - Fabric mode with reuse_session=True: keeps session alive for reuse.
        - Fabric mode with reuse_session=False: deletes the session.
        """
        with _session_lock:
            if LivySessionManager.livy_global_session is None:
                logger.debug("No session to disconnect")
                return

            session = LivySessionManager.livy_global_session
            session_id = session.session_id

            if session.is_local_mode or session.credential.reuse_session:
                logger.debug(
                    f"Disconnecting from session manager (session {session_id} kept alive for reuse)"
                )
            else:
                logger.debug(f"Deleting Fabric Livy session: {session_id}")
                session.delete_session()

            LivySessionManager.livy_global_session = None

    # Aliases preserved for explicit class-level invocation patterns.
    connect_static = staticmethod(_connect_impl)  # type: ignore[assignment]
    disconnect_static = staticmethod(_disconnect_impl)  # type: ignore[assignment]


class LivySessionConnectionWrapper(object):
    """Connection wrapper for the livy session connection method."""

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self) -> LivySessionConnectionWrapper:
        self._cursor = self.handle.cursor()
        return self

    def cancel(self):
        if self._cursor is not None:
            self._cursor.cancel()

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
            bindings = [self._fix_binding(binding) for binding in bindings]
            self._cursor.execute(sql, *bindings)

    @property
    def description(self):
        return self._cursor.description

    @classmethod
    def _fix_binding(cls, value) -> float | str:
        """Convert complex datatypes to primitives that can be loaded by
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        elif value is None:
            return "''"
        else:
            escaped = str(value).replace("'", "\\'")
            return f"'{escaped}'"
