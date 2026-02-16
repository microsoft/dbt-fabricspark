from __future__ import annotations

import datetime as dt
import json
import re
import threading
import time
from types import TracebackType
from typing import Any

import requests
from azure.core.credentials import AccessToken
from azure.identity import AzureCliCredential, ClientSecretCredential
from dbt_common.exceptions import DbtDatabaseError
from dbt_common.utils.encoding import DECIMALS
from requests.adapters import HTTPAdapter
from requests.models import Response
from urllib3.util.retry import Retry

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.shortcuts import ShortcutClient

logger = AdapterLogger("Microsoft Fabric-Spark")
NUMBERS = DECIMALS + (int, float)

livysession_credentials: FabricSparkCredentials

# Default timeouts (used as fallbacks when credentials don't specify values)
DEFAULT_POLL_WAIT = 10
DEFAULT_POLL_STATEMENT_WAIT = 5
DEFAULT_HTTP_TIMEOUT = 120  # seconds
DEFAULT_SESSION_START_TIMEOUT = 600  # 10 minutes
DEFAULT_STATEMENT_TIMEOUT = 3600  # 1 hour

AZURE_CREDENTIAL_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

# Thread-safe access token management
_token_lock = threading.Lock()
accessToken: AccessToken = None


def _build_http_session(max_retries: int = 5, backoff_factor: float = 1.0) -> requests.Session:
    """
    Build a requests.Session with transport-level retry and keep-alive.

    urllib3's Retry handles transient TCP/SSL errors (ConnectionError,
    SSLError, etc.) *before* they reach application code.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,  # 1s, 2s, 4s, 8s, 16s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "DELETE"],
        raise_on_status=False,  # let us call raise_for_status() ourselves
        connect=max_retries,  # retry on connection errors
        read=max_retries,  # retry on read errors (includes SSL EOF)
        other=max_retries,  # retry on other errors
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=4,
        pool_maxsize=4,
        pool_block=False,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def is_token_refresh_necessary(unixTimestamp: int) -> bool:
    # Convert to datetime object
    dt_object = dt.datetime.fromtimestamp(unixTimestamp)
    # Convert to local time
    local_time = time.localtime(time.time())

    # Calculate difference
    difference = dt_object - dt.datetime.fromtimestamp(time.mktime(local_time))
    if int(difference.total_seconds() / 60) < 5:
        logger.debug(f"Token refresh necessary in {int(difference.total_seconds() / 60)} minutes")
        return True
    else:
        return False


def get_cli_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an Azure access token using the CLI credentials.

    First login with:

    ```bash
    az login
    ```

    Parameters
    ----------
    credentials: FabricSparkCredentials
        The credentials.

    Returns
    -------
    out : AccessToken
        Access token.
    """
    _ = credentials
    accessToken = AzureCliCredential().get_token(AZURE_CREDENTIAL_SCOPE)
    return accessToken


def get_sp_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an Azure access token using the SP credentials.

    Parameters
    ----------
    credentials : FabricSparkCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    accessToken = ClientSecretCredential(
        str(credentials.tenant_id), str(credentials.client_id), str(credentials.client_secret)
    ).get_token(AZURE_CREDENTIAL_SCOPE)
    return accessToken


def get_default_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an Azure access token using the SP Default Credentials.

    Parameters
    ----------
    credentials : FabricSparkCredentials
        Credentials.

    Returns
    -------
    out : AccessToken
        The access token.
    """
    expires_on = 1845972874

    # Create an AccessToken instance
    accessToken = AccessToken(token=credentials.accessToken, expires_on=expires_on)
    return accessToken


def get_headers(credentials: FabricSparkCredentials) -> dict[str, str]:
    """
    Get HTTP headers with a valid Bearer token.

    Tokens are never logged. Refresh is thread-safe.
    """
    global accessToken
    with _token_lock:
        if accessToken is None or is_token_refresh_necessary(accessToken.expires_on):
            if credentials.authentication and credentials.authentication.lower() == "cli":
                logger.info("Using CLI auth")
                accessToken = get_cli_access_token(credentials)
            elif credentials.authentication and credentials.authentication.lower() == "int_tests":
                logger.info("Using int_tests auth")
                accessToken = get_default_access_token(credentials)
            else:
                logger.info("Using SPN auth")
                accessToken = get_sp_access_token(credentials)

        token = accessToken.token

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    return headers


class LivySession:
    def __init__(self, credentials: FabricSparkCredentials):
        self.credential = credentials
        self.connect_url = credentials.lakehouse_endpoint
        self.session_id = None
        self.is_new_session_required = True
        # Read timeouts from credentials with fallback defaults
        self.http_timeout = getattr(credentials, "http_timeout", DEFAULT_HTTP_TIMEOUT)
        self.session_start_timeout = getattr(
            credentials, "session_start_timeout", DEFAULT_SESSION_START_TIMEOUT
        )
        self.statement_timeout = getattr(
            credentials, "statement_timeout", DEFAULT_STATEMENT_TIMEOUT
        )
        self.poll_wait = getattr(credentials, "poll_wait", DEFAULT_POLL_WAIT)
        self.poll_statement_wait = getattr(
            credentials, "poll_statement_wait", DEFAULT_POLL_STATEMENT_WAIT
        )
        # Shared HTTP session with connection pooling and transport-level retry
        self.http_session = _build_http_session(max_retries=3, backoff_factor=1.0)

    def __enter__(self) -> LivySession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.http_session.close()
        return True

    def create_session(self, data) -> str:
        # Create sessions
        response = None
        logger.debug("Creating Livy session (this may take a few minutes)")
        try:
            response = self.http_session.post(
                self.connect_url + "/sessions",
                data=json.dumps(data),
                headers=get_headers(self.credential),
                timeout=self.http_timeout,
            )
            if response.status_code == 200:
                logger.debug("Initiated Livy Session...")
            response.raise_for_status()
        except requests.exceptions.ConnectionError as c_err:
            raise FailedToConnectError(
                f"Connection Error creating Livy session: {c_err}"
            ) from c_err
        except requests.exceptions.HTTPError as h_err:
            raise FailedToConnectError(
                f"HTTP Error creating Livy session: {h_err}"
            ) from h_err
        except requests.exceptions.Timeout as t_err:
            raise FailedToConnectError(
                f"Timeout creating Livy session (timeout={self.http_timeout}s): {t_err}"
            ) from t_err
        except requests.exceptions.RequestException as a_err:
            raise FailedToConnectError(
                f"Request Error creating Livy session: {a_err}"
            ) from a_err
        except FailedToConnectError:
            raise
        except Exception as ex:
            raise FailedToConnectError(
                f"Unexpected error creating Livy session: {ex}"
            ) from ex

        if response is None:
            raise FailedToConnectError("Invalid response from Livy server")

        self.session_id = None
        try:
            self.session_id = str(response.json()["id"])
        except (requests.exceptions.JSONDecodeError, KeyError) as json_err:
            raise FailedToConnectError(
                "Failed to parse session_id from Livy response"
            ) from json_err

        # Wait for the session to start
        self.wait_for_session_start()

        logger.debug("Livy session created successfully")
        return self.session_id

    def wait_for_session_start(self) -> None:
        """Wait for the Livy session to reach the 'idle' state, with a timeout."""
        deadline = time.monotonic() + self.session_start_timeout
        while True:
            if time.monotonic() > deadline:
                raise FailedToConnectError(
                    f"Livy session {self.session_id} did not start within "
                    f"{self.session_start_timeout} seconds"
                )

            try:
                http_res = self.http_session.get(
                    self.connect_url + "/sessions/" + self.session_id,
                    headers=get_headers(self.credential),
                    timeout=self.http_timeout,
                )
                http_res.raise_for_status()
                res = http_res.json()
            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"HTTP error polling session {self.session_id} status: {e}. Retrying..."
                )
                time.sleep(self.poll_wait)
                continue
            except (ValueError, KeyError) as e:
                logger.warning(
                    f"Error parsing session status response: {e}. Retrying..."
                )
                time.sleep(self.poll_wait)
                continue

            state = res.get("state", "unknown")
            livy_state = res.get("livyInfo", {}).get("currentState", "unknown")

            if state in ("starting", "not_started"):
                logger.debug(
                    f"Session {self.session_id} state={state}, waiting {self.poll_wait}s..."
                )
                time.sleep(self.poll_wait)
            elif livy_state == "idle":
                logger.debug(f"Livy session {self.session_id} is idle and ready")
                self.is_new_session_required = False
                break
            elif livy_state in ("dead", "killed", "error"):
                error_msg = res.get("livyInfo", {}).get("errorMessage", "No error details")
                raise FailedToConnectError(
                    f"Livy session {self.session_id} entered '{livy_state}' state: {error_msg}"
                )
            else:
                logger.debug(
                    f"Session {self.session_id} in state={state}, "
                    f"livyState={livy_state}. Waiting {self.poll_wait}s..."
                )
                time.sleep(self.poll_wait)

    def delete_session(self) -> None:
        try:
            res = self.http_session.delete(
                self.connect_url + "/sessions/" + self.session_id,
                headers=get_headers(self.credential),
                timeout=self.http_timeout,
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
            http_res = self.http_session.get(
                self.connect_url + "/sessions/" + self.session_id,
                headers=get_headers(self.credential),
                timeout=self.http_timeout,
            )
            http_res.raise_for_status()
            res = http_res.json()
        except Exception as e:
            logger.warning(f"Error checking session validity: {e}. Treating as invalid.")
            return False

        invalid_states = ["dead", "shutting_down", "killed", "error"]
        livy_state = res.get("livyInfo", {}).get("currentState", "unknown")
        return livy_state not in invalid_states


# cursor object - wrapped for livy API
class LivyCursor:
    """
    Mock a pyodbc cursor.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Cursor
    """

    def __init__(self, credential, livy_session) -> None:
        self._rows = None
        self._schema = None
        self._fetch_index = 0
        self.credential = credential
        self.connect_url = credential.lakehouse_endpoint
        self.session_id = livy_session.session_id
        self.livy_session = livy_session
        # Read timeouts from credentials
        self.http_timeout = getattr(credential, "http_timeout", DEFAULT_HTTP_TIMEOUT)
        self.statement_timeout = getattr(credential, "statement_timeout", DEFAULT_STATEMENT_TIMEOUT)
        self.poll_statement_wait = getattr(
            credential, "poll_statement_wait", DEFAULT_POLL_STATEMENT_WAIT
        )

    def __enter__(self) -> LivyCursor:
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
        """
        Get the description.

        Returns
        -------
        out : list[tuple[str, str, None, None, None, None, bool]]
            The description.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#description
        """
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
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        self._rows = None
        self._fetch_index = 0

    def _submitLivyCode(self, code) -> Response:
        if self.livy_session.is_new_session_required:
            LivySessionManager.connect(self.credential)
            self.session_id = self.livy_session.session_id

        data = {"code": code, "kind": "sql"}
        url = self.connect_url + "/sessions/" + self.session_id + "/statements"
        logger.debug(f"Submitting statement to {url}")

        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                res = self.livy_session.http_session.post(
                    url,
                    data=json.dumps(data),
                    headers=get_headers(self.credential),
                    timeout=self.http_timeout,
                )
                res.raise_for_status()
                return res
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if attempt >= max_retries:
                    raise DbtDatabaseError(
                        f"Failed to submit statement after {max_retries} attempts: {e}"
                    ) from e
                wait = min(5 * (2 ** (attempt - 1)), 60)
                logger.warning(
                    f"Connection error submitting statement (attempt {attempt}/{max_retries}): "
                    f"{type(e).__name__}: {e}. Rebuilding HTTP session and retrying in {wait}s..."
                )
                # Rebuild the HTTP session to clear stale SSL connections
                self.livy_session.http_session.close()
                self.livy_session.http_session = _build_http_session(max_retries=3, backoff_factor=1.0)
                time.sleep(wait)
            except requests.exceptions.RequestException as e:
                raise DbtDatabaseError(
                    f"HTTP error submitting statement to Livy: {e}"
                ) from e

    def _getLivySQL(self, sql) -> str:
        code = re.sub(r"\s*/\*(.|\n)*?\*/\s*", "\n", sql, re.DOTALL).strip()
        return code

    def _getLivyResult(self, res_obj) -> Response:
        try:
            json_res = res_obj.json()
        except (ValueError, KeyError) as e:
            raise DbtDatabaseError(
                f"Failed to parse statement submission response: {e}"
            ) from e

        statement_id = repr(json_res["id"])
        url = (
            self.connect_url
            + "/sessions/"
            + self.session_id
            + "/statements/"
            + statement_id
        )

        deadline = time.monotonic() + self.statement_timeout
        consecutive_errors = 0
        max_consecutive_errors = 10  # fail if we can't reach the API 10 times in a row

        while True:
            if time.monotonic() > deadline:
                raise DbtDatabaseError(
                    f"Statement {statement_id} did not complete within "
                    f"{self.statement_timeout} seconds"
                )

            try:
                http_res = self.livy_session.http_session.get(
                    url,
                    headers=get_headers(self.credential),
                    timeout=self.http_timeout,
                )
                http_res.raise_for_status()
                res = http_res.json()
                consecutive_errors = 0  # reset on success
            except requests.exceptions.ConnectionError as e:
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    raise DbtDatabaseError(
                        f"Lost connection polling statement {statement_id} after "
                        f"{max_consecutive_errors} consecutive failures: {e}"
                    ) from e
                wait = min(5 * (2 ** (consecutive_errors - 1)), 60)
                logger.warning(
                    f"Connection error polling statement {statement_id} "
                    f"(failure {consecutive_errors}/{max_consecutive_errors}): "
                    f"{type(e).__name__}. Rebuilding HTTP session, retrying in {wait}s..."
                )
                self.livy_session.http_session.close()
                self.livy_session.http_session = _build_http_session(max_retries=3, backoff_factor=1.0)
                time.sleep(wait)
                continue
            except requests.exceptions.RequestException as e:
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    raise DbtDatabaseError(
                        f"HTTP error polling statement {statement_id} after "
                        f"{max_consecutive_errors} consecutive failures: {e}"
                    ) from e
                logger.warning(f"HTTP error polling statement {statement_id}: {e}. Retrying...")
                time.sleep(self.poll_statement_wait)
                continue
            except (ValueError, KeyError) as e:
                logger.warning(
                    f"Error parsing statement poll response: {e}. Retrying..."
                )
                time.sleep(self.poll_statement_wait)
                continue

            state = res.get("state", "unknown")

            if state == "available":
                return res
            elif state in ("error", "cancelled", "cancelling"):
                error_info = res.get("output", {})
                error_msg = error_info.get("evalue", "No error details available")
                traceback = error_info.get("traceback", [])
                raise DbtDatabaseError(
                    f"Statement {statement_id} failed with state '{state}': "
                    f"{error_msg}\n{''.join(traceback)}"
                )
            elif state in ("waiting", "running"):
                time.sleep(self.poll_statement_wait)
            else:
                logger.debug(
                    f"Statement {statement_id} in state '{state}', "
                    f"waiting {self.poll_statement_wait}s..."
                )
                time.sleep(self.poll_statement_wait)

    def execute(self, sql: str, *parameters: Any) -> None:
        """
        Execute a sql statement.

        Parameters
        ----------
        sql : str
            Execute a sql statement.
        *parameters : Any
            The parameters.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executesql-parameters
        """
        if len(parameters) > 0:
            sql = sql % parameters

        res = self._getLivyResult(self._submitLivyCode(self._getLivySQL(sql)))
        logger.debug(f"Statement completed with status: {res.get('output', {}).get('status')}")
        if res["output"]["status"] == "ok":
            values = res["output"]["data"]["application/json"]
            if len(values) >= 1:
                self._rows = values["data"]
                self._schema = values["schema"]["fields"]
            else:
                self._rows = []
                self._schema = []
        else:
            self._rows = None
            self._schema = None

            raise DbtDatabaseError("Error while executing query: " + res["output"]["evalue"])

        self._fetch_index = 0

    def fetchall(self):
        """
        Fetch all data.

        Returns
        -------
        out : list() | None
            The rows.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchall
        """
        return self._rows

    def fetchone(self):
        """
        Fetch the next row.

        Returns
        -------
        out : one row | None
            The next row, or None if exhausted.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchone
        """
        if self._rows is not None and self._fetch_index < len(self._rows):
            row = self._rows[self._fetch_index]
            self._fetch_index += 1
            return row
        return None


class LivyConnection:
    """
    Mock a pyodbc connection.

    Source
    ------
    https://github.com/mkleehammer/pyodbc/wiki/Connection
    """

    def __init__(self, credentials, livy_session) -> None:
        self.credential: FabricSparkCredentials = credentials
        self.connect_url = credentials.lakehouse_endpoint
        self.session_id = livy_session.session_id

        self._cursor = LivyCursor(self.credential, livy_session)

    def get_session_id(self) -> str:
        return self.session_id

    def get_headers(self) -> dict[str, str]:
        return get_headers(self.credential)

    def get_connect_url(self) -> str:
        return self.connect_url

    def cursor(self) -> LivyCursor:
        """
        Get a cursor.

        Returns
        -------
        out : Cursor
            The cursor.
        """
        return self._cursor

    def close(self) -> None:
        """
        Close the connection.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#close
        """
        logger.debug("Connection.close()")
        self._cursor.close()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.close()
        return True


class LivySessionManager:
    livy_global_session = None
    _session_lock = threading.Lock()

    @staticmethod
    def connect(credentials: FabricSparkCredentials) -> LivyConnection:
        with LivySessionManager._session_lock:
            data = credentials.spark_config
            if LivySessionManager.livy_global_session is None:
                LivySessionManager.livy_global_session = LivySession(credentials)
                LivySessionManager.livy_global_session.create_session(data)
                LivySessionManager.livy_global_session.is_new_session_required = False
                # create shortcuts, if there are any
                if credentials.create_shortcuts:
                    try:
                        shortcut_client = ShortcutClient(
                            accessToken.token,
                            credentials.workspaceid,
                            credentials.lakehouseid,
                            credentials.endpoint,
                        )
                        shortcut_client.create_shortcuts(credentials.shortcuts_json_str)
                    except Exception as ex:
                        logger.error(f"Unable to create shortcuts: {ex}")
            elif not LivySessionManager.livy_global_session.is_valid_session():
                logger.debug("Existing session is invalid, creating a new one...")
                try:
                    LivySessionManager.livy_global_session.delete_session()
                except Exception as ex:
                    logger.debug(f"Error cleaning up old session: {ex}")
                LivySessionManager.livy_global_session = LivySession(credentials)
                LivySessionManager.livy_global_session.create_session(data)
                LivySessionManager.livy_global_session.is_new_session_required = False
            elif LivySessionManager.livy_global_session.is_new_session_required:
                LivySessionManager.livy_global_session.create_session(data)
                LivySessionManager.livy_global_session.is_new_session_required = False
            else:
                logger.debug(
                    f"Reusing session: {LivySessionManager.livy_global_session.session_id}"
                )
            livyConnection = LivyConnection(
                credentials, LivySessionManager.livy_global_session
            )
        return livyConnection

    @staticmethod
    def disconnect() -> None:
        with LivySessionManager._session_lock:
            if LivySessionManager.livy_global_session is not None:
                try:
                    LivySessionManager.livy_global_session.delete_session()
                except Exception as ex:
                    logger.debug(f"Error during session cleanup (ignored): {ex}")
                finally:
                    LivySessionManager.livy_global_session.is_new_session_required = True
                    # Close the HTTP session to release pooled connections
                    try:
                        LivySessionManager.livy_global_session.http_session.close()
                    except Exception:
                        pass
                    LivySessionManager.livy_global_session = None
            else:
                logger.debug("No session to disconnect")


class LivySessionConnectionWrapper(object):
    """Connection wrapper for the livy session connection method."""

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self) -> LivySessionConnectionWrapper:
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
        the Spark driver. Escapes strings to prevent SQL injection."""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        elif value is None:
            return "''"
        else:
            # Escape backslashes and single quotes to prevent SQL injection
            escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
