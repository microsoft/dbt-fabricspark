from __future__ import annotations

import datetime as dt
import json
import os
import re
import threading
import time
from types import TracebackType
from typing import Any, Optional
from urllib import response

import requests
from azure.core.credentials import AccessToken
from azure.identity import AzureCliCredential, ClientSecretCredential
from dbt_common.exceptions import DbtDatabaseError
from dbt_common.utils.encoding import DECIMALS
from requests.models import Response

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.shortcuts import ShortcutClient

logger = AdapterLogger("Microsoft Fabric-Spark")
NUMBERS = DECIMALS + (int, float)

livysession_credentials: FabricSparkCredentials

DEFAULT_POLL_WAIT = 10
DEFAULT_POLL_STATEMENT_WAIT = 5
AZURE_CREDENTIAL_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
accessToken: AccessToken = None

# Global lock to ensure thread-safe session creation/reuse
_session_lock = threading.Lock()


def read_session_id_from_file(file_path: str) -> Optional[str]:
    """Read session ID from file if it exists and contains a valid ID.
    
    Parameters
    ----------
    file_path : str
        Path to the session ID file.
        
    Returns
    -------
    Optional[str]
        The session ID if file exists and contains one, None otherwise.
    """
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                session_id = f.read().strip()
                if session_id:
                    logger.debug(f"Read session ID from file: {session_id}")
                    return session_id
                else:
                    logger.debug(f"Session ID file exists but is empty: {file_path}")
                    return None
        else:
            logger.debug(f"Session ID file does not exist: {file_path}")
            return None
    except Exception as ex:
        logger.debug(f"Error reading session ID file: {ex}")
        return None


def write_session_id_to_file(file_path: str, session_id: str) -> bool:
    """Write session ID to file.
    
    Parameters
    ----------
    file_path : str
        Path to the session ID file.
    session_id : str
        The session ID to write.
        
    Returns
    -------
    bool
        True if successful, False otherwise.
    """
    try:
        # Ensure directory exists
        dir_path = os.path.dirname(file_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        
        with open(file_path, 'w') as f:
            f.write(session_id)
        logger.debug(f"Wrote session ID to file: {session_id} -> {file_path}")
        return True
    except Exception as ex:
        logger.warning(f"Error writing session ID to file: {ex}")
        return False


def is_token_refresh_necessary(unixTimestamp: int) -> bool:
    # Convert to datetime object
    dt_object = dt.datetime.fromtimestamp(unixTimestamp)
    # Convert to local time
    local_time = time.localtime(time.time())

    # Calculate difference
    difference = dt_object - dt.datetime.fromtimestamp(time.mktime(local_time))
    if int(difference.total_seconds() / 60) < 5:
        logger.debug(f"Token Refresh necessary in {int(difference.total_seconds() / 60)}")
        return True
    else:
        return False


def get_cli_access_token(credentials: FabricSparkCredentials) -> AccessToken:
    """
    Get an Azure access token using the CLI credentials

    First login with:

    ```bash
    az login
    ```

    Parameters
    ----------
    credentials: FabricConnectionManager
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
    credentials : FabricCredentials
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
    credentials : FabricCredentials
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


def get_headers(credentials: FabricSparkCredentials, tokenPrint: bool = False) -> dict[str, str]:
    """Get HTTP headers for Livy requests.
    
    For local mode, no authentication is required.
    For Fabric mode, Azure authentication is used.
    """
    if credentials.is_local_mode:
        # Local Livy doesn't require authentication
        return {"Content-Type": "application/json"}
    
    global accessToken
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

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {accessToken.token}"}
    if tokenPrint:
        logger.debug(f"token is : {accessToken.token}")

    return headers


class LivySession:
    def __init__(self, credentials: FabricSparkCredentials):
        self.credential = credentials
        self.connect_url = credentials.lakehouse_endpoint
        self.session_id = None
        self.is_new_session_required = True
        self.is_local_mode = credentials.is_local_mode

    def __enter__(self) -> LivySession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        return True

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
            
            # Check if session exists and is valid
            res = requests.get(
                self.connect_url + "/sessions/" + session_id,
                headers=get_headers(self.credential, False),
            )
            
            # If session doesn't exist (404 or other error), return False
            if res.status_code != 200:
                logger.debug(f"Session {session_id} not found (status: {res.status_code})")
                self.session_id = None
                return False
            
            res_json = res.json()
            
            # Check session state
            invalid_states = ["dead", "shutting_down", "killed", "error", "not_found"]
            
            if self.is_local_mode:
                current_state = res_json.get("state", "dead")
            else:
                current_state = res_json.get("livyInfo", {}).get("currentState", "dead")
            
            if current_state in invalid_states:
                logger.debug(f"Session {session_id} is in invalid state: {current_state}")
                self.session_id = None
                return False
            
            # Check if session is idle (ready to use) or starting
            if self.is_local_mode:
                if current_state == "idle":
                    logger.info(f"Successfully reusing existing Livy session: {session_id}")
                    self.is_new_session_required = False
                    return True
                elif current_state in ("starting", "not_started", "busy"):
                    # Wait for session to become idle
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
                elif current_state in ("starting", "not_started", "busy"):
                    logger.debug(f"Session {session_id} is {current_state}, waiting...")
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
        max_attempts = 60  # Max 10 minutes (60 * 10 seconds)
        attempt = 0
        
        while attempt < max_attempts:
            res = requests.get(
                self.connect_url + "/sessions/" + session_id,
                headers=get_headers(self.credential, False),
            ).json()
            
            if self.is_local_mode:
                state = res.get("state", "")
                if state == "idle":
                    return
                elif state in ("dead", "error", "killed"):
                    raise FailedToConnectError(f"Session {session_id} died while waiting")
            else:
                state = res.get("livyInfo", {}).get("currentState", "")
                if state == "idle":
                    return
                elif state in ("dead", "error", "killed"):
                    raise FailedToConnectError(f"Session {session_id} died while waiting")
            
            attempt += 1
            time.sleep(DEFAULT_POLL_WAIT)
        
        raise FailedToConnectError(f"Timeout waiting for session {session_id}")

    def create_session(self, data) -> str:
        # Create sessions
        response = None
        logger.debug("Creating Livy session (this may take a few minutes)")
        
        # For local Livy, we need to use "kind" parameter instead of "name"
        if self.is_local_mode:
            # Local Livy expects {"kind": "sql"} or {"kind": "spark"}
            session_data = {"kind": "sql"}
            if "kind" in data:
                session_data["kind"] = data["kind"]
        else:
            session_data = data
        
        try:
            response = requests.post(
                self.connect_url + "/sessions",
                data=json.dumps(session_data),
                headers=get_headers(self.credential, False),
            )
            if response.status_code == 200 or response.status_code == 201:
                logger.debug("Initiated Livy Session...")
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

        # Wait for the session to start
        self.wait_for_session_start()

        logger.debug("Livy session created successfully")
        return self.session_id

    def wait_for_session_start(self) -> None:
        """Wait for the Livy session to reach the 'idle' state."""
        while True:
            res = requests.get(
                self.connect_url + "/sessions/" + self.session_id,
                headers=get_headers(self.credential, False),
            ).json()
            
            # Local Livy uses "state" directly, Fabric uses "livyInfo.currentState"
            if self.is_local_mode:
                state = res.get("state", "")
                if state in ("starting", "not_started"):
                    time.sleep(DEFAULT_POLL_WAIT)
                elif state == "idle":
                    logger.debug(f"New livy session id is: {self.session_id}, {res}")
                    self.is_new_session_required = False
                    break
                elif state in ("dead", "error"):
                    logger.error("ERROR, cannot create a livy session")
                    raise FailedToConnectError("failed to connect")
            else:
                if res["state"] == "starting" or res["state"] == "not_started":
                    time.sleep(DEFAULT_POLL_WAIT)
                elif res["livyInfo"]["currentState"] == "idle":
                    logger.debug(f"New livy session id is: {self.session_id}, {res}")
                    self.is_new_session_required = False
                    break
                elif res["livyInfo"]["currentState"] == "dead":
                    logger.error("ERROR, cannot create a livy session")
                    raise FailedToConnectError("failed to connect")

    def delete_session(self) -> None:

        try:
            # delete the session_id
            _ = requests.delete(
                self.connect_url + "/sessions/" + self.session_id,
                headers=get_headers(self.credential, False),
            )
            if _.status_code == 200:
                logger.debug(f"Closed the livy session: {self.session_id}")
            else:
                response.raise_for_status()

        except Exception as ex:
            logger.error(f"Unable to close the livy session {self.session_id}, error: {ex}")

    def is_valid_session(self) -> bool:
        if self.session_id is None:
            logger.error("Session ID is None")
            return False
        res = requests.get(
            self.connect_url + "/sessions/" + self.session_id,
            headers=get_headers(self.credential, False),
        ).json()

        # we can reuse the session so long as it is not dead, killed, or being shut down
        invalid_states = ["dead", "shutting_down", "killed", "error"]
        
        # Local Livy uses "state" directly, Fabric uses "livyInfo.currentState"
        if self.is_local_mode:
            current_state = res.get("state", "dead")
        else:
            current_state = res.get("livyInfo", {}).get("currentState", "dead")
        
        return current_state not in invalid_states


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
        self.credential = credential
        self.connect_url = credential.lakehouse_endpoint
        self.session_id = livy_session.session_id
        self.livy_session = livy_session
        self.is_local_mode = credential.is_local_mode

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
                    field["type"],  # field['dataType'],
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

    def _submitLivyCode(self, code) -> Response:
        if self.livy_session.is_new_session_required:
            LivySessionManager.connect(self.credential)
            self.session_id = self.livy_session.session_id

        # Submit code
        data = {"code": code, "kind": "sql"}
        logger.debug(
            f"Submitted: {data} {self.connect_url + '/sessions/' + self.session_id + '/statements'}"
        )
        res = requests.post(
            self.connect_url + "/sessions/" + self.session_id + "/statements",
            data=json.dumps(data),
            headers=get_headers(self.credential, False),
        )
        return res

    def _getLivySQL(self, sql) -> str:
        # Comment, what is going on?!
        # The following code is actually injecting SQL to pyspark object for executing it via the Livy session - over an HTTP post request.
        # Basically, it is like code inside a code. As a result the strings passed here in 'escapedSQL' variable are unescapted and interpreted on the server side.
        # This may have repurcursions of code injection not only as SQL, but also arbritary Python code. An alternate way safer way to acheive this is still unknown.
        # TODO: since the above code is not changed to sending direct SQL to the livy backend, client side string escaping is probably not needed

        code = re.sub(r"\s*/\*(.|\n)*?\*/\s*", "\n", sql, re.DOTALL).strip()
        return code

    def _getLivyResult(self, res_obj) -> Response:
        json_res = res_obj.json()
        while True:
            res = requests.get(
                self.connect_url
                + "/sessions/"
                + self.session_id
                + "/statements/"
                + repr(json_res["id"]),
                headers=get_headers(self.credential, False),
            ).json()

            if res["state"] == "available":
                return res
            time.sleep(DEFAULT_POLL_STATEMENT_WAIT)

    def execute(self, sql: str, *parameters: Any) -> None:
        """
        Execute a sql statement.

        Parameters
        ----------
        sql : str
            Execute a sql statement.
        *parameters : Any
            The parameters.

        Raises
        ------
        NotImplementedError
            If there are parameters given. We do not format sql statements.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#executesql-parameters
        """
        if len(parameters) > 0:
            sql = sql % parameters

        # TODO: handle parameterised sql

        res = self._getLivyResult(self._submitLivyCode(self._getLivySQL(sql)))
        logger.debug(res)
        if res["output"]["status"] == "ok":
            # Local and Fabric Livy have different output structures
            if self.is_local_mode:
                # Local Livy returns data in "text/plain" or "application/json" format
                output_data = res["output"].get("data", {})
                if "application/json" in output_data:
                    values = output_data["application/json"]
                    if isinstance(values, dict) and "data" in values:
                        self._rows = values["data"]
                        self._schema = values.get("schema", {}).get("fields", [])
                    elif isinstance(values, list):
                        # Direct list of results
                        self._rows = values
                        self._schema = []
                    else:
                        self._rows = []
                        self._schema = []
                elif "text/plain" in output_data:
                    # Text output - parse if possible
                    self._rows = []
                    self._schema = []
                else:
                    self._rows = []
                    self._schema = []
            else:
                # Fabric Livy format
                values = res["output"]["data"]["application/json"]
                if len(values) >= 1:
                    self._rows = values["data"]  # values[0]['values']
                    self._schema = values["schema"]["fields"]  # values[0]['schema']
                else:
                    self._rows = []
                    self._schema = []
        else:
            self._rows = None
            self._schema = None

            raise DbtDatabaseError("Error while executing query: " + res["output"]["evalue"])

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
        Fetch the first output.

        Returns
        -------
        out : one row | None
            The first row.

        Source
        ------
        https://github.com/mkleehammer/pyodbc/wiki/Cursor#fetchone
        """

        if self._rows is not None and len(self._rows) > 0:
            row = self._rows.pop(0)
        else:
            row = None

        return row


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
        return get_headers(self.credential, False)

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


# TODO: How to authenticate
class LivySessionManager:
    livy_global_session = None

    @staticmethod
    def connect(credentials: FabricSparkCredentials) -> LivyConnection:
        """Connect to a Livy session, reusing existing session if available.
        
        This method is thread-safe and uses a lock to prevent race conditions
        when multiple threads attempt to create sessions simultaneously.
        """
        # Use lock to ensure only one thread can create/check session at a time
        with _session_lock:
            # the following opens an spark / sql session
            data = credentials.spark_config
            session_file_path = credentials.resolved_session_id_file
            
            if LivySessionManager.livy_global_session is None:
                LivySessionManager.livy_global_session = LivySession(credentials)
                
                # Try to reuse session from file
                existing_session_id = read_session_id_from_file(session_file_path)
                if existing_session_id:
                    if LivySessionManager.livy_global_session.try_reuse_session(existing_session_id):
                        # Successfully reused session
                        logger.debug(f"Reused session from file: {existing_session_id}")
                    else:
                        # Session from file is invalid, create new one
                        logger.debug(f"Session from file invalid, creating new session")
                        LivySessionManager.livy_global_session.create_session(data)
                        LivySessionManager.livy_global_session.is_new_session_required = False
                        # Write new session ID to file
                        write_session_id_to_file(session_file_path, LivySessionManager.livy_global_session.session_id)
                else:
                    # No session file or empty, create new session
                    LivySessionManager.livy_global_session.create_session(data)
                    LivySessionManager.livy_global_session.is_new_session_required = False
                    # Write new session ID to file
                    write_session_id_to_file(session_file_path, LivySessionManager.livy_global_session.session_id)
                
                # create shortcuts, if there are any (only for Fabric mode)
                if credentials.create_shortcuts and not credentials.is_local_mode:
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
                # Current session is invalid, try to reuse from file or create new
                existing_session_id = read_session_id_from_file(session_file_path)
                current_session_id = LivySessionManager.livy_global_session.session_id
                
                # Only try file if it's a different session ID
                if existing_session_id and existing_session_id != current_session_id:
                    if LivySessionManager.livy_global_session.try_reuse_session(existing_session_id):
                        logger.debug(f"Reused different session from file: {existing_session_id}")
                    else:
                        # Create new session
                        LivySessionManager.livy_global_session.create_session(data)
                        LivySessionManager.livy_global_session.is_new_session_required = False
                        write_session_id_to_file(session_file_path, LivySessionManager.livy_global_session.session_id)
                else:
                    # Create new session
                    LivySessionManager.livy_global_session.create_session(data)
                    LivySessionManager.livy_global_session.is_new_session_required = False
                    write_session_id_to_file(session_file_path, LivySessionManager.livy_global_session.session_id)
            elif LivySessionManager.livy_global_session.is_new_session_required:
                LivySessionManager.livy_global_session.create_session(data)
                LivySessionManager.livy_global_session.is_new_session_required = False
                write_session_id_to_file(session_file_path, LivySessionManager.livy_global_session.session_id)
            else:
                logger.debug(f"Reusing session: {LivySessionManager.livy_global_session.session_id}")
            
            livyConnection = LivyConnection(credentials, LivySessionManager.livy_global_session)
            return livyConnection

    @staticmethod
    def disconnect() -> None:
        """Disconnect from the session manager without deleting the Livy session.
        
        The session is intentionally kept alive for reuse by subsequent dbt runs.
        The session ID is stored in a file so it can be reused.
        
        This method is thread-safe.
        """
        with _session_lock:
            if LivySessionManager.livy_global_session is not None:
                session_id = LivySessionManager.livy_global_session.session_id
                logger.debug(f"Disconnecting from session manager (session {session_id} kept alive for reuse)")
                # Don't delete the session - keep it alive for reuse
                # Just reset the local reference
                LivySessionManager.livy_global_session = None
            else:
                logger.debug("No session to disconnect")


class LivySessionConnectionWrapper(object):
    """Connection wrapper for the livy sessoin connection method."""

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
        the Spark driver"""
        if isinstance(value, NUMBERS):
            return float(value)
        elif isinstance(value, dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        elif value is None:
            return "''"
        else:
            return f"'{value}'"
