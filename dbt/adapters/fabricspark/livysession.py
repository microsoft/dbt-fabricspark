from __future__ import annotations
import json
import time
import requests
from requests.models import Response
from urllib import response
import re
import datetime as dt
from types import TracebackType
from typing import Any
import dbt.exceptions
from dbt.events import AdapterLogger
from dbt.utils import DECIMALS
from azure.core.credentials import AccessToken
from azure.identity import AzureCliCredential, ClientSecretCredential
from dbt.adapters.fabricspark.fabric_spark_credentials import SparkCredentials
from dbt.adapters.fabricspark.shortcuts import ShortcutClient

logger = AdapterLogger("Microsoft Fabric-Spark")
NUMBERS = DECIMALS + (int, float)

livysession_credentials: SparkCredentials

DEFAULT_POLL_WAIT = 45
DEFAULT_POLL_STATEMENT_WAIT = 5
AZURE_CREDENTIAL_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
accessToken: AccessToken = None


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


def get_cli_access_token(credentials: SparkCredentials) -> AccessToken:
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
    logger.debug("CLI - Fetched Access Token")
    return accessToken


def get_sp_access_token(credentials: SparkCredentials) -> AccessToken:
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
    logger.info("SPN - Fetched Access Token")
    return accessToken


def get_headers(credentials: SparkCredentials, tokenPrint: bool = False) -> dict[str, str]:
    global accessToken
    if accessToken is None or is_token_refresh_necessary(accessToken.expires_on):
        if credentials.authentication and credentials.authentication.lower() == "cli":
            logger.debug("Using CLI auth")
            accessToken = get_cli_access_token(credentials)
        elif credentials.authentication and credentials.authentication.lower() == "notebook":
            try:
                import notebookutils
            except Exception as e:
                print("Error importing notebookutils: ", e)
                try: 
                    aad_token = notebookutils.credentials.getToken('pbi')
                    expires_on = int(time.time() + 4500.0)
                    accessToken = AccessToken(
                        token=aad_token,
                        expires_on=expires_on,
                    )
                except Exception as e:
                    print("Error getting token without import: ", e)
                    raise e
            logger.debug("Using Notebook auth")
            try:
                aad_token = notebookutils.credentials.getToken('pbi')
                expires_on = int(time.time() + 4500.0)
                accessToken = AccessToken(
                    token=aad_token,
                    expires_on=expires_on,
                )
            except Exception as e:
                print("Error getting token: ", e)
                raise e
        else:
            logger.debug("Using SPN auth")
            accessToken = get_sp_access_token(credentials)

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {accessToken.token}"}
    if tokenPrint:
        logger.debug(accessToken.token)

    return headers


class LivySession:
    def __init__(self, credentials: SparkCredentials):
        self.credential = credentials
        self.connect_url = credentials.lakehouse_endpoint
        self.session_id = None
        self.is_new_session_required = True

    def __enter__(self) -> LivySession:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: Exception | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        # self.delete_session()
        return True

    def create_session(self, data) -> str:
        # Create sessions
        response = None
        print("Creating Livy session (this may take a few minutes)")
        try:
            response = requests.post(
                self.connect_url + "/sessions",
                data=json.dumps(data),
                headers=get_headers(self.credential, True),
            )
            if response.status_code == 200:
                logger.debug("Initiated Livy Session...")
            response.raise_for_status()
        except requests.exceptions.ConnectionError as c_err:
            print("Connection Error :", c_err)
        except requests.exceptions.HTTPError as h_err:
            print("Http Error: ", h_err)
        except requests.exceptions.Timeout as t_err:
            print("Timeout Error: ", t_err)
        except requests.exceptions.RequestException as a_err:
            print("Authorization Error: ", a_err)

        if response is None:
            raise Exception("Invalid response from livy server")

        self.session_id = None
        try:
            self.session_id = str(response.json()["id"])
        except requests.exceptions.JSONDecodeError as json_err:
            raise Exception("Json decode error to get session_id") from json_err

        # Wait for started state
        while True:
            res = requests.get(
                self.connect_url + "/sessions/" + self.session_id,
                headers=get_headers(self.credential, False),
            ).json()
            if res["state"] == "starting" or res["state"] == "not_started":                
                # logger.debug("Polling Session creation status - ", self.connect_url + '/sessions/' + self.session_id )
                time.sleep(DEFAULT_POLL_WAIT)
            elif res["livyInfo"]["currentState"] == "idle":
                logger.debug(f"New livy session id is: {self.session_id}, {res}")
                self.is_new_session_required = False
                break
            elif res["livyInfo"]["currentState"] == "dead":
                print("ERROR, cannot create a livy session")
                raise dbt.exceptions.FailedToConnectException("failed to connect")
        print("Livy session created successfully")
        return self.session_id

    def delete_session(self) -> None:
        logger.debug(f"Closing the livy session: {self.session_id}")

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
        res = requests.get(
            self.connect_url + "/sessions/" + self.session_id,
            headers=get_headers(self.credential, False),
        ).json()

        # we can reuse the session so long as it is not dead, killed, or being shut down
        invalid_states = ["dead", "shutting_down", "killed"]
        return res["livyInfo"]["currentState"] not in invalid_states


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
        # escapedSQL = sql.replace("\n", "\\n").replace('"', '\\\"')
        # code = "val sprk_sql = spark.sql(\"" + escapedSQL + "\")\nval sprk_res=sprk_sql.collect\n%json sprk_res"  # .format(escapedSQL)

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

            # print(res)
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
            # values = res['output']['data']['application/json']
            values = res["output"]["data"]["application/json"]
            if len(values) >= 1:
                self._rows = values["data"]  # values[0]['values']
                self._schema = values["schema"]["fields"]  # values[0]['schema']
                # print("rows", self._rows)
                # print("schema", self._schema)
            else:
                self._rows = []
                self._schema = []
        else:
            self._rows = None
            self._schema = None

            raise dbt.exceptions.DbtDatabaseError(
                "Error while executing query: " + res["output"]["evalue"]
            )

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
        self.credential: SparkCredentials = credentials
        self.connect_url = credentials.lakehouse_endpoint
        self.session_id = livy_session.session_id
        self.livy_session_parameters = credentials.livy_session_parameters

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
    def connect(credentials: SparkCredentials) -> LivyConnection:
        # the following opens an spark / sql session
        data = {"kind": "sql", "conf": credentials.livy_session_parameters}  # 'spark'
        if __class__.livy_global_session is None:
            __class__.livy_global_session = LivySession(credentials)
            __class__.livy_global_session.create_session(data)
            __class__.livy_global_session.is_new_session_required = False
            shortcut_client = ShortcutClient(accessToken.token, credentials.workspaceid, credentials.lakehouseid, credentials.endpoint)
            shortcut_client.create_shortcuts("shortcuts.json")
        elif not __class__.livy_global_session.is_valid_session():
            __class__.livy_global_session.delete_session()
            __class__.livy_global_session.create_session(data)
            __class__.livy_global_session.is_new_session_required = False
        elif __class__.livy_global_session.is_new_session_required:
            __class__.livy_global_session.create_session(data)
            __class__.livy_global_session.is_new_session_required = False
        else:
            logger.debug(f"Reusing session: {__class__.livy_global_session.session_id}")
        livyConnection = LivyConnection(credentials, __class__.livy_global_session)
        return livyConnection

    @staticmethod
    def disconnect() -> None:
        if __class__.livy_global_session.is_valid_session():
            __class__.livy_global_session.delete_session()
            __class__.livy_global_session.is_new_session_required = True


class LivySessionConnectionWrapper(object):
    """Connection wrapper for the livy sessoin connection method."""

    def __init__(self, handle):
        self.handle = handle
        self._cursor = None

    def cursor(self) -> LivySessionConnectionWrapper:
        self._cursor = self.handle.cursor()
        return self

    def cancel(self) -> None:
        logger.debug("NotImplemented: cancel")

    def close(self) -> None:
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
    def _fix_binding(cls, value):
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
