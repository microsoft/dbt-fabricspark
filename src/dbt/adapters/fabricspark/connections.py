import os
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import (
    Any,
    Generator,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

from dbt_common.dataclass_schema import StrEnum
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtConfigError, DbtRuntimeError
from dbt_common.utils.encoding import DECIMALS

from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import AdapterEventDebug, ConnectionUsed, SQLQuery, SQLQueryStatus
from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.fabricspark.livysession import LivySessionConnectionWrapper, LivySessionManager
from dbt.adapters.sql import SQLConnectionManager

logger = AdapterLogger("Microsoft Fabric-Spark")
for logger_name in [
    "fabricspark.connector",
    "botocore",
    "boto3",
    "Microsoft Fabric-Spark.connector",
]:
    logger.debug(f"Setting {logger_name} to DEBUG")
    logger.set_adapter_dependency_log_level(logger_name, "DEBUG")

NUMBERS = DECIMALS + (int, float)


class FabricSparkConnectionMethod(StrEnum):
    LIVY = "livy"


class FabricSparkConnectionWrapper(ABC):
    @abstractmethod
    def cursor(self) -> "FabricSparkConnectionWrapper":
        pass

    @abstractmethod
    def cancel(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def rollback(self) -> None:
        pass

    @abstractmethod
    def fetchall(self) -> Optional[List]:
        pass

    @abstractmethod
    def execute(self, sql: str, bindings: Optional[List[Any]] = None) -> None:
        pass

    @property
    @abstractmethod
    def description(
        self,
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        pass


class FabricSparkConnectionManager(SQLConnectionManager):
    TYPE = "fabricspark"

    connection_managers = {}
    spark_version = None

    @contextmanager
    def exception_handler(self, sql: str) -> Generator[None, None, None]:
        try:
            yield

        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            if len(exc.args) == 0:
                raise

            thrift_resp = exc.args[0]
            if hasattr(thrift_resp, "status"):
                msg = thrift_resp.status.errorMessage
                raise DbtRuntimeError(msg)
            else:
                raise DbtRuntimeError(str(exc))

    def cancel(self, connection: Connection) -> None:
        connection.handle.cancel()

    @classmethod
    def get_response(cls, cursor: Any) -> AdapterResponse:
        # https://github.com/dbt-labs/dbt-spark/issues/142
        message = "OK"
        return AdapterResponse(_message=message)

    # No transactions on Spark....
    def add_begin_query(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: add_begin_query")

    def add_commit_query(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: add_commit_query")

    def commit(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: commit")

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: rollback")

    @classmethod
    def validate_creds(cls, creds: Any, required: Iterable[str]) -> None:
        method = creds.method

        for key in required:
            if not hasattr(creds, key):
                raise DbtConfigError(
                    "The config '{}' is required when using the {} method"
                    " to connect to Spark".format(key, method)
                )

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        creds = connection.credentials
        exc = None
        handle: FabricSparkConnectionWrapper = None

        for i in range(1 + creds.connect_retries):
            try:
                if creds.method == FabricSparkConnectionMethod.LIVY:
                    thread_id = cls.get_thread_identifier()
                    if thread_id not in cls.connection_managers:
                        cls.connection_managers[thread_id] = LivySessionManager()
                    handle = LivySessionConnectionWrapper(
                        cls.connection_managers[thread_id].connect(creds)
                    )
                    connection.state = ConnectionState.OPEN

                else:
                    raise DbtConfigError(f"invalid credential method: {creds.method}")
                break
            except Exception as e:
                exc = e
                if isinstance(e, EOFError):
                    # The user almost certainly has invalid credentials.
                    # Perhaps a token expired, or something
                    msg = "Failed to connect"
                    if creds.token is not None:
                        msg += ", is your token valid?"
                    raise FailedToConnectError(msg) from e
                retryable_message = _is_retryable_error(e)
                if retryable_message and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {retryable_message}\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                elif creds.retry_all and creds.connect_retries > 0:
                    msg = (
                        f"Warning: {getattr(exc, 'message', 'No message')}, "
                        f"retrying due to 'retry_all' configuration "
                        f"set to true.\n\tRetrying in "
                        f"{creds.connect_timeout} seconds "
                        f"({i} of {creds.connect_retries})"
                    )
                    logger.warning(msg)
                    time.sleep(creds.connect_timeout)
                else:
                    raise FailedToConnectError(
                        f"failed to connect: {str(e)}. If the error did not help, common reasons for errors: \n1. Invalid/expired credentials (if using CLI authentication, re-run `az login` in your terminal) \n2. Invalid endpoint \n3. Invalid workspaceid or lakehouseid (do you have the correct permissions?) \n4. Invalid or non-existent shortcuts json path, or improperly formatted shortcuts"
                    ) from e
        else:
            raise exc  # type: ignore

        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection

    @classmethod
    def release(self) -> None:
        pass

    @classmethod
    def cleanup_all(self) -> None:
        for thread_id in self.connection_managers:
            livySession = self.connection_managers[thread_id]
            livySession.disconnect()

            # garbage collect these connections
        self.connection_managers.clear()

    @classmethod
    def close(cls, connection) -> None:
        try:
            # if the connection is in closed or init, there's nothing to do
            if connection.state in {ConnectionState.CLOSED, ConnectionState.INIT}:
                return connection

            connection = super().close(connection)
            return connection
        except Exception as err:
            logger.debug(f"Error closing connection {err}")

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[type, str]) -> str:  # type: ignore
        """
        :param Union[type, str] type_code: The sql to execute.
            * type_code is a python type (!) in pyodbc https://github.com/mkleehammer/pyodbc/wiki/Cursor#description, and a string for other spark runtimes.
            * ignoring the type annotation on the signature for this adapter instead of updating the base class because this feels like a really special case.
        :return: stringified the cursor type_code
        :rtype: str
        """
        if isinstance(type_code, str):
            return type_code
        return type_code.__name__.upper()

    @classmethod
    def fetch_spark_version(cls, connection) -> None:
        if FabricSparkConnectionManager.spark_version:
            return FabricSparkConnectionManager.spark_version

        try:
            sql = "split(version(), ' ')[0] as version"
            cursor = connection.handle.cursor()
            cursor.execute(sql)
            res = cursor.fetchall()
            FabricSparkConnectionManager.spark_version = res[0][0]

        except Exception as ex:
            # we couldn't get the spark warehouse version, default to version 2
            logger.debug(f"Cannot get spark version, defaulting to version 2. Error: {ex}")
            FabricSparkConnectionManager.spark_version = "2"

        os.environ["DBT_SPARK_VERSION"] = FabricSparkConnectionManager.spark_version
        logger.debug(f"SPARK VERSION {os.getenv('DBT_SPARK_VERSION')}")

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
        retryable_exceptions: Tuple[Type[Exception], ...] = tuple(),
        retry_limit: int = 2,
    ) -> Tuple[Connection, Any]:
        """
        Retry function encapsulated here to avoid commitment to some
        user-facing interface.
        """

        def _execute_query_with_retry(
            cursor: Any,
            sql: str,
            bindings: Optional[Any],
            retryable_exceptions: Tuple[Type[Exception], ...],
            retry_limit: int,
            attempt: int,
        ):
            """
            A success sees the try exit cleanly and avoid any recursive
            retries. Failure begins a sleep and retry routine.
            """
            retry_limit = connection.credentials.connect_retries or 3
            try:
                cursor.execute(sql, bindings)
            except retryable_exceptions as e:
                # Cease retries and fail when limit is hit.
                if attempt >= retry_limit:
                    raise e

                fire_event(
                    AdapterEventDebug(
                        message=f"Got a retryable error {type(e)}. {retry_limit-attempt} retries left. Retrying in 5 seconds.\nError:\n{e}"
                    )
                )
                time.sleep(5)

                return _execute_query_with_retry(
                    cursor=cursor,
                    sql=sql,
                    bindings=bindings,
                    retryable_exceptions=retryable_exceptions,
                    retry_limit=retry_limit,
                    attempt=attempt + 1,
                )

        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()
        fire_event(ConnectionUsed(conn_type=self.TYPE, conn_name=connection.name))

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = "{}...".format(sql[:512])
            else:
                log_sql = sql

            fire_event(SQLQuery(conn_name=connection.name, sql=log_sql))
            pre = time.time()
            query_exception = None
            cursor = connection.handle.cursor()

            try:
                _execute_query_with_retry(
                cursor=cursor,
                sql=sql,
                bindings=bindings,
                retryable_exceptions=retryable_exceptions,
                retry_limit=retry_limit,
                attempt=1,
            )
            except Exception as ex:
                query_exception = ex

            elapsed_time = time.time() - pre

            # re-raise query exception so that it propogates to dbt
            if query_exception:
                raise query_exception

            fire_event(
                SQLQueryStatus(
                    status=str(self.get_response(cursor)),
                    elapsed=round(elapsed_time, 2),
                )
            )

            return connection, cursor


def _is_retryable_error(exc: Exception) -> str:
    message = str(exc).lower()
    retryable_keywords = [
        "pending",
        "temporary",
        "retry",
        "timeout",
        "unavailable",
        "transient",
        "throttling",
        "rate limit",
        "connection reset",
        "service busy",
    ]
    for keyword in retryable_keywords:
        if keyword in message:
            return str(exc)
    return ""
