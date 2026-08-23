from __future__ import annotations

import datetime as dt
from types import TracebackType
from typing import TYPE_CHECKING, Any, Optional, Sequence, Tuple, Union

from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils.encoding import DECIMALS

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabricspark.connections import FabricSparkConnectionWrapper

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession

logger = AdapterLogger("Microsoft Fabric-Spark")
NUMBERS = DECIMALS + (int, float)


def _load_pyspark() -> tuple[Any, type[Exception]]:
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.utils import AnalysisException
    except ImportError as exc:
        raise DbtRuntimeError(
            "The session connection method requires PySpark. "
            "Install it with `pip install dbt-fabricspark[spark]` "
            "or use a runtime that already provides PySpark."
        ) from exc
    return SparkSession, AnalysisException


class SessionCursor:
    def __init__(self, spark_session: SparkSession, analysis_error: type[Exception]) -> None:
        self._spark_session = spark_session
        self._analysis_error = analysis_error
        self._df: Optional[DataFrame] = None
        self._rows: Optional[list[Row]] = None
        self._fetch_index = 0

    def __enter__(self) -> SessionCursor:
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        self.close()
        return False

    @property
    def description(
        self,
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        if self._df is None:
            return []
        return [
            (
                field.name,
                field.dataType.simpleString(),
                None,
                None,
                None,
                None,
                field.nullable,
            )
            for field in self._df.schema.fields
        ]

    def close(self) -> None:
        self._df = None
        self._rows = None
        self._fetch_index = 0

    def execute(self, sql: str, *parameters: Any) -> None:
        if parameters:
            sql = sql % parameters

        self._df = None
        self._rows = None
        self._fetch_index = 0
        try:
            self._df = self._spark_session.sql(sql)
        except self._analysis_error as exc:
            raise DbtRuntimeError(str(exc)) from exc

    def fetchall(self) -> Optional[list[Row]]:
        if self._rows is None and self._df is not None:
            self._rows = self._df.collect()
        return self._rows

    def fetchmany(self, size: Optional[int] = None) -> Optional[list[Row]]:
        rows = self.fetchall()
        if rows is None or size is None:
            return rows
        start = self._fetch_index
        self._fetch_index = min(start + size, len(rows))
        return rows[start : self._fetch_index]

    def fetchone(self) -> Optional[Row]:
        rows = self.fetchall()
        if rows is None or self._fetch_index >= len(rows):
            return None
        row = rows[self._fetch_index]
        self._fetch_index += 1
        return row


class SessionConnection:
    def __init__(self, *, spark_config: dict[str, Any]) -> None:
        spark_session_type, analysis_error = _load_pyspark()
        builder = spark_session_type.builder
        for parameter, value in spark_config.get("conf", {}).items():
            builder = builder.config(str(parameter), value)
        builder = builder.appName(str(spark_config["name"])).enableHiveSupport()
        self._spark_session = builder.getOrCreate()
        self._analysis_error = analysis_error

    def cursor(self) -> SessionCursor:
        return SessionCursor(self._spark_session, self._analysis_error)

    def close(self) -> None:
        pass


class SessionConnectionWrapper(FabricSparkConnectionWrapper):
    def __init__(self, handle: SessionConnection) -> None:
        self.handle = handle
        self._cursor: Optional[SessionCursor] = None

    def cursor(self) -> SessionConnectionWrapper:
        self._cursor = self.handle.cursor()
        return self

    def cancel(self) -> None:
        logger.debug("NotImplemented: cancel")

    def close(self) -> None:
        if self._cursor:
            self._cursor.close()
        self.handle.close()

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: rollback")

    def fetchall(self) -> Optional[list[Row]]:
        if self._cursor is None:
            raise DbtRuntimeError("Cursor not available")
        return self._cursor.fetchall()

    def fetchmany(self, size: Optional[int] = None) -> Optional[list[Row]]:
        if self._cursor is None:
            raise DbtRuntimeError("Cursor not available")
        return self._cursor.fetchmany(size)

    def fetchone(self) -> Optional[Row]:
        if self._cursor is None:
            raise DbtRuntimeError("Cursor not available")
        return self._cursor.fetchone()

    def execute(self, sql: str, bindings: Optional[list[Any]] = None) -> None:
        if sql.strip().endswith(";"):
            sql = sql.strip()[:-1]

        if self._cursor is None:
            raise DbtRuntimeError("Cursor not available")
        if bindings is None:
            self._cursor.execute(sql)
        else:
            self._cursor.execute(sql, *(self._fix_binding(binding) for binding in bindings))

    @property
    def description(
        self,
    ) -> Sequence[
        Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], bool]
    ]:
        if self._cursor is None:
            raise DbtRuntimeError("Cursor not available")
        return self._cursor.description

    @classmethod
    def _fix_binding(cls, value: Any) -> Union[str, float]:
        if isinstance(value, NUMBERS):
            return float(value)
        if isinstance(value, dt.datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        if value is None:
            return "''"
        escaped = str(value).replace("'", "\\'")
        return f"'{escaped}'"
