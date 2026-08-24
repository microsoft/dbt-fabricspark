from __future__ import annotations

import datetime as dt
import json
import re
import uuid
from contextlib import contextmanager
from types import TracebackType
from typing import TYPE_CHECKING, Any, Iterator, Optional, Sequence, Tuple, Union

from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils.encoding import DECIMALS

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabricspark.connections import FabricSparkConnectionWrapper

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession

logger = AdapterLogger("Microsoft Fabric-Spark")
NUMBERS = DECIMALS + (int, float)
DBT_QUERY_COMMENT_PATTERN = re.compile(r"/\*\s*(\{.*?\})\s*\*/", re.DOTALL)
SPARK_JOB_GROUP_PROPERTIES = (
    "spark.jobGroup.id",
    "spark.job.description",
    "spark.job.interruptOnCancel",
)


def _dbt_job_description(sql: str) -> str:
    for match in DBT_QUERY_COMMENT_PATTERN.finditer(sql):
        try:
            metadata = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if not isinstance(metadata, dict) or metadata.get("app") != "dbt":
            continue
        for key in ("node_id", "connection_name"):
            context = metadata.get(key)
            if isinstance(context, str) and context:
                return context
    return "dbt query"


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
        self._job_group_id: Optional[str] = None
        self._job_description: Optional[str] = None

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
        self._job_group_id = None
        self._job_description = None

    @contextmanager
    def _job_group(self) -> Iterator[None]:
        if self._job_group_id is None or self._job_description is None:
            yield
            return

        spark_context = self._spark_session.sparkContext
        previous_properties = {
            name: spark_context.getLocalProperty(name) for name in SPARK_JOB_GROUP_PROPERTIES
        }
        try:
            spark_context.setJobGroup(
                self._job_group_id,
                self._job_description,
                interruptOnCancel=True,
            )
            yield
        finally:
            for name, value in previous_properties.items():
                spark_context.setLocalProperty(name, value)

    def execute(self, sql: str, *parameters: Any) -> None:
        if parameters:
            sql = sql % parameters

        self._df = None
        self._rows = None
        self._fetch_index = 0
        self._job_description = _dbt_job_description(sql)
        self._job_group_id = f"dbt:{self._job_description}:{uuid.uuid4().hex}"
        try:
            with self._job_group():
                self._df = self._spark_session.sql(sql)
        except self._analysis_error as exc:
            raise DbtRuntimeError(str(exc)) from exc

    def fetchall(self) -> Optional[list[Row]]:
        if self._rows is None and self._df is not None:
            with self._job_group():
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
