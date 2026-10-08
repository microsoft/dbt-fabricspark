from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
import threading
import time
import uuid
from contextlib import contextmanager
from types import TracebackType
from typing import TYPE_CHECKING, Any, Iterator, Optional, Sequence, Tuple, Union

from dbt_common.exceptions import DbtRuntimeError
from dbt_common.utils.encoding import DECIMALS

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabricspark.connections import FabricSparkConnectionWrapper
from dbt.adapters.fabricspark.credentials import (
    DEFAULT_STREAM_STOP_TIMEOUT_SECONDS,
    _validate_stream_stop_timeout_seconds,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession
    from pyspark.sql.streaming import StreamingQuery

logger = AdapterLogger("Microsoft Fabric-Spark")
NUMBERS = DECIMALS + (int, float)
DBT_QUERY_COMMENT_PATTERN = re.compile(r"/\*\s*(\{.*?\})\s*\*/", re.DOTALL)
SPARK_JOB_GROUP_PROPERTIES = (
    "spark.jobGroup.id",
    "spark.job.description",
    "spark.job.interruptOnCancel",
)


def _log_cancel_warning(message: str) -> None:
    try:
        logger.warning(message)
    except Exception:
        try:
            logging.getLogger(__name__).warning(message, exc_info=True)
        except Exception:
            try:
                os.write(2, (message + "\n").encode("utf-8", errors="replace"))
            except OSError:
                # Broken diagnostic channels must not replace dbt's original failure.
                pass


class _StreamStopWorker(threading.Thread):
    def __init__(
        self,
        query: StreamingQuery,
        deadline: float,
        owner: SessionConnectionWrapper,
    ) -> None:
        super().__init__(name="dbt-fabricspark-stream-stop", daemon=True)
        self.query = query
        self.query_id = "unknown"
        self.run_id = "unknown"
        self.deadline = deadline
        self.owner = owner
        self.existing_worker: Optional[_StreamStopWorker] = None

    def run(self) -> None:
        key: Union[str, int] = id(self.query)
        try:
            query_id = getattr(self.query, "id", None)
            if query_id is not None:
                rendered_id = str(query_id)
                if rendered_id:
                    self.query_id = rendered_id
        except Exception as exc:
            _log_cancel_warning(
                f"Failed to read Spark streaming query ID during cancellation: {exc}"
            )

        try:
            run_id = getattr(self.query, "runId", None)
            if run_id is not None:
                rendered_id = str(run_id)
                if rendered_id:
                    self.run_id = rendered_id
                    key = rendered_id
        except Exception as exc:
            _log_cancel_warning(
                f"Failed to read Spark streaming query run ID during cancellation: {exc}"
            )

        with self.owner._stream_stop_lock:
            previous = self.owner._stream_stop_workers.get(key)
            if previous is not None and previous.is_alive():
                self.existing_worker = previous
                return
            self.owner._stream_stop_workers[key] = self

        try:
            self.query.stop()
        except Exception as exc:
            _log_cancel_warning(f"Failed to stop Spark streaming query {self.query_id}: {exc}")


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
    def __init__(self, connection: SessionConnection) -> None:
        self._connection = connection
        self._spark_session = connection._spark_session
        self._analysis_error = connection._analysis_error
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
        self._connection.require_not_cancelled()
        if self._job_group_id is None or self._job_description is None:
            yield
            return

        spark_context = self._spark_session.sparkContext
        previous_properties = {
            name: spark_context.getLocalProperty(name) for name in SPARK_JOB_GROUP_PROPERTIES
        }
        try:
            self._connection.require_not_cancelled()
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
        self._connection.require_not_cancelled()
        if parameters:
            sql = sql % parameters

        self._df = None
        self._rows = None
        self._fetch_index = 0
        self._job_description = _dbt_job_description(sql)
        self._job_group_id = f"dbt:{self._job_description}:{uuid.uuid4().hex}"
        try:
            with self._job_group():
                self._connection.require_not_cancelled()
                self._df = self._spark_session.sql(sql)
        except self._analysis_error as exc:
            raise DbtRuntimeError(str(exc)) from exc

    def execute_seed_insert(
        self,
        rows: list[tuple],
        string_schema: str,
        cast_exprs: list[str],
        table_name: str,
        num_partitions: int,
    ) -> None:
        """Bulk-load seed rows in a single, explicitly-partitioned write.

        Values arrive as strings (or ``None``) so the actual per-column
        casting still happens in Spark SQL, matching the ``cast(? as type)``
        semantics of the batched INSERT path. ``num_partitions`` is passed
        explicitly to ``parallelize`` so this local-data DataFrame does not
        silently inherit ``sc.defaultParallelism`` (see issue #290).
        """
        self._connection.require_not_cancelled()
        self._df = None
        self._rows = None
        self._fetch_index = 0
        self._job_description = f"dbt seed load: {table_name}"
        self._job_group_id = f"dbt:{self._job_description}:{uuid.uuid4().hex}"
        try:
            with self._job_group():
                spark_context = self._spark_session.sparkContext
                self._connection.require_not_cancelled()
                rdd = spark_context.parallelize(rows, numSlices=num_partitions)
                raw_df = self._spark_session.createDataFrame(rdd, schema=string_schema)
                typed_df = raw_df.selectExpr(cast_exprs)
                writer = typed_df.write
                self._connection.require_not_cancelled()
                writer.insertInto(table_name, overwrite=False)
        except self._analysis_error as exc:
            raise DbtRuntimeError(str(exc)) from exc

    def fetchall(self) -> Optional[list[Row]]:
        self._connection.require_not_cancelled()
        if self._rows is None and self._df is not None:
            with self._job_group():
                self._connection.require_not_cancelled()
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
        self._fail_fast_cancelled = threading.Event()
        spark_session_type, analysis_error = _load_pyspark()
        builder = spark_session_type.builder
        for parameter, value in spark_config.get("conf", {}).items():
            builder = builder.config(str(parameter), value)
        builder = builder.appName(str(spark_config["name"])).enableHiveSupport()
        self._spark_session = builder.getOrCreate()
        self._analysis_error = analysis_error

    def mark_fail_fast_cancelled(self) -> None:
        self._fail_fast_cancelled.set()

    def require_not_cancelled(self) -> None:
        if self._fail_fast_cancelled.is_set():
            raise DbtRuntimeError(
                "Spark session was cancelled because another dbt node failed"
            )

    def cursor(self) -> SessionCursor:
        return SessionCursor(self)

    def close(self) -> None:
        pass


class SessionConnectionWrapper(FabricSparkConnectionWrapper):
    def __init__(
        self,
        handle: SessionConnection,
        *,
        stream_stop_timeout_seconds: float = DEFAULT_STREAM_STOP_TIMEOUT_SECONDS,
    ) -> None:
        _validate_stream_stop_timeout_seconds(stream_stop_timeout_seconds)
        self.handle = handle
        self._cursor: Optional[SessionCursor] = None
        self._stream_stop_timeout_seconds = stream_stop_timeout_seconds
        self._stream_stop_workers: dict[Union[str, int], _StreamStopWorker] = {}
        self._stream_stop_lock = threading.Lock()

    def cursor(self) -> SessionConnectionWrapper:
        self._cursor = self.handle.cursor()
        return self

    def cancel(self) -> None:
        """Cancel jobs first; all stream-stop waits share one bounded deadline."""
        self.handle.mark_fail_fast_cancelled()
        cursor = self._cursor
        job_group_id = getattr(cursor, "_job_group_id", None) if cursor else None
        spark_session = self.handle._spark_session
        spark_context = spark_session.sparkContext

        if job_group_id is not None:
            try:
                spark_context.cancelJobGroup(job_group_id)
            except Exception as exc:
                _log_cancel_warning(f"Failed to cancel dbt Spark job group {job_group_id}: {exc}")

        try:
            spark_context.cancelAllJobs()
        except Exception as exc:
            _log_cancel_warning(f"Failed to cancel all Spark jobs during dbt fail-fast: {exc}")

        try:
            active_queries = tuple(spark_session.streams.active)
        except Exception as exc:
            _log_cancel_warning(f"Failed to enumerate active Spark streaming queries: {exc}")
            active_queries = ()

        with self._stream_stop_lock:
            for key, worker in tuple(self._stream_stop_workers.items()):
                if not worker.is_alive():
                    del self._stream_stop_workers[key]

        if not active_queries:
            return

        deadline = time.monotonic() + self._stream_stop_timeout_seconds
        workers: list[_StreamStopWorker] = []
        for query in active_queries:
            worker = _StreamStopWorker(query, deadline, self)
            try:
                worker.start()
            except Exception as exc:
                _log_cancel_warning(f"Failed to start Spark streaming query stop worker: {exc}")
                continue
            workers.append(worker)

        for worker in workers:
            worker.join(max(0.0, min(threading.TIMEOUT_MAX, deadline - time.monotonic())))
            if worker.existing_worker is not None:
                worker = worker.existing_worker
                remaining = min(deadline, worker.deadline) - time.monotonic()
                worker.join(max(0.0, min(threading.TIMEOUT_MAX, remaining)))
            if worker.is_alive():
                _log_cancel_warning(
                    f"Timed out stopping Spark streaming query {worker.query_id} "
                    f"(run {worker.run_id}); "
                    "continuing dbt cancellation"
                )

    def close(self) -> None:
        if self._cursor:
            self._cursor.close()
        self.handle.close()

    def rollback(self, *args: Any, **kwargs: Any) -> None:
        logger.debug("NotImplemented: rollback")

    def load_seed(
        self,
        rows: list[tuple],
        string_schema: str,
        cast_exprs: list[str],
        table_name: str,
        num_partitions: int,
    ) -> None:
        if self._cursor is None:
            raise DbtRuntimeError("Cursor not available")
        self._cursor.execute_seed_insert(
            rows, string_schema, cast_exprs, table_name, num_partitions
        )

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
