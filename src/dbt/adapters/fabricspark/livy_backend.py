from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from typing import Any, Optional

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials

logger = AdapterLogger("fabricspark")

_DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
)


def _parse_datetime(value: str) -> Optional[dt.datetime]:
    text = value.strip()
    if not text:
        return None
    normalized = text[:-1] if text.endswith("Z") else text
    try:
        return dt.datetime.fromisoformat(normalized)
    except ValueError:
        pass
    for fmt in _DATETIME_FORMATS:
        try:
            return dt.datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _parse_date(value: str) -> Optional[dt.date]:
    text = value.strip()
    if not text:
        return None
    try:
        return dt.date.fromisoformat(text)
    except ValueError:
        parsed = _parse_datetime(text)
        return parsed.date() if parsed is not None else None


def _coerce_value(value: Any, column_type: str) -> Any:
    """Coerce a single Livy result value to a native datetime/date.

    Only string values in time-typed columns are converted. ``None`` and
    values that fail to parse are returned untouched so a malformed value can
    never turn a successful query into a failure.
    """
    if value is None or not isinstance(value, str):
        return value
    if column_type in ("timestamp", "timestamp_ntz"):
        parsed = _parse_datetime(value)
        return parsed if parsed is not None else value
    if column_type == "date":
        parsed = _parse_date(value)
        return parsed if parsed is not None else value
    return value


def coerce_time_columns(rows: Optional[list], schema: Optional[list]) -> Optional[list]:
    """Convert time-typed columns in Livy result rows to native Python objects.

    Fabric's Livy statement-result API returns ``timestamp``, ``timestamp_ntz``
    and ``date`` columns as strings. dbt-core (and ``run_query`` callers) expect
    native ``datetime``/``date`` objects — most visibly, ``dbt source freshness``
    fails with "received value of type 'str'" otherwise. This normalizes the
    values in-place using the positional column types from ``schema.fields``.
    """
    if not rows or not schema:
        return rows

    time_columns = {
        idx: field["type"]
        for idx, field in enumerate(schema)
        if isinstance(field, dict) and field.get("type") in ("timestamp", "timestamp_ntz", "date")
    }
    if not time_columns:
        return rows

    for row in rows:
        if not isinstance(row, list):
            continue
        for idx, column_type in time_columns.items():
            if idx < len(row):
                row[idx] = _coerce_value(row[idx], column_type)
    return rows


class LivyBackend(ABC):
    """Pluggable Livy backend.

    Two implementations live in this package:

    - :class:`dbt.adapters.fabricspark.singleton_livy.LivySessionManager` —
      one Livy session per process; statements run sequentially inside that
      session's single interpreter.
    - :class:`dbt.adapters.fabricspark.concurrent_livy.HighConcurrencySessionManager` —
      one HC session (= one REPL) per dbt thread, all sharing one underlying
      Livy session via a deterministic ``sessionTag``. Different REPLs run in
      parallel inside the same Spark application.

    Selection is driven by ``FabricSparkCredentials.high_concurrency``.
    ``open()`` in :mod:`connections` instantiates one backend per thread and
    calls :meth:`connect` to obtain a DB-API-shaped connection wrapper.
    """

    @abstractmethod
    def connect(self, credentials: FabricSparkCredentials) -> Any:
        """Acquire (or reuse) a Livy session/REPL and return a connection handle.

        The returned object must expose ``cursor()`` and ``close()`` methods
        plus the cursor surface used by the SQL connection manager
        (``execute``, ``fetchall``, ``fetchmany``, ``fetchone``, ``description``).
        """

    @abstractmethod
    def disconnect(self) -> None:
        """Release backend-owned resources for this instance.

        Singleton mode keeps the underlying Livy session alive when
        ``reuse_session`` is true; HC mode always deletes its per-thread HC
        session so the REPL slot frees up immediately.
        """
