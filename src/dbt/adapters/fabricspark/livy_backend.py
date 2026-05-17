from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from dbt.adapters.fabricspark.credentials import FabricSparkCredentials


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
