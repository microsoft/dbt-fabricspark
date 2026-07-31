"""Live-Fabric coverage for the automatic post-build ``OPTIMIZE``.

Every assertion reads Delta's own transaction log via ``describe history`` so we
observe what actually reached the server rather than what the adapter believed
it sent.  ``describe history`` cannot be wrapped in a subquery on Fabric Spark
(the parser rejects it), so the whole history is fetched and the ``operation``
column is read positionally.
"""

from __future__ import annotations

from typing import Any, List

import pytest

from dbt.tests.util import relation_from_name, run_dbt
from tests.functional.adapter.optimize.fixtures import (
    base_seed_csv,
    env_table_sql,
    optimized_incremental_sql,
    optimized_snapshot_sql,
    optimized_table_sql,
    skipped_table_sql,
)

SKIP_OPTIMIZE_ENV_VAR = "DBT_FABRICSPARK_SKIP_OPTIMIZE"

# Positional layout of ``describe history``: version, timestamp, userId,
# userName, operation, operationParameters, ...
_OPERATION_INDEX = 4
_OPERATION_PARAMETERS_INDEX = 5


def _is_auto_compaction(parameters: Any) -> bool:
    """Delta tags engine-driven compaction with ``auto=true`` in its parameters.

    Fabric may compact on its own, and those entries also surface as OPTIMIZE.
    Excluding them keeps the opt-out assertions from failing on activity the
    adapter never requested.
    """
    if isinstance(parameters, dict):
        return str(parameters.get("auto", "")).lower() == "true"
    normalized = str(parameters).replace(" ", "").replace("'", '"')
    return '"auto":"true"' in normalized or '"auto":true' in normalized


def adapter_optimize_count(project, name: str) -> int:
    relation = relation_from_name(project.adapter, name)
    rows: List[Any] = project.run_sql(f"describe history {relation}", fetch="all")
    return sum(
        1
        for row in rows
        if str(row[_OPERATION_INDEX]).upper() == "OPTIMIZE"
        and not _is_auto_compaction(row[_OPERATION_PARAMETERS_INDEX])
    )


class BaseAutoOptimize:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base_seed.csv": base_seed_csv}


class TestAutoOptimizeEnabledByDefault(BaseAutoOptimize):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "optimized_table.sql": optimized_table_sql,
            "optimized_incremental.sql": optimized_incremental_sql,
        }

    def test_table_and_incremental_are_optimized(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        assert adapter_optimize_count(project, "optimized_table") >= 1
        assert adapter_optimize_count(project, "optimized_incremental") == 1

        # The second run merges instead of creating, and that is the path that
        # accumulates small files, so it must be optimized as well.  A `table`
        # model may be dropped and recreated, resetting its history, so only
        # the incremental relation can be asserted cumulatively.
        run_dbt(["run"])

        assert adapter_optimize_count(project, "optimized_table") >= 1
        assert adapter_optimize_count(project, "optimized_incremental") >= 2


class TestAutoOptimizeSnapshot(BaseAutoOptimize):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"optimized_snapshot.sql": optimized_snapshot_sql}

    def test_snapshot_is_optimized(self, project):
        run_dbt(["seed"])
        run_dbt(["snapshot"])

        # Only the initial build is asserted: a second snapshot over unchanged
        # data writes no new files, and Delta commits nothing for an OPTIMIZE
        # that has nothing to compact.  The merge path is covered by the
        # incremental model above.
        assert adapter_optimize_count(project, "optimized_snapshot") == 1


class TestAutoOptimizeDisabledByModelConfig(BaseAutoOptimize):
    @pytest.fixture(scope="class")
    def models(self):
        return {"skipped_table.sql": skipped_table_sql}

    def test_model_config_opts_out(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        assert adapter_optimize_count(project, "skipped_table") == 0


class TestAutoOptimizeDisabledByEnvVar(BaseAutoOptimize):
    @pytest.fixture(scope="class")
    def models(self):
        return {"env_table.sql": env_table_sql}

    def test_env_var_kill_switch(self, project, monkeypatch):
        run_dbt(["seed"])

        monkeypatch.setenv(SKIP_OPTIMIZE_ENV_VAR, "true")
        run_dbt(["run"])
        assert adapter_optimize_count(project, "env_table") == 0

        monkeypatch.delenv(SKIP_OPTIMIZE_ENV_VAR)
        run_dbt(["run"])
        assert adapter_optimize_count(project, "env_table") >= 1
