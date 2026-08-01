"""Live-Fabric regression coverage for the MERGE schema-evolution session conf.

``merge_with_schema_evolution`` is applied by switching on a Spark session conf.
Livy sessions outlive a single model — they are reused across models, across dbt
invocations and, with ``reuse_session``, across runs — so leaving that conf set
silently applies schema evolution to every later ``MERGE`` in the session.

The damage lands on snapshots: a snapshot merge absorbs dbt's internal
``dbt_change_type`` / ``dbt_unique_key`` staging columns into the snapshot table,
and the *next* snapshot run then fails permanently with ``AMBIGUOUS_REFERENCE``.

This test reproduces that sequence end-to-end in one Livy session: run the model
that turns the conf on, then snapshot repeatedly. Everything is asserted through
``describe``, which reads real server-side state — the session conf itself is not
readable from the test harness because ``project.run_sql`` does not share a
connection with ``run_dbt``.
"""

from __future__ import annotations

import pytest

from dbt.tests.util import relation_from_name, run_dbt
from tests.functional.adapter.schema_evolution.fixtures import (
    base_seed_csv,
    evolution_snapshot_sql,
    evolving_incremental_sql,
)

SNAPSHOT_INTERNAL_COLUMNS = ("dbt_unique_key", "dbt_change_type")


def column_names(project, name: str) -> list:
    """Return the relation's column names, stopping at ``describe``'s metadata block."""
    relation = relation_from_name(project.adapter, name)
    rows = project.run_sql(f"describe {relation}", fetch="all")
    names = []
    for row in rows:
        column = str(row[0]).strip()
        if not column or column.startswith("#"):
            break
        names.append(column.lower())
    return names


class TestSnapshotSurvivesSchemaEvolutionMerge:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base_seed.csv": base_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"evolving.sql": evolving_incremental_sql}

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"evo_snapshot.sql": evolution_snapshot_sql}

    def test_snapshot_is_not_corrupted_by_an_earlier_merge(self, project):
        run_dbt(["seed"])

        # First run creates the table; the second takes the incremental branch and
        # runs the MERGE that switches the schema-evolution conf on.
        run_dbt(["run", "--select", "evolving"])
        run_dbt(["run", "--select", "evolving"])

        assert "extra" in column_names(project, "evolving"), (
            "merge_with_schema_evolution stopped working — the new source column "
            "was not added to the target"
        )

        run_dbt(["snapshot"])
        baseline = column_names(project, "evo_snapshot")
        for column in SNAPSHOT_INTERNAL_COLUMNS:
            assert column not in baseline, (
                f"snapshot absorbed dbt's internal {column} column, which means the "
                "schema-evolution conf leaked out of the merge above"
            )

        # The corruption only became fatal on the run after the columns appeared,
        # so the cycle is repeated to prove the table stays usable.
        for _ in range(2):
            results = run_dbt(["snapshot"])
            assert len(results) == 1
            assert column_names(project, "evo_snapshot") == baseline
