"""Shared logic for the three snapshot-check-cols split tests.

Splits dbt-core's ``BaseSnapshotCheckCols`` into per-strategy siblings so
xdist ``loadscope`` spreads them across workers (~3× faster floor).

Each subclass declares ``snapshots`` with exactly one of the three strategies
(cc_all / cc_date / cc_name) and asserts only the rowcounts applicable to
that strategy, using the same seeds/updates sequence as the original test.
"""

from __future__ import annotations

import pytest

from dbt.tests.adapter.basic.files import (
    cc_all_snapshot_sql,
    cc_date_snapshot_sql,
    cc_name_snapshot_sql,
    seeds_added_csv,
    seeds_base_csv,
)
from dbt.tests.util import relation_from_name, run_dbt, update_rows


_SNAPSHOT_SQL_BY_NAME = {
    "cc_all_snapshot": cc_all_snapshot_sql,
    "cc_date_snapshot": cc_date_snapshot_sql,
    "cc_name_snapshot": cc_name_snapshot_sql,
}


def check_relation_rows(project, snapshot_name, count):
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
    assert int(result[0]) == count


class SnapshotCheckColsSplitBase:
    """Per-strategy snapshot test.

    Subclasses set:
      - ``snapshot_name``  (e.g. ``"cc_all_snapshot"``)
      - ``expected_counts`` 4-tuple of rowcounts after each of the 4
        snapshot invocations in the sequence.
    """

    snapshot_name: str
    expected_counts: tuple[int, int, int, int]

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "snapshot_strategy_check_cols"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "added.csv": seeds_added_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {f"{self.snapshot_name}.sql": _SNAPSHOT_SQL_BY_NAME[self.snapshot_name]}

    def test_snapshot_check_cols(self, project):
        c0, c1, c2, c3 = self.expected_counts

        results = run_dbt(["seed"])
        assert len(results) == 2

        # 1. initial snapshot (base seed)
        results = run_dbt(["snapshot"])
        for r in results:
            assert r.status == "success"
        check_relation_rows(project, self.snapshot_name, c0)

        # 2. snapshot with the "added" seed
        results = run_dbt(["--no-partial-parse", "snapshot", "--vars", "seed_name: added"])
        for r in results:
            assert r.status == "success"
        check_relation_rows(project, self.snapshot_name, c1)

        # 3. update timestamps and snapshot again
        update_rows(
            project.adapter,
            {
                "name": "added",
                "dst_col": "some_date",
                "clause": {"src_col": "some_date", "type": "add_timestamp"},
                "where": "id > 10 and id < 21",
            },
        )
        results = run_dbt(["snapshot", "--vars", "seed_name: added"])
        for r in results:
            assert r.status == "success"
        check_relation_rows(project, self.snapshot_name, c2)

        # 4. update names and snapshot again
        update_rows(
            project.adapter,
            {
                "name": "added",
                "dst_col": "name",
                "clause": {"src_col": "name", "type": "add_string", "value": "_updated"},
                "where": "id < 11",
            },
        )
        results = run_dbt(["snapshot", "--vars", "seed_name: added"])
        for r in results:
            assert r.status == "success"
        check_relation_rows(project, self.snapshot_name, c3)
