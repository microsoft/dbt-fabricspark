import pytest

from dbt.tests.adapter.basic.files import (
    seeds_added_csv,
    seeds_base_csv,
    seeds_newcolumns_csv,
    ts_snapshot_sql,
)
from dbt.tests.util import relation_from_name, run_dbt, update_rows


def check_relation_rows(project, snapshot_name, count):
    relation = relation_from_name(project.adapter, snapshot_name)
    result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
    assert int(result[0]) == count


class BaseSnapshotTimestamp:
    @pytest.fixture(scope="class")
    def dbt_profile_data(unique_schema, dbt_profile_target, profiles_config_update):
        profile = {
            "test": {
                "outputs": {
                    "default": {},
                },
                "target": "default",
            },
        }
        target = dbt_profile_target
        target["schema"] = target["lakehouse"]
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seeds_base_csv,
            "newcolumns.csv": seeds_newcolumns_csv,
            "added.csv": seeds_added_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "ts_snapshot.sql": ts_snapshot_sql,  # .replace("target_database=database","target_database=\"dbttest\"").replace("target_schema=schema","target_schema=\"dbttest\""),
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "snapshot_strategy_timestamp"}

    def test_snapshot_timestamp(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 3

        # snapshot command
        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # snapshot has 10 rows
        check_relation_rows(project, "ts_snapshot", 10)

        # point at the "added" seed so the snapshot sees 10 new rows
        results = run_dbt(["snapshot", "--vars", "seed_name: added"])

        # snapshot now has 20 rows
        check_relation_rows(project, "ts_snapshot", 20)

        # update some timestamps in the "added" seed so the snapshot sees 10 more new rows
        update_rows_config = {
            "name": "added",
            "dst_col": "some_date",
            "clause": {
                "src_col": "some_date",
                "type": "add_timestamp",
            },
            "where": "id > 10 and id < 21",
        }
        update_rows(project.adapter, update_rows_config)

        results = run_dbt(["snapshot", "--vars", "seed_name: added"])

        # snapshot now has 30 rows
        check_relation_rows(project, "ts_snapshot", 30)

        update_rows_config = {
            "name": "added",
            "dst_col": "name",
            "clause": {
                "src_col": "name",
                "type": "add_string",
                "value": "_updated",
            },
            "where": "id < 11",
        }
        update_rows(project.adapter, update_rows_config)

        results = run_dbt(["snapshot", "--vars", "seed_name: added"])

        # snapshot still has 30 rows because timestamp not updated
        check_relation_rows(project, "ts_snapshot", 30)


class TestSnapshotTimestamp(BaseSnapshotTimestamp):
    pass
