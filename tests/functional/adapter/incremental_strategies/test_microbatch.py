import pytest

from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
    patch_microbatch_end_time,
)
from dbt.tests.util import run_dbt

# No requirement for a unique_id for spark microbatch!
_microbatch_model_no_unique_id_sql = """
{{ config(materialized='incremental', incremental_strategy='microbatch', event_time='event_time', batch_size='day', begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0), partition_by=['date_day'], file_format='delta') }}
select *, cast(event_time as date) as date_day
from {{ ref('input_model') }}
"""


@pytest.mark.skip("TODO: re-enable after fixing microbatch tests for lakehouse namespace changes")
class TestMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return _microbatch_model_no_unique_id_sql

    @pytest.fixture(scope="class")
    def insert_two_rows_sql(self, project) -> str:
        # Override base: Fabric Lakehouse overrides schema to the database name,
        # so use database directly instead of test_schema.
        db = project.database
        return f"insert into {db}.input_model (id, event_time) values (4, TIMESTAMP '2020-01-04 00:00:00-0'), (5, TIMESTAMP '2020-01-05 00:00:00-0')"

    def test_run_with_event_time(self, project, insert_two_rows_sql):
        # Fabric Lakehouse uses a flat namespace — all tests share the same schema.
        # Use --full-refresh on the first run to ensure a clean microbatch_model
        # regardless of stale tables left by previous test runs.
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            run_dbt(["run", "--full-refresh"])
        self.assert_row_count(project, "microbatch_model", 3)

        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run"])
        self.assert_row_count(project, "microbatch_model", 3)

        project.run_sql(insert_two_rows_sql)
        self.assert_row_count(project, "input_model", 5)

        with patch_microbatch_end_time("2020-01-03 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 3)

        with patch_microbatch_end_time("2020-01-04 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 4)

        with patch_microbatch_end_time("2020-01-05 14:57:00"):
            run_dbt(["run", "--select", "microbatch_model"])
        self.assert_row_count(project, "microbatch_model", 5)
