import pytest

from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase
from dbt.tests.util import check_relations_equal, run_dbt
from tests.functional.adapter.incremental_strategies.fixtures import (
    append_delta_sql,
    bad_delete_insert_no_key_sql,
    bad_file_format_sql,
    bad_merge_not_delta_sql,
    bad_strategy_sql,
    default_append_sql,
    delete_insert_unique_key_sql,
    delta_merge_no_key_sql,
    delta_merge_unique_key_sql,
    delta_merge_update_columns_sql,
    # Skip: CT-1873 insert_overwrite_partitions_delta_sql,
    insert_overwrite_no_partitions_sql,
    insert_overwrite_partitions_sql,
    merge_full_refresh_sql,
    merge_matched_condition_sql,
    merge_not_matched_by_source_delete_sql,
    merge_not_matched_by_source_update_sql,
    merge_schema_evolution_sql,
    merge_skip_matched_sql,
    merge_skip_not_matched_sql,
)
from tests.functional.adapter.incremental_strategies.seeds import (
    expected_append_csv,
    expected_matched_condition_csv,
    expected_merge_schema_evolution_csv,
    expected_not_matched_by_source_delete_csv,
    expected_not_matched_by_source_update_csv,
    expected_overwrite_csv,
    expected_partial_upsert_csv,
    expected_skip_matched_csv,
    expected_skip_not_matched_csv,
    expected_upsert_csv,
)


class BaseIncrementalStrategies(SeedConfigBase):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_append.csv": expected_append_csv,
            "expected_overwrite.csv": expected_overwrite_csv,
            "expected_upsert.csv": expected_upsert_csv,
            "expected_partial_upsert.csv": expected_partial_upsert_csv,
        }

    @staticmethod
    def seed_and_run_once():
        run_dbt(["seed"])
        run_dbt(["run"])

    @staticmethod
    def seed_and_run_twice():
        run_dbt(["seed"])
        run_dbt(["run"])
        run_dbt(["run"])


class TestDefaultAppend(BaseIncrementalStrategies):
    @pytest.fixture(scope="class")
    def models(self):
        return {"default_append.sql": default_append_sql}

    def run_and_test(self, project):
        self.seed_and_run_twice()
        check_relations_equal(project.adapter, ["default_append", "expected_append"])

    def test_default_append(self, project):
        self.run_and_test(project)


class TestInsertOverwrite(BaseIncrementalStrategies):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "insert_overwrite_no_partitions.sql": insert_overwrite_no_partitions_sql,
            "insert_overwrite_partitions.sql": insert_overwrite_partitions_sql,
        }

    def run_and_test(self, project):
        self.seed_and_run_twice()
        check_relations_equal(
            project.adapter, ["insert_overwrite_no_partitions", "expected_overwrite"]
        )
        check_relations_equal(project.adapter, ["insert_overwrite_partitions", "expected_upsert"])

    def test_insert_overwrite(self, project):
        self.run_and_test(project)


class TestDeltaStrategies(BaseIncrementalStrategies):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "append_delta.sql": append_delta_sql,
            "merge_no_key.sql": delta_merge_no_key_sql,
            "merge_unique_key.sql": delta_merge_unique_key_sql,
            "merge_update_columns.sql": delta_merge_update_columns_sql,
            "delete_insert_unique_key.sql": delete_insert_unique_key_sql,
            # Skip: cannot be acnive on any endpoint with grants
            # "insert_overwrite_partitions_delta.sql": insert_overwrite_partitions_delta_sql,
        }

    def run_and_test(self, project):
        self.seed_and_run_twice()
        # Invalidate Spark's cached metadata for seed tables to avoid
        # TABLE_OR_VIEW_NOT_FOUND flakes caused by cross-catalog metastore
        # propagation delays in Fabric.
        for seed in ["expected_append", "expected_upsert", "expected_partial_upsert"]:
            project.run_sql(f"REFRESH TABLE {{schema}}.{seed}")
        check_relations_equal(project.adapter, ["append_delta", "expected_append"])
        check_relations_equal(project.adapter, ["merge_no_key", "expected_append"])
        check_relations_equal(project.adapter, ["merge_unique_key", "expected_upsert"])
        check_relations_equal(project.adapter, ["merge_update_columns", "expected_partial_upsert"])
        check_relations_equal(project.adapter, ["delete_insert_unique_key", "expected_upsert"])

    def test_delta_strategies(self, project):
        self.run_and_test(project)


class BaseAdvancedMerge(BaseIncrementalStrategies):
    """Shared driver for the advanced ``merge`` option scenarios.

    Each subclass supplies a single model plus its expected seed and asserts the
    materialized relation matches the expected one after two runs (create, then
    incremental merge).
    """

    model_name: str = ""
    expected_name: str = ""

    def run_and_test(self, project):
        self.seed_and_run_twice()
        project.run_sql(f"REFRESH TABLE {{schema}}.{self.expected_name}")
        check_relations_equal(project.adapter, [self.model_name, self.expected_name])


class TestMergeSkipMatched(BaseAdvancedMerge):
    model_name = "merge_skip_matched"
    expected_name = "expected_skip_matched"

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_skip_matched.csv": expected_skip_matched_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_skip_matched.sql": merge_skip_matched_sql}

    def test_skip_matched(self, project):
        self.run_and_test(project)


class TestMergeSkipNotMatched(BaseAdvancedMerge):
    model_name = "merge_skip_not_matched"
    expected_name = "expected_skip_not_matched"

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_skip_not_matched.csv": expected_skip_not_matched_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_skip_not_matched.sql": merge_skip_not_matched_sql}

    def test_skip_not_matched(self, project):
        self.run_and_test(project)


class TestMergeMatchedCondition(BaseAdvancedMerge):
    model_name = "merge_matched_condition"
    expected_name = "expected_matched_condition"

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_matched_condition.csv": expected_matched_condition_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_matched_condition.sql": merge_matched_condition_sql}

    def test_matched_condition(self, project):
        self.run_and_test(project)


class TestMergeNotMatchedBySourceDelete(BaseAdvancedMerge):
    model_name = "merge_not_matched_by_source_delete"
    expected_name = "expected_not_matched_by_source_delete"

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_not_matched_by_source_delete.csv": expected_not_matched_by_source_delete_csv
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_not_matched_by_source_delete.sql": merge_not_matched_by_source_delete_sql}

    def test_not_matched_by_source_delete(self, project):
        self.run_and_test(project)


class TestMergeNotMatchedBySourceUpdate(BaseAdvancedMerge):
    model_name = "merge_not_matched_by_source_update"
    expected_name = "expected_not_matched_by_source_update"

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected_not_matched_by_source_update.csv": expected_not_matched_by_source_update_csv
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_not_matched_by_source_update.sql": merge_not_matched_by_source_update_sql}

    def test_not_matched_by_source_update(self, project):
        self.run_and_test(project)


class TestMergeSchemaEvolution(BaseAdvancedMerge):
    model_name = "merge_schema_evolution"
    expected_name = "expected_merge_schema_evolution"

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_merge_schema_evolution.csv": expected_merge_schema_evolution_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_schema_evolution.sql": merge_schema_evolution_sql}

    def test_schema_evolution(self, project):
        self.run_and_test(project)


class TestBadStrategies(BaseIncrementalStrategies):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad_file_format.sql": bad_file_format_sql,
            "bad_merge_not_delta.sql": bad_merge_not_delta_sql,
            "bad_strategy.sql": bad_strategy_sql,
            "bad_delete_insert_no_key.sql": bad_delete_insert_no_key_sql,
        }

    @staticmethod
    def run_and_test():
        run_results = run_dbt(["run"], expect_pass=False)
        # assert all models fail with compilation errors
        for result in run_results:
            assert result.status == "error"
            assert "Compilation Error in model" in result.message

    def test_bad_strategies(self, project):
        self.run_and_test()


class TestIncrementalFullRefresh(BaseIncrementalStrategies):
    """Regression test for TABLE_OR_VIEW_ALREADY_EXISTS on --full-refresh.

    When an incremental model backed by a Delta table is re-run with
    --full-refresh and file_format is NOT explicitly set in the model config,
    the materialization must still drop the existing table before recreating it
    rather than relying on CREATE OR REPLACE TABLE (which requires
    target_relation.is_delta to be set on the `this` relation).
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_full_refresh.sql": merge_full_refresh_sql}

    def test_full_refresh(self, project):
        # First run: create the incremental model
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].status == "success"

        # Second run with --full-refresh: must not raise TABLE_OR_VIEW_ALREADY_EXISTS
        results = run_dbt(["run", "--full-refresh"])
        assert len(results) == 1
        assert results[0].status == "success"
