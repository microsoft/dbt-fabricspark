import pytest

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChangeSetup,
)
from dbt.tests.util import relation_from_name, run_dbt


class IncrementalOnSchemaChangeIgnoreFail(BaseIncrementalOnSchemaChangeSetup):
    def test_run_incremental_ignore(self, project):
        select = "model_a incremental_ignore incremental_ignore_target"
        compare_source = "incremental_ignore"
        compare_target = "incremental_ignore_target"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def test_run_incremental_fail_on_schema_change(self, project):
        select = "model_a incremental_fail"
        run_dbt(["run", "--models", select, "--full-refresh"])
        results_two = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results_two[1].message


class TestAppendOnSchemaChange(IncrementalOnSchemaChangeIgnoreFail):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "append",
            }
        }


class TestInsertOverwriteOnSchemaChange(IncrementalOnSchemaChangeIgnoreFail):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
                "+partition_by": "id",
                "+incremental_strategy": "insert_overwrite",
            }
        }


class TestDeltaOnSchemaChange(BaseIncrementalOnSchemaChangeSetup):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
                "+incremental_strategy": "merge",
                "+unique_key": "id",
            }
        }

    def run_incremental_sync_all_columns(self, project):
        select = "model_a incremental_sync_all_columns incremental_sync_all_columns_target"
        run_dbt(["run", "--models", select, "--full-refresh"])
        results = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results[1].message
        assert "enable_column_mapping_on_drop" in results[1].message

    def run_incremental_sync_remove_only(self, project):
        select = "model_a incremental_sync_remove_only incremental_sync_remove_only_target"
        run_dbt(["run", "--models", select, "--full-refresh"])
        results = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results[1].message
        assert "enable_column_mapping_on_drop" in results[1].message

    def test_run_incremental_append_new_columns(self, project):
        self.run_incremental_append_new_columns(project)

    def test_run_incremental_sync_all_columns(self, project):
        self.run_incremental_sync_all_columns(project)
        self.run_incremental_sync_remove_only(project)


def _column_mapping_mode(project, name):
    relation = relation_from_name(project.adapter, name)
    rows = project.run_sql(f"show tblproperties {relation}", fetch="all")
    return {row[0]: row[1] for row in rows}.get("delta.columnMapping.mode")


class TestDeltaOnSchemaChangeEnableColumnMappingOnDrop(BaseIncrementalOnSchemaChangeSetup):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
                "+incremental_strategy": "merge",
                "+unique_key": "id",
                "+enable_column_mapping_on_drop": True,
            }
        }

    def test_run_incremental_sync_all_columns(self, project):
        self.run_incremental_sync_all_columns(project)
        assert _column_mapping_mode(project, "incremental_sync_all_columns") == "name"

    def test_run_incremental_sync_remove_only(self, project):
        self.run_incremental_sync_remove_only(project)
        assert _column_mapping_mode(project, "incremental_sync_remove_only") == "name"


class TestDeltaOnSchemaChangeExistingColumnMapping(BaseIncrementalOnSchemaChangeSetup):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
                "+incremental_strategy": "merge",
                "+unique_key": "id",
                "+tblproperties": {
                    "delta.columnMapping.mode": "name",
                    "delta.minReaderVersion": "2",
                    "delta.minWriterVersion": "5",
                },
            }
        }

    def test_run_incremental_sync_remove_only(self, project):
        self.run_incremental_sync_remove_only(project)
