import pytest

from dbt.tests.util import run_dbt

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChangeSetup,
)


class IncrementalOnSchemaChangeIgnoreFail(BaseIncrementalOnSchemaChangeSetup):
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
        # Delta Lake doesn"t support removing columns -- show a nice compilation error
        results = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results[1].message

    def run_incremental_sync_remove_only(self, project):
        select = "model_a incremental_sync_remove_only incremental_sync_remove_only_target"
        run_dbt(["run", "--models", select, "--full-refresh"])
        # Delta Lake doesn"t support removing columns -- show a nice compilation error
        results = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results[1].message

    def test_run_incremental_append_new_columns(self, project):
        # only adding new columns in supported
        self.run_incremental_append_new_columns(project)
        # handling columns that have been removed doesn"t work on Delta Lake today
        # self.run_incremental_append_new_columns_remove_one(project)

    def test_run_incremental_sync_all_columns(self, project):
        self.run_incremental_sync_all_columns(project)
        self.run_incremental_sync_remove_only(project)
