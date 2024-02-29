import pytest
from dbt.tests.util import run_dbt
from tests.functional.adapter.seed_column_types.fixtures import (
    _MACRO_TEST_IS_TYPE_SQL,
    _SEED_CSV,
    _SEED_YML,
)


class TestSeedColumnTypesCast:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_is_type.sql": _MACRO_TEST_IS_TYPE_SQL}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"payments.csv": _SEED_CSV, "schema.yml": _SEED_YML}

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

    #  We want to test seed types because hive would cause all fields to be strings.
    # setting column_types in project.yml should change them and pass.
    def test_column_seed_type(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        run_dbt(["test"], expect_pass=False)
