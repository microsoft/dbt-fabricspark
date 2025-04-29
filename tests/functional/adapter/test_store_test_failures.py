import pytest

from dbt.tests.adapter.store_test_failures_tests.fixtures import (
    models__file_model_but_with_a_no_good_very_long_name,
    models__fine_model,
    models__problematic_model,
    properties__schema_yml,
    seeds__expected_accepted_values,
    seeds__expected_failing_test,
    seeds__expected_not_null_problematic_model_id,
    seeds__expected_unique_problematic_model_id,
    seeds__people,
    tests__failing_test,
    tests__passing_test,
)
from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)


class StoreTestFailuresBase:
    schemaname: str = None
    audit_schema_suffix: str = ""
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
        StoreTestFailuresBase.schemaname = target["lakehouse"]
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    @pytest.fixture(scope="function", autouse=True)
    def setUp(self, project):
        self.test_audit_schema = project.adapter.config.credentials.schema
        run_dbt(["seed"])
        run_dbt(["run"])

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "people.csv": seeds__people,
            "expected_accepted_values.csv": seeds__expected_accepted_values,
            "expected_failing_test.csv": seeds__expected_failing_test,
            "expected_not_null_problematic_model_id.csv": seeds__expected_not_null_problematic_model_id,
            "expected_unique_problematic_model_id.csv": seeds__expected_unique_problematic_model_id,
        }

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "failing_test.sql": tests__failing_test,
            "passing_test.sql": tests__passing_test,
        }

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": properties__schema_yml}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fine_model.sql": models__fine_model,
            "fine_model_but_with_a_no_good_very_long_name.sql": models__file_model_but_with_a_no_good_very_long_name,
            "problematic_model.sql": models__problematic_model,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": True,
            },
            "tests": {"+store_failures": True, "schema": StoreTestFailuresBase.schemaname},
        }

    def column_type_overrides(self):
        return {}

    def run_tests_store_one_failure(self, project):
        self.test_audit_schema = project.adapter.config.credentials.schema
        run_dbt(["test"], expect_pass=False)

        # one test is configured with store_failures: true, make sure it worked
        check_relations_equal(
            project.adapter,
            [
                f"{self.test_audit_schema}.unique_problematic_model_id",
                "expected_unique_problematic_model_id",
            ],
        )

    def run_tests_store_failures_and_assert(self, project):
        # make sure this works idempotently for all tests
        run_dbt(["test", "--store-failures"], expect_pass=False)
        results = run_dbt(["test", "--store-failures"], expect_pass=False)

        # compare test results
        actual = [(r.status, r.failures) for r in results]
        expected = [
            ("pass", 0),
            ("pass", 0),
            ("pass", 0),
            ("pass", 0),
            ("fail", 2),
            ("fail", 2),
            ("fail", 2),
            ("fail", 10),
        ]
        assert sorted(actual) == sorted(expected)

        # compare test results stored in database
        check_relations_equal(
            project.adapter, [f"{self.test_audit_schema}.failing_test", "expected_failing_test"]
        )
        check_relations_equal(
            project.adapter,
            [
                f"{self.test_audit_schema}.not_null_problematic_model_id",
                "expected_not_null_problematic_model_id",
            ],
        )
        check_relations_equal(
            project.adapter,
            [
                f"{self.test_audit_schema}.unique_problematic_model_id",
                "expected_unique_problematic_model_id",
            ],
        )
        check_relations_equal(
            project.adapter,
            [
                f"{self.test_audit_schema}.accepted_values_problemat"
                "ic_mo_c533ab4ca65c1a9dbf14f79ded49b628",
                "expected_accepted_values",
            ],
        )

    def test_store_and_assert(self, project):
        self.run_tests_store_one_failure(project)
        self.run_tests_store_failures_and_assert(project)


class TestSparkStoreTestFailures(StoreTestFailuresBase):
    pass
