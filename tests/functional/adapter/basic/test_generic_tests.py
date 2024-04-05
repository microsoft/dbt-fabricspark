
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
import pytest

class TestGenericTestsSpark(BaseGenericTests):
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
    