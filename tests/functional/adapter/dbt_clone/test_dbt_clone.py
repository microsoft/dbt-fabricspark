import os
import shutil
from collections import Counter
from copy import deepcopy

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.tests.adapter.dbt_clone import fixtures
from dbt.tests.util import run_dbt

get_schema_name_sql = """
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is not none -%}
        {{ return(default_schema|trim) }}
    -- put seeds into a separate schema in "prod", to verify that cloning in "dev" still works
    {%- elif target.name == 'default' and node.resource_type == 'seed' -%}
        {{ return(default_schema) }}
    {%- else -%}
        {{ return(default_schema) }}
    {%- endif -%}
{%- endmacro %}
"""

class BaseClone:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": fixtures.table_model_sql,
            "view_model.sql": fixtures.view_model_sql,
            "ephemeral_model.sql": fixtures.ephemeral_model_sql,
            "schema.yml": fixtures.schema_yml,
            "exposures.yml": fixtures.exposures_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": fixtures.macros_sql,
            "infinite_macros.sql": fixtures.infinite_macros_sql,
            "get_schema_name.sql": get_schema_name_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": fixtures.seed_csv,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot.sql": fixtures.snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def other_schema(self, unique_schema):
        return unique_schema + "_other"

    @property
    def project_config_update(self):
        return {
            "seeds": {
                "test": {
                    "quote_columns": False,
                }
            }
        }

    def copy_state(self, project_root):
        state_path = os.path.join(project_root, "state")
        if not os.path.exists(state_path):
            os.makedirs(state_path)
        shutil.copyfile(
            f"{project_root}/target/manifest.json", f"{project_root}/state/manifest.json"
        )

    def run_and_save_state(self, project_root, with_snapshot=False):
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt(["run"])
        assert len(results) == 2
        results = run_dbt(["test"])
        assert len(results) == 2

        if with_snapshot:
            results = run_dbt(["snapshot"])
            assert len(results) == 1

        # copy files
        self.copy_state(project_root)

@pytest.mark.skip("Cloning cross schema is not supported")
class TestSparkClonePossible(BaseClone):
    schemaname:str = None

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
        TestSparkClonePossible.schemaname = target["lakehouse"]
        profile["test"]["outputs"]["default"] = target

        if profiles_config_update:
            profile.update(profiles_config_update)
        return profile

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema, other_schema):
        outputs = {"default": dbt_profile_target, "otherschema": deepcopy(dbt_profile_target)}
        return {"test": {"outputs": outputs, "target": "default"}}


    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
            },
            "seeds": {
                "test": {
                    "quote_columns": False,
                },
                "+file_format": "delta",
            },
            "snapshots": {
                "+file_format": "delta",
            },
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_seeds"
            )
            project.adapter.drop_schema(relation)

            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    def test_can_clone_true(self, project, unique_schema, other_schema):
        project.create_test_schema(TestSparkClonePossible.schemaname)
        self.run_and_save_state(project.project_root, with_snapshot=True)

        clone_args = [
            "clone",
            "--state",
            "state",
            "--target",
            "otherschema",
        ]

        results = run_dbt(clone_args)
        assert len(results) == 4

        schema_relations = project.adapter.list_relations(
            database=project.database, schema=TestSparkClonePossible.schemaname
        )
        filtered_schema_relations = [relation for relation in schema_relations if relation.identifier in ["seed","table_model","my_cool_snapshot","view_model"]]
        types = [r.type for r in filtered_schema_relations]
        count_types = Counter(types)
        assert count_types == Counter({"table": 3, "view": 1})

        # objects already exist, so this is a no-op
        results = run_dbt(clone_args)
        assert len(results) == 4
        assert all("no-op" in r.message.lower() for r in results)

        # recreate all objects
        results = run_dbt([*clone_args, "--full-refresh"])
        assert len(results) == 4

        # select only models this time
        results = run_dbt([*clone_args, "--resource-type", "model"])
        assert len(results) == 2
        assert all("no-op" in r.message.lower() for r in results)

    def test_clone_no_state(self, project, unique_schema, other_schema):
        project.create_test_schema(other_schema)
        self.run_and_save_state(project.project_root, with_snapshot=True)

        clone_args = [
            "clone",
            "--target",
            "otherschema",
        ]

        with pytest.raises(
            DbtRuntimeError,
            match="--state or --defer-state are required for deferral, but neither was provided",
        ):
            run_dbt(clone_args)
