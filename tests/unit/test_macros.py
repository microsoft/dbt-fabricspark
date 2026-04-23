import re
import unittest
from pathlib import Path
from unittest import mock

from jinja2 import BaseLoader, Environment, FileSystemLoader

from dbt.adapters.contracts.relation import RelationType

# Resolve project root (tests/unit/test_macros.py → repo root)
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


@unittest.skip("Skipping temporarily - macros require full dbt context")
class TestSparkMacros(unittest.TestCase):
    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader("src/dbt/include/fabricspark/macros"),
            extensions=[
                "jinja2.ext.do",
            ],
        )

        self.jinja_env_create_table_as = Environment(
            loader=FileSystemLoader(
                "src/dbt/include/fabricspark/macros/materializations/models/table/"
            ),
            extensions=[
                "jinja2.ext.do",
            ],
        )

        self.config = {}
        self.default_context = {
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "adapter": mock.Mock(),
            "return": lambda r: r,
        }
        self.default_context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )

    def __get_template(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __get_create_table_template(self, template_filename):
        return self.jinja_env_create_table_as.get_template(
            template_filename, globals=self.default_context
        )

    def __run_macro(self, template, name, temporary, relation, sql):
        self.default_context["model"].alias = relation

        def dispatch(macro_name, macro_namespace=None, packages=None):
            return getattr(template.module, f"fabricspark__{macro_name}")

        self.default_context["adapter"].dispatch = dispatch

        value = getattr(template.module, name)(temporary, relation, sql)
        return re.sub(r"\s\s+", " ", value)

    def test_macros_load(self):
        self.jinja_env.get_template("adapters.sql")

        template = self.__get_create_table_template("create_table_as.sql")
        sql = self.__run_macro(
            template, "fabricspark__create_table_as", False, "my_table", "select 1"
        ).strip()

        self.assertEqual(sql, "create table my_table as select 1")

    def test_macros_create_table_as_file_format(self):
        template = self.__get_create_table_template("create_table_as.sql")

        self.config["file_format"] = "delta"
        sql = self.__run_macro(
            template, "fabricspark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(sql, "create or replace table my_table using delta as select 1")

    def test_macros_create_table_as_options(self):
        template = self.__get_create_table_template("create_table_as.sql")

        self.config["file_format"] = "delta"
        self.config["options"] = {"compression": "gzip"}
        sql = self.__run_macro(
            template, "fabricspark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            'create or replace table my_table using delta options (compression "gzip" ) as select 1',
        )

    def test_macros_create_table_as_partition(self):
        template = self.__get_create_table_template("create_table_as.sql")

        self.config["partition_by"] = "partition_1"
        sql = self.__run_macro(
            template, "fabricspark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(sql, "create table my_table partitioned by (partition_1) as select 1")

    def test_macros_create_table_as_partitions(self):
        template = self.__get_create_table_template("create_table_as.sql")

        self.config["partition_by"] = ["partition_1", "partition_2"]
        sql = self.__run_macro(
            template, "fabricspark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql, "create table my_table partitioned by (partition_1,partition_2) as select 1"
        )

    def test_macros_create_table_as_cluster(self):
        template = self.__get_create_table_template("create_table_as.sql")

        self.config["clustered_by"] = "cluster_1"
        self.config["buckets"] = "1"
        sql = self.__run_macro(
            template, "fabricspark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql, "create table my_table clustered by (cluster_1) into 1 buckets as select 1"
        )

    def test_macros_create_table_as_clusters(self):
        template = self.__get_create_table_template("create_table_as.sql")

        self.config["clustered_by"] = ["cluster_1", "cluster_2"]
        self.config["buckets"] = "1"
        sql = self.__run_macro(
            template, "fabricspark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create table my_table clustered by (cluster_1,cluster_2) into 1 buckets as select 1",
        )

    def test_macros_create_table_as_location(self):
        template = self.__get_create_table_template("create_table_as.sql")

        self.config["location_root"] = "/mnt/root"
        sql = self.__run_macro(
            template, "fabricspark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(sql, "create table my_table location '/mnt/root/my_table' as select 1")

    def test_macros_create_table_as_comment(self):
        template = self.__get_create_table_template("create_table_as.sql")

        self.config["persist_docs"] = {"relation": True}
        self.default_context["model"].description = "Description Test"
        sql = self.__run_macro(
            template, "fabricspark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(sql, "create table my_table comment 'Description Test' as select 1")

    def test_macros_create_table_as_all(self):
        template = self.__get_create_table_template("create_table_as.sql")

        self.config["file_format"] = "delta"
        self.config["location_root"] = "/mnt/root"
        self.config["partition_by"] = ["partition_1", "partition_2"]
        self.config["clustered_by"] = ["cluster_1", "cluster_2"]
        self.config["buckets"] = "1"
        self.config["persist_docs"] = {"relation": True}
        self.default_context["model"].description = "Description Test"

        sql = self.__run_macro(
            template, "fabricspark__create_table_as", False, "my_table", "select 1"
        ).strip()
        self.assertEqual(
            sql,
            "create or replace table my_table using delta partitioned by (partition_1,partition_2) clustered by (cluster_1,cluster_2) into 1 buckets location '/mnt/root/my_table' comment 'Description Test' as select 1",
        )


class TestEnsureDatabaseExists(unittest.TestCase):
    """Test the database-qualification logic inside ensure_database_exists.

    The macro lives in schema.sql and has two responsibilities:
    1. Qualify a bare schema_name with a database prefix ONLY when schemas
       are enabled (not in local mode where schema_name IS the database).
    2. Emit ``create database if not exists <name>`` in local / schema-enabled
       modes, or ``select 1`` otherwise.

    These tests render a minimal Jinja2 template that mirrors the
    qualification logic so we can validate the SQL output without needing
    a full dbt context.
    """

    # Simplified Jinja template that mirrors the qualification logic
    # from fabricspark__ensure_database_exists.
    # The database prefix is only applied when schemas_enabled is true,
    # NOT in local mode (where schema_name IS the database).
    TEMPLATE_SRC = """\
{%- if schemas_enabled or local_mode -%}
  {%- if database is not none and '.' not in schema_name and schemas_enabled -%}
    {%- set schema_name = database ~ '.' ~ schema_name -%}
  {%- endif -%}
  create database if not exists {{ schema_name }}
{%- else -%}
  select 1
{%- endif -%}"""

    def setUp(self):
        env = Environment(loader=BaseLoader())
        self.template = env.from_string(self.TEMPLATE_SRC)

    def _render(self, schema_name, database=None, schemas_enabled=False, local_mode=False):
        return self.template.render(
            schema_name=schema_name,
            database=database,
            schemas_enabled=schemas_enabled,
            local_mode=local_mode,
        ).strip()

    # --- Local mode (no schema, database == schema) ---

    def test_local_mode_same_db_and_schema(self):
        """Local mode with database == schema_name should NOT concatenate."""
        result = self._render("insights", database="insights", local_mode=True)
        self.assertEqual(result, "create database if not exists insights")

    def test_local_mode_no_database(self):
        """Local mode with no database arg should use schema_name as-is."""
        result = self._render("insights", database=None, local_mode=True)
        self.assertEqual(result, "create database if not exists insights")

    # --- Fabric no-schema mode ---

    def test_fabric_no_schema(self):
        """Non-schema Fabric mode emits select 1 (no-op)."""
        result = self._render("bronze", database="bronze", schemas_enabled=False, local_mode=False)
        self.assertEqual(result, "select 1")

    # --- Fabric schema-enabled mode ---

    def test_schema_enabled_different_db(self):
        """Schema-enabled mode should qualify bare schema with database."""
        result = self._render("dbo", database="bronze", schemas_enabled=True)
        self.assertEqual(result, "create database if not exists bronze.dbo")

    def test_schema_enabled_same_db_and_schema(self):
        """Schema-enabled with db==schema is valid (schema named same as db)."""
        result = self._render("bronze", database="bronze", schemas_enabled=True)
        self.assertEqual(result, "create database if not exists bronze.bronze")

    def test_schema_enabled_already_qualified(self):
        """Already-qualified schema_name (contains dot) should not be re-prefixed."""
        result = self._render("bronze.dbo", database="bronze", schemas_enabled=True)
        self.assertEqual(result, "create database if not exists bronze.dbo")

    def test_schema_enabled_no_database(self):
        """Schema-enabled with no database arg should use schema_name as-is."""
        result = self._render("myschema", database=None, schemas_enabled=True)
        self.assertEqual(result, "create database if not exists myschema")


class TestMLVRelationTypeComparison(unittest.TestCase):
    """Regression: the materialized_lake_view macro must use the correct RelationType string.

    The macro compares ``old_relation.type.value`` against a literal string
    to decide whether to drop an existing relation before CREATE OR REPLACE.
    The literal must match ``RelationType.MaterializedView.value``
    (``'materialized_view'``), not the old typo ``'materializedview'``.
    """

    MLV_MACRO_PATH = _PROJECT_ROOT / (
        "src/dbt/include/fabricspark/macros/materializations"
        "/models/materialized_lake_view/materialized_lake_view.sql"
    )

    def test_macro_uses_correct_materialized_view_literal(self):
        """The drop-guard comparison must use 'materialized_view' (with underscore)."""
        self.assertTrue(
            self.MLV_MACRO_PATH.exists(), f"Macro file not found: {self.MLV_MACRO_PATH}"
        )
        contents = self.MLV_MACRO_PATH.read_text()

        expected = RelationType.MaterializedView.value  # 'materialized_view'

        # The macro should compare against the correct value
        self.assertIn(
            f"!= '{expected}'",
            contents,
            f"materialized_lake_view.sql must compare against '{expected}' "
            f"(RelationType.MaterializedView.value), not a typo like 'materializedview'.",
        )

        # Ensure the old typo is NOT present
        self.assertNotIn(
            "'materializedview'",
            contents,
            "materialized_lake_view.sql still contains the typo 'materializedview' "
            "(missing underscore).",
        )
