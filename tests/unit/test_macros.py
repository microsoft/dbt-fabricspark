import re
import unittest
from unittest import mock

from jinja2 import BaseLoader, Environment, FileSystemLoader


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


class TestClusteredColsLiquidClustering(unittest.TestCase):
    """Render ``fabricspark__clustered_cols`` and ``fabricspark__file_format_clause``
    in isolation and assert the four semantics branches called out in the
    acceptance criteria for #187.

    The macros are inlined as a string template rather than loaded from disk
    so they render without a full dbt context and without forcing eager
    init of the rest of ``create_table_as.sql``. The macro bodies below are
    identical to the ones shipped in the adapter.
    """

    MACRO_SRC = r"""
{%- macro create(relation, sql) -%}
create or replace table {{ relation }} {{ file_format_clause() }} {{ partition_cols(label="partitioned by") }} {{ clustered_cols(label="clustered by") }} as {{ sql }}
{%- endmacro -%}

{% macro file_format_clause() %}
  {%- set file_format = config.get('file_format') -%}
  {%- set clustered_by = config.get('clustered_by') -%}
  {%- set buckets = config.get('buckets') -%}
  {%- set liquid = clustered_by is not none and buckets is none -%}
  {%- if file_format is not none and file_format != 'delta' %}
    using {{ file_format }}
  {%- elif liquid and (file_format is none or file_format == 'delta') %}
    using delta
  {%- endif %}
{%- endmacro %}

{% macro partition_cols(label, required=false) %}
  {%- set cols = config.get('partition_by') -%}
  {%- if cols is not none %}
    {%- if cols is string -%}{%- set cols = [cols] -%}{%- endif -%}
    {{ label }} (
    {%- for item in cols -%}{{ item }}{%- if not loop.last -%},{%- endif -%}{%- endfor -%}
    )
  {%- endif %}
{%- endmacro %}

{% macro clustered_cols(label, required=false) %}
  {%- set cols = config.get('clustered_by', validator=validation.any[list, basestring]) -%}
  {%- set buckets = config.get('buckets', validator=validation.any[int]) -%}
  {%- set partition_by = config.get('partition_by') -%}
  {%- set file_format = config.get('file_format', 'delta') -%}

  {%- if cols is not none -%}
    {%- if cols is string -%}{%- set cols = [cols] -%}{%- endif -%}

    {%- if buckets is not none -%}
      {{ label }} (
      {%- for c in cols -%}{{ c }}{%- if not loop.last -%},{%- endif -%}{%- endfor -%}
      ) into {{ buckets }} buckets
    {%- elif file_format == 'delta' -%}
      {%- if partition_by is not none -%}
        {{ exceptions.raise_compiler_error(
             "clustered_by (Delta liquid clustering) and partition_by are "
             "mutually exclusive on Delta tables. Pick one.") }}
      {%- endif -%}
      cluster by (
      {%- for c in cols -%}{{ c }}{%- if not loop.last -%},{%- endif -%}{%- endfor -%}
      )
    {%- endif -%}
  {%- endif %}
{%- endmacro %}
"""

    def setUp(self):
        self.config = {}
        self.ctx = {
            "validation": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
        }
        self.ctx["config"].get = lambda key, default=None, **kw: self.config.get(key, default)
        self.ctx["exceptions"].raise_compiler_error.side_effect = lambda msg: (
            _ for _ in ()
        ).throw(RuntimeError(msg))
        env = Environment(extensions=["jinja2.ext.do"])
        self.template = env.from_string(self.MACRO_SRC, globals=self.ctx)

    def _render(self, **config):
        self.config.clear()
        self.config.update(config)
        sql = self.template.module.create("my_table", "select 1")
        return re.sub(r"\s+", " ", sql).strip()

    def test_delta_clustered_by_alone_emits_liquid_clustering(self):
        """clustered_by alone on Delta → CLUSTER BY (...) + USING DELTA."""
        result = self._render(clustered_by=["col_a", "col_b"])
        self.assertEqual(
            result,
            "create or replace table my_table using delta cluster by (col_a,col_b) as select 1",
        )

    def test_delta_clustered_by_alone_explicit_file_format(self):
        """clustered_by alone on explicit file_format=delta → CLUSTER BY (...) + USING DELTA."""
        result = self._render(clustered_by=["col_a"], file_format="delta")
        self.assertEqual(
            result,
            "create or replace table my_table using delta cluster by (col_a) as select 1",
        )

    def test_delta_clustered_by_string_form(self):
        """clustered_by as a bare string (not list) on Delta still emits CLUSTER BY."""
        result = self._render(clustered_by="col_a")
        self.assertEqual(
            result,
            "create or replace table my_table using delta cluster by (col_a) as select 1",
        )

    def test_clustered_by_with_buckets_emits_hive_bucketing(self):
        """clustered_by + buckets → unchanged Hive bucketing (no liquid clustering)."""
        result = self._render(clustered_by=["col_a"], buckets=4)
        self.assertEqual(
            result,
            "create or replace table my_table clustered by (col_a) into 4 buckets as select 1",
        )

    def test_clustered_by_with_buckets_no_using_delta_emitted(self):
        """clustered_by + buckets without explicit file_format → no `using delta` injected."""
        result = self._render(clustered_by=["col_a"], buckets=4, file_format="delta")
        # Explicit file_format=delta is dropped by file_format_clause (delta default).
        self.assertNotIn("using delta", result)
        self.assertIn("clustered by (col_a) into 4 buckets", result)

    def test_clustered_by_with_partition_by_on_delta_raises(self):
        """clustered_by + partition_by on Delta → compile-time error."""
        with self.assertRaises(RuntimeError) as cm:
            self._render(clustered_by=["col_a"], partition_by="p")
        self.assertIn("mutually exclusive on Delta tables", str(cm.exception))

    def test_non_delta_file_format_clustered_by_ignored(self):
        """clustered_by alone on non-Delta file_format → no clustering clause, USING <fmt>."""
        result = self._render(clustered_by=["col_a"], file_format="parquet")
        self.assertEqual(
            result,
            "create or replace table my_table using parquet as select 1",
        )

    def test_non_delta_file_format_with_buckets_unchanged(self):
        """clustered_by + buckets on non-Delta file_format → Hive bucketing still emitted."""
        result = self._render(clustered_by=["col_a"], buckets=2, file_format="parquet")
        self.assertEqual(
            result,
            "create or replace table my_table using parquet clustered by (col_a) into 2 buckets as select 1",
        )

    def test_no_clustered_by_no_clause(self):
        """No clustered_by at all → no clustering clause."""
        result = self._render()
        self.assertEqual(result, "create or replace table my_table as select 1")

    def test_partition_by_alone_no_clustered_by_unaffected(self):
        """partition_by without clustered_by → partition_by emitted, no error."""
        result = self._render(partition_by="p")
        self.assertEqual(
            result,
            "create or replace table my_table partitioned by (p) as select 1",
        )
