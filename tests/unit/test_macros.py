import os
import re
import unittest
from unittest import mock

from jinja2 import Environment, FileSystemLoader

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_MACROS_DIR = os.path.join(_PROJECT_ROOT, "src", "dbt", "include", "fabricspark", "macros")
_TABLE_MACROS_DIR = os.path.join(_MACROS_DIR, "materializations", "models", "table")


class TestSparkMacros(unittest.TestCase):
    def setUp(self):
        self.config = {}

        self.default_context = {
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "adapter": mock.Mock(),
            "return": lambda r: r,
            # Globals required by macros in create_table_as.sql that aren't
            # under test but must be parseable by Jinja
            "statement": mock.Mock(return_value=mock.MagicMock(__enter__=mock.Mock(), __exit__=mock.Mock())),
            "is_incremental": lambda: False,
            "local_md5": lambda *args, **kwargs: "mock_hash",
            "alter_column_set_constraints": mock.Mock(),
            "alter_table_add_constraints": mock.Mock(),
            "get_assert_columns_equivalent": mock.Mock(return_value=""),
            "get_select_subquery": mock.Mock(return_value="select 1"),
            "create_temporary_view": mock.Mock(return_value=""),
        }
        self.default_context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )

        self.jinja_env = Environment(
            loader=FileSystemLoader(_MACROS_DIR),
            extensions=["jinja2.ext.do"],
        )

        self.jinja_env_create_table_as = Environment(
            loader=FileSystemLoader(_TABLE_MACROS_DIR),
            extensions=["jinja2.ext.do"],
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
