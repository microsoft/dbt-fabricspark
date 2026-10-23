import unittest
from unittest import mock

from jinja2 import Environment, FileSystemLoader


class CompilerError(Exception):
    pass


class FakeRelation:
    type = "table"

    def __init__(self, is_delta=True):
        self.is_delta = is_delta

    def __str__(self):
        return "lh.dbo.orders"


class Column:
    def __init__(self, name, data_type="string"):
        self.name = name
        self.data_type = data_type


class TestAlterRelationAddRemoveColumns(unittest.TestCase):
    def setUp(self):
        self.queries = []
        self.tblproperties = {}
        self.config = {}

        def run_query(sql):
            sql = " ".join(str(sql).split())
            self.queries.append(sql)
            if sql.startswith("show tblproperties"):
                return list(self.tblproperties.items())
            return []

        def raise_compiler_error(message):
            raise CompilerError(message)

        adapter = mock.Mock()
        adapter.quote = lambda name: f"`{name}`"
        config = mock.Mock()
        config.get = lambda key, default=None: self.config.get(key, default)

        env = Environment(
            loader=FileSystemLoader("src/dbt/include/fabricspark/macros/adapters"),
            extensions=["jinja2.ext.do"],
        )
        self.template = env.get_template(
            "columns.sql",
            globals={
                "adapter": adapter,
                "config": config,
                "run_query": run_query,
                "exceptions": mock.Mock(raise_compiler_error=raise_compiler_error),
                "return": lambda value: value,
            },
        )

    def _alter(self, add_columns=None, remove_columns=None, relation=None):
        self.template.module.fabricspark__alter_relation_add_remove_columns(
            relation or FakeRelation(), add_columns, remove_columns
        )
        return [q for q in self.queries if not q.startswith("show tblproperties")]

    def test_add_only_emits_add_columns(self):
        self.assertEqual(
            self._alter(add_columns=[Column("a"), Column("b", "int")]),
            ["alter table lh.dbo.orders add columns a string, b int"],
        )

    def test_nothing_to_do_emits_no_statement(self):
        self.assertEqual(self._alter(add_columns=[], remove_columns=[]), [])
        self.assertEqual(self._alter(add_columns=None, remove_columns=None), [])

    def test_remove_without_column_mapping_or_opt_in_raises(self):
        with self.assertRaises(CompilerError) as ctx:
            self._alter(remove_columns=[Column("field2")])
        self.assertIn("enable_column_mapping_on_drop", str(ctx.exception))
        self.assertIn("lh.dbo.orders", str(ctx.exception))
        self.assertEqual(self.queries, ["show tblproperties lh.dbo.orders"])

    def test_remove_with_opt_in_enables_column_mapping_then_drops(self):
        self.config["enable_column_mapping_on_drop"] = True
        self.tblproperties = {"delta.minReaderVersion": "1", "delta.minWriterVersion": "2"}
        self.assertEqual(
            self._alter(add_columns=[Column("field5")], remove_columns=[Column("field2")]),
            [
                "alter table lh.dbo.orders set tblproperties ('delta.columnMapping.mode' = 'name', "
                "'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')",
                "alter table lh.dbo.orders add columns field5 string",
                "alter table lh.dbo.orders drop columns ( `field2` )",
            ],
        )

    def test_opt_in_never_downgrades_the_protocol(self):
        self.config["enable_column_mapping_on_drop"] = True
        self.tblproperties = {"delta.minReaderVersion": "3", "delta.minWriterVersion": "7"}
        self.assertEqual(
            self._alter(remove_columns=[Column("field2"), Column("field3")]),
            [
                "alter table lh.dbo.orders set tblproperties ('delta.columnMapping.mode' = 'name')",
                "alter table lh.dbo.orders drop columns ( `field2`, `field3` )",
            ],
        )

    def test_existing_column_mapping_drops_without_opt_in(self):
        for mode in ("name", "id", "NAME"):
            with self.subTest(mode=mode):
                self.queries.clear()
                self.tblproperties = {"delta.columnMapping.mode": mode}
                self.assertEqual(
                    self._alter(remove_columns=[Column("field2")]),
                    ["alter table lh.dbo.orders drop columns ( `field2` )"],
                )

    def test_column_mapping_mode_none_still_requires_opt_in(self):
        self.tblproperties = {"delta.columnMapping.mode": "none"}
        with self.assertRaises(CompilerError):
            self._alter(remove_columns=[Column("field2")])

    def test_remove_on_non_delta_relation_raises(self):
        with self.assertRaises(CompilerError) as ctx:
            self._alter(remove_columns=[Column("field2")], relation=FakeRelation(is_delta=False))
        self.assertIn("only supported on Delta tables", str(ctx.exception))
        self.assertEqual(self.queries, [])


if __name__ == "__main__":
    unittest.main()
