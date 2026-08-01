"""Unit tests for the Delta MERGE schema-evolution session conf lifecycle."""

import unittest
from multiprocessing import get_context
from unittest import mock

from dbt.adapters.fabricspark import FabricSparkAdapter
from dbt.adapters.fabricspark.impl import SCHEMA_EVOLUTION_CONF

from .utils import config_from_parts_or_dicts

PROJECT_CFG = {
    "name": "X",
    "version": "0.1",
    "profile": "test",
    "project-root": "/tmp/dbt/does-not-exist",
    "quoting": {"identifier": False, "schema": False},
    "config-version": 2,
}


def _adapter():
    profile = {
        "type": "fabricspark",
        "method": "livy",
        "authentication": "CLI",
        "lakehouse": "dbtsparktest",
        "workspaceid": "1de8390c-9aca-4790-bee8-72049109c0f4",
        "lakehouseid": "8c5bc260-bc3a-4898-9ada-01e433d461ba",
        "connect_retries": 0,
        "connect_timeout": 10,
        "threads": 1,
        "spark_config": {"name": "test-session"},
    }
    config = config_from_parts_or_dicts(
        PROJECT_CFG, {"outputs": {"test": profile}, "target": "test"}
    )
    return FabricSparkAdapter(config, get_context("spawn"))


def _result(value):
    table = mock.Mock()
    table.rows = [(SCHEMA_EVOLUTION_CONF, value)] if value is not None else []
    return (mock.Mock(), table)


class TestSetSchemaEvolution(unittest.TestCase):
    def test_no_statement_when_already_matching(self):
        adapter = _adapter()
        with mock.patch.object(adapter, "execute", return_value=_result("false")) as execute:
            self.assertEqual(adapter.set_schema_evolution(False), "")
        self.assertEqual(execute.call_count, 1)

    def test_enables_and_reports_previous(self):
        adapter = _adapter()
        with mock.patch.object(adapter, "execute", return_value=_result("false")) as execute:
            self.assertEqual(adapter.set_schema_evolution(True), "false")
        self.assertEqual(execute.call_args_list[1][0][0], f"set {SCHEMA_EVOLUTION_CONF} = true")

    def test_disables_and_reports_previous(self):
        adapter = _adapter()
        with mock.patch.object(adapter, "execute", return_value=_result("true")) as execute:
            self.assertEqual(adapter.set_schema_evolution(False), "true")
        self.assertEqual(execute.call_args_list[1][0][0], f"set {SCHEMA_EVOLUTION_CONF} = false")

    def test_unset_conf_is_treated_as_false(self):
        adapter = _adapter()
        with mock.patch.object(adapter, "execute", return_value=_result("<undefined>")):
            self.assertEqual(adapter.set_schema_evolution(True), "false")

    def test_empty_result_is_treated_as_false(self):
        adapter = _adapter()
        with mock.patch.object(adapter, "execute", return_value=_result(None)):
            self.assertEqual(adapter.set_schema_evolution(True), "false")

    def test_unreadable_conf_is_treated_as_false(self):
        adapter = _adapter()
        with mock.patch.object(
            adapter, "execute", side_effect=[RuntimeError("no session"), _result(None)]
        ):
            self.assertEqual(adapter.set_schema_evolution(True), "false")


class TestRestoreSchemaEvolution(unittest.TestCase):
    def test_noop_when_nothing_was_changed(self):
        adapter = _adapter()
        with mock.patch.object(adapter, "execute") as execute:
            adapter.restore_schema_evolution("")
        execute.assert_not_called()

    def test_restores_previous_value(self):
        adapter = _adapter()
        with mock.patch.object(adapter, "execute") as execute:
            adapter.restore_schema_evolution("true")
        execute.assert_called_once_with(
            f"set {SCHEMA_EVOLUTION_CONF} = true", auto_begin=False, fetch=False
        )

    def test_failure_is_swallowed_and_warns(self):
        adapter = _adapter()
        with mock.patch.object(adapter, "execute", side_effect=RuntimeError("gone")):
            with mock.patch("dbt.adapters.fabricspark.impl.logger") as logger:
                adapter.restore_schema_evolution("false")
        self.assertIn("Could not restore", logger.warning.call_args[0][0])


class TestRoundTrip(unittest.TestCase):
    """A merge must leave the session exactly as it found it."""

    def test_enable_then_restore_returns_to_original(self):
        adapter = _adapter()
        state = {"value": "false"}

        def execute(sql, **kwargs):
            if sql == f"set {SCHEMA_EVOLUTION_CONF}":
                return _result(state["value"])
            state["value"] = sql.rsplit("=", 1)[1].strip()
            return (mock.Mock(), mock.Mock(rows=[]))

        with mock.patch.object(adapter, "execute", side_effect=execute):
            previous = adapter.set_schema_evolution(True)
            self.assertEqual(state["value"], "true")
            adapter.restore_schema_evolution(previous)

        self.assertEqual(state["value"], "false")

    def test_user_enabled_conf_survives_a_snapshot(self):
        adapter = _adapter()
        state = {"value": "true"}

        def execute(sql, **kwargs):
            if sql == f"set {SCHEMA_EVOLUTION_CONF}":
                return _result(state["value"])
            state["value"] = sql.rsplit("=", 1)[1].strip()
            return (mock.Mock(), mock.Mock(rows=[]))

        with mock.patch.object(adapter, "execute", side_effect=execute):
            previous = adapter.set_schema_evolution(False)
            self.assertEqual(state["value"], "false")
            adapter.restore_schema_evolution(previous)

        self.assertEqual(state["value"], "true")
