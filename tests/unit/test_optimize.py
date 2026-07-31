"""Unit tests for the automatic post-build ``OPTIMIZE``."""

import os
import re
import threading
import unittest
from multiprocessing import get_context
from unittest import mock

from jinja2 import Environment, FileSystemLoader

import dbt.flags as flags
from dbt.adapters.fabricspark import FabricSparkAdapter
from dbt.adapters.fabricspark.connections import FabricSparkConnectionManager
from dbt.adapters.fabricspark.impl import SKIP_OPTIMIZE_ENV_VAR, _as_bool

from .utils import config_from_parts_or_dicts

MACRO_DIR = "src/dbt/include/fabricspark/macros/adapters"

PROJECT_CFG = {
    "name": "X",
    "version": "0.1",
    "profile": "test",
    "project-root": "/tmp/dbt/does-not-exist",
    "quoting": {"identifier": False, "schema": False},
    "config-version": 2,
}


def _adapter(auto_optimize=None):
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
    if auto_optimize is not None:
        profile["auto_optimize"] = auto_optimize

    config = config_from_parts_or_dicts(
        PROJECT_CFG, {"outputs": {"test": profile}, "target": "test"}
    )
    return FabricSparkAdapter(config, get_context("spawn"))


class TestAsBool(unittest.TestCase):
    def test_none_returns_default(self):
        self.assertIsNone(_as_bool(None, default=None))
        self.assertTrue(_as_bool(None, default=True))
        self.assertFalse(_as_bool(None, default=False))

    def test_bools_pass_through(self):
        self.assertTrue(_as_bool(True, default=False))
        self.assertFalse(_as_bool(False, default=True))

    def test_truthy_strings(self):
        for value in ("1", "true", "TRUE", " True ", "t", "yes", "y", "on"):
            self.assertTrue(_as_bool(value, default=False), value)

    def test_falsey_strings(self):
        for value in ("0", "false", "FALSE", " False ", "f", "no", "n", "off"):
            self.assertFalse(_as_bool(value, default=True), value)

    def test_unrecognized_string_falls_back_to_default(self):
        self.assertTrue(_as_bool("maybe", default=True))
        self.assertFalse(_as_bool("maybe", default=False))


class TestShouldAutoOptimize(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = False
        env_patch = mock.patch.dict(os.environ, {}, clear=False)
        env_patch.start()
        self.addCleanup(env_patch.stop)
        os.environ.pop(SKIP_OPTIMIZE_ENV_VAR, None)

    def test_enabled_by_default(self):
        self.assertTrue(_adapter().should_auto_optimize())

    def test_profile_flag_disables(self):
        self.assertFalse(_adapter(auto_optimize=False).should_auto_optimize())

    def test_model_config_overrides_profile(self):
        self.assertTrue(_adapter(auto_optimize=False).should_auto_optimize(auto_optimize=True))
        self.assertFalse(_adapter(auto_optimize=True).should_auto_optimize(auto_optimize=False))

    def test_model_config_accepts_strings(self):
        self.assertFalse(_adapter().should_auto_optimize(auto_optimize="false"))
        self.assertTrue(_adapter(auto_optimize=False).should_auto_optimize(auto_optimize="true"))

    def test_env_var_overrides_everything(self):
        with mock.patch.dict(os.environ, {SKIP_OPTIMIZE_ENV_VAR: "true"}):
            self.assertFalse(_adapter().should_auto_optimize(auto_optimize=True))
            self.assertFalse(_adapter(auto_optimize=True).should_auto_optimize(auto_optimize=True))

    def test_env_var_falsey_value_does_not_disable(self):
        with mock.patch.dict(os.environ, {SKIP_OPTIMIZE_ENV_VAR: "false"}):
            self.assertTrue(_adapter().should_auto_optimize())

    def test_missing_profile_attribute_defaults_to_enabled(self):
        adapter = _adapter()
        del adapter.config.credentials.auto_optimize
        self.assertTrue(adapter.should_auto_optimize())

    def test_unset_file_format_is_treated_as_delta(self):
        self.assertTrue(_adapter().should_auto_optimize(file_format=None, relation_is_delta=None))

    def test_explicit_delta_file_format_optimizes(self):
        self.assertTrue(
            _adapter().should_auto_optimize(file_format="delta", relation_is_delta=False)
        )

    def test_existing_delta_relation_optimizes_despite_file_format(self):
        self.assertTrue(
            _adapter().should_auto_optimize(file_format="parquet", relation_is_delta=True)
        )

    def test_non_delta_relation_is_skipped(self):
        for file_format in ("parquet", "csv", "hudi"):
            self.assertFalse(
                _adapter().should_auto_optimize(file_format=file_format, relation_is_delta=False),
                file_format,
            )

    def test_non_delta_is_skipped_even_when_explicitly_enabled(self):
        self.assertFalse(
            _adapter().should_auto_optimize(
                auto_optimize=True, file_format="csv", relation_is_delta=None
            )
        )


class TestRunOptimize(unittest.TestCase):
    def test_success_returns_true(self):
        adapter = _adapter()
        with mock.patch.object(adapter, "execute") as execute:
            self.assertTrue(adapter.run_optimize("optimize `db`.`sch`.`tbl`"))
        execute.assert_called_once_with("optimize `db`.`sch`.`tbl`", auto_begin=False, fetch=False)

    def test_failure_is_swallowed_and_warns(self):
        adapter = _adapter()
        with mock.patch.object(adapter, "execute", side_effect=RuntimeError("conflict")):
            with mock.patch("dbt.adapters.fabricspark.impl.logger") as logger:
                self.assertFalse(adapter.run_optimize("optimize tbl"))
        self.assertIn("OPTIMIZE failed", logger.warning.call_args[0][0])

    def test_executes_with_retries_disabled(self):
        adapter = _adapter()
        observed = []

        with mock.patch.object(
            adapter,
            "execute",
            side_effect=lambda *a, **kw: observed.append(
                FabricSparkConnectionManager.retries_disabled()
            ),
        ):
            adapter.run_optimize("optimize tbl")

        self.assertEqual(observed, [True])
        self.assertFalse(FabricSparkConnectionManager.retries_disabled())

    def test_retry_scope_is_restored_after_failure(self):
        adapter = _adapter()
        with mock.patch.object(adapter, "execute", side_effect=RuntimeError("boom")):
            with mock.patch("dbt.adapters.fabricspark.impl.logger"):
                adapter.run_optimize("optimize tbl")
        self.assertFalse(FabricSparkConnectionManager.retries_disabled())


class TestNoRetryScope(unittest.TestCase):
    def test_disabled_by_default(self):
        self.assertFalse(FabricSparkConnectionManager.retries_disabled())

    def test_nesting_restores_previous_state(self):
        with FabricSparkConnectionManager.no_retry():
            self.assertTrue(FabricSparkConnectionManager.retries_disabled())
            with FabricSparkConnectionManager.no_retry():
                self.assertTrue(FabricSparkConnectionManager.retries_disabled())
            self.assertTrue(FabricSparkConnectionManager.retries_disabled())
        self.assertFalse(FabricSparkConnectionManager.retries_disabled())

    def test_is_thread_local(self):
        seen = []

        def observe():
            seen.append(FabricSparkConnectionManager.retries_disabled())

        with FabricSparkConnectionManager.no_retry():
            worker = threading.Thread(target=observe)
            worker.start()
            worker.join()

        self.assertEqual(seen, [False])


class TestRetryLoopBypass(unittest.TestCase):
    """The retry loop itself must honour the no-retry scope, not just the flag."""

    def _manager(self, cursor):
        manager = FabricSparkConnectionManager.__new__(FabricSparkConnectionManager)
        connection = mock.Mock()
        connection.transaction_open = True
        connection.name = "test"
        connection.credentials = mock.Mock(connect_retries=20, retry_all=True)
        connection.handle.cursor.return_value = cursor
        manager.get_thread_connection = mock.Mock(return_value=connection)
        return manager

    def test_retries_when_scope_is_inactive(self):
        cursor = mock.Mock()
        cursor.execute.side_effect = [RuntimeError("transient"), None]
        manager = self._manager(cursor)

        with mock.patch("dbt.adapters.fabricspark.connections.time.sleep") as sleep:
            manager.add_query("optimize tbl", auto_begin=False)

        self.assertEqual(cursor.execute.call_count, 2)
        sleep.assert_called_once()

    def test_does_not_retry_inside_scope(self):
        cursor = mock.Mock()
        cursor.execute.side_effect = RuntimeError("transient")
        manager = self._manager(cursor)

        with mock.patch("dbt.adapters.fabricspark.connections.time.sleep") as sleep:
            with FabricSparkConnectionManager.no_retry():
                with self.assertRaises(RuntimeError):
                    manager.add_query("optimize tbl", auto_begin=False)

        self.assertEqual(cursor.execute.call_count, 1)
        sleep.assert_not_called()


class TestOptimizeMacros(unittest.TestCase):
    def setUp(self):
        self.model_config = {}
        self.adapter = mock.Mock()
        self.context = {
            "config": mock.Mock(),
            "adapter": self.adapter,
            "return": lambda value: value,
        }
        self.context["config"].get = lambda key, default=None, **kwargs: self.model_config.get(
            key, default
        )
        env = Environment(loader=FileSystemLoader(MACRO_DIR), extensions=["jinja2.ext.do"])
        self.template = env.get_template("optimize.sql", globals=self.context)

        def dispatch(macro_name, macro_namespace=None, packages=None):
            return getattr(self.template.module, f"fabricspark__{macro_name}")

        self.adapter.dispatch = dispatch

    def _relation(self, rendered="`db`.`sch`.`tbl`", is_delta=None):
        relation = mock.Mock()
        relation.render.return_value = rendered
        relation.is_delta = is_delta
        return relation

    def _run(self, name, *args):
        return getattr(self.template.module, name)(*args)

    def test_optimize_sql_renders_relation(self):
        sql = self._run("fabricspark__get_optimize_sql", self._relation())
        self.assertEqual(re.sub(r"\s+", " ", sql).strip(), "optimize `db`.`sch`.`tbl`")

    def test_optimize_runs_when_adapter_approves(self):
        self.adapter.should_auto_optimize.return_value = True
        self._run("fabricspark__optimize", self._relation())
        self.adapter.run_optimize.assert_called_once()
        self.assertEqual(
            re.sub(r"\s+", " ", self.adapter.run_optimize.call_args[0][0]).strip(),
            "optimize `db`.`sch`.`tbl`",
        )

    def test_optimize_skipped_when_adapter_declines(self):
        self.adapter.should_auto_optimize.return_value = False
        self._run("fabricspark__optimize", self._relation())
        self.adapter.run_optimize.assert_not_called()

    def test_model_config_and_relation_state_are_forwarded(self):
        self.adapter.should_auto_optimize.return_value = False
        self.model_config["auto_optimize"] = False
        self.model_config["file_format"] = "parquet"
        self._run("fabricspark__optimize", self._relation(is_delta=True))
        self.adapter.should_auto_optimize.assert_called_once_with(
            auto_optimize=False, file_format="parquet", relation_is_delta=True
        )
