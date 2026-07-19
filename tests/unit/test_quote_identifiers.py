from __future__ import annotations

import unittest
from unittest import mock

import dbt.flags as flags
from dbt.adapters.fabricspark import FabricSparkCredentials
from dbt.adapters.fabricspark.connections import (
    warn_if_quote_identifiers_without_case_sensitivity,
)
from dbt.adapters.fabricspark.relation import FabricSparkRelation
from dbt.artifacts.resources import FileHash, NodeConfig
from dbt.artifacts.resources.types import NodeType
from dbt.contracts.graph.nodes import ModelNode

from .utils import config_from_parts_or_dicts


def _make_node(uid, *, database, schema, alias):
    return ModelNode(
        database=database,
        schema=schema,
        name=uid,
        resource_type=NodeType.Model,
        package_name="pkg",
        path=f"{uid}.sql",
        original_file_path=f"models/{uid}.sql",
        unique_id=f"model.pkg.{uid}",
        fqn=["pkg", uid],
        alias=alias,
        checksum=FileHash.empty(),
        config=NodeConfig(),
    )


def _make_credentials(*, quote_identifiers=False, conf=None):
    spark_config = {"name": "test-session"}
    if conf is not None:
        spark_config["conf"] = conf
    return FabricSparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="tests",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        spark_config=spark_config,
        quote_identifiers=quote_identifiers,
    )


class TestQuoteIdentifiersRendering(unittest.TestCase):
    """``quote_identifiers`` backtick-quotes the identifier segment on every
    relation so Fabric Spark preserves casing instead of folding to lowercase."""

    def setUp(self):
        flags.STRICT_MODE = False
        FabricSparkRelation._schemas_enabled = True
        FabricSparkRelation._quote_identifiers = False
        self.project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            # Prove the profile flag beats an explicit project quoting.identifier=False.
            "quoting": {"identifier": False},
            "config-version": 2,
        }

    def tearDown(self):
        FabricSparkRelation._schemas_enabled = False
        FabricSparkRelation._quote_identifiers = False

    def _config(self):
        return config_from_parts_or_dicts(
            self.project_cfg,
            {
                "outputs": {
                    "test": {
                        "type": "fabricspark",
                        "method": "livy",
                        "authentication": "CLI",
                        "lakehouse": "bronze",
                        "schema": "dbo",
                        "workspaceid": "1de8390c-9aca-4790-bee8-72049109c0f4",
                        "lakehouseid": "8c5bc260-bc3a-4898-9ada-01e433d461ba",
                        "endpoint": "https://api.fabric.microsoft.com/v1",
                        "connect_retries": 0,
                        "threads": 1,
                        "spark_config": {"name": "test-session"},
                    }
                },
                "target": "test",
            },
        )

    def test_create_default_off_leaves_identifier_unquoted(self):
        rel = FabricSparkRelation.create(database="lh", schema="dbo", identifier="Orders")
        self.assertEqual(str(rel), "`lh`.`dbo`.Orders")

    def test_create_flag_on_quotes_identifier(self):
        FabricSparkRelation._quote_identifiers = True
        rel = FabricSparkRelation.create(database="lh", schema="dbo", identifier="Orders")
        self.assertEqual(str(rel), "`lh`.`dbo`.`Orders`")

    def test_create_from_flag_on_overrides_project_quoting(self):
        """create_from deep-merges the parse-time project quoting dict (identifier
        False) over the default policy; the flag must still force quoting."""
        FabricSparkRelation._quote_identifiers = True
        node = _make_node("m", database="lh", schema="dbo", alias="Orders")
        rel = FabricSparkRelation.create_from(quoting=self._config(), relation_config=node)
        self.assertEqual(str(rel), "`lh`.`dbo`.`Orders`")

    def test_create_from_flag_off_leaves_identifier_unquoted(self):
        node = _make_node("m", database="lh", schema="dbo", alias="Orders")
        rel = FabricSparkRelation.create_from(quoting=self._config(), relation_config=node)
        self.assertEqual(str(rel), "`lh`.`dbo`.Orders")

    def test_flag_on_cross_workspace_quotes_identifier(self):
        FabricSparkRelation._quote_identifiers = True
        rel = FabricSparkRelation.create(
            database="lh", schema="dbo", identifier="Orders", workspace="Prod WS"
        )
        self.assertEqual(str(rel), "`Prod WS`.`lh`.`dbo`.`Orders`")


class TestQuoteIdentifiersCredentials(unittest.TestCase):
    def test_default_is_false(self):
        self.assertFalse(_make_credentials().quote_identifiers)

    def test_quote_identifiers_in_connection_keys(self):
        creds = _make_credentials(quote_identifiers=True)
        self.assertIn("quote_identifiers", creds._connection_keys())


class TestCaseSensitivityWarning(unittest.TestCase):
    """The adapter warns when quoting is on without ``spark.sql.caseSensitive``."""

    def _assert_warned(self, creds, expected):
        with mock.patch("dbt.adapters.fabricspark.connections.logger") as mock_logger:
            warn_if_quote_identifiers_without_case_sensitivity(creds)
        self.assertEqual(mock_logger.warning.called, expected)

    def test_no_warning_when_flag_off(self):
        self._assert_warned(_make_credentials(quote_identifiers=False), expected=False)

    def test_warns_when_case_sensitive_missing(self):
        self._assert_warned(_make_credentials(quote_identifiers=True), expected=True)

    def test_warns_when_case_sensitive_false(self):
        creds = _make_credentials(
            quote_identifiers=True, conf={"spark.sql.caseSensitive": "false"}
        )
        self._assert_warned(creds, expected=True)

    def test_no_warning_when_case_sensitive_true(self):
        creds = _make_credentials(quote_identifiers=True, conf={"spark.sql.caseSensitive": "true"})
        self._assert_warned(creds, expected=False)
