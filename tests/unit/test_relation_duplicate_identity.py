from __future__ import annotations

import unittest

import dbt.flags as flags
from dbt.adapters.fabricspark.relation import FabricSparkRelation
from dbt.artifacts.resources import FileHash, NodeConfig
from dbt.artifacts.resources.types import NodeType
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode
from dbt.exceptions import AmbiguousAliasError
from dbt.parser.manifest import _check_resource_uniqueness

from .utils import config_from_parts_or_dicts


def _make_node(uid, *, database, schema, alias, workspace_name=None):
    config = NodeConfig()
    if workspace_name:
        config._extra = {"workspace_name": workspace_name}
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
        config=config,
    )


class TestDuplicateRelationIdentity(unittest.TestCase):
    """Two models with the same ``schema.alias`` but different Fabric targets
    must resolve to distinct relation identities (issue #221)."""

    def setUp(self):
        flags.STRICT_MODE = False
        FabricSparkRelation._schemas_enabled = False
        self.project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        }

    def tearDown(self):
        FabricSparkRelation._schemas_enabled = False

    def _config(self, *, schema="dbo", lakehouse="bronze"):
        return config_from_parts_or_dicts(
            self.project_cfg,
            {
                "outputs": {
                    "test": {
                        "type": "fabricspark",
                        "method": "livy",
                        "authentication": "CLI",
                        "lakehouse": lakehouse,
                        "schema": schema,
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

    def test_cross_workspace_relations_have_distinct_identity(self):
        """The exact issue #221 case: same ``dbo.d_company`` alias, different
        workspace + lakehouse → distinct 4-part identities."""
        config = self._config(schema="dbo", lakehouse="bronze")
        common = _make_node(
            "m_common",
            database="lh_common",
            schema="dbo",
            alias="d_company",
            workspace_name="wks_common",
        )
        dwh = _make_node(
            "m_dwh",
            database="lh_dwh",
            schema="dbo",
            alias="d_company",
            workspace_name="wks_dwh",
        )
        rel_common = FabricSparkRelation.create_from(quoting=config, relation_config=common)
        rel_dwh = FabricSparkRelation.create_from(quoting=config, relation_config=dwh)

        self.assertEqual(str(rel_common), "`wks_common`.`lh_common`.`dbo`.d_company")
        self.assertEqual(str(rel_dwh), "`wks_dwh`.`lh_dwh`.`dbo`.d_company")
        self.assertNotEqual(str(rel_common), str(rel_dwh))

    def test_cross_lakehouse_relations_have_distinct_identity(self):
        """Cross-lakehouse without ``workspace_name``: the schema-enabled
        profile fallback (``schema != lakehouse``) still includes the database
        segment so the two relations stay distinct."""
        config = self._config(schema="dbo", lakehouse="bronze")
        common = _make_node("m_common", database="lh_common", schema="dbo", alias="d_company")
        dwh = _make_node("m_dwh", database="lh_dwh", schema="dbo", alias="d_company")
        rel_common = FabricSparkRelation.create_from(quoting=config, relation_config=common)
        rel_dwh = FabricSparkRelation.create_from(quoting=config, relation_config=dwh)

        self.assertEqual(str(rel_common), "`lh_common`.`dbo`.d_company")
        self.assertEqual(str(rel_dwh), "`lh_dwh`.`dbo`.d_company")
        self.assertNotEqual(str(rel_common), str(rel_dwh))

    def test_non_schema_duplicate_relations_still_collide(self):
        """Non-schema lakehouse (``schema == lakehouse``): two models with the
        same alias resolve to the *same* two-part identity, so a genuine
        collision is still detected."""
        config = self._config(schema="bronze", lakehouse="bronze")
        a = _make_node("m_a", database="bronze", schema="bronze", alias="d_company")
        b = _make_node("m_b", database="bronze", schema="bronze", alias="d_company")
        rel_a = FabricSparkRelation.create_from(quoting=config, relation_config=a)
        rel_b = FabricSparkRelation.create_from(quoting=config, relation_config=b)

        self.assertFalse(rel_a.include_policy.database)
        self.assertEqual(str(rel_a), "`bronze`.d_company")
        self.assertEqual(str(rel_a), str(rel_b))

    def test_check_uniqueness_allows_cross_workspace_dupes(self):
        config = self._config(schema="dbo", lakehouse="bronze")
        n1 = _make_node(
            "m_common",
            database="lh_common",
            schema="dbo",
            alias="d_company",
            workspace_name="wks_common",
        )
        n2 = _make_node(
            "m_dwh",
            database="lh_dwh",
            schema="dbo",
            alias="d_company",
            workspace_name="wks_dwh",
        )
        manifest = Manifest(nodes={n1.unique_id: n1, n2.unique_id: n2})
        _check_resource_uniqueness(manifest, config)

    def test_check_uniqueness_allows_cross_lakehouse_dupes(self):
        config = self._config(schema="dbo", lakehouse="bronze")
        n1 = _make_node("m_common", database="lh_common", schema="dbo", alias="d_company")
        n2 = _make_node("m_dwh", database="lh_dwh", schema="dbo", alias="d_company")
        manifest = Manifest(nodes={n1.unique_id: n1, n2.unique_id: n2})
        _check_resource_uniqueness(manifest, config)

    def test_check_uniqueness_still_flags_genuine_dupes(self):
        """Same lakehouse + schema + alias on a non-schema lakehouse is a real
        duplicate and must still raise."""
        config = self._config(schema="bronze", lakehouse="bronze")
        n1 = _make_node("m_a", database="bronze", schema="bronze", alias="d_company")
        n2 = _make_node("m_b", database="bronze", schema="bronze", alias="d_company")
        manifest = Manifest(nodes={n1.unique_id: n1, n2.unique_id: n2})
        with self.assertRaises(AmbiguousAliasError):
            _check_resource_uniqueness(manifest, config)
