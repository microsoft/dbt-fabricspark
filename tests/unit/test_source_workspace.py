from __future__ import annotations

import unittest

import dbt.flags as flags
from dbt.adapters.fabricspark.relation import FabricSparkRelation
from dbt.artifacts.resources.types import NodeType
from dbt.artifacts.resources.v1.source_definition import SourceConfig
from dbt.contracts.graph.nodes import SourceDefinition

from .utils import config_from_parts_or_dicts


def _make_source(*, database, schema, identifier, workspace_name=None, use_meta=False):
    """Build a ``SourceDefinition`` mirroring a ``sources.yml`` entry.

    ``workspace_name`` is applied under the source ``config`` block, matching
    the native cross-workspace pattern:

        sources:
          - name: my_source
            database: remote_lh
            schema: my_schema
            config:
              workspace_name: RemoteWorkspaceName
            tables:
              - name: my_table
    """
    config = SourceConfig()
    if workspace_name is not None:
        if use_meta:
            config.meta = {"workspace_name": workspace_name}
        else:
            config._extra = {"workspace_name": workspace_name}
    return SourceDefinition(
        name=identifier,
        resource_type=NodeType.Source,
        package_name="pkg",
        path="models/sources.yml",
        original_file_path="models/sources.yml",
        unique_id=f"source.pkg.my_source.{identifier}",
        fqn=["pkg", "my_source", identifier],
        database=database,
        schema=schema,
        source_name="my_source",
        source_description="",
        loader="",
        identifier=identifier,
        config=config,
    )


class TestSourceWorkspaceResolution(unittest.TestCase):
    """A source declaring ``config.workspace_name`` in ``sources.yml`` resolves
    ``{{ source(...) }}`` (and source tests / freshness) to a 4-part
    cross-workspace relation, using the same ``create_from`` path as models."""

    def setUp(self):
        flags.STRICT_MODE = False
        FabricSparkRelation._schemas_enabled = True
        self.project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "config-version": 2,
        }

    def tearDown(self):
        FabricSparkRelation._schemas_enabled = False

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

    def test_source_config_workspace_name_renders_four_part(self):
        source = _make_source(
            database="remote_lh",
            schema="my_schema",
            identifier="my_table",
            workspace_name="RemoteWorkspaceName",
        )
        rel = FabricSparkRelation.create_from(quoting=self._config(), relation_config=source)
        self.assertEqual(rel.workspace, "RemoteWorkspaceName")
        self.assertEqual(str(rel), "`RemoteWorkspaceName`.`remote_lh`.`my_schema`.my_table")

    def test_source_config_meta_workspace_name_renders_four_part(self):
        source = _make_source(
            database="remote_lh",
            schema="my_schema",
            identifier="my_table",
            workspace_name="RemoteWorkspaceName",
            use_meta=True,
        )
        rel = FabricSparkRelation.create_from(quoting=self._config(), relation_config=source)
        self.assertEqual(str(rel), "`RemoteWorkspaceName`.`remote_lh`.`my_schema`.my_table")

    def test_source_without_workspace_name_renders_three_part(self):
        source = _make_source(database="remote_lh", schema="my_schema", identifier="my_table")
        rel = FabricSparkRelation.create_from(quoting=self._config(), relation_config=source)
        self.assertIsNone(rel.workspace)
        self.assertEqual(str(rel), "`remote_lh`.`my_schema`.my_table")
