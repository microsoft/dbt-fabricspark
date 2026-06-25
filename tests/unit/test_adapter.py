import unittest
from multiprocessing import get_context
from unittest import mock

from agate import Row

import dbt.flags as flags
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.fabricspark import FabricSparkAdapter, FabricSparkRelation

from .utils import config_from_parts_or_dicts


class TestSparkAdapter(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = False
        self.mp_context = get_context("spawn")

        self.project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": False,
            },
            "config-version": 2,
        }

    def _get_target_livy(self, project):
        return config_from_parts_or_dicts(
            project,
            {
                "outputs": {
                    "test": {
                        "type": "fabricspark",
                        "method": "livy",
                        "authentication": "CLI",
                        "lakehouse": "dbtsparktest",
                        "workspaceid": "1de8390c-9aca-4790-bee8-72049109c0f4",
                        "lakehouseid": "8c5bc260-bc3a-4898-9ada-01e433d461ba",
                        "connect_retries": 0,
                        "connect_timeout": 10,
                        "threads": 1,
                        "endpoint": "https://dailyapi.fabric.microsoft.com/v1",
                        "spark_config": {"name": "test-session"},
                    }
                },
                "target": "test",
            },
        )

    def _get_target_livy_local(self, project):
        """Get config for local Livy mode."""
        return config_from_parts_or_dicts(
            project,
            {
                "outputs": {
                    "test": {
                        "type": "fabricspark",
                        "method": "livy",
                        "livy_mode": "local",
                        "livy_url": "http://localhost:8998",
                        "connect_retries": 0,
                        "connect_timeout": 10,
                        "threads": 1,
                        "spark_config": {"name": "test-session"},
                    }
                },
                "target": "test",
            },
        )

    @unittest.skip("Requires Azure CLI authentication - integration test")
    def test_livy_connection(self):
        config = self._get_target_livy(self.project_cfg)
        adapter = FabricSparkAdapter(config, self.mp_context)

        def fabric_spark_livy_connect(configuration):
            self.assertEqual(configuration.method, "livy")
            self.assertEqual(configuration.type, "fabricspark")

        # with mock.patch.object(hive, 'connect', new=hive_http_connect):
        with mock.patch(
            "dbt.adapters.fabricspark.livysession.LivySessionConnectionWrapper",
            new=fabric_spark_livy_connect,
        ):
            connection = adapter.acquire_connection("dummy")
            connection.handle  # trigger lazy-load

            self.assertEqual(connection.state, "open")
            self.assertIsNotNone(connection.handle)
            self.assertEqual(connection.credentials.authentication, "CLI")
            self.assertEqual(connection.credentials.database, "dbtsparktest")

    def test_local_livy_credentials(self):
        """Test that local Livy mode credentials are properly set up."""
        config = self._get_target_livy_local(self.project_cfg)
        FabricSparkAdapter(config, self.mp_context)

        # Get credentials from config
        creds = config.credentials
        self.assertEqual(creds.livy_mode, "local")
        self.assertTrue(creds.is_local_mode)
        self.assertEqual(creds.lakehouse_endpoint, "http://localhost:8998")
        self.assertIsNone(creds.workspaceid)
        self.assertIsNone(creds.lakehouseid)

    def test_fabric_livy_credentials(self):
        """Test that Fabric Livy mode credentials are properly set up."""
        config = self._get_target_livy(self.project_cfg)

        creds = config.credentials
        self.assertEqual(creds.livy_mode, "fabric")
        self.assertFalse(creds.is_local_mode)
        self.assertIn("workspaces", creds.lakehouse_endpoint)
        self.assertIsNotNone(creds.workspaceid)
        self.assertIsNotNone(creds.lakehouseid)

    def test_parse_relation(self):
        self.maxDiff = None
        rel_type = FabricSparkRelation.get_relation_type.Table

        relation = FabricSparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)"),
            (
                "col2",
                "string",
            ),
            ("dt", "date"),
            ("struct_col", "struct<struct_inner_col:string>"),
            ("# Partition Information", "data_type"),
            ("# col_name", "data_type"),
            ("dt", "date"),
            (None, None),
            ("# Detailed Table Information", None),
            ("Database", None),
            ("Owner", "root"),
            ("Created Time", "Wed Feb 04 18:15:00 UTC 1815"),
            ("Last Access", "Wed May 20 19:25:00 UTC 1925"),
            ("Type", "MANAGED"),
            ("Provider", "delta"),
            ("Location", "/mnt/vo"),
            ("Serde Library", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
            ("InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat"),
            ("OutputFormat", "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
            ("Partition Provider", "Catalog"),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        config = self._get_target_livy(self.project_cfg)
        rows = FabricSparkAdapter(config, self.mp_context).parse_describe_extended(
            relation, input_cols
        )
        self.assertEqual(len(rows), 4)
        self.assertEqual(
            rows[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            rows[1].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col2",
                "column_index": 1,
                "dtype": "string",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            rows[2].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "dt",
                "column_index": 2,
                "dtype": "date",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            rows[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct<struct_inner_col:string>",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

    def test_parse_relation_with_integer_owner(self):
        self.maxDiff = None
        rel_type = FabricSparkRelation.get_relation_type.Table

        relation = FabricSparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)"),
            ("# Detailed Table Information", None),
            ("Owner", 1234),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        config = self._get_target_livy(self.project_cfg)
        rows = FabricSparkAdapter(config, self.mp_context).parse_describe_extended(
            relation, input_cols
        )

        self.assertEqual(rows[0].to_column_dict().get("table_owner"), "1234")

    def test_parse_relation_with_statistics(self):
        self.maxDiff = None
        rel_type = FabricSparkRelation.get_relation_type.Table

        relation = FabricSparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type
        )
        assert relation.database is None

        # Mimics the output of Spark with a DESCRIBE TABLE EXTENDED
        plain_rows = [
            ("col1", "decimal(22,0)"),
            ("# Partition Information", "data_type"),
            (None, None),
            ("# Detailed Table Information", None),
            ("Database", None),
            ("Owner", "root"),
            ("Created Time", "Wed Feb 04 18:15:00 UTC 1815"),
            ("Last Access", "Wed May 20 19:25:00 UTC 1925"),
            ("Statistics", "1109049927 bytes, 14093476 rows"),
            ("Type", "MANAGED"),
            ("Provider", "delta"),
            ("Location", "/mnt/vo"),
            ("Serde Library", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
            ("InputFormat", "org.apache.hadoop.mapred.SequenceFileInputFormat"),
            ("OutputFormat", "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
            ("Partition Provider", "Catalog"),
        ]

        input_cols = [Row(keys=["col_name", "data_type"], values=r) for r in plain_rows]

        config = self._get_target_livy(self.project_cfg)
        rows = FabricSparkAdapter(config, self.mp_context).parse_describe_extended(
            relation, input_cols
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1109049927,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 14093476,
            },
        )

    def test_relation_with_database(self):
        config = self._get_target_livy(self.project_cfg)
        adapter = FabricSparkAdapter(config, self.mp_context)
        # fine - database excluded from rendering by include_policy
        adapter.Relation.create(schema="different", identifier="table")
        # also fine now - database is excluded from rendering
        adapter.Relation.create(database="something", schema="different", identifier="table")

    def test_relation_two_part_mode(self):
        """Test that non-schema mode uses two-part naming (schema.identifier)."""
        FabricSparkRelation._schemas_enabled = False
        try:
            rel = FabricSparkRelation.create(
                database="my_lakehouse", schema="my_lakehouse", identifier="my_table"
            )
            # include_policy.database should be False
            assert rel.include_policy.database is False
            assert rel.include_policy.schema is True
            # Renders as `lakehouse_name`.identifier (two-part; the lakehouse name occupies the
            # schema position and is backtick-quoted to preserve its original casing)
            assert str(rel) == "`my_lakehouse`.my_table"
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_relation_three_part_mode(self):
        """Test that schema-enabled mode uses three-part naming (database.schema.identifier)."""
        FabricSparkRelation._schemas_enabled = True
        try:
            rel = FabricSparkRelation.create(
                database="my_lakehouse", schema="dbo", identifier="my_table"
            )
            # include_policy.database should be True
            assert rel.include_policy.database is True
            assert rel.include_policy.schema is True
            # Renders as `database`.`schema`.identifier (three-part, both quoted)
            assert str(rel) == "`my_lakehouse`.`dbo`.my_table"
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_relation_mode_isolation(self):
        """Test that changing the ClassVar affects new relations, not existing ones."""
        FabricSparkRelation._schemas_enabled = False
        try:
            rel_two = FabricSparkRelation.create(database="lh", schema="lh", identifier="t1")
            FabricSparkRelation._schemas_enabled = True
            rel_three = FabricSparkRelation.create(database="lh", schema="dbo", identifier="t2")
            # Existing relation retains its original policy
            assert str(rel_two) == "`lh`.t1"
            # New relation uses updated policy (both database and schema quoted)
            assert str(rel_three) == "`lh`.`dbo`.t2"
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_mixed_case_database_no_approximate_match_error(self):
        """Regression test for ApproximateMatchError when lakehouse name uses mixed case.

        When the Fabric lakehouse displayName contains uppercase characters
        (e.g. 'DBTTest'), the relation cache stores relations with the
        case-preserved database name from the catalog.  Before the fix,
        FabricSparkQuotePolicy.database was False, which caused
        _make_match_kwargs to lowercase the search term ('dbttest'), while
        the cached relation kept the original casing ('DBTTest').  This
        mismatch triggered ApproximateMatchError on every rerun.

        The fix sets FabricSparkQuotePolicy.database to True so dbt
        preserves the original casing through the cache-lookup path.
        """
        FabricSparkRelation._schemas_enabled = True
        try:
            # Simulate a cached relation returned by Fabric's catalog with
            # case-preserved lakehouse name (as list_relations_without_caching
            # populates it).
            cached_relation = FabricSparkRelation.create(
                database="DBTTest",
                schema="dbo",
                identifier="my_first_model",
                type=FabricSparkRelation.get_relation_type.Table,
            )

            # Verify that the quote policy defaults database to True
            assert cached_relation.quote_policy.database is True

            # The search term that dbt passes through _make_match_kwargs.
            # With quoting.database=True, the database name is NOT lowercased,
            # so it stays as "DBTTest" and matches exactly.
            assert cached_relation.matches(
                database="DBTTest", schema="dbo", identifier="my_first_model"
            )

            # Also verify that mixed-case database renders correctly
            # with backtick quoting (harmless in Spark SQL).
            assert str(cached_relation) == "`DBTTest`.`dbo`.my_first_model"
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_mixed_case_database_lowered_raises_approximate_match(self):
        """Verify that searching with a lowered database name vs. mixed-case
        cached relation correctly raises ApproximateMatchError, confirming
        the root cause described in the issue."""
        from dbt_common.exceptions import CompilationError

        FabricSparkRelation._schemas_enabled = True
        try:
            cached_relation = FabricSparkRelation.create(
                database="DBTTest",
                schema="dbo",
                identifier="my_first_model",
                type=FabricSparkRelation.get_relation_type.Table,
            )
            # Searching with the lowered form should raise ApproximateMatchError
            # (which is a CompilationError subclass) since 'dbttest' != 'DBTTest'.
            with self.assertRaises(CompilationError):
                cached_relation.matches(
                    database="dbttest", schema="dbo", identifier="my_first_model"
                )
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_mixed_case_schema_no_approximate_match_error(self):
        """Regression test: mixed-case schema names must not trigger ApproximateMatchError.

        When a lakehouse has schemas enabled and the schema name contains
        uppercase characters (e.g. 'Operations'), FabricSparkQuotePolicy.schema
        must be True so that dbt's _make_match_kwargs does NOT lowercase the
        search term — otherwise 'operations' would fail to match 'Operations'
        in the relation cache and trigger ApproximateMatchError.
        """
        FabricSparkRelation._schemas_enabled = True
        try:
            cached_relation = FabricSparkRelation.create(
                database="my_lakehouse",
                schema="Operations",
                identifier="my_table",
                type=FabricSparkRelation.get_relation_type.Table,
            )

            assert cached_relation.quote_policy.schema is True
            assert cached_relation.matches(
                database="my_lakehouse", schema="Operations", identifier="my_table"
            )
            assert str(cached_relation) == "`my_lakehouse`.`Operations`.my_table"
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_mixed_case_schema_no_schema_mode_no_approximate_match_error(self):
        """Regression test: mixed-case lakehouse name used as schema in no-schema mode.

        In no-schema mode the schema field is set to the lakehouse name
        (e.g. 'Demo_Bronze').  With FabricSparkQuotePolicy.schema
        previously False, dbt would lowercase this to 'demo_bronze'
        in _make_match_kwargs, while the catalog returned the original casing,
        causing ApproximateMatchError.  The fix sets schema to True.
        """
        FabricSparkRelation._schemas_enabled = False
        try:
            lakehouse_name = "Demo_Bronze"
            cached_relation = FabricSparkRelation.create(
                database=lakehouse_name,
                schema=lakehouse_name,
                identifier="my_table",
                type=FabricSparkRelation.get_relation_type.Table,
            )

            assert cached_relation.quote_policy.schema is True
            assert cached_relation.matches(schema=lakehouse_name, identifier="my_table")
            assert str(cached_relation) == f"`{lakehouse_name}`.my_table"
        finally:
            FabricSparkRelation._schemas_enabled = False

    # -------------------------------------------------------------------- #
    # Cross-workspace 4-part naming                                        #
    # -------------------------------------------------------------------- #

    def test_relation_four_part_mode(self):
        """Setting `workspace` on a relation renders 4-part SQL.

        Each segment is independently backtick-quoted so Spark/Livy parses
        it as four distinct identifiers (per Fabric docs for cross-workspace
        access against schema-enabled lakehouses).
        """
        FabricSparkRelation._schemas_enabled = True
        try:
            rel = FabricSparkRelation.create(
                database="silver_lh",
                schema="dbo",
                identifier="orders",
                workspace="prod_ws",
            )
            assert rel.workspace == "prod_ws"
            assert str(rel) == "`prod_ws`.`silver_lh`.`dbo`.orders"
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_relation_workspace_none_preserves_three_part(self):
        """Relations without workspace render unchanged 3-part SQL (no regression)."""
        FabricSparkRelation._schemas_enabled = True
        try:
            rel = FabricSparkRelation.create(
                database="silver_lh", schema="dbo", identifier="orders", workspace=None
            )
            assert rel.workspace is None
            assert str(rel) == "`silver_lh`.`dbo`.orders"
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_relation_workspace_mixed_case_preserved(self):
        """Workspace names like 'dbt Fabric Spark 1' (spaces, mixed case) are preserved."""
        FabricSparkRelation._schemas_enabled = True
        try:
            rel = FabricSparkRelation.create(
                database="silver_lh",
                schema="dbo",
                identifier="orders",
                workspace="dbt Fabric Spark 1",
            )
            # Mixed-case + space-bearing workspace round-trips through the cache
            # match path (workspace is a render decoration, not a matching field).
            assert rel.matches(database="silver_lh", schema="dbo", identifier="orders")
            assert str(rel) == "`dbt Fabric Spark 1`.`silver_lh`.`dbo`.orders"
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_relation_workspace_temp_view_excludes_prefix(self):
        """Temp views (.include(database=False, schema=False)) drop the workspace prefix.

        Temp views are session-scoped — emitting a 4-part name would cause
        Spark to complain about cross-namespace temporary views.  The render
        gating on ``include_policy.database`` ensures workspace is only emitted
        when the database segment is also included.
        """
        FabricSparkRelation._schemas_enabled = True
        try:
            rel = FabricSparkRelation.create(
                database="silver_lh",
                schema="dbo",
                identifier="my_temp",
                workspace="prod_ws",
            )
            temp = rel.include(database=False, schema=False)
            # Workspace remains on the dataclass (preserved through incorporate)
            # but is not rendered when database is excluded.
            assert temp.workspace == "prod_ws"
            assert str(temp) == "my_temp"
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_relation_workspace_incorporate_preserves_field(self):
        """``incorporate`` round-trips the workspace field via to_dict/from_dict."""
        FabricSparkRelation._schemas_enabled = True
        try:
            rel = FabricSparkRelation.create(
                database="silver_lh",
                schema="dbo",
                identifier="orders",
                workspace="prod_ws",
            )
            renamed = rel.incorporate(path={"identifier": "orders_renamed"})
            assert renamed.workspace == "prod_ws"
            assert str(renamed) == "`prod_ws`.`silver_lh`.`dbo`.orders_renamed"
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_validate_workspace_name_supported_runtime_schema_enabled(self):
        """When the runtime flag says schema-enabled, workspace_name is allowed."""
        config = self._get_target_livy(self.project_cfg)
        adapter = FabricSparkAdapter(config, self.mp_context)
        FabricSparkRelation._schemas_enabled = True
        try:
            # Should not raise
            adapter.validate_workspace_name_supported(
                "prod_ws",
                target_database="silver_lh",
                target_schema="dbo",
                target_lakehouse="bronze_lh",
            )
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_validate_workspace_name_supported_parse_time_schema_enabled(self):
        """Parse-time heuristic: schema != lakehouse implies schema-enabled, allowed."""
        config = self._get_target_livy(self.project_cfg)
        adapter = FabricSparkAdapter(config, self.mp_context)
        FabricSparkRelation._schemas_enabled = False  # runtime not yet set
        try:
            # schema=dbo != lakehouse=bronze_lh → parse-time inference says schema-enabled
            adapter.validate_workspace_name_supported(
                "prod_ws",
                target_database="silver_lh",
                target_schema="dbo",
                target_lakehouse="bronze_lh",
            )
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_validate_workspace_name_supported_no_op_when_unset(self):
        """When workspace_name is None or empty, validation is a no-op."""
        config = self._get_target_livy(self.project_cfg)
        adapter = FabricSparkAdapter(config, self.mp_context)
        FabricSparkRelation._schemas_enabled = False
        try:
            adapter.validate_workspace_name_supported(
                None, target_schema="lh", target_lakehouse="lh"
            )
            adapter.validate_workspace_name_supported(
                "", target_schema="lh", target_lakehouse="lh"
            )
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_validate_workspace_name_supported_blocks_no_schema_mode(self):
        """workspace_name on a non-schema-enabled lakehouse raises a clear error."""
        from dbt_common.exceptions import DbtRuntimeError

        config = self._get_target_livy(self.project_cfg)
        adapter = FabricSparkAdapter(config, self.mp_context)
        FabricSparkRelation._schemas_enabled = False
        try:
            with self.assertRaises(DbtRuntimeError) as ctx:
                adapter.validate_workspace_name_supported(
                    "prod_ws",
                    target_database="my_lh",
                    target_schema="my_lh",  # schema == lakehouse → not schema-enabled
                    target_lakehouse="my_lh",
                )
            msg = str(ctx.exception)
            assert "workspace_name" in msg
            assert "schema-enabled" in msg
            assert "Cross-Workspace" in msg or "OneLake shortcut" in msg
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_workspace_name_registered_on_adapter_config(self):
        """workspace_name is a first-class FabricSparkConfig key."""
        from dataclasses import fields

        from dbt.adapters.fabricspark.impl import FabricSparkConfig

        field_names = {f.name for f in fields(FabricSparkConfig)}
        assert "workspace_name" in field_names, (
            "workspace_name must be registered on FabricSparkConfig so model "
            "config() values flow through node.config rather than being silently ignored."
        )

    def test_create_from_falls_back_to_creds_workspace_name(self):
        """create_from uses profile-level workspace_name when model config doesn't set one."""
        from dbt.adapters.contracts.relation import RelationConfig

        FabricSparkRelation._schemas_enabled = True
        try:
            config = config_from_parts_or_dicts(
                self.project_cfg,
                {
                    "outputs": {
                        "test": {
                            "type": "fabricspark",
                            "method": "livy",
                            "authentication": "CLI",
                            "lakehouse": "silver_lh",
                            "schema": "dbo",
                            "workspace_name": "profile-ws",
                            "workspaceid": "1de8390c-9aca-4790-bee8-72049109c0f4",
                            "lakehouseid": "8c5bc260-bc3a-4898-9ada-01e433d461ba",
                            "connect_retries": 0,
                            "connect_timeout": 10,
                            "threads": 1,
                            "endpoint": "https://dailyapi.fabric.microsoft.com/v1",
                            "spark_config": {"name": "test-session"},
                        }
                    },
                    "target": "test",
                },
            )
            relation_config = mock.MagicMock(spec=RelationConfig)
            relation_config.database = "silver_lh"
            relation_config.schema = "dbo"
            relation_config.identifier = "orders"
            relation_config.quoting_dict = {}
            relation_config.config = mock.MagicMock()
            relation_config.config.get = mock.MagicMock(return_value=None)
            relation_config.catalog_name = None

            rel = FabricSparkRelation.create_from(config, relation_config)
            assert rel.workspace == "profile-ws", (
                "create_from should fall back to creds.workspace_name when model config is unset"
            )
            assert "`profile-ws`" in str(rel)
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_create_from_model_workspace_name_overrides_profile(self):
        """Model config(workspace_name=...) takes precedence over the profile-level value."""
        from dbt.adapters.contracts.relation import RelationConfig

        FabricSparkRelation._schemas_enabled = True
        try:
            config = config_from_parts_or_dicts(
                self.project_cfg,
                {
                    "outputs": {
                        "test": {
                            "type": "fabricspark",
                            "method": "livy",
                            "authentication": "CLI",
                            "lakehouse": "silver_lh",
                            "schema": "dbo",
                            "workspace_name": "profile-ws",
                            "workspaceid": "1de8390c-9aca-4790-bee8-72049109c0f4",
                            "lakehouseid": "8c5bc260-bc3a-4898-9ada-01e433d461ba",
                            "connect_retries": 0,
                            "connect_timeout": 10,
                            "threads": 1,
                            "endpoint": "https://dailyapi.fabric.microsoft.com/v1",
                            "spark_config": {"name": "test-session"},
                        }
                    },
                    "target": "test",
                },
            )
            relation_config = mock.MagicMock(spec=RelationConfig)
            relation_config.database = "silver_lh"
            relation_config.schema = "dbo"
            relation_config.identifier = "orders"
            relation_config.quoting_dict = {}
            relation_config.config = mock.MagicMock()
            relation_config.config.get = mock.MagicMock(return_value="model-ws")
            relation_config.catalog_name = None

            rel = FabricSparkRelation.create_from(config, relation_config)
            assert rel.workspace == "model-ws", (
                "model config(workspace_name=...) must override profile-level workspace_name"
            )
            assert "`model-ws`" in str(rel)
            assert "profile-ws" not in str(rel)
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_create_from_no_workspace_when_neither_set(self):
        """create_from produces no workspace when neither model config nor profile sets one."""
        from dbt.adapters.contracts.relation import RelationConfig

        FabricSparkRelation._schemas_enabled = True
        try:
            config = self._get_target_livy(self.project_cfg)
            relation_config = mock.MagicMock(spec=RelationConfig)
            relation_config.database = "dbtsparktest"
            relation_config.schema = "dbtsparktest"
            relation_config.identifier = "orders"
            relation_config.quoting_dict = {}
            relation_config.config = mock.MagicMock()
            relation_config.config.get = mock.MagicMock(return_value=None)
            relation_config.catalog_name = None

            rel = FabricSparkRelation.create_from(config, relation_config)
            assert rel.workspace is None
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_profile_with_schema(self):
        """Test that schema is accepted as user input and database is derived from lakehouse."""
        profile = {
            "outputs": {
                "test": {
                    "type": "fabricspark",
                    "method": "livy",
                    "authentication": "CLI",
                    "schema": "custom_schema",
                    "lakehouse": "dbtsparktest",
                    "workspaceid": "1de8390c-9aca-4790-bee8-72049109c0f4",
                    "lakehouseid": "8c5bc260-bc3a-4898-9ada-01e433d461ba",
                    "connect_retries": 0,
                    "connect_timeout": 10,
                    "threads": 1,
                    "endpoint": "https://dailyapi.fabric.microsoft.com/v1",
                    "spark_config": {"name": "test-session"},
                }
            },
            "target": "test",
        }
        config = config_from_parts_or_dicts(self.project_cfg, profile)
        # schema is user input
        assert config.credentials.schema == "custom_schema"
        # database is always derived from lakehouse name
        assert config.credentials.database == "dbtsparktest"

    def test_parse_columns_from_information_with_table_type_and_delta_provider(self):
        self.maxDiff = None
        rel_type = FabricSparkRelation.get_relation_type.Table

        # Mimics the output of Spark in the information column
        information = (
            "Database: default_schema\n"
            "Table: mytable\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: Wed May 20 19:25:00 UTC 1925\n"
            "Created By: Spark 3.0.1\n"
            "Type: MANAGED\n"
            "Provider: delta\n"
            "Statistics: 123456789 bytes\n"
            "Location: /mnt/vo\n"
            "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\n"
            "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat\n"
            "Partition Provider: Catalog\n"
            "Partition Columns: [`dt`]\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = FabricSparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type, information=information
        )

        config = self._get_target_livy(self.project_cfg)
        columns = FabricSparkAdapter(config, self.mp_context).parse_columns_from_information(
            relation
        )
        self.assertEqual(len(columns), 4)
        self.assertEqual(
            columns[0].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col1",
                "column_index": 0,
                "dtype": "decimal(22,0)",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 123456789,
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 123456789,
            },
        )

    def test_parse_columns_from_information_with_view_type(self):
        self.maxDiff = None
        rel_type = FabricSparkRelation.get_relation_type.View
        information = (
            "Database: default_schema\n"
            "Table: myview\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: UNKNOWN\n"
            "Created By: Spark 3.0.1\n"
            "Type: VIEW\n"
            "View Text: WITH base (\n"
            "    SELECT * FROM source_table\n"
            ")\n"
            "SELECT col1, col2, dt FROM base\n"
            "View Original Text: WITH base (\n"
            "    SELECT * FROM source_table\n"
            ")\n"
            "SELECT col1, col2, dt FROM base\n"
            "View Catalog and Namespace: spark_catalog.default\n"
            "View Query Output Columns: [col1, col2, dt]\n"
            "Table Properties: [view.query.out.col.1=col1, view.query.out.col.2=col2, "
            "transient_lastDdlTime=1618324324, view.query.out.col.3=dt, "
            "view.catalogAndNamespace.part.0=spark_catalog, "
            "view.catalogAndNamespace.part.1=default]\n"
            "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\n"
            "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat\n"
            "Storage Properties: [serialization.format=1]\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = FabricSparkRelation.create(
            schema="default_schema", identifier="myview", type=rel_type, information=information
        )

        config = self._get_target_livy(self.project_cfg)
        columns = FabricSparkAdapter(config, self.mp_context).parse_columns_from_information(
            relation
        )
        self.assertEqual(len(columns), 4)
        self.assertEqual(
            columns[1].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "col2",
                "column_index": 1,
                "dtype": "string",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
            },
        )

    def test_parse_columns_from_information_with_table_type_and_parquet_provider(self):
        self.maxDiff = None
        rel_type = FabricSparkRelation.get_relation_type.Table

        information = (
            "Database: default_schema\n"
            "Table: mytable\n"
            "Owner: root\n"
            "Created Time: Wed Feb 04 18:15:00 UTC 1815\n"
            "Last Access: Wed May 20 19:25:00 UTC 1925\n"
            "Created By: Spark 3.0.1\n"
            "Type: MANAGED\n"
            "Provider: parquet\n"
            "Statistics: 1234567890 bytes, 12345678 rows\n"
            "Location: /mnt/vo\n"
            "Serde Library: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\n"
            "InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\n"
            "OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\n"
            "Schema: root\n"
            " |-- col1: decimal(22,0) (nullable = true)\n"
            " |-- col2: string (nullable = true)\n"
            " |-- dt: date (nullable = true)\n"
            " |-- struct_col: struct (nullable = true)\n"
            " |    |-- struct_inner_col: string (nullable = true)\n"
        )
        relation = FabricSparkRelation.create(
            schema="default_schema", identifier="mytable", type=rel_type, information=information
        )

        config = self._get_target_livy(self.project_cfg)
        columns = FabricSparkAdapter(config, self.mp_context).parse_columns_from_information(
            relation
        )
        self.assertEqual(len(columns), 4)

        self.assertEqual(
            columns[2].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "dt",
                "column_index": 2,
                "dtype": "date",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1234567890,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 12345678,
            },
        )

        self.assertEqual(
            columns[3].to_column_dict(omit_none=False),
            {
                "table_database": None,
                "table_schema": relation.schema,
                "table_name": relation.name,
                "table_type": rel_type,
                "table_owner": "root",
                "column": "struct_col",
                "column_index": 3,
                "dtype": "struct",
                "numeric_scale": None,
                "numeric_precision": None,
                "char_size": None,
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1234567890,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 12345678,
            },
        )

    def test_build_spark_relation_list_materialized_lake_view(self):
        """MATERIALIZED_LAKE_VIEW from Fabric should be classified as MaterializedView."""
        config = self._get_target_livy(self.project_cfg)
        adapter = FabricSparkAdapter(config, self.mp_context)

        schema_relation = FabricSparkRelation.create(
            database="mydb", schema="dbo", identifier="dummy"
        )

        def fake_info(row):
            return row

        rows = [
            ("dbo", "mlv_table", "Type: MATERIALIZED_LAKE_VIEW\nProvider: delta\n"),
            ("dbo", "mv_table", "Type: MATERIALIZED_VIEW\nProvider: delta\n"),
            ("dbo", "view_table", "Type: VIEW\nProvider: delta\n"),
            ("dbo", "regular_table", "Type: MANAGED\nProvider: delta\n"),
        ]

        relations = adapter._build_spark_relation_list(
            row_list=rows,
            relation_info_func=fake_info,
            schema_relation=schema_relation,
        )

        self.assertEqual(len(relations), 4)
        self.assertEqual(relations[0].type, RelationType.MaterializedView)
        self.assertEqual(relations[1].type, RelationType.MaterializedView)
        self.assertEqual(relations[2].type, RelationType.View)
        self.assertEqual(relations[3].type, RelationType.Table)


class TestCatalogPerLakehouseScoping(unittest.TestCase):
    """Regression coverage for issue #209.

    ``dbt docs generate`` against a project that writes to multiple Fabric
    lakehouses sharing schema names (e.g. both ``silver_lh`` and ``gold_lh``
    have a ``finance`` schema) previously mis-attributed silver-layer models
    to the gold lakehouse during catalog enumeration, surfacing as
    ``[TABLE_OR_VIEW_NOT_FOUND]`` from ``DESCRIBE TABLE``.

    The root cause: ``FabricSparkRelation.include_policy.database`` is
    captured from the class-level ``_schemas_enabled`` flag at relation
    creation time. The cache pre-population step in dbt-core (via
    ``_get_cache_schemas`` → ``Relation.create_from``) may build the schema
    listing relations during manifest parsing — before ``connections.open``
    flips ``_schemas_enabled`` to True. The rendered SHOW SQL is then
    database-unqualified, runs against the session-bound default lakehouse,
    and the cache stores the returned tables under whichever
    ``schema_relation.database`` the caller asked about — a silent
    cross-lakehouse mis-attribution.

    These tests cover the two defensive guards added to
    ``FabricSparkAdapter``:

    1. ``list_relations_without_caching`` forces
       ``schema_relation.include_policy.database = True`` whenever the
       active mode requires three-part naming and the incoming relation
       lost that flag — so the SHOW SQL is always database-qualified.
    2. ``_get_one_catalog`` skips any relation whose ``database`` does not
       match the catalog cell being iterated for — belt-and-suspenders so a
       stale cache entry can never DESCRIBE a table in the wrong lakehouse.
    """

    def setUp(self):
        flags.STRICT_MODE = False
        self.mp_context = get_context("spawn")

        self.project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {"identifier": False, "schema": False},
            "config-version": 2,
        }

    def _adapter(self, schema: str = "dbo", lakehouse: str = "silver_lh"):
        config = config_from_parts_or_dicts(
            self.project_cfg,
            {
                "outputs": {
                    "test": {
                        "type": "fabricspark",
                        "method": "livy",
                        "authentication": "CLI",
                        "lakehouse": lakehouse,
                        "workspaceid": "1de8390c-9aca-4790-bee8-72049109c0f4",
                        "lakehouseid": "8c5bc260-bc3a-4898-9ada-01e433d461ba",
                        "connect_retries": 0,
                        "connect_timeout": 10,
                        "threads": 1,
                        "endpoint": "https://dailyapi.fabric.microsoft.com/v1",
                        "schema": schema,
                        "spark_config": {"name": "test-session"},
                    }
                },
                "target": "test",
            },
        )
        return FabricSparkAdapter(config, self.mp_context)

    def test_list_relations_qualifies_database_when_schemas_enabled_flag_set(self):
        """After connections.open flips ``_schemas_enabled``, list_relations
        must always render database-qualified SHOW SQL — even when the
        incoming relation was created earlier with ``include_policy.database
        = False`` (e.g. during manifest parsing)."""
        adapter = self._adapter()
        # Simulate the post-open state: the class flag is True, but a
        # relation built during parsing locked include_policy.database=False.
        FabricSparkRelation._schemas_enabled = True
        try:
            stale_relation = FabricSparkRelation.create(
                database="gold_lh",
                schema="finance",
                identifier="anything",
            ).include(database=False)
            self.assertFalse(stale_relation.include_policy.database)
            self.assertTrue(adapter._catalog_requires_database_scoping(stale_relation))

            captured = {}

            def fake_execute_macro(macro_name, kwargs):
                # Capture the schema_relation seen by the macro to assert that
                # list_relations_without_caching re-included the database
                # segment before emitting the SHOW SQL.
                captured["schema_relation"] = kwargs["schema_relation"]
                return []

            with mock.patch.object(adapter, "execute_macro", side_effect=fake_execute_macro):
                adapter.list_relations_without_caching(stale_relation)

            rendered = captured["schema_relation"]
            self.assertTrue(rendered.include_policy.database)
            self.assertEqual(rendered.database, "gold_lh")
            self.assertEqual(rendered.schema, "finance")

        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_list_relations_qualifies_database_via_parse_time_fallback(self):
        """When ``_schemas_enabled`` is still False but ``schema != lakehouse``
        in profiles.yml (the parse-time fallback used by
        ``generate_schema_name``), database scoping must still be enforced —
        the credentials reliably signal schema-enabled mode even before
        connections.open finishes."""
        # Schema 'dbo' != lakehouse 'silver_lh' → fallback should trigger.
        adapter = self._adapter(schema="dbo", lakehouse="silver_lh")
        # Confirm we are testing the pre-open state.
        self.assertFalse(FabricSparkRelation._schemas_enabled)

        stale_relation = FabricSparkRelation.create(
            database="gold_lh",
            schema="finance",
            identifier="anything",
        ).include(database=False)
        self.assertTrue(adapter._catalog_requires_database_scoping(stale_relation))

        captured = {}

        def fake_execute_macro(macro_name, kwargs):
            captured["schema_relation"] = kwargs["schema_relation"]
            return []

        with mock.patch.object(adapter, "execute_macro", side_effect=fake_execute_macro):
            adapter.list_relations_without_caching(stale_relation)

        rendered = captured["schema_relation"]
        self.assertTrue(rendered.include_policy.database)
        self.assertEqual(rendered.database, "gold_lh")

    def test_list_relations_does_not_qualify_in_no_schema_mode(self):
        """In genuine no-schema mode (``schema == lakehouse``, no flag set),
        the SHOW SQL must remain database-unqualified — three-part naming is
        not supported against non-schema lakehouses."""
        adapter = self._adapter(schema="silver_lh", lakehouse="silver_lh")
        self.assertFalse(FabricSparkRelation._schemas_enabled)

        relation = FabricSparkRelation.create(
            database="silver_lh",
            schema="silver_lh",
            identifier="x",
        ).include(database=False)
        self.assertFalse(adapter._catalog_requires_database_scoping(relation))

        captured = {}

        def fake_execute_macro(macro_name, kwargs):
            captured["schema_relation"] = kwargs["schema_relation"]
            return []

        with mock.patch.object(adapter, "execute_macro", side_effect=fake_execute_macro):
            adapter.list_relations_without_caching(relation)

        # include_policy untouched — macro keeps emitting two-part naming.
        self.assertFalse(captured["schema_relation"].include_policy.database)

    def test_list_relations_preserves_already_qualified_policy(self):
        """A relation already carrying ``include_policy.database=True`` must
        pass through unchanged — the fix only patches the stale case."""
        adapter = self._adapter()
        FabricSparkRelation._schemas_enabled = True
        try:
            relation = FabricSparkRelation.create(
                database="gold_lh",
                schema="finance",
                identifier="x",
            )
            self.assertTrue(relation.include_policy.database)

            captured = {}

            def fake_execute_macro(macro_name, kwargs):
                captured["schema_relation"] = kwargs["schema_relation"]
                return []

            with mock.patch.object(adapter, "execute_macro", side_effect=fake_execute_macro):
                adapter.list_relations_without_caching(relation)

            # Same identity / policy preserved.
            self.assertIs(captured["schema_relation"], relation)
            self.assertTrue(captured["schema_relation"].include_policy.database)
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_get_one_catalog_skips_relations_with_mismatched_database(self):
        """If a stale cache entry lists a silver-layer model under the gold
        lakehouse, _get_one_catalog must skip it so DESCRIBE TABLE never
        runs against the wrong lakehouse."""
        from dbt.adapters.base.relation import InformationSchema

        adapter = self._adapter()
        FabricSparkRelation._schemas_enabled = True
        try:
            # Three relations: one matches the gold catalog cell (kept), one
            # mis-attributed silver model (must be skipped), one unqualified
            # (kept, since the listing returned it under no database).
            kept = FabricSparkRelation.create(
                database="gold_lh",
                schema="finance",
                identifier="dim_account",
                type=FabricSparkRelation.get_relation_type.Table,
            )
            stale = FabricSparkRelation.create(
                database="silver_lh",
                schema="finance",
                identifier="stg_account",
                type=FabricSparkRelation.get_relation_type.Table,
            )

            with (
                mock.patch.object(adapter, "list_relations", return_value=[kept, stale]),
                mock.patch.object(
                    adapter, "_get_columns_for_catalog", return_value=[]
                ) as columns_for,
            ):
                # Build the information_schema for the gold cell.
                gold_relation = FabricSparkRelation.create(
                    database="gold_lh",
                    schema="finance",
                    identifier="placeholder",
                )
                info_schema = InformationSchema.from_relation(gold_relation, None)
                adapter._get_one_catalog(info_schema, {"finance"}, frozenset())

            described = [call.args[0].identifier for call in columns_for.call_args_list]
            self.assertIn("dim_account", described)
            self.assertNotIn(
                "stg_account",
                described,
                "Silver-layer relation must not be DESCRIBE'd against the "
                "gold lakehouse during gold's catalog iteration (issue #209).",
            )
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_get_one_catalog_database_mismatch_is_case_insensitive(self):
        """Database comparison must be case-insensitive — Fabric preserves
        the original casing of lakehouse names through the cache, but two
        relations differing only in case still refer to the same lakehouse."""
        from dbt.adapters.base.relation import InformationSchema

        adapter = self._adapter()
        FabricSparkRelation._schemas_enabled = True
        try:
            relation = FabricSparkRelation.create(
                database="Gold_LH",
                schema="finance",
                identifier="dim_account",
                type=FabricSparkRelation.get_relation_type.Table,
            )
            with (
                mock.patch.object(adapter, "list_relations", return_value=[relation]),
                mock.patch.object(
                    adapter, "_get_columns_for_catalog", return_value=[]
                ) as columns_for,
            ):
                cell = FabricSparkRelation.create(
                    database="gold_lh",
                    schema="finance",
                    identifier="x",
                )
                info_schema = InformationSchema.from_relation(cell, None)
                adapter._get_one_catalog(info_schema, {"finance"}, frozenset())

            # Same lakehouse (case-only difference) → not skipped.
            self.assertEqual(columns_for.call_count, 1)
        finally:
            FabricSparkRelation._schemas_enabled = False

    def test_catalog_schema_map_keeps_same_schema_different_database_distinct(self):
        """``_get_catalog_schemas`` must keep ``(silver_lh, finance)`` and
        ``(gold_lh, finance)`` as separate entries — otherwise the catalog
        executor only spawns one future and the two lakehouses collide."""
        from dbt.adapters.contracts.relation import RelationConfig

        adapter = self._adapter()
        FabricSparkRelation._schemas_enabled = True
        try:
            # Mock relation configs with shared schema, distinct databases.
            silver_cfg = mock.MagicMock(spec=RelationConfig)
            silver_cfg.database = "silver_lh"
            silver_cfg.schema = "finance"
            silver_cfg.identifier = "stg_account"
            silver_cfg.quoting_dict = {}
            silver_cfg.config = mock.MagicMock()
            silver_cfg.config.get = mock.MagicMock(return_value=None)
            silver_cfg.catalog_name = None

            gold_cfg = mock.MagicMock(spec=RelationConfig)
            gold_cfg.database = "gold_lh"
            gold_cfg.schema = "finance"
            gold_cfg.identifier = "dim_account"
            gold_cfg.quoting_dict = {}
            gold_cfg.config = mock.MagicMock()
            gold_cfg.config.get = mock.MagicMock(return_value=None)
            gold_cfg.catalog_name = None

            schema_map = adapter._get_catalog_schemas([silver_cfg, gold_cfg])

            databases = sorted(info.database for info in schema_map.keys())
            self.assertEqual(databases, ["gold_lh", "silver_lh"])
            for schemas in schema_map.values():
                self.assertEqual(schemas, {"finance"})
        finally:
            FabricSparkRelation._schemas_enabled = False
