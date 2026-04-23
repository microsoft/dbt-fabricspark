import unittest
from multiprocessing import get_context
from unittest import mock

import dbt.flags as flags
from agate import Row
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
            # Renders as schema.identifier (two-part)
            assert str(rel) == "my_lakehouse.my_table"
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
            # Renders as `database`.schema.identifier (three-part, database quoted)
            assert str(rel) == "`my_lakehouse`.dbo.my_table"
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
            assert str(rel_two) == "lh.t1"
            # New relation uses updated policy (database quoted)
            assert str(rel_three) == "`lh`.dbo.t2"
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
            assert str(cached_relation) == "`DBTTest`.dbo.my_first_model"
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
