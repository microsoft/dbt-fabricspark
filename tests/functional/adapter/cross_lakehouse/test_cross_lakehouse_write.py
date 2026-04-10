"""Cross-lakehouse write tests.

These tests verify that dbt can write data to a *different* lakehouse
within the same Fabric workspace. Two scenarios are covered:

1. Writing to a destination lakehouse that does NOT have schemas enabled.
2. Writing to a destination lakehouse that DOES have schemas enabled.

Before running these tests, fill in the placeholder values in test.env:

    # Lakehouse WITHOUT schema enabled
    CROSS_LAKEHOUSE_NO_SCHEMA_NAME=<your-lakehouse-name>

    # Lakehouse WITH schema enabled
    CROSS_LAKEHOUSE_SCHEMA_NAME=<your-lakehouse-name>
    CROSS_LAKEHOUSE_SCHEMA=<your-schema-name>

Run with:
    pytest tests/functional/adapter/cross_lakehouse/ -v --profile az_cli
"""

import os

import pytest
from dbt.tests.util import run_dbt


# ---------------------------------------------------------------------------
# Env-var helpers
# ---------------------------------------------------------------------------

def _cross_lakehouse_no_schema_configured() -> bool:
    """Return True when the non-schema destination lakehouse env vars are set."""
    name = os.getenv("CROSS_LAKEHOUSE_NO_SCHEMA_NAME", "")
    return bool(name and not name.startswith("<"))


def _cross_lakehouse_schema_configured() -> bool:
    """Return True when the schema-enabled destination lakehouse env vars are set."""
    name = os.getenv("CROSS_LAKEHOUSE_SCHEMA_NAME", "")
    schema = os.getenv("CROSS_LAKEHOUSE_SCHEMA", "")
    return bool(name and schema and not name.startswith("<"))


# ---------------------------------------------------------------------------
# Shared seed data
# ---------------------------------------------------------------------------

_seeds_csv = """id,name,value
1,alice,100
2,bob,200
3,charlie,300
""".strip()

# Source table — always written to the source lakehouse (default profile target)
_source_table_sql = """
{{
    config(
        materialized='table'
    )
}}

select id, name, value from {{ ref('cross_seed') }}
"""


# ---------------------------------------------------------------------------
# Test 1 — Cross-lakehouse write to a lakehouse WITHOUT schemas
# ---------------------------------------------------------------------------

# Reads from the source table in the source lakehouse and writes to
# the destination lakehouse. The ``database`` config maps to the
# lakehouse name in Fabric Spark.
_cross_lh_no_schema_table_sql = """
{{{{
    config(
        materialized='table',
        database='{dest_lakehouse}'
    )
}}}}

select id, name, value from {{{{ ref('source_table') }}}}
"""

_cross_lh_no_schema_incremental_sql = """
{{{{
    config(
        materialized='incremental',
        database='{dest_lakehouse}',
        incremental_strategy='append'
    )
}}}}

select id, name, value from {{{{ ref('source_table') }}}}
"""


@pytest.mark.skipif(
    not _cross_lakehouse_no_schema_configured(),
    reason="CROSS_LAKEHOUSE_NO_SCHEMA_NAME not configured in test.env",
)
class TestCrossLakehouseWriteNoSchema:
    """Write data from the source lakehouse into a different lakehouse that
    does NOT have schemas enabled (two-part naming: lakehouse.table)."""

    @pytest.fixture(scope="class")
    def dest_lakehouse(self):
        return os.environ["CROSS_LAKEHOUSE_NO_SCHEMA_NAME"]

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "cross_lakehouse_no_schema"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"cross_seed.csv": _seeds_csv}

    @pytest.fixture(scope="class")
    def models(self, dest_lakehouse):
        return {
            "source_table.sql": _source_table_sql,
            "cross_table_no_schema.sql": _cross_lh_no_schema_table_sql.format(
                dest_lakehouse=dest_lakehouse,
            ),
            "cross_incremental_no_schema.sql": _cross_lh_no_schema_incremental_sql.format(
                dest_lakehouse=dest_lakehouse,
            ),
        }

    def test_seed_and_source_table(self, project):
        """Seed data and create a source table in the source lakehouse."""
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt(["run", "--select", "source_table"])
        assert len(results) == 1

    def test_cross_lakehouse_table(self, project, dest_lakehouse):
        """Read from source lakehouse table, write to destination lakehouse (no schema)."""
        results = run_dbt(["run", "--select", "cross_table_no_schema"])
        assert len(results) == 1

        # Verify data landed in the destination lakehouse
        result = project.run_sql(
            f"select count(*) from {dest_lakehouse}.cross_table_no_schema",
            fetch="one",
        )
        assert int(result[0]) == 3

    def test_cross_lakehouse_incremental(self, project, dest_lakehouse):
        """Read from source lakehouse table, incremental append to destination (no schema)."""
        # First run — create
        results = run_dbt(["run", "--select", "cross_incremental_no_schema"])
        assert len(results) == 1

        # Second run — append
        results = run_dbt(["run", "--select", "cross_incremental_no_schema"])
        assert len(results) == 1

        # Should have 6 rows after two appends
        result = project.run_sql(
            f"select count(*) from {dest_lakehouse}.cross_incremental_no_schema",
            fetch="one",
        )
        assert int(result[0]) == 6


# ---------------------------------------------------------------------------
# Test 2 — Cross-lakehouse write to a lakehouse WITH schemas
# ---------------------------------------------------------------------------

_cross_lh_schema_table_sql = """
{{{{
    config(
        materialized='table',
        database='{dest_lakehouse}',
        schema='{dest_schema}'
    )
}}}}

select id, name, value from {{{{ ref('source_table') }}}}
"""

_cross_lh_schema_incremental_sql = """
{{{{
    config(
        materialized='incremental',
        database='{dest_lakehouse}',
        schema='{dest_schema}',
        incremental_strategy='append'
    )
}}}}

select id, name, value from {{{{ ref('source_table') }}}}
"""


@pytest.mark.skipif(
    not _cross_lakehouse_schema_configured(),
    reason="CROSS_LAKEHOUSE_SCHEMA_NAME / _SCHEMA not configured in test.env",
)
class TestCrossLakehouseWriteWithSchema:
    """Write data from the source lakehouse into a different lakehouse that
    HAS schemas enabled (three-part naming: lakehouse.schema.table)."""

    @pytest.fixture(scope="class")
    def dest_lakehouse(self):
        return os.environ["CROSS_LAKEHOUSE_SCHEMA_NAME"]

    @pytest.fixture(scope="class")
    def dest_schema(self):
        return os.environ["CROSS_LAKEHOUSE_SCHEMA"]

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "cross_lakehouse_with_schema"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"cross_seed.csv": _seeds_csv}

    @pytest.fixture(scope="class")
    def models(self, dest_lakehouse, dest_schema):
        return {
            "source_table.sql": _source_table_sql,
            "cross_table_with_schema.sql": _cross_lh_schema_table_sql.format(
                dest_lakehouse=dest_lakehouse,
                dest_schema=dest_schema,
            ),
            "cross_incremental_with_schema.sql": _cross_lh_schema_incremental_sql.format(
                dest_lakehouse=dest_lakehouse,
                dest_schema=dest_schema,
            ),
        }

    def test_seed_and_source_table(self, project):
        """Seed data and create a source table in the source lakehouse."""
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt(["run", "--select", "source_table"])
        assert len(results) == 1

    def test_cross_lakehouse_table(self, project, dest_lakehouse, dest_schema):
        """Read from source lakehouse table, write to destination lakehouse (schema-enabled)."""
        results = run_dbt(["run", "--select", "cross_table_with_schema"])
        assert len(results) == 1

        # Verify data landed in destination lakehouse.schema
        result = project.run_sql(
            f"select count(*) from {dest_lakehouse}.{dest_schema}.cross_table_with_schema",
            fetch="one",
        )
        assert int(result[0]) == 3

    def test_cross_lakehouse_incremental(self, project, dest_lakehouse, dest_schema):
        """Read from source lakehouse table, incremental append to destination (schema-enabled)."""
        # First run — create
        results = run_dbt(["run", "--select", "cross_incremental_with_schema"])
        assert len(results) == 1

        # Second run — append
        results = run_dbt(["run", "--select", "cross_incremental_with_schema"])
        assert len(results) == 1

        # Should have 6 rows after two appends
        result = project.run_sql(
            f"select count(*) from {dest_lakehouse}.{dest_schema}.cross_incremental_with_schema",
            fetch="one",
        )
        assert int(result[0]) == 6
