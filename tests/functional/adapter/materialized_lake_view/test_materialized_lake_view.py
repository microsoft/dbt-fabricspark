"""Functional tests for the Materialized Lake View (MLV) materialization.

These tests exercise the full MLV lifecycle — from prerequisite validation
through CREATE OR REPLACE MATERIALIZED LAKE VIEW — against a **real**
schema-enabled lakehouse.

Prerequisites:
  - A schema-enabled lakehouse (the default target in test.env)
  - Fabric Runtime 1.3+ (Apache Spark >= 3.5)
  - ``WORKSPACE_ID``, ``LAKEHOUSE_ID``, ``LAKEHOUSE_NAME`` set in test.env

Run with:
    pytest tests/functional/adapter/materialized_lake_view/ -v --profile az_cli
"""

import os

import pytest

from dbt.tests.util import run_dbt

# ---------------------------------------------------------------------------
# Skip guard — only run on schema-enabled lakehouses
# ---------------------------------------------------------------------------


def _schema_enabled_configured() -> bool:
    """Return True when the target lakehouse appears to be schema-enabled."""
    schema = os.getenv("SCHEMA_NAME", "")
    lakehouse = os.getenv("LAKEHOUSE_NAME", "")
    return bool(schema and lakehouse and schema != lakehouse)


skip_no_schema = pytest.mark.skipif(
    not _schema_enabled_configured(),
    reason="Schema-enabled lakehouse not configured (SCHEMA_NAME == LAKEHOUSE_NAME or not set)",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fq_schema(project) -> str:
    """Return the fully-qualified schema for raw SQL queries.

    In three-part naming mode (schema-enabled), this is ``database.schema``.
    In two-part naming mode, this is just ``schema``.
    """
    db = project.database
    schema = project.test_schema
    if db:
        return f"{db}.{schema}"
    return schema


# ---------------------------------------------------------------------------
# Seed data — simple Delta source tables
# ---------------------------------------------------------------------------

_seeds_csv = """id,name,amount
1,alice,100
2,bob,200
3,charlie,300
""".strip()


# ---------------------------------------------------------------------------
# Source model — a Delta table the MLV will be built on
# ---------------------------------------------------------------------------

_source_table_sql = """
{{ config(
    materialized='table'
) }}

select id, name, amount from {{ ref('mlv_seed') }}
"""


# ---------------------------------------------------------------------------
# MLV model — on-demand refresh (same lakehouse)
# ---------------------------------------------------------------------------

_mlv_on_demand_sql = """
{{ config(
    materialized='materialized_lake_view',
    mlv_on_demand=true,
    mlv_comment='Functional test MLV with on-demand refresh'
) }}

select
    id,
    name,
    amount,
    case
        when amount >= 200 then 'high'
        else 'low'
    end as tier
from {{ ref('mlv_source_table') }}
"""


# ---------------------------------------------------------------------------
# MLV model — scheduled refresh (same lakehouse)
# ---------------------------------------------------------------------------

_mlv_scheduled_sql = """
{{ config(
    materialized='materialized_lake_view',
    mlv_schedule={
        "enabled": true,
        "configuration": {
            "startDateTime": "2026-04-10T00:00:00",
            "endDateTime": "2099-04-10T00:00:00",
            "localTimeZoneId": "Central Standard Time",
            "type": "Daily",
            "times": ["06:00"]
        }
    },
    mlv_comment='Functional test MLV with daily schedule'
) }}

select
    name,
    sum(amount) as total_amount
from {{ ref('mlv_source_table') }}
group by name
"""


# ---------------------------------------------------------------------------
# MLV model — missing refresh config (should fail validation)
# ---------------------------------------------------------------------------

_mlv_no_refresh_sql = """
{{ config(
    materialized='materialized_lake_view'
) }}

select id, name from {{ ref('mlv_source_table') }}
"""


# ---------------------------------------------------------------------------
# MLV model — non-delta source (should fail delta validation)
# An MLV that references a raw view instead of a Delta table.
# ---------------------------------------------------------------------------

_non_delta_view_sql = """
{{ config(
    materialized='view'
) }}

select 1 as id, 'test' as name
"""

_mlv_on_non_delta_sql = """
{{ config(
    materialized='materialized_lake_view',
    mlv_on_demand=true
) }}

select id, name from {{ ref('non_delta_view') }}
"""


# ===========================================================================
# Happy-path test classes — split for xdist parallel distribution
# ===========================================================================


@skip_no_schema
class TestMLVOnDemand:
    """On-demand MLV: creation, data verification, computed columns, idempotency."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "mlv_on_demand_test"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"mlv_seed.csv": _seeds_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "mlv_source_table.sql": _source_table_sql,
            "mlv_on_demand.sql": _mlv_on_demand_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_mlv(self, project):
        """Seed data, create source table, and create on-demand MLV."""
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt(["run", "--select", "mlv_source_table"])
        assert len(results) == 1 and results[0].status == "success"
        results = run_dbt(["run", "--select", "mlv_on_demand"])
        assert len(results) == 1 and results[0].status == "success"

    def test_on_demand_has_data(self, project):
        """The on-demand MLV should contain the expected rows."""
        result = project.run_sql(
            f"select count(*) from {_fq_schema(project)}.mlv_on_demand",
            fetch="one",
        )
        assert int(result[0]) == 3

    def test_on_demand_has_computed_column(self, project):
        """Verify the tier column is computed correctly."""
        result = project.run_sql(
            f"select tier from {_fq_schema(project)}.mlv_on_demand where name = 'alice'",
            fetch="one",
        )
        assert result[0] == "low"  # alice has amount=100

    def test_rerun_is_idempotent(self, project):
        """Running the same MLV again should succeed (CREATE OR REPLACE)."""
        results = run_dbt(["run", "--select", "mlv_on_demand"])
        assert len(results) == 1
        assert results[0].status == "success"


@skip_no_schema
class TestMLVScheduled:
    """Scheduled MLV: creation and data verification."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "mlv_scheduled_test"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"mlv_seed.csv": _seeds_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "mlv_source_table.sql": _source_table_sql,
            "mlv_scheduled.sql": _mlv_scheduled_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_mlv(self, project):
        """Seed data, create source table, and create scheduled MLV."""
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt(["run", "--select", "mlv_source_table"])
        assert len(results) == 1 and results[0].status == "success"
        results = run_dbt(["run", "--select", "mlv_scheduled"])
        assert len(results) == 1 and results[0].status == "success"

    def test_scheduled_has_data(self, project):
        """The scheduled MLV should have aggregated data."""
        result = project.run_sql(
            f"select count(*) from {_fq_schema(project)}.mlv_scheduled",
            fetch="one",
        )
        assert int(result[0]) == 3  # 3 distinct names


@skip_no_schema
class TestMLVFullBuild:
    """Full dbt build including all MLV models."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "mlv_full_build_test"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"mlv_seed.csv": _seeds_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "mlv_source_table.sql": _source_table_sql,
            "mlv_on_demand.sql": _mlv_on_demand_sql,
            "mlv_scheduled.sql": _mlv_scheduled_sql,
        }

    def test_full_build(self, project):
        """dbt build should succeed for the entire project including MLVs."""
        results = run_dbt(["build"])
        for r in results:
            assert r.status in ("success", "pass"), (
                f"Model {r.node.name} failed with status {r.status}"
            )


# ===========================================================================
# Validation failure tests — separate classes to avoid polluting the happy path
# ===========================================================================


@skip_no_schema
class TestMLVMissingRefreshConfig:
    """MLV model without mlv_on_demand or mlv_schedule should fail."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "mlv_no_refresh_test"}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"mlv_seed.csv": _seeds_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "mlv_source_table.sql": _source_table_sql,
            "mlv_no_refresh.sql": _mlv_no_refresh_sql,
        }

    def test_fails_without_refresh_config(self, project):
        """Model should fail because neither mlv_on_demand nor mlv_schedule is set."""
        run_dbt(["seed"])
        run_dbt(["run", "--select", "mlv_source_table"])
        results = run_dbt(["run", "--select", "mlv_no_refresh"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"


@skip_no_schema
class TestMLVNonDeltaSourceValidation:
    """MLV model referencing a non-Delta view should fail delta validation."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "mlv_non_delta_test"}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "non_delta_view.sql": _non_delta_view_sql,
            "mlv_on_non_delta.sql": _mlv_on_non_delta_sql,
        }

    def test_fails_on_non_delta_source(self, project):
        """Model should fail because the upstream source is a view, not a Delta table."""
        run_dbt(["run", "--select", "non_delta_view"])
        results = run_dbt(["run", "--select", "mlv_on_non_delta"], expect_pass=False)
        assert len(results) == 1
        assert results[0].status == "error"
