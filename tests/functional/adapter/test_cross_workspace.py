"""Functional tests for cross-workspace 4-part naming.

These tests run as part of the ``with_schema`` orchestrator pass — Fabric Livy
only supports 4-part naming against schema-enabled lakehouses, so cross-workspace
tests are inherently a schema-enabled-only feature.

Setup contract (driven by the orchestrator):
  1. WS1 has a ``with_schema`` lakehouse (the test write target). Provided
     by the existing ``provision --schema-mode with_schema`` task.
  2. WS2 has a ``with_schema`` lakehouse seeded with ``cross_ws_fixture``
     (4 rows). Provided by the
     ``provision --schema-mode with_schema --workspace ws2`` and ``seed-ws2``
     tasks. The seed CSV is ``tests/functional/fixtures/ws2_seed/seeds/cross_ws_fixture.csv``.
  3. The Livy session for the test runs **in WS1** (the profile target). 4-part
     naming is what bridges the boundary.

All four multi-workspace env vars (``WORKSPACE_ID_1``, ``WORKSPACE_NAME_1``,
``WORKSPACE_ID_2``, ``WORKSPACE_NAME_2``) plus the orchestrator-populated
``WS2_LAKEHOUSE_*`` vars are required; the corresponding fixtures raise
``RuntimeError`` if any are missing.
"""

from __future__ import annotations

import pytest

from dbt.adapters.fabricspark.relation import FabricSparkRelation
from dbt.tests.util import run_dbt

# ---------------------------------------------------------------------------
# Models used by both positive and negative tests
# ---------------------------------------------------------------------------

# A "stub" model that *represents* the WS2 fixture seed table from WS1's
# project. It is never selected for materialization in these tests; only its
# resolved relation (with workspace_name + database + schema + alias) is used.
# We set ``alias='cross_ws_fixture'`` so ``ref()`` resolves to the seed's
# physical identifier in WS2.
CROSS_WS_STUB_SQL = """
{{ config(
    materialized='view',
    workspace_name=var('ws2_workspace_name'),
    database=var('ws2_lakehouse_name'),
    schema='dbo',
    alias='cross_ws_fixture'
) }}

-- This SQL is never executed because the model is never selected for
-- materialization in the cross-workspace tests. It exists solely to provide
-- a manifest node whose resolved relation points at WS2.
select cast(null as int) as id, cast(null as string) as name, cast(null as double) as price
"""


# A consumer model in WS1 that reads the WS2 fixture via ``ref()``. dbt-core
# resolves the ref to a 4-part ``\`WS2\`.\`WS2_LH\`.\`dbo\`.cross_ws_fixture``
# relation (because the upstream stub has ``workspace_name`` set), and Fabric
# Livy's WS1 session executes the federated SELECT against WS2's catalog.
CROSS_WS_CONSUMER_SQL = """
{{ config(materialized='table') }}

select id, name, price
from {{ ref('cross_ws_stub') }}
order by id
"""


# Cross-workspace WRITE is **not** supported by Fabric Livy: a 4-part DDL
# target (e.g. ``CREATE OR REPLACE VIEW \`WS2\`.\`lh\`.\`dbo\`.t``) returns
# ``Artifact not found`` because Fabric Livy resolves the DDL target inside
# the session's bound workspace regardless of the prefix. 4-part naming is
# therefore a READ-ONLY feature (federated SELECT) and we don't ship a WRITE
# round-trip test.


# Negative-test model: setting ``workspace_name`` against a non-schema-enabled
# write target must raise a parse-time ``DbtRuntimeError``.
CROSS_WS_INVALID_NO_SCHEMA_SQL = """
{{ config(
    materialized='view',
    workspace_name='other_ws',
    database='other_lh'
) }}

select 1 as id
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Positive: cross-workspace ref() resolution + execution
# ---------------------------------------------------------------------------


class TestCrossWorkspace4PartReadViaRef:
    """End-to-end cross-workspace read via ``ref()`` to a workspace_name-tagged stub.

    Flow:
      1. Orchestrator pre-seeds WS2's lakehouse with ``cross_ws_fixture`` (4 rows).
      2. WS1's project defines:
         - ``cross_ws_stub`` — a stub with workspace_name=WS2, alias=cross_ws_fixture.
         - ``cross_ws_consumer`` — a table that selects from ``ref('cross_ws_stub')``.
      3. Test invokes ``dbt run --select cross_ws_consumer`` (no ``+``) so dbt-core
         only materializes the consumer; the upstream stub is never created.
         dbt-core's schema pre-creation only fires for selected nodes, so we
         never attempt ``CREATE DATABASE`` against WS2 from WS1's session.
      4. ``ref('cross_ws_stub')`` resolves to a 4-part relation; the consumer's
         SELECT is rendered as ``select * from \\`WS2\\`.\\`WS2_LH\\`.\\`dbo\\`.cross_ws_fixture``.
      5. Asserts the consumer table has 4 rows matching the seed.
    """

    @pytest.fixture(scope="class", autouse=True)
    def _skip_unless_schema_enabled(self, is_schema_enabled):
        if not is_schema_enabled:
            pytest.skip(
                "Cross-workspace 4-part naming is only supported on schema-enabled "
                "lakehouses (Fabric Livy limitation)."
            )

    @pytest.fixture(scope="class")
    def project_config_update(self, ws2_workspace_name, ws2_lakehouse_name):
        # Inject WS2 details as project vars so the model SQL can resolve them
        # at parse time (env_var would also work; vars are easier in tests).
        return {
            "name": "cross_workspace_4part",
            "vars": {
                "ws2_workspace_name": ws2_workspace_name,
                "ws2_lakehouse_name": ws2_lakehouse_name,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cross_ws_stub.sql": CROSS_WS_STUB_SQL,
            "cross_ws_consumer.sql": CROSS_WS_CONSUMER_SQL,
        }

    def test_cross_workspace_ref_renders_four_part(
        self, project, ws2_workspace_name, ws2_lakehouse_name
    ):
        # Compile (no execution) and verify the rendered SQL contains a 4-part
        # reference to WS2. This guards rendering even if Fabric Livy execution
        # later changes behavior.
        compile_results = run_dbt(["compile", "--select", "cross_ws_consumer"])
        assert len(compile_results) == 1
        compiled_sql = compile_results[0].node.compiled_code or ""
        assert f"`{ws2_workspace_name}`" in compiled_sql, (
            "Expected workspace name backtick-quoted in compiled SQL.\n"
            f"Compiled SQL:\n{compiled_sql}"
        )
        assert f"`{ws2_lakehouse_name}`" in compiled_sql, (
            "Expected WS2 lakehouse name backtick-quoted in compiled SQL.\n"
            f"Compiled SQL:\n{compiled_sql}"
        )
        assert "`dbo`.cross_ws_fixture" in compiled_sql, (
            f"Expected schema and identifier in compiled SQL.\nCompiled SQL:\n{compiled_sql}"
        )

    def test_cross_workspace_ref_executes(self, project):
        # Run only the consumer; the upstream stub is intentionally unselected
        # so dbt does not try to materialize it (or pre-create its schema in WS2).
        run_results = run_dbt(["run", "--select", "cross_ws_consumer"])
        assert len(run_results) == 1
        # Verify the consumer table has the 4 seeded rows. Fabric Livy returns
        # numeric aggregates as strings, so cast before comparing — the existing
        # functional tests follow the same pattern.
        sql = "select count(*) as n, sum(price) as total from {schema}.cross_ws_consumer"
        rows = project.run_sql(sql, fetch="all")
        assert len(rows) == 1
        n, total = rows[0]
        assert int(n) == 4, f"expected 4 rows from cross-workspace seed, got {n}"
        assert abs(float(total) - (10.5 + 20.0 + 30.25 + 40.75)) < 1e-6, (
            f"price sum mismatch: {total}"
        )


# ---------------------------------------------------------------------------
# Negative: workspace_name on a non-schema-enabled write target is rejected
# ---------------------------------------------------------------------------


class TestCrossWorkspaceRejectedInNoSchemaMode:
    """In ``no_schema`` mode, setting ``workspace_name`` raises at parse time.

    This test is a ``no_schema``-only counterpart and is skipped in
    ``with_schema`` mode. It uses ``dbt parse`` (not ``run``) so no Livy
    interaction is required for the assertion.
    """

    @pytest.fixture(scope="class", autouse=True)
    def _skip_unless_no_schema(self, is_schema_enabled):
        if is_schema_enabled:
            pytest.skip(
                "Negative test for non-schema-enabled mode — runs only in `no_schema` "
                "orchestrator pass."
            )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cross_ws_invalid.sql": CROSS_WS_INVALID_NO_SCHEMA_SQL,
        }

    def test_workspace_name_rejected_when_target_is_not_schema_enabled(self, project):
        # ``run_dbt(['parse'])`` triggers ``generate_database_name`` for every
        # node, which is where our validation hook lives. The Jinja-side
        # ``adapter.validate_workspace_name_supported`` raises a
        # ``DbtRuntimeError`` mentioning the workspace_name + schema-enabled
        # requirement; ``run_dbt`` re-raises it directly.
        with pytest.raises(Exception) as ctx:
            run_dbt(["parse"])
        msg = str(ctx.value)
        assert "workspace_name" in msg, (
            f"expected error message to mention 'workspace_name', got: {msg}"
        )
        assert "schema-enabled" in msg, (
            f"expected error message to mention 'schema-enabled', got: {msg}"
        )


# ---------------------------------------------------------------------------
# Adapter-level smoke: rendering at the relation layer
# ---------------------------------------------------------------------------


class TestCrossWorkspaceRelationRenderingSmoke:
    """Adapter-layer smoke that doesn't require WS2 to be provisioned.

    Verifies that even within the existing functional `with_schema` pass —
    where the runtime ``_schemas_enabled`` flag is True — the FabricSparkRelation
    correctly renders a workspace-bearing relation as a 4-part SQL fragment.
    """

    @pytest.fixture(scope="class", autouse=True)
    def _skip_unless_schema_enabled(self, is_schema_enabled):
        if not is_schema_enabled:
            pytest.skip(
                "Smoke test runs in with_schema pass only — workspace_name is invalid "
                "in non-schema-enabled lakehouses."
            )

    @pytest.fixture(scope="class")
    def models(self):
        # Minimal model so the project fixture builds — never selected.
        return {"_smoke_placeholder.sql": "select 1 as id"}

    def test_relation_renders_four_part(self, project):
        rel = FabricSparkRelation.create(
            database="ws2_lakehouse",
            schema="dbo",
            identifier="some_table",
            workspace="My WS 2",
        )
        rendered = str(rel)
        assert rendered == "`My WS 2`.`ws2_lakehouse`.`dbo`.some_table", (
            f"unexpected 4-part rendering: {rendered}"
        )
