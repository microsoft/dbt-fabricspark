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


# Cross-workspace WRITE is supported by Fabric Livy: a 4-part DDL target
# (e.g. ``CREATE TABLE \`WS2\`.\`lh\`.\`schema\`.t AS SELECT …``) executes
# successfully, and ``CREATE DATABASE IF NOT EXISTS \`WS2\`.\`lh\`.\`schema\```
# is also supported, so the adapter creates the target schema automatically
# via the standard ``fabricspark__create_schema`` flow before materializing.
# The ``TestCrossWorkspace4PartWriteCTAS`` class below covers the happy path
# end-to-end.


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


# ---------------------------------------------------------------------------
# Positive: cross-workspace WRITE (CTAS) into a schema pre-created in WS2
# ---------------------------------------------------------------------------


# A model whose physical relation lives in WS2 — materialized into the
# pre-created ``cross_ws_write`` schema in WS2's lakehouse from a Livy session
# that is bound to WS1. Idempotency relies on ``file_format='delta'`` so the
# adapter emits ``CREATE OR REPLACE TABLE`` (the default ``parquet`` path
# would otherwise fail on the second run because ``adapter.get_relation`` is
# not workspace-aware and reports no existing relation).
_WS2_WRITE_SCHEMA = "cross_ws_write"


CROSS_WS_WRITE_TABLE_SQL = """
{{ config(
    materialized='table',
    file_format='delta',
    workspace_name=var('ws2_workspace_name'),
    database=var('ws2_lakehouse_name'),
    schema=var('ws2_write_schema')
) }}

select 1 as id, 'alpha' as name, cast(10.5 as double) as price
union all
select 2 as id, 'beta'  as name, cast(20.0 as double) as price
union all
select 3 as id, 'gamma' as name, cast(30.25 as double) as price
union all
select 4 as id, 'delta' as name, cast(40.75 as double) as price
"""


class TestCrossWorkspace4PartWriteCTAS:
    """End-to-end cross-workspace WRITE via ``CREATE TABLE AS SELECT``.

    Flow:
      1. The model ``cross_ws_write_target`` is configured with
         ``workspace_name=WS2``, ``database=WS2_LH``,
         ``schema=cross_ws_write`` (a schema that does **not** exist in WS2
         beforehand), ``materialized=table``, ``file_format=delta``.
      2. ``dbt run`` issues an in-workspace ``CREATE DATABASE IF NOT EXISTS
         \\`WS2\\`.\\`WS2_LH\\`.\\`cross_ws_write\\``` from WS1's Livy session
         (Fabric Livy supports cross-workspace CREATE DATABASE), then a
         ``CREATE OR REPLACE TABLE \\`WS2\\`.\\`WS2_LH\\`.\\`cross_ws_write\\`.cross_ws_write_target AS SELECT …``
         to materialize the model.
      3. Test verifies the table now exists in WS2 by issuing a 4-part
         SELECT and counting rows / summing the price column.
      4. A second ``dbt run`` validates idempotency — ``CREATE OR REPLACE
         TABLE`` re-materializes cleanly.
      5. Cleanup: the post-test workspace nuke wipes WS2.
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
        return {
            "name": "cross_workspace_4part_write",
            "vars": {
                "ws2_workspace_name": ws2_workspace_name,
                "ws2_lakehouse_name": ws2_lakehouse_name,
                "ws2_write_schema": _WS2_WRITE_SCHEMA,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cross_ws_write_target.sql": CROSS_WS_WRITE_TABLE_SQL,
        }

    def test_cross_workspace_write_renders_four_part(
        self, project, ws2_workspace_name, ws2_lakehouse_name
    ):
        # Compile only — assert the model parses and resolves to the
        # WS2-bound relation. The actual 4-part CTAS rendering is covered
        # end-to-end by ``test_cross_workspace_write_executes`` below
        # (a wrong target would land the table in the wrong workspace and
        # the post-run SELECT would fail).
        compile_results = run_dbt(["compile", "--select", "cross_ws_write_target"])
        assert len(compile_results) == 1
        node = compile_results[0].node
        # ``relation_name`` is the rendered relation dbt would issue DDL
        # against — for a workspace-tagged model this is the 4-part name.
        rendered_target = node.relation_name or ""
        assert f"`{ws2_workspace_name}`" in rendered_target, (
            f"expected WS2 name in rendered relation, got: {rendered_target}"
        )
        assert f"`{ws2_lakehouse_name}`" in rendered_target, (
            f"expected WS2 lakehouse in rendered relation, got: {rendered_target}"
        )
        assert f"`{_WS2_WRITE_SCHEMA}`" in rendered_target, (
            f"expected WS2 write schema in rendered relation, got: {rendered_target}"
        )

    def test_cross_workspace_write_executes(
        self,
        project,
        ws2_workspace_name,
        ws2_lakehouse_name,
    ):
        # First run — adapter creates the cross_ws_write schema in WS2 (via
        # `CREATE DATABASE IF NOT EXISTS \`WS2\`.\`WS2_LH\`.cross_ws_write`)
        # and materializes the model into it (CTAS) from WS1's Livy session.
        run_results = run_dbt(["run", "--select", "cross_ws_write_target"])
        assert len(run_results) == 1, f"expected 1 run result, got {len(run_results)}"

        # Verify the table now exists in WS2 and has the expected rows.
        # Issue a raw 4-part SELECT against the WS1-bound Livy session;
        # Fabric Livy resolves the federated SELECT through WS2's catalog.
        sql = (
            "select count(*) as n, sum(price) as total "
            f"from `{ws2_workspace_name}`.`{ws2_lakehouse_name}`."
            f"`{_WS2_WRITE_SCHEMA}`.cross_ws_write_target"
        )
        rows = project.run_sql(sql, fetch="all")
        assert len(rows) == 1
        n, total = rows[0]
        assert int(n) == 4, f"expected 4 rows in WS2 cross-workspace target, got {n}"
        assert abs(float(total) - (10.5 + 20.0 + 30.25 + 40.75)) < 1e-6, (
            f"price sum mismatch in WS2 target: {total}"
        )

    def test_cross_workspace_write_is_idempotent(
        self,
        project,
        ws2_workspace_name,
        ws2_lakehouse_name,
    ):
        # Re-materialize. Because the model uses file_format='delta', the
        # adapter emits CREATE OR REPLACE TABLE which succeeds against the
        # existing 4-part relation in WS2. The pre-run schema creation
        # (`CREATE DATABASE IF NOT EXISTS …`) is also idempotent.
        run_results = run_dbt(["run", "--select", "cross_ws_write_target"])
        assert len(run_results) == 1

        sql = (
            "select count(*) as n "
            f"from `{ws2_workspace_name}`.`{ws2_lakehouse_name}`."
            f"`{_WS2_WRITE_SCHEMA}`.cross_ws_write_target"
        )
        rows = project.run_sql(sql, fetch="all")
        assert int(rows[0][0]) == 4, (
            "expected the WS2 cross-workspace target to still hold 4 rows after "
            "an idempotent re-run"
        )


# ---------------------------------------------------------------------------
# Positive: cross-workspace WRITE via incremental materialization
# (initial CTAS + subsequent MERGE INTO across the workspace boundary)
# ---------------------------------------------------------------------------


# Incremental model whose physical relation lives in WS2. The body is
# toggled by ``is_incremental()``:
#   * First run / full-refresh → 4 rows (id 1-4).
#   * Subsequent incremental runs → 2 new rows (id 5-6) that get
#     MERGE-INTO-ed via the unique_key.
# After the first run the target table holds 4 rows; after the second
# run it holds 6 rows. After ``--full-refresh`` it's back to 4 rows.
#
# ``file_format='delta'`` is required for ``incremental_strategy='merge'``
# to work and for the full-refresh path to emit ``CREATE OR REPLACE TABLE``.
CROSS_WS_WRITE_INCREMENTAL_SQL = """
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    file_format='delta',
    workspace_name=var('ws2_workspace_name'),
    database=var('ws2_lakehouse_name'),
    schema=var('ws2_write_schema')
) }}

{% if is_incremental() %}
  select 5 as id, 'epsilon' as name, cast(50.5 as double) as price union all
  select 6 as id, 'zeta'    as name, cast(60.0 as double) as price
{% else %}
  select 1 as id, 'alpha' as name, cast(10.5 as double) as price union all
  select 2 as id, 'beta'  as name, cast(20.0 as double) as price union all
  select 3 as id, 'gamma' as name, cast(30.25 as double) as price union all
  select 4 as id, 'delta' as name, cast(40.75 as double) as price
{% endif %}
"""


class TestCrossWorkspace4PartWriteIncremental:
    """End-to-end cross-workspace WRITE via incremental materialization.

    Validates that ``incremental_strategy='merge'`` against a 4-part
    relation works for both the first-run CTAS and subsequent MERGE INTO,
    and that ``--full-refresh`` correctly drops + recreates the
    cross-workspace target.

    Flow:
      1. First ``dbt run``: ``is_incremental()`` is False, so the model
         emits a 4-row SELECT and the materialization takes the
         ``existing_relation is none`` branch → ``CREATE TABLE AS SELECT``.
         Same cross-workspace path as ``TestCrossWorkspace4PartWriteCTAS``,
         but routed through the incremental materialization.
      2. Second ``dbt run`` (no flag): ``is_incremental()`` is True, the
         body returns 2 new rows, and the materialization issues a
         ``MERGE INTO \\`WS2\\`.\\`lh\\`.\\`schema\\`.\\`cross_ws_write_target_inc\\``` against
         a staging view (also created cross-workspace). Result: 6 rows.
      3. ``dbt run --full-refresh``: drops the existing relation and
         re-runs the first-run path → 4 rows again.
      4. The cross_ws_write schema in WS2 is auto-created by the adapter
         on first-run pre-pass (shared with TestCrossWorkspace4PartWriteCTAS
         since both classes use the same schema). Cleanup is handled by
         the post-test workspace nuke.
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
        return {
            "name": "cross_workspace_4part_write_incremental",
            "vars": {
                "ws2_workspace_name": ws2_workspace_name,
                "ws2_lakehouse_name": ws2_lakehouse_name,
                "ws2_write_schema": _WS2_WRITE_SCHEMA,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cross_ws_write_target_inc.sql": CROSS_WS_WRITE_INCREMENTAL_SQL,
        }

    def _count_rows(self, project, ws2_workspace_name, ws2_lakehouse_name) -> int:
        sql = (
            "select count(*) as n "
            f"from `{ws2_workspace_name}`.`{ws2_lakehouse_name}`."
            f"`{_WS2_WRITE_SCHEMA}`.cross_ws_write_target_inc"
        )
        rows = project.run_sql(sql, fetch="all")
        return int(rows[0][0])

    def test_renders_four_part(self, project, ws2_workspace_name, ws2_lakehouse_name):
        compile_results = run_dbt(["compile", "--select", "cross_ws_write_target_inc"])
        assert len(compile_results) == 1
        node = compile_results[0].node
        rendered_target = node.relation_name or ""
        assert f"`{ws2_workspace_name}`" in rendered_target, (
            f"expected WS2 name in rendered relation, got: {rendered_target}"
        )
        assert f"`{ws2_lakehouse_name}`" in rendered_target, (
            f"expected WS2 lakehouse in rendered relation, got: {rendered_target}"
        )
        assert f"`{_WS2_WRITE_SCHEMA}`" in rendered_target, (
            f"expected WS2 write schema in rendered relation, got: {rendered_target}"
        )

    def test_first_run_creates_table_with_4_rows(
        self, project, ws2_workspace_name, ws2_lakehouse_name
    ):
        # First run goes through the incremental materialization's
        # ``existing_relation is none`` branch → CTAS.
        run_results = run_dbt(["run", "--select", "cross_ws_write_target_inc"])
        assert len(run_results) == 1

        n = self._count_rows(project, ws2_workspace_name, ws2_lakehouse_name)
        assert n == 4, f"expected 4 rows after initial cross-workspace incremental CTAS, got {n}"

    def test_second_run_merges_2_new_rows(self, project, ws2_workspace_name, ws2_lakehouse_name):
        # Second run: is_incremental() is True, body emits ids 5+6,
        # MERGE INTO targets the cross-workspace 4-part relation.
        run_results = run_dbt(["run", "--select", "cross_ws_write_target_inc"])
        assert len(run_results) == 1

        n = self._count_rows(project, ws2_workspace_name, ws2_lakehouse_name)
        assert n == 6, (
            "expected 6 rows after cross-workspace incremental MERGE INTO "
            f"(4 original + 2 new), got {n}"
        )

    def test_full_refresh_resets_table(self, project, ws2_workspace_name, ws2_lakehouse_name):
        # --full-refresh drops the existing relation and re-runs the
        # first-run path. Validates the cross-workspace DROP + CTAS path
        # in the incremental materialization.
        run_results = run_dbt(["run", "--select", "cross_ws_write_target_inc", "--full-refresh"])
        assert len(run_results) == 1

        n = self._count_rows(project, ws2_workspace_name, ws2_lakehouse_name)
        assert n == 4, (
            "expected 4 rows after cross-workspace --full-refresh "
            f"(reset to first-run body), got {n}"
        )


# ---------------------------------------------------------------------------
# Positive: cross-workspace WRITE via VIEW materialization (issue #172)
# ---------------------------------------------------------------------------


CROSS_WS_WRITE_VIEW_SQL = """
{{ config(
    materialized='view',
    workspace_name=var('ws2_workspace_name'),
    database=var('ws2_lakehouse_name'),
    schema=var('ws2_write_schema')
) }}

select 1 as id, 'alpha' as name, cast(10.5 as double) as price
union all
select 2 as id, 'beta'  as name, cast(20.0 as double) as price
union all
select 3 as id, 'gamma' as name, cast(30.25 as double) as price
union all
select 4 as id, 'delta' as name, cast(40.75 as double) as price
"""


class TestCrossWorkspace4PartWriteView:
    @pytest.fixture(scope="class", autouse=True)
    def _skip_unless_schema_enabled(self, is_schema_enabled):
        if not is_schema_enabled:
            pytest.skip(
                "Cross-workspace 4-part naming is only supported on schema-enabled "
                "lakehouses (Fabric Livy limitation)."
            )

    @pytest.fixture(scope="class")
    def project_config_update(self, ws2_workspace_name, ws2_lakehouse_name):
        return {
            "name": "cross_workspace_4part_write_view",
            "vars": {
                "ws2_workspace_name": ws2_workspace_name,
                "ws2_lakehouse_name": ws2_lakehouse_name,
                "ws2_write_schema": _WS2_WRITE_SCHEMA,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cross_ws_write_view.sql": CROSS_WS_WRITE_VIEW_SQL,
        }

    def test_cross_workspace_view_renders_four_part(
        self, project, ws2_workspace_name, ws2_lakehouse_name
    ):
        compile_results = run_dbt(["compile", "--select", "cross_ws_write_view"])
        assert len(compile_results) == 1
        node = compile_results[0].node
        rendered_target = node.relation_name or ""
        assert f"`{ws2_workspace_name}`" in rendered_target, (
            f"expected WS2 name in rendered relation, got: {rendered_target}"
        )
        assert f"`{ws2_lakehouse_name}`" in rendered_target, (
            f"expected WS2 lakehouse in rendered relation, got: {rendered_target}"
        )
        assert f"`{_WS2_WRITE_SCHEMA}`" in rendered_target, (
            f"expected WS2 write schema in rendered relation, got: {rendered_target}"
        )

    def test_cross_workspace_view_executes(
        self,
        project,
        ws2_workspace_name,
        ws2_lakehouse_name,
    ):
        run_results = run_dbt(["run", "--select", "cross_ws_write_view"])
        assert len(run_results) == 1, f"expected 1 run result, got {len(run_results)}"

        sql = (
            "select count(*) as n, sum(price) as total "
            f"from `{ws2_workspace_name}`.`{ws2_lakehouse_name}`."
            f"`{_WS2_WRITE_SCHEMA}`.cross_ws_write_view"
        )
        rows = project.run_sql(sql, fetch="all")
        assert len(rows) == 1
        n, total = rows[0]
        assert int(n) == 4, f"expected 4 rows in WS2 cross-workspace view, got {n}"
        assert abs(float(total) - (10.5 + 20.0 + 30.25 + 40.75)) < 1e-6, (
            f"price sum mismatch in WS2 view: {total}"
        )

    def test_cross_workspace_view_is_idempotent(
        self,
        project,
        ws2_workspace_name,
        ws2_lakehouse_name,
    ):
        run_results = run_dbt(["run", "--select", "cross_ws_write_view"])
        assert len(run_results) == 1

        sql = (
            "select count(*) as n "
            f"from `{ws2_workspace_name}`.`{ws2_lakehouse_name}`."
            f"`{_WS2_WRITE_SCHEMA}`.cross_ws_write_view"
        )
        rows = project.run_sql(sql, fetch="all")
        assert int(rows[0][0]) == 4, (
            "expected the WS2 cross-workspace view to still resolve to 4 rows "
            "after an idempotent re-run"
        )


# ---------------------------------------------------------------------------
# Positive: cross-workspace WRITE via SNAPSHOT materialization (issue #172)
# ---------------------------------------------------------------------------


CROSS_WS_SNAPSHOT_SOURCE_SQL = """
{{ config(materialized='table', file_format='delta') }}

{% set bump = var('snap_source_bump', 0) %}

select 1 as id, 'alpha' as name, cast(10.5 as double)  as price union all
select 2 as id, 'beta'  as name, cast(20.0 as double)  as price union all
select 3 as id, 'gamma' as name, cast(30.25 as double) as price union all
select 4 as id, 'delta_v{{ bump }}' as name, cast(40.75 as double) as price
"""


CROSS_WS_SNAPSHOT_SQL = """
{% snapshot cross_ws_write_snapshot %}
    {{ config(
        strategy='check',
        unique_key='id',
        check_cols=['name'],
        file_format='delta',
        workspace_name=var('ws2_workspace_name'),
        target_database=var('ws2_lakehouse_name'),
        target_schema=var('ws2_write_schema')
    ) }}
    select * from {{ ref('cross_ws_snapshot_source') }}
{% endsnapshot %}
"""


class TestCrossWorkspace4PartWriteSnapshot:
    @pytest.fixture(scope="class", autouse=True)
    def _skip_unless_schema_enabled(self, is_schema_enabled):
        if not is_schema_enabled:
            pytest.skip(
                "Cross-workspace 4-part naming is only supported on schema-enabled "
                "lakehouses (Fabric Livy limitation)."
            )

    @pytest.fixture(scope="class")
    def project_config_update(self, ws2_workspace_name, ws2_lakehouse_name):
        return {
            "name": "cross_workspace_4part_write_snapshot",
            "vars": {
                "ws2_workspace_name": ws2_workspace_name,
                "ws2_lakehouse_name": ws2_lakehouse_name,
                "ws2_write_schema": _WS2_WRITE_SCHEMA,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "cross_ws_snapshot_source.sql": CROSS_WS_SNAPSHOT_SOURCE_SQL,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "cross_ws_write_snapshot.sql": CROSS_WS_SNAPSHOT_SQL,
        }

    def _count_rows(self, project, ws2_workspace_name, ws2_lakehouse_name) -> int:
        sql = (
            "select count(*) as n "
            f"from `{ws2_workspace_name}`.`{ws2_lakehouse_name}`."
            f"`{_WS2_WRITE_SCHEMA}`.cross_ws_write_snapshot"
        )
        rows = project.run_sql(sql, fetch="all")
        return int(rows[0][0])

    def test_snapshot_renders_four_part(self, project, ws2_workspace_name, ws2_lakehouse_name):
        compile_results = run_dbt(["compile", "--select", "cross_ws_write_snapshot"])
        snapshot_results = [r for r in compile_results if r.node.name == "cross_ws_write_snapshot"]
        assert len(snapshot_results) == 1, (
            f"expected exactly 1 snapshot compile result, got {len(snapshot_results)}"
        )
        node = snapshot_results[0].node
        rendered_target = node.relation_name or ""
        assert f"`{ws2_workspace_name}`" in rendered_target, (
            f"expected WS2 name in rendered relation, got: {rendered_target}"
        )
        assert f"`{ws2_lakehouse_name}`" in rendered_target, (
            f"expected WS2 lakehouse in rendered relation, got: {rendered_target}"
        )
        assert f"`{_WS2_WRITE_SCHEMA}`" in rendered_target, (
            f"expected WS2 write schema in rendered relation, got: {rendered_target}"
        )

    def test_snapshot_first_run_ctas_into_ws2(
        self, project, ws2_workspace_name, ws2_lakehouse_name
    ):
        run_results = run_dbt(["run", "--select", "cross_ws_snapshot_source"])
        assert len(run_results) == 1

        snap_results = run_dbt(["snapshot", "--select", "cross_ws_write_snapshot"])
        assert len(snap_results) == 1

        n = self._count_rows(project, ws2_workspace_name, ws2_lakehouse_name)
        assert n == 4, f"expected 4 rows after initial cross-workspace snapshot CTAS, got {n}"

    def test_snapshot_second_run_merges_into_ws2(
        self, project, ws2_workspace_name, ws2_lakehouse_name
    ):
        run_results = run_dbt(
            [
                "run",
                "--select",
                "cross_ws_snapshot_source",
                "--vars",
                "{snap_source_bump: 1}",
            ]
        )
        assert len(run_results) == 1

        snap_results = run_dbt(["snapshot", "--select", "cross_ws_write_snapshot"])
        assert len(snap_results) == 1

        n = self._count_rows(project, ws2_workspace_name, ws2_lakehouse_name)
        assert n == 5, (
            "expected 5 SCD2 rows in WS2 after MERGE INTO "
            f"(4 original incl. 1 closed-out + 1 new current), got {n}"
        )

        scd_sql = (
            "select dbt_valid_to is null as is_current, count(*) as n "
            f"from `{ws2_workspace_name}`.`{ws2_lakehouse_name}`."
            f"`{_WS2_WRITE_SCHEMA}`.cross_ws_write_snapshot "
            "where id = 4 group by dbt_valid_to is null"
        )
        scd_rows = {bool(r[0]): int(r[1]) for r in project.run_sql(scd_sql, fetch="all")}
        assert scd_rows.get(True) == 1, (
            f"expected exactly 1 current row for id=4 in WS2 snapshot, got {scd_rows}"
        )
        assert scd_rows.get(False) == 1, (
            f"expected exactly 1 historical (closed-out) row for id=4 in WS2 snapshot, "
            f"got {scd_rows}"
        )
