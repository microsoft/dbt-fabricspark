"""Functional regression for issue #209 — `dbt docs generate` must not
cross-attribute models between lakehouses that share a schema name.

Scenario (mirrors the user's mesh-style report on
https://github.com/microsoft/dbt-fabricspark/issues/209):

  * Two **schema-enabled** Fabric lakehouses are visible to the same dbt
    project. In the orchestrator-provisioned environment those are WS1's
    ``with_schema`` lakehouse (session-bound — the "silver" stand-in) and
    WS2's ``with_schema`` lakehouse (reached via ``workspace_name`` 4-part
    naming — the "gold" stand-in).
  * Both lakehouses host a ``finance`` schema. The dbt project materializes
    one model into each: ``stg_account`` into silver, ``dim_account`` into
    gold.
  * Before the fix, the cache pre-population step in ``dbt docs generate``
    rendered ``SHOW TABLE EXTENDED IN finance LIKE '*'`` against the
    session-bound lakehouse (because ``include_policy.database`` was
    captured before ``connections.open`` finalised schema-mode detection),
    returned silver's tables, and stored them under the gold cache key —
    causing ``DESCRIBE TABLE \\`gold_lh\\`.\\`finance\\`.stg_account`` and
    ``[TABLE_OR_VIEW_NOT_FOUND]``.

The fix is to force database-qualified SHOW SQL whenever three-part naming
is required, and defensively skip mis-attributed cache entries during
``_get_one_catalog``. This test guards against the regression end-to-end.
"""

from __future__ import annotations

import os

import pytest

from dbt.tests.util import run_dbt

# Silver-layer model: schema "finance", materialized in the session-bound
# (WS1) lakehouse. The ``+database=target.lakehouse`` override is required
# so that ``fabricspark__generate_schema_name`` honours the ``+schema=`` value
# (the schema-name routing branches on whether a node has an explicit
# ``database`` config; without one the macro falls back to the target schema
# and the ``+schema=finance`` would be silently dropped — a test-infra quirk,
# not part of the regression).
SILVER_FINANCE_STG_ACCOUNT_SQL = """
{{ config(
    materialized='table',
    schema='finance',
    database=target.lakehouse
) }}

select 1 as account_id, 'cash'        as account_name
union all
select 2 as account_id, 'receivables' as account_name
"""


# Gold-layer model: schema "finance" (intentionally the SAME name as silver's
# schema), materialized into a different lakehouse via ``+database`` override.
# ``workspace_name`` is set so the rendered relation is 4-part — this is the
# only way to reach a lakehouse outside the session's bound workspace.
GOLD_FINANCE_DIM_ACCOUNT_SQL = """
{{ config(
    materialized='table',
    schema='finance',
    database=var('gold_lakehouse_name'),
    workspace_name=var('gold_workspace_name')
) }}

select cast(account_id as int)             as account_id,
       upper(cast(account_name as string)) as account_name
from   {{ ref('stg_account') }}
"""


class TestDocsMultiLakehouseSharedSchema:
    """``dbt docs generate`` must attribute each model to its correct lakehouse,
    even when two lakehouses share a schema name (regression for #209).
    """

    @pytest.fixture(scope="class", autouse=True)
    def _skip_unless_schema_enabled(self, is_schema_enabled):
        if not is_schema_enabled:
            pytest.skip(
                "Multi-lakehouse cross-attribution (issue #209) only manifests in "
                "schema-enabled mode — three-part naming is required to write to "
                "a non-session-bound lakehouse."
            )

    @pytest.fixture(scope="class")
    def project_config_update(self, ws2_workspace_name, ws2_lakehouse_name):
        # Expose the WS2 (gold) lakehouse coordinates as project vars so the
        # gold model's ``config()`` can render the four-part name.
        return {
            "name": "docs_multi_lakehouse_209",
            "vars": {
                "gold_workspace_name": ws2_workspace_name,
                "gold_lakehouse_name": ws2_lakehouse_name,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "stg_account.sql": SILVER_FINANCE_STG_ACCOUNT_SQL,
            "dim_account.sql": GOLD_FINANCE_DIM_ACCOUNT_SQL,
        }

    def test_docs_generate_does_not_cross_attribute_models(
        self,
        project,
        ws2_workspace_name,
        ws2_lakehouse_name,
    ):
        # 1. Materialize both models. Without the fix, this step succeeds —
        #    the bug only shows up during catalog enumeration.
        run_results = run_dbt(["run", "--select", "stg_account", "dim_account"])
        assert len(run_results) == 2, (
            f"expected both models to materialise, got {len(run_results)}"
        )

        # 2. Simulate the cold-start condition from the original bug report
        #    by resetting ``_schemas_enabled`` to its pre-``connections.open``
        #    state. In the test framework the project fixture already opened
        #    a connection (flipping the flag to True early), but in the wild
        #    a user invoking ``dbt docs generate`` starts from a fresh Python
        #    process where the flag is False until the first connection.open
        #    succeeds. The cache pre-population fans out *before* that flip
        #    finalises, locking in stale ``include_policy.database=False`` on
        #    the schema-listing relations — the exact race issue #209 reports.
        #
        #    Clearing the flag here forces the same race window inside the
        #    test, so a regression in the fix surfaces as a catalog error.
        from dbt.adapters.fabricspark.relation import FabricSparkRelation

        original_flag = FabricSparkRelation._schemas_enabled
        FabricSparkRelation._schemas_enabled = False
        try:
            # 3. Run ``dbt docs generate``. Pre-fix this raised
            #    ``[TABLE_OR_VIEW_NOT_FOUND]`` because the cache mis-attributed
            #    silver's ``stg_account`` to the gold lakehouse and the
            #    catalog iterator then DESCRIBE'd it there.
            catalog = run_dbt(["docs", "generate"])
        finally:
            FabricSparkRelation._schemas_enabled = original_flag

        # 4. The catalog object exposes both models, each attributed to its
        #    own lakehouse — no silent drops, no cross-attribution.
        model_keys_by_name = {
            v.metadata.name: (v.metadata.database, v.metadata.schema, k)
            for k, v in catalog.nodes.items()
        }

        silver_lakehouse = project.adapter.config.credentials.lakehouse
        assert "stg_account" in model_keys_by_name, (
            f"silver model missing from catalog: {sorted(model_keys_by_name)}"
        )
        assert "dim_account" in model_keys_by_name, (
            f"gold model missing from catalog (silent drop — see #209): "
            f"{sorted(model_keys_by_name)}"
        )

        stg_db, stg_schema, _ = model_keys_by_name["stg_account"]
        dim_db, dim_schema, _ = model_keys_by_name["dim_account"]

        assert stg_db.casefold() == silver_lakehouse.casefold(), (
            f"silver model attributed to wrong lakehouse: expected={silver_lakehouse} got={stg_db}"
        )
        assert stg_schema.casefold() == "finance"
        assert dim_db.casefold() == ws2_lakehouse_name.casefold(), (
            f"gold model attributed to wrong lakehouse: expected={ws2_lakehouse_name} got={dim_db}"
        )
        assert dim_schema.casefold() == "finance"

        # 5. Belt-and-suspenders: parse the on-disk catalog.json and confirm
        #    the artifact has no ``errors`` list (older dbt-core versions
        #    surface catalog failures here even when ``run_dbt`` succeeds).
        import json

        catalog_path = os.path.join(project.project_root, "target", "catalog.json")
        with open(catalog_path) as f:
            artifact = json.load(f)
        assert not artifact.get("errors"), (
            "docs generate reported catalog errors — issue #209 has regressed:\n"
            f"{artifact.get('errors')}"
        )
