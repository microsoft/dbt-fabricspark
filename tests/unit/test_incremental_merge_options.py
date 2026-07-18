import re
from unittest import mock

import pytest
from jinja2 import Environment, FileSystemLoader

MACRO_DIR = "src/dbt/include/fabricspark/macros"
STRATEGIES = "materializations/models/incremental/strategies.sql"


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().lower()


def _render(config: dict, unique_key="id", update_columns=None) -> str:
    """Render ``fabricspark__get_merge_sql`` with a dict-backed ``config``."""
    env = Environment(loader=FileSystemLoader(MACRO_DIR), extensions=["jinja2.ext.do"])
    config_mock = mock.Mock()
    config_mock.get.side_effect = lambda key, default=None: config.get(key, default)
    adapter = mock.Mock()
    adapter.get_columns_in_relation.return_value = []
    env.globals.update(
        config=config_mock,
        adapter=adapter,
        sql_header=None,
        get_merge_update_columns=lambda *a, **k: update_columns,
    )
    macro = env.get_template(STRATEGIES).module.fabricspark__get_merge_sql
    return _norm(macro("target_rel", "source_rel", unique_key, None, None))


class TestMergeBackwardCompatible:
    """With no advanced options the merge SQL is unchanged from prior behavior."""

    def test_default_shape(self):
        sql = _render({})
        assert "merge into target_rel as dbt_internal_dest" in sql
        assert "using source_rel as dbt_internal_source" in sql
        assert "on dbt_internal_source.id = dbt_internal_dest.id" in sql
        assert "when matched then update set *" in sql
        assert "when not matched then insert *" in sql

    def test_default_has_no_advanced_clauses(self):
        sql = _render({})
        assert "with schema evolution" not in sql
        assert "not matched by source" not in sql
        assert " and (" not in sql

    def test_update_columns_use_source_alias(self):
        sql = _render({}, update_columns=["a", "b"])
        assert "update set a = dbt_internal_source.a, b = dbt_internal_source.b" in sql


class TestMergeAliases:
    def test_aliases_flow_through_predicates_and_clauses(self):
        sql = _render({"target_alias": "t", "source_alias": "s"})
        assert "merge into target_rel as t" in sql
        assert "using source_rel as s" in sql
        assert "on s.id = t.id" in sql

    def test_aliases_flow_through_update_columns(self):
        sql = _render({"source_alias": "s"}, update_columns=["a"])
        assert "update set a = s.a" in sql

    def test_composite_key_uses_aliases(self):
        sql = _render({"target_alias": "t", "source_alias": "s"}, unique_key=["id", "grp"])
        assert "s.id = t.id and s.grp = t.grp" in sql


class TestMergeConditions:
    def test_matched_condition(self):
        sql = _render({"matched_condition": "s.ts > t.ts"})
        assert "when matched and (s.ts > t.ts) then update set" in sql

    def test_not_matched_condition(self):
        sql = _render({"not_matched_condition": "s.a is not null"})
        assert "when not matched and (s.a is not null) then insert *" in sql

    def test_no_condition_omits_and(self):
        sql = _render({})
        assert "when matched then update set" in sql
        assert "when not matched then insert *" in sql


class TestMergeSkipSteps:
    def test_skip_matched_step(self):
        sql = _render({"skip_matched_step": True})
        assert "when matched" not in sql
        assert "when not matched then insert *" in sql

    def test_skip_not_matched_step(self):
        sql = _render({"skip_not_matched_step": True})
        assert "when matched then update set" in sql
        assert "when not matched then insert" not in sql

    def test_skip_flag_accepts_string(self):
        sql = _render({"skip_matched_step": "true"})
        assert "when matched" not in sql


class TestMergeNotMatchedBySource:
    def test_delete_action(self):
        sql = _render({"not_matched_by_source_action": "delete"})
        assert "when not matched by source then delete" in sql

    def test_update_action(self):
        sql = _render({"not_matched_by_source_action": "update set t.v = -1"})
        assert "when not matched by source then update set t.v = -1" in sql

    def test_action_with_condition(self):
        sql = _render(
            {
                "not_matched_by_source_action": "delete",
                "not_matched_by_source_condition": "t.v > 0",
            }
        )
        assert "when not matched by source and (t.v > 0) then delete" in sql

    def test_no_action_omits_clause(self):
        sql = _render({})
        assert "not matched by source" not in sql

    @pytest.mark.parametrize("action", ["insert", "foo", "  "])
    def test_invalid_action_omits_clause(self, action):
        sql = _render({"not_matched_by_source_action": action})
        assert "not matched by source" not in sql


class TestMergeSchemaEvolution:
    """Schema evolution is enabled via a session conf in the materialization, not a
    SQL clause, so the merge statement itself must never carry ``with schema
    evolution`` (open-source Delta / Fabric reject that syntax)."""

    def test_schema_evolution_does_not_alter_merge_sql(self):
        sql = _render({"merge_with_schema_evolution": True})
        assert "with schema evolution" not in sql
        assert sql.startswith("merge into target_rel")

    def test_schema_evolution_disabled(self):
        sql = _render({"merge_with_schema_evolution": False})
        assert "with schema evolution" not in sql
        assert sql.startswith("merge into target_rel")

    def test_schema_evolution_string_true_does_not_alter_merge_sql(self):
        sql = _render({"merge_with_schema_evolution": "true"})
        assert "with schema evolution" not in sql
        assert sql.startswith("merge into target_rel")


class TestMergeCombined:
    def test_cdc_delete_scenario(self):
        sql = _render(
            {
                "target_alias": "t",
                "source_alias": "s",
                "matched_condition": "s.ts > t.ts",
                "skip_not_matched_step": True,
                "not_matched_by_source_condition": "t.ts < current_timestamp()",
                "not_matched_by_source_action": "delete",
                "merge_with_schema_evolution": True,
            }
        )
        assert "with schema evolution" not in sql
        assert "merge into target_rel as t" in sql
        assert "using source_rel as s" in sql
        assert "when matched and (s.ts > t.ts) then update set" in sql
        assert "when not matched then insert" not in sql
        assert "when not matched by source and (t.ts < current_timestamp()) then delete" in sql
