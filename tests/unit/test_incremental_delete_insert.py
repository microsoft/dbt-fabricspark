import re
from unittest import mock

import pytest
from jinja2 import Environment, FileSystemLoader

MACRO_DIR = "src/dbt/include/fabricspark/macros"
STRATEGIES = "materializations/models/incremental/strategies.sql"
VALIDATE = "materializations/models/incremental/validate.sql"


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().lower()


def _env() -> Environment:
    return Environment(loader=FileSystemLoader(MACRO_DIR), extensions=["jinja2.ext.do"])


class TestDeleteInsertDeleteSql:
    """Rendered SQL for the ``delete+insert`` DELETE step (MERGE ... WHEN MATCHED THEN DELETE)."""

    def _render(self, unique_key, predicates=None) -> str:
        macro = _env().get_template(STRATEGIES).module.get_delete_insert_delete_sql
        return _norm(macro("tmp_rel", "target_rel", unique_key, predicates))

    def test_single_key(self):
        sql = self._render("id")
        assert "merge into target_rel as dbt_internal_dest" in sql
        assert "select distinct id from tmp_rel" in sql
        assert "dbt_internal_dest.id = dbt_internal_source.id" in sql
        assert "when matched then delete" in sql

    def test_composite_key(self):
        sql = self._render(["id", "grp"])
        assert "select distinct id, grp from tmp_rel" in sql
        assert (
            "dbt_internal_dest.id = dbt_internal_source.id "
            "and dbt_internal_dest.grp = dbt_internal_source.grp"
        ) in sql

    def test_predicate_string(self):
        sql = self._render("id", "dbt_internal_dest.dt > '2020-01-01'")
        assert "and dbt_internal_dest.dt > '2020-01-01'" in sql

    def test_predicate_list(self):
        sql = self._render("id", ["a = 1", "b = 2"])
        assert "and a = 1" in sql
        assert "and b = 2" in sql

    def test_no_predicate_omits_extra_and(self):
        sql = self._render("id")
        # only the key match remains after the ON, no dangling predicate clause
        assert sql.count(" and ") == 0


class TestValidateDeleteInsert:
    """``dbt_spark_validate_get_incremental_strategy`` gating for ``delete+insert``."""

    def _validate(self, raw_strategy, file_format, unique_key):
        env = _env()
        errors: list[str] = []
        exceptions = mock.Mock()
        exceptions.raise_compiler_error.side_effect = lambda msg: errors.append(str(msg))
        config = mock.Mock()
        config.get.side_effect = lambda k, default=None: {"unique_key": unique_key}.get(k, default)
        env.globals.update(exceptions=exceptions, config=config, **{"return": lambda *a, **k: ""})
        macro = env.get_template(VALIDATE).module.dbt_spark_validate_get_incremental_strategy
        macro(raw_strategy, file_format)
        return errors

    def test_valid_delta_with_unique_key(self):
        assert self._validate("delete+insert", "delta", "id") == []

    def test_missing_unique_key_errors(self):
        errors = self._validate("delete+insert", "delta", None)
        assert errors
        assert "unique_key" in errors[-1]

    def test_non_delta_errors(self):
        errors = self._validate("delete+insert", "parquet", "id")
        assert errors
        assert "delta" in errors[-1]

    @pytest.mark.parametrize("strategy", ["append", "merge", "insert_overwrite", "microbatch"])
    def test_existing_strategies_still_valid(self, strategy):
        fmt = "delta" if strategy != "append" else "parquet"
        assert self._validate(strategy, fmt, None) == []
