from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import agate
import pytest
from dbt_common.exceptions import CompilationError, DbtRuntimeError
from jinja2 import Environment

from dbt.adapters.fabricspark import FabricSparkAdapter

QUERY_HASH = "a" * 32
MACRO = (
    Path(__file__).resolve().parents[2]
    / "src/dbt/include/fabricspark/macros/materializations/materialized_view.sql"
)
MISSING_DELTA = "[DELTA_PATH_DOES_NOT_EXIST] /warehouse/mv_example is not a Delta table"


def _materialize(*, error=None, stored_hash=QUERY_HASH, policy="rebuild", exists=True):
    """Render the real materialization body with dbt side effects stubbed."""
    relation = MagicMock()
    relation.__str__.return_value = "`analytics`.`dbo`.`mv_example`"
    relation.incorporate.return_value = relation
    relation.schema = "dbo"
    relation.database = "analytics"
    relation.workspace = None
    adapter = MagicMock(spec=FabricSparkAdapter)
    rows = [] if stored_hash is None else [["query_hash", stored_hash]]
    adapter.execute.return_value = (None, agate.Table(rows, ["key", "value"]))
    adapter.execute.side_effect = error
    adapter.get_materialized_view_query_hash = MagicMock(
        side_effect=lambda target, **kwargs: FabricSparkAdapter.get_materialized_view_query_hash(
            adapter, target, **kwargs
        )
    )
    config = MagicMock()
    config.get.side_effect = lambda key, default=None: {"on_query_change": policy}.get(
        key, default
    )
    statements = []

    def statement(name, fetch_result=False, caller=None):
        sql = " ".join(caller().split())
        if sql.startswith("SHOW TBLPROPERTIES"):
            adapter.execute(sql, auto_begin=False, fetch=True)
        else:
            statements.append(sql)
        return ""

    def compiler_error(message):
        raise CompilationError(message)

    body = (
        MACRO.read_text()
        .split("{% materialization materialized_view, adapter='fabricspark' %}", 1)[1]
        .split("{% endmaterialization %}", 1)[0]
    )
    context = {
        "adapter": adapter,
        "config": config,
        "model": {"language": "sql"},
        "flags": SimpleNamespace(FULL_REFRESH=False),
        "should_full_refresh": lambda: False,
        "fabricspark_materialized_view_columns": lambda *args: [],
        "compiled_code": "select id from source_events",
        "local_md5": lambda code: QUERY_HASH,
        "this": relation,
        "load_cached_relation": lambda target: relation if exists else None,
        "ensure_database_exists": MagicMock(),
        "run_hooks": lambda *args, **kwargs: "",
        "pre_hooks": [],
        "post_hooks": [],
        "statement": statement,
        "load_result": lambda name: {"data": rows},
        "exceptions": SimpleNamespace(raise_compiler_error=compiler_error),
        "return": lambda value: "",
    }
    return (
        adapter,
        relation,
        statements,
        lambda: Environment(extensions=["jinja2.ext.do"]).from_string(body).render(context),
    )


@pytest.mark.parametrize("nested", [False, True])
def test_missing_delta_rebuilds_cached_target(nested):
    error = DbtRuntimeError(MISSING_DELTA)
    if nested:
        error = DbtRuntimeError(str(error))
    adapter, relation, statements, run = _materialize(error=error)
    with patch("dbt.adapters.fabricspark.impl.logger") as logger:
        run()
    logger.warning.assert_called_once()
    adapter.drop_relation.assert_called_once_with(relation)
    assert len(statements) == 1
    assert statements[0].startswith("CREATE MATERIALIZED VIEW ")
    assert "IF NOT EXISTS" not in statements[0]
    assert QUERY_HASH in statements[0]
    adapter.execute.assert_called_once_with(
        f"SHOW TBLPROPERTIES {relation} ('dbt.fabricspark.materialized_view.query_hash')",
        auto_begin=False,
        fetch=True,
    )


@pytest.mark.parametrize(("policy", "exists"), [("fail", True), ("rebuild", False)])
def test_missing_delta_requires_existing_target_and_explicit_rebuild(policy, exists):
    error = DbtRuntimeError(MISSING_DELTA)
    adapter, _, statements, run = _materialize(error=error, policy=policy, exists=exists)
    with pytest.raises(DbtRuntimeError) as raised:
        run()
    assert raised.value is error
    adapter.drop_relation.assert_not_called()
    assert len(statements) == (0 if exists else 1)


@pytest.mark.parametrize(
    "message",
    [
        "[PERMISSION_DENIED] access denied",
        "[PERMISSION_DENIED] diagnostic mentions [DELTA_PATH_DOES_NOT_EXIST]",
        "[SPARK_JOB_CANCELLED] job cancelled",
        "[TABLE_OR_VIEW_NOT_FOUND] target missing",
        "BlobStorageException: Status code 503 ServerBusy",
        "UnknownHostException: Temporary failure in name resolution",
    ],
)
def test_unrelated_errors_are_not_recovered(message):
    error = DbtRuntimeError(message)
    adapter, _, statements, run = _materialize(error=error)
    with pytest.raises(DbtRuntimeError) as raised:
        run()
    assert raised.value is error
    adapter.drop_relation.assert_not_called()
    assert statements == []


def test_matching_hash_refreshes_without_drop():
    adapter, _, statements, run = _materialize()
    run()
    adapter.drop_relation.assert_not_called()
    assert statements == ["REFRESH MATERIALIZED VIEW `analytics`.`dbo`.`mv_example`"]


def test_changed_managed_hash_uses_materialized_view_drop():
    adapter, _, statements, run = _materialize(stored_hash="b" * 32)
    run()
    adapter.drop_relation.assert_not_called()
    assert statements[0] == "DROP MATERIALIZED VIEW `analytics`.`dbo`.`mv_example`"
    assert statements[1].startswith("CREATE MATERIALIZED VIEW ")


@pytest.mark.parametrize("stored_hash", [None, "missing property"])
def test_unmanaged_target_rebuild_behavior_is_preserved(stored_hash):
    adapter, relation, statements, run = _materialize(stored_hash=stored_hash)
    run()
    adapter.drop_relation.assert_called_once_with(relation)
    assert len(statements) == 1
    assert statements[0].startswith("CREATE MATERIALIZED VIEW ")


def test_changed_hash_still_fails_without_rebuild():
    adapter, _, statements, run = _materialize(stored_hash="b" * 32, policy="fail")
    with pytest.raises(CompilationError, match="SQL changed"):
        run()
    adapter.drop_relation.assert_not_called()
    assert statements == []


def test_new_target_still_creates_then_refreshes():
    adapter, _, statements, run = _materialize(exists=False)
    run()
    adapter.drop_relation.assert_not_called()
    assert statements[0].startswith("CREATE MATERIALIZED VIEW IF NOT EXISTS ")
    assert statements[1].startswith("REFRESH MATERIALIZED VIEW ")
