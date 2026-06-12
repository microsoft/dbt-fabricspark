from __future__ import annotations

import inspect

from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.fabricspark.relation import FabricSparkRelation

_FORBIDDEN_LITERAL = "Cannot set database in spark!"


def test_relation_with_distinct_database_and_schema_does_not_raise() -> None:
    rel = FabricSparkRelation.create(
        database="lakehouse_a",
        schema="dbo",
        identifier="my_table",
        type=RelationType.Table,
    )

    assert rel.database == "lakehouse_a"
    assert rel.schema == "dbo"
    assert rel.identifier == "my_table"
    assert rel.matches(database="lakehouse_a", schema="dbo", identifier="my_table")


def test_relation_with_distinct_database_and_schema_schemas_enabled_does_not_raise() -> None:
    FabricSparkRelation._schemas_enabled = True
    try:
        rel = FabricSparkRelation.create(
            database="lakehouse_a",
            schema="dbo",
            identifier="my_table",
            type=RelationType.Table,
        )
        assert str(rel) == "`lakehouse_a`.`dbo`.my_table"
    finally:
        FabricSparkRelation._schemas_enabled = False


def test_relation_source_does_not_contain_legacy_database_check_message() -> None:
    src = inspect.getsource(FabricSparkRelation)
    assert _FORBIDDEN_LITERAL not in src, (
        f"FabricSparkRelation source contains the legacy "
        f"{_FORBIDDEN_LITERAL!r} message. This guard was removed in v1.9.3 "
        "(commit 2dba5bc) to support schema-enabled lakehouses and "
        "cross-lakehouse / cross-workspace writes — it must not be "
        "re-introduced."
    )
