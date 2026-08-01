"""Rendering of Spark JSON schema types into SQL DDL strings.

Livy reports primitive column types as plain strings but complex ones
(``array`` / ``map`` / ``struct``) as nested dicts. ``data_type_code_to_name``
previously assumed ``str`` or a Python ``type`` and raised
``AttributeError: 'dict' object has no attribute '__name__'`` on the dict form,
which broke contract enforcement for any model with a complex column.

Both sides of ``assert_columns_equivalent`` (the model SQL and the yaml-derived
"empty schema" query) route through this function, so rendering the full nested
type keeps ``array<string>`` and ``array<int>`` distinguishable.
"""

from __future__ import annotations

import pytest

from dbt.adapters.fabricspark.connections import (
    FabricSparkConnectionManager,
    render_spark_type,
)

_STRING = "string"
_INT = "integer"


def _array(element_type, contains_null: bool = True) -> dict:
    return {"type": "array", "elementType": element_type, "containsNull": contains_null}


def _map(key_type, value_type) -> dict:
    return {
        "type": "map",
        "keyType": key_type,
        "valueType": value_type,
        "valueContainsNull": True,
    }


def _struct(*fields) -> dict:
    return {
        "type": "struct",
        "fields": [
            {"name": name, "type": dtype, "nullable": True, "metadata": {}}
            for name, dtype in fields
        ],
    }


class TestRenderSparkType:
    @pytest.mark.parametrize(
        "type_code,expected",
        [
            (_STRING, "string"),
            ("decimal(10,2)", "decimal(10,2)"),
            (_array(_STRING), "array<string>"),
            (_array(_INT), "array<integer>"),
            (_map(_STRING, _INT), "map<string,integer>"),
            (_struct(("a", _STRING), ("b", _INT)), "struct<a:string,b:integer>"),
            (_array(_array(_STRING)), "array<array<string>>"),
            (_map(_STRING, _array(_INT)), "map<string,array<integer>>"),
            (
                _array(_struct(("id", _INT), ("tags", _array(_STRING)))),
                "array<struct<id:integer,tags:array<string>>>",
            ),
            (_struct(), "struct<>"),
        ],
    )
    def test_renders_expected_ddl(self, type_code, expected):
        assert render_spark_type(type_code) == expected

    def test_unknown_dict_kind_falls_back_to_type_value(self):
        assert render_spark_type({"type": "udt", "class": "org.example.Point"}) == "udt"

    def test_dict_without_type_key_does_not_raise(self):
        rendered = render_spark_type({"elementType": "string"})
        assert isinstance(rendered, str)

    def test_non_dict_non_str_does_not_raise(self):
        assert render_spark_type(42) == "42"

    def test_struct_ignores_malformed_fields(self):
        malformed = {"type": "struct", "fields": ["not-a-dict"]}
        assert render_spark_type(malformed) == "struct<>"


class TestDataTypeCodeToName:
    def test_string_passthrough_is_unchanged(self):
        assert FabricSparkConnectionManager.data_type_code_to_name("string") == "string"

    def test_python_type_still_uppercased(self):
        """pyodbc-style Python types keep the legacy uppercase behavior."""
        assert FabricSparkConnectionManager.data_type_code_to_name(str) == "STR"

    @pytest.mark.parametrize(
        "type_code,expected",
        [
            (_array(_STRING), "array<string>"),
            (_map(_STRING, _INT), "map<string,integer>"),
            (_struct(("a", _STRING)), "struct<a:string>"),
        ],
    )
    def test_complex_types_render_instead_of_raising(self, type_code, expected):
        assert FabricSparkConnectionManager.data_type_code_to_name(type_code) == expected

    def test_array_of_string_and_array_of_int_are_not_equal(self):
        """Contract fidelity: element types must stay distinguishable."""
        of_string = FabricSparkConnectionManager.data_type_code_to_name(_array(_STRING))
        of_int = FabricSparkConnectionManager.data_type_code_to_name(_array(_INT))
        assert of_string != of_int
