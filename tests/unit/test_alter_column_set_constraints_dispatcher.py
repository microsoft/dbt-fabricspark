from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest
from jinja2 import Environment, FileSystemLoader

MACROS_DIR = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "dbt"
    / "include"
    / "fabricspark"
    / "macros"
    / "materializations"
    / "models"
    / "table"
)


@pytest.fixture
def env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(MACROS_DIR)),
        extensions=["jinja2.ext.do"],
    )


def _dispatch_to_recorder(recorded: list[tuple]):
    def _dispatch(macro_name, macro_namespace=None, packages=None):
        def _impl(*args, **kwargs):
            recorded.append((macro_name, args, kwargs))
            return ""

        return _impl

    return _dispatch


def test_alter_column_set_constraints_dispatches(env: Environment) -> None:
    template = env.get_template(
        "create_table_as.sql",
        globals={
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "return": lambda r: r,
        },
    )
    recorded: list[tuple] = []
    adapter = mock.Mock()
    adapter.dispatch = _dispatch_to_recorder(recorded)

    template.globals["adapter"] = adapter

    rendered = template.module.alter_column_set_constraints("my_relation", {"col": {}})

    assert "return(adapter.dispatch" not in str(rendered), (
        "alter_column_set_constraints macro is emitting its own source as text — "
        "the dispatcher is missing its `{{ … }}` wrap. Got:\n" + str(rendered)
    )
    assert recorded == [
        ("alter_column_set_constraints", ("my_relation", {"col": {}}), {}),
    ], (
        "Expected the dispatcher to invoke adapter.dispatch exactly once "
        "with the original args. Got: " + repr(recorded)
    )
