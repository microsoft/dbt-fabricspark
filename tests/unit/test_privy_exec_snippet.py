import json

from dbt.adapters.fabricspark.privysession import (
    _build_exec_snippet,
    _extract_marked_json,
    _job_group_for,
)

CTAS = (
    '/* {"app": "dbt", "node_id": "model.insights.fact_machine"} */ '
    "create or replace table dbo.fact_machine as select 1 as a"
)


def test_job_group_uses_node_id_from_query_comment():
    assert _job_group_for(CTAS) == "model.insights.fact_machine"


def test_job_group_falls_back_when_comment_absent():
    assert _job_group_for("select 1") == "dbt"


def test_snippet_sets_and_clears_job_group():
    snippet = _build_exec_snippet(CTAS, "MARKER")
    assert 'setJobGroup("model.insights.fact_machine"' in snippet
    assert "finally:" in snippet
    # clearJobGroup() is missing on some Fabric runtimes.
    assert "clearJobGroup" not in snippet
    for prop in ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel"):
        assert prop in snippet


def test_snippet_truncates_long_job_description():
    snippet = _build_exec_snippet("select " + "x" * 5000, "MARKER")
    description = json.loads(
        snippet.split("setJobGroup(", 1)[1].split(", True)", 1)[0].split(", ", 1)[1]
    )
    assert len(description) <= 400


def _run(snippet, fields, rows):
    """Execute the snippet with a stubbed ``spark`` global."""

    class _Field:
        def __init__(self, name):
            self.name = name
            self.nullable = True
            self.dataType = type("_T", (), {"simpleString": staticmethod(lambda: "int")})()

    collected = []

    class _DF:
        schema = type("_S", (), {"fields": [_Field(f) for f in fields]})()

        def collect(self):
            collected.append(True)
            return rows

    class _Ctx:
        def __init__(self):
            self.props = {}

        def setJobGroup(self, group, description, interrupt):
            self.props["spark.jobGroup.id"] = group

        def setLocalProperty(self, key, value):
            self.props[key] = value

    class _Spark:
        def __init__(self):
            self.sparkContext = _Ctx()

        def sql(self, _sql):
            return _DF()

    spark = _Spark()
    out = []
    env = {"spark": spark, "print": out.append}
    exec(snippet, env)  # noqa: S102 - exercising generated code is the point
    payload = _extract_marked_json("\n".join(out), "MARKER")
    return payload, collected, spark.sparkContext.props


def test_command_without_output_schema_skips_collect():
    payload, collected, props = _run(_build_exec_snippet(CTAS, "MARKER"), fields=[], rows=[])
    assert payload == {"data": [], "schema": {"fields": []}}
    assert collected == []
    assert props["spark.jobGroup.id"] is None


def test_query_with_output_schema_collects_rows():
    snippet = _build_exec_snippet("select 1 as id", "MARKER")
    payload, collected, props = _run(snippet, fields=["id"], rows=[[1]])
    assert payload["data"] == [[1]]
    assert payload["schema"]["fields"][0]["name"] == "id"
    assert collected == [True]
    assert props["spark.jobGroup.id"] is None
