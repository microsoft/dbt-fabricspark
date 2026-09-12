import builtins
import datetime as dt
import io
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest
from dbt_common.exceptions import DbtDatabaseError
from privy import executor
from privy.client import RELAY_RESPONSE_LIMIT_S, ExecResult
from privy.protocol import ExecRequest, ExecResponse

from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.privysession import (
    _PROBE_TIMEOUT_S,
    _UNBOUNDED_TIMEOUT_S,
    PrivyConnectionWrapper,
    _build_exec_snippet,
    _build_relay_client,
    _extract_marked_json,
    _job_group_for,
    _probe,
)

CTAS = (
    '/* {"app": "dbt", "node_id": "model.insights.fact_machine"} */ '
    "create or replace table dbo.fact_machine as select 1 as a"
)
JOB_PROPERTIES = ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel")


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
    for prop in JOB_PROPERTIES:
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
    out = io.StringIO()
    env = {"spark": spark}
    with redirect_stdout(out):
        exec(snippet, env)  # noqa: S102 - exercising generated code is the point
    payload = _extract_marked_json(out.getvalue(), "MARKER")
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


def _credentials(statement_timeout):
    return FabricSparkCredentials(
        method="privy",
        privy_relay_namespace="test-relay",
        privy_relay_path="test",
        privy_relay_keyrule="test",
        privy_relay_key="unused-test-key",
        privy_notebook_url="https://app.fabric.microsoft.com/groups/"
        "00000000-0000-0000-0000-000000000001/synapsenotebooks/"
        "00000000-0000-0000-0000-000000000002",
        spark_config={"name": "test"},
        statement_timeout=statement_timeout,
    )


def _field(name, data_type, nullable):
    return SimpleNamespace(
        name=name,
        dataType=SimpleNamespace(simpleString=lambda: data_type),
        nullable=nullable,
    )


@pytest.fixture
def inprocess_relay(monkeypatch):
    spark = Mock()
    shared_globals = {
        "__builtins__": builtins.__dict__,
        "spark": spark,
        "sc": spark.sparkContext,
        "notebook_state": object(),
        **{
            name: object()
            for name in (
                "__privy_json",
                "__privy_df",
                "__privy_fields",
                "__privy_rows",
                "__privy_prop",
            )
        },
    }
    monkeypatch.setattr(executor, "_INPROCESS_GLOBALS", shared_globals)
    monkeypatch.setattr(executor, "_SERIALIZE_INPROCESS", False)
    monkeypatch.setattr(executor, "_JOBS", {})
    monkeypatch.setattr(executor, "_ROUTERS", {})
    # Privy installs per-thread stream routers; restore pytest's streams afterwards.
    monkeypatch.setattr(sys, "stdout", sys.stdout)
    monkeypatch.setattr(sys, "stderr", sys.stderr)

    client = _build_relay_client(_credentials(10), http_timeout_s=40)
    requests = []
    responses = []

    def post_json(payload, *, http_timeout_s=None):
        request = ExecRequest.from_json(payload)
        requests.append(request)
        response = executor.execute(request)
        responses.append(response)
        return json.loads(response.to_json())

    monkeypatch.setattr(client, "_post_json", post_json)
    return SimpleNamespace(
        client=client,
        spark=spark,
        globals=shared_globals,
        requests=requests,
        responses=responses,
    )


@pytest.mark.parametrize("statement_timeout", [10, 55, 56, 120, 0])
def test_wrapper_preserves_sync_api_and_automatic_long_request_protocol(
    inprocess_relay, statement_timeout
):
    relay = inprocess_relay
    before = relay.globals.copy()
    rows = [[7, dt.datetime(2026, 9, 12, 12, 0), dt.date(2026, 9, 12)]]
    frame = Mock()
    frame.schema.fields = [
        _field("id", "int", False),
        _field("created_at", "timestamp", True),
        _field("day", "date", True),
    ]
    frame.collect.return_value = rows
    relay.spark.sql.return_value = frame
    wrapper = PrivyConnectionWrapper(relay.client, _credentials(statement_timeout))

    wrapper.execute(" select 'quoted \\' value\\n雪' as id; ")

    relay.spark.sql.assert_called_once_with("select 'quoted \\' value\\n雪' as id")
    assert wrapper.fetchall() == rows
    assert wrapper.description == [
        ("id", "int", None, None, None, None, False),
        ("created_at", "timestamp", None, None, None, None, True),
        ("day", "date", None, None, None, None, True),
    ]
    timeout_s = statement_timeout or _UNBOUNDED_TIMEOUT_S
    actions = [request.action for request in relay.requests]
    if timeout_s > RELAY_RESPONSE_LIMIT_S:
        assert actions == ["submit", "poll"]
    else:
        assert actions == ["exec"]
    assert all(request.mode == "inprocess" for request in relay.requests)
    assert all(request.timeout_s == timeout_s for request in relay.requests)
    assert relay.globals == before
    assert relay.spark.sparkContext.setLocalProperty.call_args_list == [
        call(prop, None) for prop in JOB_PROPERTIES
    ]


@pytest.mark.parametrize("statement_timeout", [10, 120])
@pytest.mark.parametrize("pause_at", ["schema", "collect", "cleanup"])
def test_interleaved_requests_keep_distinct_results_and_job_groups(
    inprocess_relay, statement_timeout, pause_at
):
    relay = inprocess_relay
    before = relay.globals.copy()
    paused = threading.Event()
    second_finished = threading.Event()
    active_group = threading.local()
    cleared = {"model.first": [], "model.second": []}
    first_fields = [_field("first_id", "int", False)]
    second_fields = [_field("second_name", "string", True), _field("value", "double", False)]
    first_rows = [[1], [2]]
    second_rows = [["second", 3.5]]

    def pause():
        paused.set()
        assert second_finished.wait(5), "second request did not execute concurrently"

    class FirstFrame:
        @property
        def schema(self):
            if pause_at == "schema":
                pause()
            return SimpleNamespace(fields=first_fields)

        def collect(self):
            if pause_at == "collect":
                pause()
            return first_rows

    second_frame = Mock()
    second_frame.schema.fields = second_fields
    second_frame.collect.return_value = second_rows

    def sql(query):
        print("Spark diagnostic output")
        return FirstFrame() if "model.first" in query else second_frame

    def set_job_group(group, description, interrupt):
        active_group.name = group
        assert interrupt is True

    def clear_property(prop, value):
        group = active_group.name
        if group == "model.first" and prop == JOB_PROPERTIES[0] and pause_at == "cleanup":
            pause()
        cleared[group].append((prop, value))

    relay.spark.sql.side_effect = sql
    relay.spark.sparkContext.setJobGroup.side_effect = set_job_group
    relay.spark.sparkContext.setLocalProperty.side_effect = clear_property
    first = PrivyConnectionWrapper(relay.client, _credentials(statement_timeout))
    second = PrivyConnectionWrapper(relay.client, _credentials(statement_timeout))

    def run_second():
        try:
            second.execute('/* {"node_id": "model.second"} */ select second_name, value')
        finally:
            second_finished.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        first_result = pool.submit(
            first.execute, '/* {"node_id": "model.first"} */ select first_id'
        )
        assert paused.wait(5), "first request did not reach the interleaving point"
        second_result = pool.submit(run_second)
        second_result.result(timeout=10)
        first_result.result(timeout=10)

    assert first.fetchall() == first_rows
    assert first.description == [("first_id", "int", None, None, None, None, False)]
    assert second.fetchall() == second_rows
    assert second.description == [
        ("second_name", "string", None, None, None, None, True),
        ("value", "double", None, None, None, None, False),
    ]
    for props in cleared.values():
        assert props == [(prop, None) for prop in JOB_PROPERTIES]
    assert relay.globals == before


@pytest.mark.parametrize("statement_timeout", [10, 120])
def test_wrapper_ddl_skips_collect(inprocess_relay, statement_timeout):
    relay = inprocess_relay
    frame = relay.spark.sql.return_value
    frame.schema.fields = []
    wrapper = PrivyConnectionWrapper(relay.client, _credentials(statement_timeout))

    wrapper.execute(CTAS)

    assert wrapper.fetchall() == []
    assert wrapper.description == []
    frame.collect.assert_not_called()
    assert relay.spark.sparkContext.setLocalProperty.call_count == len(JOB_PROPERTIES)


@pytest.mark.parametrize("statement_timeout", [10, 120])
@pytest.mark.parametrize("failure_at", ["sql", "collect"])
def test_wrapper_query_errors_clear_job_group(inprocess_relay, statement_timeout, failure_at):
    relay = inprocess_relay
    frame = relay.spark.sql.return_value
    frame.schema.fields = [_field("id", "int", False)]
    failing_call = relay.spark.sql if failure_at == "sql" else frame.collect
    failing_call.side_effect = RuntimeError("query failed")
    wrapper = PrivyConnectionWrapper(relay.client, _credentials(statement_timeout))

    with pytest.raises(DbtDatabaseError, match="query failed"):
        wrapper.execute("select 1")

    assert relay.responses[-1].stdout == b""
    assert relay.spark.sparkContext.setLocalProperty.call_args_list == [
        call(prop, None) for prop in JOB_PROPERTIES
    ]


def test_probe_uses_short_inprocess_request(inprocess_relay):
    relay = inprocess_relay

    assert _probe(relay.client) is True

    assert len(relay.requests) == 1
    request = relay.requests[0]
    assert (request.code, request.mode, request.action, request.timeout_s) == (
        "1",
        "inprocess",
        "exec",
        _PROBE_TIMEOUT_S,
    )
    relay.spark.sql.assert_not_called()


@pytest.mark.parametrize("statement_timeout", [10, 120, 0])
def test_wrapper_propagates_timeout_without_retry(statement_timeout):
    client = Mock()
    client.run_python.return_value = ExecResult.from_response(
        ExecResponse.from_output(
            exit_code=1, stdout=b"", stderr=b"deadline exceeded", duration_ms=0, timed_out=True
        )
    )
    wrapper = PrivyConnectionWrapper(client, _credentials(statement_timeout))

    with pytest.raises(DbtDatabaseError, match=r"\(timed out\).*deadline exceeded"):
        wrapper.execute("select 1")

    client.run_python.assert_called_once()
    assert client.run_python.call_args.kwargs == {
        "mode": "inprocess",
        "timeout_s": statement_timeout or _UNBOUNDED_TIMEOUT_S,
    }


def test_probe_returns_false_on_transport_error():
    client = Mock()
    client.run_python.side_effect = RuntimeError("relay unavailable")

    assert _probe(client) is False
    client.run_python.assert_called_once_with("1", mode="inprocess", timeout_s=_PROBE_TIMEOUT_S)


@pytest.mark.parametrize(
    ("stdout", "error"),
    [
        ("Spark output only", "missing the result marker"),
        ("MARKER\n{}", "missing the closing result marker"),
        ("MARKER\nnot-json\nMARKER", "Could not parse Privy result JSON"),
    ],
)
def test_invalid_result_markers_raise_database_error(stdout, error):
    with pytest.raises(DbtDatabaseError, match=error):
        _extract_marked_json(stdout, "MARKER")
