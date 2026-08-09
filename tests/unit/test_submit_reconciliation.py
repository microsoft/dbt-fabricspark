"""Tests for ambiguous-submit reconciliation and REPL cap sizing.

Both defend correctness properties that only bite under Fabric throttling, so
they are easy to regress silently.
"""

from unittest.mock import MagicMock, patch

import pytest
import requests
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.fabricspark.concurrent_livy import (
    _HC_MAX_CONF,
    _SUBMIT_MARKER_PREFIX,
    HighConcurrencyCursor,
    HighConcurrencySession,
)
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials


def _make_creds(**overrides) -> FabricSparkCredentials:
    base = dict(
        method="livy",
        livy_mode="fabric",
        authentication="CLI",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        lakehouse="tests",
        endpoint="https://api.fabric.microsoft.com/v1",
        spark_config={"name": "test-session"},
        session_start_timeout=10,
        statement_timeout=30,
        poll_wait=0,
        poll_statement_wait=0,
    )
    base.update(overrides)
    return FabricSparkCredentials(**base)


def _response(status_code: int, json_body=None, text: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.json.return_value = json_body if json_body is not None else {}
    return resp


def _cursor(creds: FabricSparkCredentials) -> HighConcurrencyCursor:
    session = HighConcurrencySession(creds, creds.spark_config)
    session.hc_id = "hc-1"
    session.session_id = "sess-1"
    session.repl_id = "repl-1"
    session.is_new_session_required = False
    return HighConcurrencyCursor(creds, session)


@patch("dbt.adapters.fabricspark.concurrent_livy._get_headers", return_value={})
class TestAmbiguousSubmitReconciliation:
    """A POST that dies mid-flight may still have started running server-side.

    Resubmitting would run side-effecting DDL/DML twice. Livy echoes the
    submitted ``code`` back in its statement list, so the marker the adapter
    stamps into every statement lets it adopt the in-flight statement instead.
    """

    def test_marker_is_embedded_in_submitted_code(self, _headers):
        cursor = _cursor(_make_creds())
        with patch("requests.post", return_value=_response(200, {"id": 7})) as post:
            cursor._submit("select 1")
        body = post.call_args.kwargs["data"]
        assert _SUBMIT_MARKER_PREFIX in body
        assert "select 1" in body

    def test_each_submit_gets_a_distinct_marker(self, _headers):
        cursor = _cursor(_make_creds())
        with patch("requests.post", return_value=_response(200, {"id": 7})) as post:
            cursor._submit("select 1")
            cursor._submit("select 1")
        first, second = (c.kwargs["data"] for c in post.call_args_list)
        assert first != second

    @pytest.mark.parametrize(
        "exc",
        [
            requests.exceptions.ConnectionError("boom"),
            requests.exceptions.Timeout("boom"),
            requests.exceptions.SSLError("boom"),
            requests.exceptions.ChunkedEncodingError("boom"),
        ],
    )
    def test_adopts_in_flight_statement_instead_of_resubmitting(self, _headers, exc):
        cursor = _cursor(_make_creds())
        captured: dict = {}

        def fake_post(url, **kwargs):
            captured["data"] = kwargs["data"]
            raise exc

        def fake_get(url, **kwargs):
            marker = captured["data"].split(_SUBMIT_MARKER_PREFIX)[1].split(" ")[0].strip()
            return _response(
                200,
                {
                    "statements": [
                        {"id": 3, "code": "/* unrelated */ select 2", "state": "available"},
                        {
                            "id": 9,
                            "code": f"/* {_SUBMIT_MARKER_PREFIX}{marker} */ select 1",
                            "state": "running",
                        },
                    ]
                },
            )

        with (
            patch("requests.post", side_effect=fake_post) as post,
            patch("requests.get", side_effect=fake_get),
        ):
            result = cursor._submit("select 1")

        assert result.json()["id"] == 9
        assert post.call_count == 1, "must not resubmit once the statement is found"

    def test_falls_back_to_retry_when_no_matching_statement(self, _headers):
        cursor = _cursor(_make_creds())
        attempts = {"n": 0}

        def fake_post(url, **kwargs):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise requests.exceptions.ConnectionError("boom")
            return _response(200, {"id": 11})

        with (
            patch("requests.post", side_effect=fake_post),
            patch("requests.get", return_value=_response(200, {"statements": []})),
            patch("time.sleep"),
        ):
            result = cursor._submit("select 1")

        assert result.json()["id"] == 11
        assert attempts["n"] == 2

    def test_inconclusive_lookup_refuses_to_resubmit(self, _headers):
        """An unreadable statement list is not proof the statement never landed.

        The lookup travels the same network path that just failed the POST, so
        the failures are correlated. Treating "could not tell" as "not
        submitted" would run side-effecting DDL/DML twice.
        """
        cursor = _cursor(_make_creds())

        with (
            patch(
                "requests.post", side_effect=requests.exceptions.ConnectionError("boom")
            ) as post,
            patch("requests.get", side_effect=RuntimeError("list failed")),
            patch("time.sleep"),
        ):
            with pytest.raises(DbtRuntimeError, match="Refusing to resubmit"):
                cursor._submit("merge into t using s on t.id = s.id when matched then delete")

        assert post.call_count == 1

    def test_http_error_on_statement_list_refuses_to_resubmit(self, _headers):
        cursor = _cursor(_make_creds())

        with (
            patch("requests.post", side_effect=requests.exceptions.Timeout("boom")) as post,
            patch("requests.get", return_value=_response(500, text="server error")),
            patch("time.sleep"),
        ):
            with pytest.raises(DbtRuntimeError, match="Refusing to resubmit"):
                cursor._submit("insert into t values (1)")

        assert post.call_count == 1

    def test_each_retry_uses_a_fresh_marker(self, _headers):
        """Two landed submissions must stay distinguishable in the statement list."""
        cursor = _cursor(_make_creds())
        attempts = {"n": 0}

        def fake_post(url, **kwargs):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise requests.exceptions.ConnectionError("boom")
            return _response(200, {"id": 11})

        with (
            patch("requests.post", side_effect=fake_post) as post,
            patch("requests.get", return_value=_response(200, {"statements": []})),
            patch("time.sleep"),
        ):
            cursor._submit("select 1")

        markers = [
            c.kwargs["data"].split(_SUBMIT_MARKER_PREFIX)[1][:32] for c in post.call_args_list
        ]
        assert len(set(markers)) == len(markers)

    def test_http_429_still_resubmits(self, _headers):
        """A 429 is an unambiguous rejection — nothing ran, so resubmitting is safe."""
        cursor = _cursor(_make_creds())
        responses = [_response(429), _response(200, {"id": 5})]

        with patch("requests.post", side_effect=responses) as post:
            assert cursor._submit("select 1").json()["id"] == 5
        assert post.call_count == 2


class TestReplCapSizing:
    """The telemetry monitor needs its own REPL slot.

    Fabric packs 5 REPLs per session by default and silently spills the 6th onto
    a different SparkContext, where the monitor would observe none of the
    workers' jobs.
    """

    def test_cap_is_not_set_when_adaptive_polling_is_off(self):
        creds = _make_creds()
        payload = HighConcurrencySession(creds, creds.spark_config)._build_acquire_payload()
        assert _HC_MAX_CONF not in payload.get("conf", {})

    def test_cap_defaults_to_floor_when_thread_count_is_unknown(self):
        creds = _make_creds(adaptive_polling=True)
        payload = HighConcurrencySession(creds, creds.spark_config)._build_acquire_payload()
        assert payload["conf"][_HC_MAX_CONF] == "5"

    @pytest.mark.parametrize(
        "threads,expected",
        [(1, "5"), (3, "5"), (4, "6"), (8, "10"), (48, "50"), (100, "50")],
    )
    def test_cap_leaves_room_for_the_monitor(self, threads, expected):
        creds = _make_creds(adaptive_polling=True)
        creds.dbt_threads = threads
        payload = HighConcurrencySession(creds, creds.spark_config)._build_acquire_payload()
        assert payload["conf"][_HC_MAX_CONF] == expected

    def test_explicit_user_value_is_respected_verbatim(self):
        creds = _make_creds(
            adaptive_polling=True,
            spark_config={"name": "test-session", "conf": {_HC_MAX_CONF: "3"}},
        )
        creds.dbt_threads = 16
        payload = HighConcurrencySession(creds, creds.spark_config)._build_acquire_payload()
        assert payload["conf"][_HC_MAX_CONF] == "3"

    def test_cap_never_exceeds_fabric_ceiling(self):
        creds = _make_creds(adaptive_polling=True)
        creds.dbt_threads = 10_000
        payload = HighConcurrencySession(creds, creds.spark_config)._build_acquire_payload()
        assert int(payload["conf"][_HC_MAX_CONF]) <= 50

    def test_other_injected_conf_survives(self):
        creds = _make_creds(
            adaptive_polling=True,
            environmentId="11111111-2222-3333-4444-555555555555",
            session_idle_timeout="45m",
        )
        creds.dbt_threads = 4
        payload = HighConcurrencySession(creds, creds.spark_config)._build_acquire_payload()
        conf = payload["conf"]
        assert conf[_HC_MAX_CONF] == "6"
        assert conf["spark.fabric.environment.id"] == "11111111-2222-3333-4444-555555555555"
        assert conf["spark.livy.session.idle.timeout"] == "45m"


def _singleton_cursor(creds: FabricSparkCredentials):
    from dbt.adapters.fabricspark.singleton_livy import LivyCursor, LivySession

    session = LivySession(creds)
    session.session_id = "sess-1"
    session.is_new_session_required = False
    cursor = LivyCursor(creds, session)
    cursor.session_id = "sess-1"
    cursor.connect_url = "https://example.invalid/livy"
    return cursor


class TestSingletonSubmitReconciliation:
    """The singleton backend retries submits too, so it needs the same guard.

    Without it a ``Timeout`` on a POST that Fabric already accepted would
    resubmit an ``INSERT``/``MERGE`` and double-apply it.
    """

    def test_marker_is_injected_into_submitted_code(self):
        cursor = _singleton_cursor(_make_creds())
        with patch(
            "dbt.adapters.fabricspark.singleton_livy._governed",
            return_value=_response(200, {"id": 7}),
        ) as governed:
            cursor._submitLivyCode("select 1")

        body = governed.call_args.kwargs["data"]
        assert _SUBMIT_MARKER_PREFIX in body
        assert "select 1" in body

    def test_adopts_statement_when_ambiguous_submit_landed(self):
        cursor = _singleton_cursor(_make_creds())
        seen: list[str] = []

        def fake(governor, priority, func, url, **kwargs):
            if func is requests.post:
                seen.append(kwargs["data"])
                raise requests.exceptions.Timeout("boom")
            marker = next(
                token.split("*/")[0].strip()
                for token in seen[-1].split("/*")
                if _SUBMIT_MARKER_PREFIX in token
            )
            return _response(
                200, {"statements": [{"id": 42, "code": f"/* {marker} */\nselect 1"}]}
            )

        with patch("dbt.adapters.fabricspark.singleton_livy._governed", side_effect=fake):
            res = cursor._submitLivyCode("select 1")

        assert res.json()["id"] == 42
        assert len(seen) == 1, "must not resubmit once the statement was found"

    def test_refuses_to_resubmit_when_statement_list_unreadable(self):
        cursor = _singleton_cursor(_make_creds())
        posts = 0

        def fake(governor, priority, func, url, **kwargs):
            nonlocal posts
            if func is requests.post:
                posts += 1
            raise requests.exceptions.ConnectionError("network down")

        with (
            patch("dbt.adapters.fabricspark.singleton_livy._governed", side_effect=fake),
            patch("dbt.adapters.fabricspark.singleton_livy._sleep_until"),
            pytest.raises(DbtRuntimeError, match="Refusing to resubmit"),
        ):
            cursor._submitLivyCode("insert into t values (1)")

        assert posts == 1, "an unreadable statement list must never trigger a resubmit"

    def test_resubmits_only_when_statement_definitely_absent(self):
        cursor = _singleton_cursor(_make_creds())
        posts = 0

        def fake(governor, priority, func, url, **kwargs):
            nonlocal posts
            if func is requests.post:
                posts += 1
                if posts == 1:
                    raise requests.exceptions.Timeout("boom")
                return _response(200, {"id": 9})
            return _response(200, {"statements": []})

        with (
            patch("dbt.adapters.fabricspark.singleton_livy._governed", side_effect=fake),
            patch("dbt.adapters.fabricspark.singleton_livy._sleep_until"),
            patch("time.sleep"),
        ):
            res = cursor._submitLivyCode("select 1")

        assert res.json()["id"] == 9
        assert posts == 2

    def test_each_attempt_uses_a_distinct_marker(self):
        cursor = _singleton_cursor(_make_creds())
        markers: list[str] = []

        def fake(governor, priority, func, url, **kwargs):
            if func is requests.post:
                markers.append(kwargs["data"])
                if len(markers) < 3:
                    raise requests.exceptions.Timeout("boom")
                return _response(200, {"id": 1})
            return _response(200, {"statements": []})

        with (
            patch("dbt.adapters.fabricspark.singleton_livy._governed", side_effect=fake),
            patch("dbt.adapters.fabricspark.singleton_livy._sleep_until"),
            patch("time.sleep"),
        ):
            cursor._submitLivyCode("select 1")

        assert len(markers) == 3
        assert len(set(markers)) == 3, "reusing a marker makes two landed submits ambiguous"
