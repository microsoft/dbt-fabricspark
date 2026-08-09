"""Safety tests for the two P0 ambiguous-submit data-corruption paths.

Both Livy backends stamp a unique marker into every statement so a lost POST
response can be reconciled against the session's statement list instead of
blindly resubmitted (which would double-apply side-effecting DDL/DML). These
tests pin the tri-state contract of ``_find_submitted_statement`` and prove the
outer ``add_query`` retry loop cannot resurrect a resubmit once a backend has
refused one.
"""

from contextlib import contextmanager
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
import requests
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.fabricspark.connections import FabricSparkConnectionManager
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.errors import AmbiguousSubmissionError

_MARKER = "dbt-fabricspark-submit:mine"


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


def _hc_cursor(creds: FabricSparkCredentials):
    from dbt.adapters.fabricspark.concurrent_livy import (
        HighConcurrencyCursor,
        HighConcurrencySession,
    )

    session = HighConcurrencySession(creds, creds.spark_config)
    session.hc_id = "hc-1"
    session.session_id = "sess-1"
    session.repl_id = "repl-1"
    session.is_new_session_required = False
    return HighConcurrencyCursor(creds, session)


def _singleton_cursor(creds: FabricSparkCredentials):
    from dbt.adapters.fabricspark.singleton_livy import LivyCursor, LivySession

    session = LivySession(creds)
    session.session_id = "sess-1"
    session.is_new_session_required = False
    cursor = LivyCursor(creds, session)
    cursor.session_id = "sess-1"
    cursor.connect_url = "https://example.invalid/livy"
    return cursor


# Both backends carry a ~96%-identical copy of the reconciliation logic on
# purpose (unit tests bind patches to module-level names), so every safety
# property is asserted against both.
BACKENDS = [
    pytest.param("dbt.adapters.fabricspark.concurrent_livy", _hc_cursor, "_submit", id="hc"),
    pytest.param(
        "dbt.adapters.fabricspark.singleton_livy",
        _singleton_cursor,
        "_submitLivyCode",
        id="singleton",
    ),
]


@contextmanager
def _patched_backend(module: str, *, governed_return=None, governed_side_effect=None):
    """Replace the module-level network + sleep hooks for one backend."""
    governed_kwargs = (
        {"side_effect": governed_side_effect}
        if governed_side_effect is not None
        else {"return_value": governed_return}
    )
    with (
        patch(f"{module}._governed", **governed_kwargs),
        patch(f"{module}._sleep_until"),
        patch(f"{module}._get_headers", return_value={}),
        patch("time.sleep"),
    ):
        yield


class TestFindSubmittedStatementTriState:
    """``_find_submitted_statement`` must never call an inconclusive read absent."""

    @pytest.mark.parametrize("module, cursor_factory, submit_name", BACKENDS)
    def test_unknown_when_our_statement_lacks_code(self, module, cursor_factory, submit_name):
        # Entry 2 is our just-accepted statement, but Fabric did not echo its
        # `code`, so it could be hiding there — absence is not provable.
        listing = {
            "statements": [
                {
                    "id": 1,
                    "code": "/* dbt-fabricspark-submit:old */ select 1",
                    "state": "available",
                },
                {"id": 2, "state": "running"},
            ]
        }
        cursor = cursor_factory(_make_creds())
        with _patched_backend(module, governed_return=_response(200, listing)):
            outcome, statement_id = cursor._find_submitted_statement(_MARKER)
        assert outcome == "unknown"
        assert statement_id is None

    @pytest.mark.parametrize("module, cursor_factory, submit_name", BACKENDS)
    def test_absent_when_every_entry_is_interpretable(self, module, cursor_factory, submit_name):
        # Every entry carried a `code` we could scan and none matched, so
        # absence is genuinely provable — legitimate recovery must still work.
        listing = {
            "statements": [
                {"id": 1, "code": "/* dbt-fabricspark-submit:old */ select 1"},
                {"id": 2, "code": "/* dbt-fabricspark-submit:other */ select 2"},
            ]
        }
        cursor = cursor_factory(_make_creds())
        with _patched_backend(module, governed_return=_response(200, listing)):
            outcome, statement_id = cursor._find_submitted_statement(_MARKER)
        assert outcome == "absent"
        assert statement_id is None

    @pytest.mark.parametrize("module, cursor_factory, submit_name", BACKENDS)
    def test_absent_when_listing_is_empty(self, module, cursor_factory, submit_name):
        cursor = cursor_factory(_make_creds())
        with _patched_backend(module, governed_return=_response(200, {"statements": []})):
            outcome, statement_id = cursor._find_submitted_statement(_MARKER)
        assert outcome == "absent"
        assert statement_id is None

    @pytest.mark.parametrize("module, cursor_factory, submit_name", BACKENDS)
    def test_unknown_when_a_non_dict_entry_is_present(self, module, cursor_factory, submit_name):
        listing = {
            "statements": [
                {"id": 1, "code": "/* dbt-fabricspark-submit:old */ select 1"},
                "not-a-dict",
            ]
        }
        cursor = cursor_factory(_make_creds())
        with _patched_backend(module, governed_return=_response(200, listing)):
            outcome, statement_id = cursor._find_submitted_statement(_MARKER)
        assert outcome == "unknown"
        assert statement_id is None

    @pytest.mark.parametrize("module, cursor_factory, submit_name", BACKENDS)
    def test_found_returns_the_matching_id(self, module, cursor_factory, submit_name):
        listing = {
            "statements": [
                {"id": 3, "code": "/* dbt-fabricspark-submit:old */ select 1"},
                {"id": 7, "code": f"/* {_MARKER} */ select 1", "state": "running"},
            ]
        }
        cursor = cursor_factory(_make_creds())
        with _patched_backend(module, governed_return=_response(200, listing)):
            outcome, statement_id = cursor._find_submitted_statement(_MARKER)
        assert outcome == "found"
        assert statement_id == 7


class TestAmbiguousSubmissionRaised:
    """Both inconclusive submit paths must raise the dedicated non-retryable error."""

    @pytest.mark.parametrize("module, cursor_factory, submit_name", BACKENDS)
    def test_network_path_raises_ambiguous(self, module, cursor_factory, submit_name):
        def fake_governed(governor, priority, func, url, **kwargs):
            if func is requests.post:
                raise requests.exceptions.Timeout("connection timed out")
            # The reconcile read fails too, so absence cannot be proven.
            raise requests.exceptions.ConnectionError("statement list unreadable")

        cursor = cursor_factory(_make_creds())
        with _patched_backend(module, governed_side_effect=fake_governed):
            with pytest.raises(AmbiguousSubmissionError):
                getattr(cursor, submit_name)("insert into t values (1)")

    @pytest.mark.parametrize("module, cursor_factory, submit_name", BACKENDS)
    def test_http_5xx_path_raises_ambiguous(self, module, cursor_factory, submit_name):
        def fake_governed(governor, priority, func, url, **kwargs):
            if func is requests.post:
                return _response(500, text="server error")
            raise requests.exceptions.ConnectionError("statement list unreadable")

        cursor = cursor_factory(_make_creds())
        with _patched_backend(module, governed_side_effect=fake_governed):
            with pytest.raises(AmbiguousSubmissionError):
                getattr(cursor, submit_name)("insert into t values (1)")


def _manager(cursor, *, connect_retries, retry_all=False):
    manager = FabricSparkConnectionManager.__new__(FabricSparkConnectionManager)
    connection = mock.Mock()
    connection.transaction_open = True
    connection.name = "test"
    connection.credentials = mock.Mock(connect_retries=connect_retries, retry_all=retry_all)
    connection.handle.cursor.return_value = cursor
    manager.get_thread_connection = mock.Mock(return_value=connection)
    return manager


class TestRetryLayerHonoursRefusal:
    """``add_query`` must never resubmit a statement a backend already refused."""

    def test_ambiguous_submission_is_not_retried(self):
        # The network-timeout refusal message contains "Timeout"/"timed out",
        # which _is_retryable_error matches, and retry_all would retry the rest;
        # the dedicated type must short-circuit both escape routes.
        cursor = mock.Mock()
        cursor.execute.side_effect = AmbiguousSubmissionError(
            "Livy statement submit failed with Timeout and the statement list could not be "
            "read. Refusing to resubmit, which could execute this statement twice. "
            "Original error: connection timed out"
        )
        manager = _manager(cursor, connect_retries=5, retry_all=True)

        with patch("dbt.adapters.fabricspark.connections.time.sleep") as sleep:
            with pytest.raises(DbtRuntimeError):
                manager.add_query("insert into t values (1)", auto_begin=False)

        assert cursor.execute.call_count == 1
        sleep.assert_not_called()

    def test_connect_retries_zero_means_a_single_attempt(self):
        cursor = mock.Mock()
        cursor.execute.side_effect = RuntimeError("transient")
        manager = _manager(cursor, connect_retries=0)

        with patch("dbt.adapters.fabricspark.connections.time.sleep"):
            with pytest.raises(DbtRuntimeError):
                manager.add_query("select 1", auto_begin=False)

        assert cursor.execute.call_count == 1

    def test_connect_retries_none_uses_the_default(self):
        cursor = mock.Mock()
        cursor.execute.side_effect = RuntimeError("transient")
        manager = _manager(cursor, connect_retries=None)

        with patch("dbt.adapters.fabricspark.connections.time.sleep"):
            with pytest.raises(DbtRuntimeError):
                manager.add_query("select 1", auto_begin=False)

        assert cursor.execute.call_count == 3
