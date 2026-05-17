"""Tests for the high-concurrency Livy backend.

Mocked-HTTP coverage of the HC lifecycle:
- ``derive_session_tag`` returns the same value across managers when reuse_session
  is true, and is uuid-stable per process when reuse_session is false.
- ``HighConcurrencySession.acquire`` follows the documented state machine:
  POST returns NotStarted, GET polls through AcquiringHighConcurrencySession,
  GET returns Idle with sessionId+replId.
- ``HighConcurrencyCursor.execute`` POSTs to ``/repls/{replId}/statements``,
  polls until ``state == available``, and parses Fabric's standard
  ``output.data.application/json.{schema,data}`` envelope.
- ``HighConcurrencySessionManager.disconnect`` DELETEs the HC id.
- The HC session manager is registered as a :class:`LivyBackend`.
- 404 on submit flags the REPL for re-acquire.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.fabricspark import concurrent_livy
from dbt.adapters.fabricspark.concurrent_livy import (
    HighConcurrencyConnection,
    HighConcurrencyConnectionWrapper,
    HighConcurrencyCursor,
    HighConcurrencySession,
    HighConcurrencySessionManager,
    derive_session_tag,
)
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.livy_backend import LivyBackend


def _make_creds(reuse_session: bool = False, **overrides) -> FabricSparkCredentials:
    base = dict(
        method="livy",
        livy_mode="fabric",
        authentication="CLI",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        lakehouse="tests",
        endpoint="https://api.fabric.microsoft.com/v1",
        spark_config={"name": "test-session", "numExecutors": 4},
        reuse_session=reuse_session,
        session_start_timeout=10,
        statement_timeout=30,
        poll_wait=0,
        poll_statement_wait=0,
    )
    base.update(overrides)
    return FabricSparkCredentials(**base)


def _mock_response(status_code: int, json_body=None, text: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    if json_body is not None:
        resp.json.return_value = json_body
    return resp


@pytest.fixture(autouse=True)
def _reset_module_state():
    """Reset module-level caches between tests so they don't bleed across cases."""
    concurrent_livy._session_tags.clear()
    concurrent_livy._active_sessions.clear()
    concurrent_livy._shortcuts_done.clear()
    yield
    concurrent_livy._session_tags.clear()
    concurrent_livy._active_sessions.clear()
    concurrent_livy._shortcuts_done.clear()


# --------------------------------------------------------------------------- #
# derive_session_tag                                                          #
# --------------------------------------------------------------------------- #


class TestDeriveSessionTag:
    def test_reuse_session_true_returns_deterministic_hash(self):
        creds = _make_creds(reuse_session=True)
        tag1 = derive_session_tag(creds)
        tag2 = derive_session_tag(creds)
        assert tag1 == tag2
        # Hash content includes the workspace+lakehouse pair.
        assert tag1.startswith("dbt-fabricspark-")

    def test_reuse_session_true_same_pair_yields_same_tag_across_creds(self):
        a = _make_creds(reuse_session=True)
        b = _make_creds(reuse_session=True)
        # Two credential objects targeting the same lakehouse must hit the
        # same Spark cluster, so the tag must collide.
        assert derive_session_tag(a) == derive_session_tag(b)

    def test_reuse_session_true_different_lakehouse_yields_different_tag(self):
        a = _make_creds(
            reuse_session=True,
            lakehouseid="11111111-1111-1111-1111-111111111111",
        )
        # Reset so the second creds gets a fresh tag computation.
        concurrent_livy._session_tags.clear()
        b = _make_creds(
            reuse_session=True,
            lakehouseid="22222222-2222-2222-2222-222222222222",
        )
        # Different lakehouses → distinct underlying Spark clusters → distinct tags.
        assert derive_session_tag(a) != derive_session_tag(b)

    def test_reuse_session_false_caches_uuid_per_process(self):
        creds = _make_creds(reuse_session=False)
        tag1 = derive_session_tag(creds)
        tag2 = derive_session_tag(creds)
        # Same process, same creds → cached uuid, so every per-thread manager
        # acquires onto the same underlying Livy session for this run.
        assert tag1 == tag2
        assert tag1.startswith("dbt-fabricspark-")


# --------------------------------------------------------------------------- #
# Acquire                                                                     #
# --------------------------------------------------------------------------- #


class TestHighConcurrencySessionAcquire:
    @patch("dbt.adapters.fabricspark.concurrent_livy._get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.concurrent_livy.time.sleep")
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.get")
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.post")
    def test_happy_path(self, mock_post, mock_get, _sleep, _headers):
        mock_post.return_value = _mock_response(202, {"id": "hc-1", "state": "NotStarted"})
        mock_get.side_effect = [
            _mock_response(200, {"state": "AcquiringHighConcurrencySession"}),
            _mock_response(
                200,
                {
                    "state": "Idle",
                    "sessionId": "livy-42",
                    "replId": "repl-7",
                },
            ),
        ]

        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        hc.acquire()

        assert hc.hc_id == "hc-1"
        assert hc.session_id == "livy-42"
        assert hc.repl_id == "repl-7"
        assert hc.is_new_session_required is False
        # POST sent sessionTag and conf
        post_body = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert "sessionTag" in post_body
        # Session is now in the active registry so atexit will reap it.
        assert hc in concurrent_livy._active_sessions

    @patch("dbt.adapters.fabricspark.concurrent_livy._get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.concurrent_livy.time.sleep")
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.get")
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.post")
    def test_terminal_dead_state_raises(self, mock_post, mock_get, _sleep, _headers):
        mock_post.return_value = _mock_response(202, {"id": "hc-2", "state": "NotStarted"})
        mock_get.return_value = _mock_response(
            200,
            {
                "state": "Dead",
                "fabricSessionStateInfo": {"errorMessage": "out of capacity"},
            },
        )
        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        with pytest.raises(Exception) as exc:
            hc.acquire()
        assert "Dead" in str(exc.value) or "out of capacity" in str(exc.value)

    @patch("dbt.adapters.fabricspark.concurrent_livy._get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.concurrent_livy.time.sleep")
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.post")
    def test_404_on_post_retries_then_succeeds(self, mock_post, _sleep, _headers):
        mock_post.side_effect = [
            _mock_response(404, text="livy not yet up"),
            _mock_response(202, {"id": "hc-3", "state": "NotStarted"}),
        ]
        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        with patch("dbt.adapters.fabricspark.concurrent_livy.requests.get") as mock_get:
            mock_get.return_value = _mock_response(
                200, {"state": "Idle", "sessionId": "s", "replId": "r"}
            )
            hc.acquire()
        assert hc.hc_id == "hc-3"


# --------------------------------------------------------------------------- #
# Cursor execute                                                              #
# --------------------------------------------------------------------------- #


class TestHighConcurrencyCursorExecute:
    @patch("dbt.adapters.fabricspark.concurrent_livy._get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.concurrent_livy.time.sleep")
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.get")
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.post")
    def test_select_returns_rows_and_schema(self, mock_post, mock_get, _sleep, _headers):
        mock_post.return_value = _mock_response(200, {"id": 1, "state": "waiting"})
        mock_get.return_value = _mock_response(
            200,
            {
                "id": 1,
                "state": "available",
                "output": {
                    "status": "ok",
                    "data": {
                        "application/json": {
                            "schema": {
                                "fields": [{"name": "version", "type": "string", "nullable": True}]
                            },
                            "data": [["3.5.5"]],
                        }
                    },
                },
            },
        )

        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        hc.hc_id = "hc-x"
        hc.session_id = "s"
        hc.repl_id = "r"
        hc.is_new_session_required = False

        cursor = HighConcurrencyCursor(creds, hc)
        cursor.execute("SELECT version()")

        assert cursor.fetchall() == [["3.5.5"]]
        assert cursor.fetchone() == ["3.5.5"]
        assert cursor.fetchone() is None
        assert cursor.description[0][0] == "version"

    @patch("dbt.adapters.fabricspark.concurrent_livy._get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.concurrent_livy.time.sleep")
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.get")
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.post")
    def test_ddl_returns_empty_result(self, mock_post, mock_get, _sleep, _headers):
        mock_post.return_value = _mock_response(200, {"id": 1, "state": "waiting"})
        # Fabric returns an envelope without `data` for DDL statements.
        mock_get.return_value = _mock_response(
            200,
            {"id": 1, "state": "available", "output": {"status": "ok", "data": {}}},
        )

        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        hc.session_id = "s"
        hc.repl_id = "r"
        hc.is_new_session_required = False

        cursor = HighConcurrencyCursor(creds, hc)
        cursor.execute("CREATE TABLE foo (a int)")
        assert cursor.fetchall() == []

    @patch("dbt.adapters.fabricspark.concurrent_livy._get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.concurrent_livy.time.sleep")
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.post")
    def test_404_on_submit_marks_repl_dead(self, mock_post, _sleep, _headers):
        mock_post.return_value = _mock_response(404, text="repl gone")

        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        hc.session_id = "s"
        hc.repl_id = "r"
        hc.is_new_session_required = False

        cursor = HighConcurrencyCursor(creds, hc)
        with pytest.raises(Exception):
            cursor.execute("SELECT 1")
        assert hc.is_dead is True
        assert hc.is_new_session_required is True

    @patch("dbt.adapters.fabricspark.concurrent_livy._get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.concurrent_livy.time.sleep")
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.get")
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.post")
    def test_statement_error_raises(self, mock_post, mock_get, _sleep, _headers):
        mock_post.return_value = _mock_response(200, {"id": 1, "state": "waiting"})
        mock_get.return_value = _mock_response(
            200,
            {
                "id": 1,
                "state": "error",
                "output": {"status": "error", "evalue": "table not found"},
            },
        )

        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        hc.session_id = "s"
        hc.repl_id = "r"
        hc.is_new_session_required = False

        cursor = HighConcurrencyCursor(creds, hc)
        with pytest.raises(Exception) as exc:
            cursor.execute("SELECT * FROM nope")
        assert "table not found" in str(exc.value)


# --------------------------------------------------------------------------- #
# Delete / disconnect                                                         #
# --------------------------------------------------------------------------- #


class TestHighConcurrencyDelete:
    @patch("dbt.adapters.fabricspark.concurrent_livy._get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.delete")
    def test_delete_calls_api_and_clears_state(self, mock_delete, _headers):
        mock_delete.return_value = _mock_response(200)

        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        hc.hc_id = "hc-del"
        concurrent_livy._active_sessions.add(hc)

        hc.delete()

        mock_delete.assert_called_once()
        assert hc.hc_id is None
        assert hc.session_id is None
        assert hc.repl_id is None
        assert hc not in concurrent_livy._active_sessions


# --------------------------------------------------------------------------- #
# Manager lifecycle                                                           #
# --------------------------------------------------------------------------- #


class TestHighConcurrencySessionManager:
    def test_satisfies_livy_backend_abc(self):
        mgr = HighConcurrencySessionManager()
        assert isinstance(mgr, LivyBackend)
        # Both methods are required by the ABC and must be callable.
        assert callable(mgr.connect)
        assert callable(mgr.disconnect)

    @patch("dbt.adapters.fabricspark.concurrent_livy._maybe_create_shortcuts")
    def test_connect_acquires_once(self, _shortcuts):
        def _fake_acquire(self):
            # Mimic real acquire — set the flag so the manager's healthy-fast-path triggers.
            self.is_new_session_required = False
            self.session_id = "s"
            self.repl_id = "r"

        with patch.object(HighConcurrencySession, "acquire", _fake_acquire):
            creds = _make_creds()
            mgr = HighConcurrencySessionManager()
            conn1 = mgr.connect(creds)
            conn2 = mgr.connect(creds)
            assert conn1 is conn2
            assert isinstance(conn1, HighConcurrencyConnection)

    @patch("dbt.adapters.fabricspark.concurrent_livy._maybe_create_shortcuts")
    @patch.object(HighConcurrencySession, "delete")
    @patch.object(HighConcurrencySession, "acquire")
    def test_disconnect_releases_hc(self, _acquire, mock_delete, _shortcuts):
        creds = _make_creds()
        mgr = HighConcurrencySessionManager()
        mgr.connect(creds)
        mgr.disconnect()
        mock_delete.assert_called_once()
        assert mgr._hc_session is None


# --------------------------------------------------------------------------- #
# Connection wrapper                                                          #
# --------------------------------------------------------------------------- #


class TestHighConcurrencyConnectionWrapper:
    def test_wrapper_delegates_to_cursor(self):
        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        hc.session_id = "s"
        hc.repl_id = "r"
        hc.is_new_session_required = False
        conn = HighConcurrencyConnection(creds, hc)
        wrapper = HighConcurrencyConnectionWrapper(conn)

        cursor = wrapper.cursor()
        assert cursor is wrapper
        # The cursor returned by the wrapper must expose execute/fetch* surface.
        assert hasattr(wrapper, "execute")
        assert hasattr(wrapper, "fetchall")
        assert hasattr(wrapper, "fetchmany")
        assert hasattr(wrapper, "fetchone")

    def test_execute_strips_trailing_semicolon(self):
        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        hc.session_id = "s"
        hc.repl_id = "r"
        hc.is_new_session_required = False
        conn = HighConcurrencyConnection(creds, hc)
        wrapper = HighConcurrencyConnectionWrapper(conn)
        wrapper.cursor()

        with patch.object(HighConcurrencyCursor, "execute") as mock_exec:
            wrapper.execute("SELECT 1;")
            mock_exec.assert_called_once_with("SELECT 1")
