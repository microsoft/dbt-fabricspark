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
- ``HighConcurrencySessionManager.disconnect`` DELETEs the HC id, unless
  ``reuse_session`` is set, in which case the session is kept warm.
- The HC session manager is registered as a :class:`LivyBackend`.
- 404 on submit flags the REPL for re-acquire.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import get_context
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.fabricspark import concurrent_livy
from dbt.adapters.fabricspark.concurrent_livy import (
    HighConcurrencyConnection,
    HighConcurrencyConnectionWrapper,
    HighConcurrencyCursor,
    HighConcurrencyReplPool,
    HighConcurrencySession,
    HighConcurrencySessionManager,
    derive_session_tag,
)
from dbt.adapters.fabricspark.connections import FabricSparkConnectionManager
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
    FabricSparkConnectionManager.connection_managers.clear()
    FabricSparkConnectionManager._hc_pools.clear()
    yield
    concurrent_livy._session_tags.clear()
    concurrent_livy._active_sessions.clear()
    concurrent_livy._shortcuts_done.clear()
    FabricSparkConnectionManager.connection_managers.clear()
    FabricSparkConnectionManager._hc_pools.clear()


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
    def test_select_coerces_timestamp_columns(self, mock_post, mock_get, _sleep, _headers):
        """Timestamp/date columns come back as native datetimes (#237)."""
        import datetime as dt

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
                                "fields": [
                                    {
                                        "name": "max_loaded_at",
                                        "type": "timestamp",
                                        "nullable": True,
                                    },
                                    {
                                        "name": "snapshotted_at",
                                        "type": "timestamp",
                                        "nullable": True,
                                    },
                                    {"name": "d", "type": "date", "nullable": True},
                                ]
                            },
                            "data": [
                                ["2024-01-01 12:00:00.123456", "2024-01-02 00:00:00", "2024-03-04"]
                            ],
                        }
                    },
                },
            },
        )

        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        hc.session_id = "s"
        hc.repl_id = "r"
        hc.is_new_session_required = False

        cursor = HighConcurrencyCursor(creds, hc)
        cursor.execute("SELECT max_loaded_at, snapshotted_at, d FROM src")

        row = cursor.fetchone()
        assert row[0] == dt.datetime(2024, 1, 1, 12, 0, 0, 123456)
        assert row[1] == dt.datetime(2024, 1, 2, 0, 0, 0)
        assert row[2] == dt.date(2024, 3, 4)

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
    def test_disconnect_releases_hc(self, mock_delete, _shortcuts):
        def fake_acquire(session):
            session.hc_id = "hc"
            session.session_id = "session"
            session.repl_id = "repl"
            session.is_new_session_required = False

        creds = _make_creds()
        mgr = HighConcurrencySessionManager()
        with patch.object(HighConcurrencySession, "acquire", fake_acquire):
            mgr.connect(creds)
            mgr.disconnect()
        mock_delete.assert_called_once()
        assert mgr._hc_session is None

    @patch("dbt.adapters.fabricspark.concurrent_livy._maybe_create_shortcuts")
    @patch.object(HighConcurrencySession, "delete")
    def test_disconnect_keeps_session_alive_when_reuse_session(self, mock_delete, _shortcuts):
        # reuse_session=True must keep the underlying Livy session warm for the
        # next invocation instead of deleting the HC id (issue #232).
        def fake_acquire(session):
            session.hc_id = "hc"
            session.session_id = "session"
            session.repl_id = "repl"
            session.is_new_session_required = False

        creds = _make_creds(reuse_session=True)
        mgr = HighConcurrencySessionManager()
        with patch.object(HighConcurrencySession, "acquire", fake_acquire):
            mgr.connect(creds)
            mgr.disconnect()
        mock_delete.assert_not_called()
        assert mgr._hc_session is None


# --------------------------------------------------------------------------- #
# Process-local REPL pool                                                     #
# --------------------------------------------------------------------------- #


class TestHighConcurrencyReplPool:
    def test_caps_metadata_model_and_teardown_phases_at_dbt_threads(self):
        creds = _make_creds(statement_timeout=5)
        pool = HighConcurrencyReplPool(max_size=3)
        acquire_lock = threading.Lock()
        acquire_count = 0
        capacity_reached = threading.Event()
        release_metadata = threading.Event()

        def fake_acquire(session):
            nonlocal acquire_count
            with acquire_lock:
                acquire_count += 1
                index = acquire_count
                if acquire_count == 3:
                    capacity_reached.set()
            session.hc_id = f"hc-{index}"
            session.session_id = "physical-1"
            session.repl_id = f"repl-{index}"
            session.is_new_session_required = False

        def metadata_task():
            manager = HighConcurrencySessionManager(pool)
            connection = manager.connect(creds)
            assert release_metadata.wait(timeout=5)
            connection.close()

        def model_task(barrier):
            manager = HighConcurrencySessionManager(pool)
            connection = manager.connect(creds)
            barrier.wait(timeout=5)
            connection.close()

        with patch.object(HighConcurrencySession, "acquire", fake_acquire):
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(metadata_task) for _ in range(5)]
                assert capacity_reached.wait(timeout=5)
                assert acquire_count == 3
                release_metadata.set()
                for future in futures:
                    future.result(timeout=5)

            model_barrier = threading.Barrier(4)
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(model_task, model_barrier) for _ in range(3)]
                model_barrier.wait(timeout=5)
                for future in futures:
                    future.result(timeout=5)

            manager = HighConcurrencySessionManager(pool)
            manager.connect(creds).close()

        assert acquire_count == 3

    def test_reuses_worker_repl_for_after_run(self):
        creds = _make_creds(reuse_session=True, statement_timeout=5)
        pool = HighConcurrencyReplPool(max_size=4)
        acquire_lock = threading.Lock()
        acquired = []
        worker_sessions = []

        def fake_acquire(session):
            with acquire_lock:
                index = len(acquired) + 1
                session.hc_id = f"hc-{index}"
                session.session_id = "physical-1"
                session.repl_id = f"repl-{index}"
                session.is_new_session_required = False
                acquired.append(session)

        def worker(barrier):
            manager = HighConcurrencySessionManager(pool)
            connection = manager.connect(creds)
            with acquire_lock:
                worker_sessions.append(connection.hc_session)
            barrier.wait(timeout=5)
            connection.close()

        with patch.object(HighConcurrencySession, "acquire", fake_acquire):
            before_run = HighConcurrencySessionManager(pool)
            before_run.connect(creds).close()

            worker_barrier = threading.Barrier(5)
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(worker, worker_barrier) for _ in range(4)]
                worker_barrier.wait(timeout=5)
                for future in futures:
                    future.result(timeout=5)

            after_run = HighConcurrencySessionManager(pool)
            after_connection = after_run.connect(creds)
            assert after_connection.hc_session in acquired
            after_connection.close()

        assert len(acquired) == 4
        assert len(set(worker_sessions)) == 4

    def test_failed_acquire_restores_capacity(self):
        creds = _make_creds(statement_timeout=5)
        pool = HighConcurrencyReplPool(max_size=1)
        attempts = 0

        def flaky_acquire(session):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("acquire failed")
            session.hc_id = "hc-ok"
            session.session_id = "physical-1"
            session.repl_id = "repl-ok"
            session.is_new_session_required = False

        with patch.object(HighConcurrencySession, "acquire", flaky_acquire):
            with pytest.raises(RuntimeError, match="acquire failed"):
                pool.borrow(creds)
            session = pool.borrow(creds)

        assert session.hc_id == "hc-ok"
        assert pool.leased_count == 1
        pool.release(session)

    def test_stale_idle_repl_is_deleted_and_replaced(self):
        creds = _make_creds(statement_timeout=5)
        pool = HighConcurrencyReplPool(max_size=1)
        acquired = []
        deleted = []

        def fake_acquire(session):
            index = len(acquired) + 1
            session.hc_id = f"hc-{index}"
            session.session_id = "physical-1"
            session.repl_id = f"repl-{index}"
            session.is_new_session_required = False
            acquired.append(session)

        def fake_delete(session):
            deleted.append(session)
            session.hc_id = None
            session.session_id = None
            session.repl_id = None
            session.is_new_session_required = True

        with (
            patch.object(HighConcurrencySession, "acquire", fake_acquire),
            patch.object(HighConcurrencySession, "delete", fake_delete),
        ):
            stale = pool.borrow(creds)
            pool.release(stale)
            stale.is_dead = True
            replacement = pool.borrow(creds)

        assert replacement is not stale
        assert deleted == [stale]
        assert len(acquired) == 2
        pool.release(replacement)

    def test_replaces_a_stale_leased_repl_through_the_pool(self):
        creds = _make_creds(statement_timeout=5)
        pool = HighConcurrencyReplPool(max_size=1)
        acquired = []
        deleted = []

        def fake_acquire(session):
            index = len(acquired) + 1
            session.hc_id = f"hc-{index}"
            session.session_id = "physical-1"
            session.repl_id = f"repl-{index}"
            session.is_new_session_required = False
            session.is_dead = False
            acquired.append(session)

        def fake_delete(session):
            deleted.append(session)
            session.hc_id = None
            session.session_id = None
            session.repl_id = None
            session.is_new_session_required = True

        with (
            patch.object(HighConcurrencySession, "acquire", fake_acquire),
            patch.object(HighConcurrencySession, "delete", fake_delete),
        ):
            manager = HighConcurrencySessionManager(pool)
            connection = manager.connect(creds)
            stale = connection.hc_session
            stale.is_dead = True

            connection.cursor()._ensure_repl()

            assert connection.hc_session is acquired[1]
            assert manager._hc_session is acquired[1]
            assert pool.leased_count == 1
            connection.close()

        assert deleted == [stale]

    def test_retries_after_stale_replacement_acquire_fails(self):
        creds = _make_creds(statement_timeout=5)
        pool = HighConcurrencyReplPool(max_size=1)
        successful = []
        attempts = 0

        def flaky_acquire(session):
            nonlocal attempts
            attempts += 1
            if attempts == 2:
                raise RuntimeError("replacement unavailable")
            session.hc_id = f"hc-{attempts}"
            session.session_id = "physical-1"
            session.repl_id = f"repl-{attempts}"
            session.is_new_session_required = False
            session.is_dead = False
            successful.append(session)

        def fake_delete(session):
            session.hc_id = None
            session.session_id = None
            session.repl_id = None
            session.is_new_session_required = True

        with (
            patch.object(HighConcurrencySession, "acquire", flaky_acquire),
            patch.object(HighConcurrencySession, "delete", fake_delete),
        ):
            manager = HighConcurrencySessionManager(pool)
            connection = manager.connect(creds)
            connection.hc_session.is_dead = True

            with pytest.raises(RuntimeError, match="replacement unavailable"):
                connection.cursor()._ensure_repl()

            connection.cursor()._ensure_repl()

        assert attempts == 3
        assert connection.hc_session is successful[1]
        assert pool.leased_count == 1
        connection.close()

    def test_cleanup_deletes_or_preserves_idle_sessions(self):
        for reuse_session, expected_deletes in ((False, 1), (True, 0)):
            creds = _make_creds(reuse_session=reuse_session)
            pool = HighConcurrencyReplPool(max_size=1)
            session = HighConcurrencySession(creds, creds.spark_config)
            session.hc_id = "hc"
            session.session_id = "physical"
            session.repl_id = "repl"
            session.is_new_session_required = False
            pool._leased.add(session)
            pool.release(session)

            with patch.object(HighConcurrencySession, "delete") as mock_delete:
                pool.cleanup(delete_sessions=not reuse_session)

            assert mock_delete.call_count == expected_deletes
            assert pool.idle_count == int(reuse_session)


# --------------------------------------------------------------------------- #
# dbt connection-manager integration                                         #
# --------------------------------------------------------------------------- #


class TestHighConcurrencyConnectionManager:
    @staticmethod
    def make_manager(creds, threads=4):
        profile = SimpleNamespace(credentials=creds, threads=threads)
        return FabricSparkConnectionManager(profile, get_context("spawn"))

    def test_profile_threads_set_pool_capacity_without_credential_field(self):
        creds = _make_creds()
        manager = self.make_manager(creds, threads=3)

        assert manager._hc_pool is not None
        assert manager._hc_pool.max_size == 3
        assert not hasattr(creds, "threads")

    def test_release_closes_hc_connection(self):
        creds = _make_creds()
        manager = self.make_manager(creds)
        handle = MagicMock()
        connection = Connection(
            type="fabricspark",
            name="model",
            credentials=creds,
            state=ConnectionState.OPEN,
            handle=handle,
        )
        manager.thread_connections[manager.get_thread_identifier()] = connection

        manager.release()

        handle.close.assert_called_once_with()
        assert connection.state == ConnectionState.CLOSED

    def test_release_remains_noop_for_non_hc_livy(self):
        creds = _make_creds(high_concurrency=False)
        manager = self.make_manager(creds)
        handle = MagicMock()
        connection = Connection(
            type="fabricspark",
            name="model",
            credentials=creds,
            state=ConnectionState.OPEN,
            handle=handle,
        )
        manager.thread_connections[manager.get_thread_identifier()] = connection

        manager.release()

        handle.close.assert_not_called()
        assert connection.state == ConnectionState.OPEN

    @pytest.mark.parametrize(
        ("reuse_session", "expected_deletes", "expected_idle"),
        [(False, 1, 0), (True, 0, 1)],
    )
    def test_cleanup_all_returns_shared_leases_before_pool_cleanup(
        self, reuse_session, expected_deletes, expected_idle
    ):
        creds = _make_creds(reuse_session=reuse_session)
        manager = self.make_manager(creds, threads=1)
        pool = manager._hc_pool

        def fake_acquire(session):
            session.hc_id = "hc"
            session.session_id = "physical"
            session.repl_id = "repl"
            session.is_new_session_required = False

        with (
            patch.object(HighConcurrencySession, "acquire", fake_acquire),
            patch.object(HighConcurrencySession, "delete") as mock_delete,
        ):
            backend = HighConcurrencySessionManager(pool)
            raw_handle = backend.connect(creds)
            connection = Connection(
                type="fabricspark",
                name="model",
                credentials=creds,
                state=ConnectionState.OPEN,
                handle=HighConcurrencyConnectionWrapper(raw_handle),
            )
            thread_id = manager.get_thread_identifier()
            manager.thread_connections[thread_id] = connection
            manager.connection_managers[(id(creds), thread_id)] = backend

            manager.cleanup_all()

        assert connection.state == ConnectionState.CLOSED
        assert mock_delete.call_count == expected_deletes
        assert pool.idle_count == expected_idle
        assert not manager.connection_managers


# --------------------------------------------------------------------------- #
# atexit cleanup                                                              #
# --------------------------------------------------------------------------- #


class TestHighConcurrencyAtexitCleanup:
    @patch("dbt.adapters.fabricspark.concurrent_livy._get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.concurrent_livy.requests.delete")
    def test_atexit_deletes_only_non_reuse_sessions(self, mock_delete, _headers):
        # reuse_session sessions are left alive so the underlying Livy session
        # stays warm; non-reuse sessions are deleted to free REPL slots (#232).
        mock_delete.return_value = _mock_response(200)

        hc_fresh = HighConcurrencySession(_make_creds(reuse_session=False), {})
        hc_fresh.hc_id = "hc-del"
        hc_reuse = HighConcurrencySession(_make_creds(reuse_session=True), {})
        hc_reuse.hc_id = "hc-keep"
        concurrent_livy._active_sessions.update({hc_fresh, hc_reuse})

        concurrent_livy._atexit_cleanup_hc()

        mock_delete.assert_called_once()
        deleted_url = mock_delete.call_args.args[0]
        assert "hc-del" in deleted_url

        # Non-reuse session was deleted and de-registered.
        assert hc_fresh.hc_id is None
        assert hc_fresh not in concurrent_livy._active_sessions

        # reuse_session session is untouched and still active.
        assert hc_reuse.hc_id == "hc-keep"
        assert hc_reuse in concurrent_livy._active_sessions


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


# --------------------------------------------------------------------------- #
# _build_acquire_payload — session_idle_timeout injection                     #
# --------------------------------------------------------------------------- #


class TestBuildAcquirePayloadIdleTimeout:
    """Guard rails for the starter-pool fallback bug.

    Fabric treats ``spark.livy.session.idle.timeout`` as a session-immutable
    SparkConf; its mere presence in the acquire ``conf`` disqualifies
    starter-pool matching. The adapter must therefore omit the key unless
    the user has explicitly opted in by setting ``session_idle_timeout``.
    """

    def test_default_credentials_omit_idle_timeout(self):
        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        payload = hc._build_acquire_payload()
        assert "spark.livy.session.idle.timeout" not in payload.get("conf", {})

    def test_empty_string_idle_timeout_omits_key(self):
        creds = _make_creds(session_idle_timeout="")
        hc = HighConcurrencySession(creds, creds.spark_config)
        payload = hc._build_acquire_payload()
        assert "spark.livy.session.idle.timeout" not in payload.get("conf", {})

    def test_explicit_idle_timeout_injects_key(self):
        creds = _make_creds(session_idle_timeout="45m")
        hc = HighConcurrencySession(creds, creds.spark_config)
        payload = hc._build_acquire_payload()
        assert payload["conf"]["spark.livy.session.idle.timeout"] == "45m"

    def test_environment_id_still_injects_when_idle_timeout_omitted(self):
        creds = _make_creds(environmentId="11111111-2222-3333-4444-555555555555")
        hc = HighConcurrencySession(creds, creds.spark_config)
        payload = hc._build_acquire_payload()
        assert (
            payload["conf"]["spark.fabric.environment.id"]
            == "11111111-2222-3333-4444-555555555555"
        )
        assert "spark.livy.session.idle.timeout" not in payload["conf"]


# --------------------------------------------------------------------------- #
# _build_acquire_payload — verbatim spark_config forwarding                    #
# --------------------------------------------------------------------------- #


class TestBuildAcquirePayloadForwarding:
    """The HC payload forwards spark_config verbatim.

    It previously copied a fixed 16-key allowlist and dropped everything else
    without a log line, while the singleton path POSTed the whole dict. The
    same profile therefore behaved differently depending on ``high_concurrency``
    with no way to tell what was actually sent.
    """

    def test_allowlisted_keys_still_forwarded(self):
        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        payload = hc._build_acquire_payload()
        assert payload["name"] == "test-session"
        assert payload["numExecutors"] == 4

    @pytest.mark.parametrize(
        "key,value",
        [
            ("heartbeatTimeoutInSecond", 3600),
            ("queue", "default"),
            ("proxyUser", "svc-dbt"),
            ("kind", "sql"),
            ("someFutureFabricKey", {"nested": True}),
        ],
    )
    def test_previously_dropped_keys_are_forwarded(self, key, value):
        creds = _make_creds(spark_config={"name": "test-session", key: value})
        hc = HighConcurrencySession(creds, creds.spark_config)
        payload = hc._build_acquire_payload()
        assert payload[key] == value

    def test_session_tag_is_injected(self):
        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        assert hc._build_acquire_payload()["sessionTag"] == hc.session_tag

    def test_user_session_tag_is_overridden_with_warning(self):
        creds = _make_creds(spark_config={"name": "test-session", "sessionTag": "mine"})
        hc = HighConcurrencySession(creds, creds.spark_config)
        with patch.object(concurrent_livy.logger, "warning") as mock_warn:
            payload = hc._build_acquire_payload()
        assert payload["sessionTag"] == hc.session_tag
        assert mock_warn.call_count == 1
        assert "mine" in mock_warn.call_args[0][0]

    def test_matching_user_session_tag_does_not_warn(self):
        creds = _make_creds()
        hc = HighConcurrencySession(creds, creds.spark_config)
        hc.spark_config = {"name": "test-session", "sessionTag": hc.session_tag}
        with patch.object(concurrent_livy.logger, "warning") as mock_warn:
            hc._build_acquire_payload()
        mock_warn.assert_not_called()

    def test_user_conf_survives_alongside_injected_conf(self):
        creds = _make_creds(
            spark_config={"name": "test-session", "conf": {"spark.dbt.canary": "alive"}},
            environmentId="11111111-2222-3333-4444-555555555555",
        )
        hc = HighConcurrencySession(creds, creds.spark_config)
        conf = hc._build_acquire_payload()["conf"]
        assert conf["spark.dbt.canary"] == "alive"
        assert conf["spark.fabric.environment.id"] == "11111111-2222-3333-4444-555555555555"

    def test_payload_build_does_not_mutate_credentials_spark_config(self):
        creds = _make_creds(spark_config={"name": "test-session", "conf": {"a": "b"}})
        hc = HighConcurrencySession(creds, creds.spark_config)
        hc._build_acquire_payload()
        assert creds.spark_config == {"name": "test-session", "conf": {"a": "b"}}
