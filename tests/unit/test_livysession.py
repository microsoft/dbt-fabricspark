"""Tests for livysession module, focusing on local vs Fabric mode routing."""

import datetime as dt
import os
import tempfile
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
import requests

from dbt.adapters.fabricspark.connections import LivySessionConnectionWrapper
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.livysession import (
    LivyConnection,
    LivyCursor,
    LivySession,
    LivySessionManager,
    get_headers,
    read_session_id_from_file,
    write_session_id_to_file,
)


class TestGetHeaders:
    """Tests for the get_headers function."""

    def test_local_mode_no_auth_header(self):
        """In local mode, no Authorization header should be present."""
        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="local",
            spark_config={"name": "test-session"},
        )
        headers = get_headers(credentials)

        assert "Content-Type" in headers
        assert headers["Content-Type"] == "application/json"
        assert "Authorization" not in headers

    @patch("dbt.adapters.fabricspark.livysession.get_cli_access_token")
    def test_fabric_mode_has_auth_header(self, mock_get_token):
        """In Fabric mode, Authorization header should be present."""
        mock_token = MagicMock()
        mock_token.token = "test-token"
        mock_token.expires_on = 9999999999  # Far future
        mock_get_token.return_value = mock_token

        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="fabric",
            authentication="CLI",
            workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
            lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
            lakehouse="tests",
            spark_config={"name": "test-session"},
        )

        # Reset global accessToken
        import dbt.adapters.fabricspark.livysession as livysession_module

        livysession_module.accessToken = None

        headers = get_headers(credentials)

        assert "Content-Type" in headers
        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer test-token"


class TestLivySession:
    """Tests for the LivySession class."""

    def test_init_local_mode(self):
        """Test LivySession initialization in local mode."""
        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="local",
            livy_url="http://localhost:8998",
            spark_config={"name": "test-session"},
        )

        session = LivySession(credentials)

        assert session.is_local_mode is True
        assert session.connect_url == "http://localhost:8998"
        assert session.session_id is None
        assert session.is_new_session_required is True

    def test_init_fabric_mode(self):
        """Test LivySession initialization in Fabric mode."""
        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="fabric",
            authentication="CLI",
            workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
            lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
            lakehouse="tests",
            endpoint="https://api.fabric.microsoft.com/v1",
            spark_config={"name": "test-session"},
        )

        session = LivySession(credentials)

        assert session.is_local_mode is False
        assert "workspaces/1de8390c-9aca-4790-bee8-72049109c0f4" in session.connect_url
        assert "lakehouses/8c5bc260-bc3a-4898-9ada-01e433d461ba" in session.connect_url


class TestCreateSessionRetry:
    """Tests for retry logic in LivySession.create_session()."""

    def _make_credentials(self):
        return FabricSparkCredentials(
            method="livy",
            livy_mode="local",
            livy_url="http://localhost:8998",
            spark_config={"name": "test-session"},
        )

    def _make_response(self, status_code, json_body=None):
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.json.return_value = json_body or {"id": 42}
        mock_resp.raise_for_status.return_value = None
        if status_code >= 400:
            mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
                response=mock_resp
            )
        return mock_resp

    @patch("dbt.adapters.fabricspark.livysession.get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.livysession.time.sleep")
    @patch("dbt.adapters.fabricspark.livysession.requests.post")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.wait_for_session_start")
    def test_create_session_succeeds_immediately(
        self, mock_wait, mock_post, mock_sleep, mock_headers
    ):
        """create_session should succeed on the first attempt with no retries."""
        mock_post.return_value = self._make_response(201)
        session = LivySession(self._make_credentials())

        session.create_session({"kind": "sql"})

        assert mock_post.call_count == 1
        mock_sleep.assert_not_called()

    @patch("dbt.adapters.fabricspark.livysession.get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.livysession.time.sleep")
    @patch("dbt.adapters.fabricspark.livysession.requests.post")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.wait_for_session_start")
    def test_create_session_accepts_202_without_retry(
        self, mock_wait, mock_post, mock_sleep, mock_headers
    ):
        """create_session should treat HTTP 202 as successful initiation."""
        mock_post.return_value = self._make_response(202)
        session = LivySession(self._make_credentials())

        session.create_session({"kind": "sql"})

        assert mock_post.call_count == 1
        mock_sleep.assert_not_called()

    @patch("dbt.adapters.fabricspark.livysession.get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.livysession.time.sleep")
    @patch("dbt.adapters.fabricspark.livysession.requests.post")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.wait_for_session_start")
    def test_create_session_retries_on_404_then_succeeds(
        self, mock_wait, mock_post, mock_sleep, mock_headers
    ):
        """create_session should retry on HTTP 404 and succeed on the next attempt."""
        mock_post.side_effect = [
            self._make_response(404),
            self._make_response(201),
        ]
        session = LivySession(self._make_credentials())

        session.create_session({"kind": "sql"})

        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(5)  # 5 * 2**0 = 5s on first retry

    @patch("dbt.adapters.fabricspark.livysession.get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.livysession.time.sleep")
    @patch("dbt.adapters.fabricspark.livysession.requests.post")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.wait_for_session_start")
    def test_create_session_retries_on_500_then_succeeds(
        self, mock_wait, mock_post, mock_sleep, mock_headers
    ):
        """create_session should retry on HTTP 5xx and succeed on the next attempt."""
        mock_post.side_effect = [
            self._make_response(500),
            self._make_response(201),
        ]
        session = LivySession(self._make_credentials())

        session.create_session({"kind": "sql"})

        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(5)

    @patch("dbt.adapters.fabricspark.livysession.get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.livysession.time.sleep")
    @patch("dbt.adapters.fabricspark.livysession.requests.post")
    def test_create_session_raises_after_all_retries_exhausted(
        self, mock_post, mock_sleep, mock_headers
    ):
        """create_session should raise after all 5 retry attempts return 404."""
        mock_post.return_value = self._make_response(404)
        session = LivySession(self._make_credentials())

        with pytest.raises(Exception, match="Http Error"):
            session.create_session({"kind": "sql"})

        # 5 total attempts (initial + 4 retries)
        assert mock_post.call_count == 5
        # sleep called 4 times: 5, 10, 20, 40 seconds
        assert mock_sleep.call_count == 4
        mock_sleep.assert_any_call(5)
        mock_sleep.assert_any_call(10)
        mock_sleep.assert_any_call(20)
        mock_sleep.assert_any_call(40)

    @patch("dbt.adapters.fabricspark.livysession.get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.livysession.time.sleep")
    @patch("dbt.adapters.fabricspark.livysession.requests.post")
    def test_create_session_does_not_retry_on_401(self, mock_post, mock_sleep, mock_headers):
        """create_session should NOT retry on 401 auth errors (non-transient)."""
        mock_post.return_value = self._make_response(401)
        session = LivySession(self._make_credentials())

        with pytest.raises(Exception, match="Http Error"):
            session.create_session({"kind": "sql"})

        assert mock_post.call_count == 1
        mock_sleep.assert_not_called()


class TestWaitForSessionStartTransientErrors:
    """Tests for transient error handling in LivySession.wait_for_session_start()."""

    def _make_credentials(self):
        return FabricSparkCredentials(
            method="livy",
            livy_mode="local",
            livy_url="http://localhost:8998",
            spark_config={"name": "test-session"},
        )

    def _make_idle_response(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"state": "idle"}
        return mock_resp

    @patch("dbt.adapters.fabricspark.livysession.get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.livysession.time.sleep")
    @patch("dbt.adapters.fabricspark.livysession.time.time")
    @patch("dbt.adapters.fabricspark.livysession.requests.get")
    def test_wait_retries_on_request_exception(
        self, mock_get, mock_time, mock_sleep, mock_headers
    ):
        """wait_for_session_start should retry when a transient RequestException occurs."""
        mock_time.return_value = 0  # Always within deadline
        mock_get.side_effect = [
            requests.exceptions.ConnectionError("connection refused"),
            self._make_idle_response(),
        ]

        session = LivySession(self._make_credentials())
        session.session_id = "42"

        session.wait_for_session_start()

        assert mock_get.call_count == 2
        # sleep called once for the retry after the transient error
        mock_sleep.assert_called_once()

    @patch("dbt.adapters.fabricspark.livysession.get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.livysession.time.sleep")
    @patch("dbt.adapters.fabricspark.livysession.time.time")
    @patch("dbt.adapters.fabricspark.livysession.requests.get")
    def test_wait_retries_on_json_decode_error(
        self, mock_get, mock_time, mock_sleep, mock_headers
    ):
        """wait_for_session_start should retry when the response body is not valid JSON."""
        mock_time.return_value = 0  # Always within deadline

        bad_resp = MagicMock()
        bad_resp.json.side_effect = requests.exceptions.JSONDecodeError("not valid JSON", "", 0)

        mock_get.side_effect = [
            bad_resp,
            self._make_idle_response(),
        ]

        session = LivySession(self._make_credentials())
        session.session_id = "42"

        session.wait_for_session_start()

        assert mock_get.call_count == 2
        mock_sleep.assert_called_once()

    @patch("dbt.adapters.fabricspark.livysession.get_headers", return_value={})
    @patch("dbt.adapters.fabricspark.livysession.time.sleep")
    @patch("dbt.adapters.fabricspark.livysession.time.time")
    @patch("dbt.adapters.fabricspark.livysession.requests.get")
    def test_wait_succeeds_immediately_without_retries(
        self, mock_get, mock_time, mock_sleep, mock_headers
    ):
        """wait_for_session_start should complete without sleeping when session is immediately idle."""
        mock_time.return_value = 0  # Always within deadline

        mock_get.return_value = self._make_idle_response()

        session = LivySession(self._make_credentials())
        session.session_id = "42"

        session.wait_for_session_start()

        assert mock_get.call_count == 1
        mock_sleep.assert_not_called()


class TestLivyCursor:
    """Tests for the LivyCursor class."""

    def test_init_local_mode(self):
        """Test LivyCursor initialization in local mode."""
        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="local",
            spark_config={"name": "test-session"},
        )

        mock_livy_session = MagicMock()
        mock_livy_session.session_id = "test-session-id"

        cursor = LivyCursor(credentials, mock_livy_session)

        assert cursor.is_local_mode is True
        assert cursor.session_id == "test-session-id"

    def test_init_fabric_mode(self):
        """Test LivyCursor initialization in Fabric mode."""
        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="fabric",
            authentication="CLI",
            workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
            lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
            lakehouse="tests",
            spark_config={"name": "test-session"},
        )

        mock_livy_session = MagicMock()
        mock_livy_session.session_id = "test-session-id"

        cursor = LivyCursor(credentials, mock_livy_session)

        assert cursor.is_local_mode is False


class TestLivyConnection:
    """Tests for the LivyConnection class."""

    def test_init(self):
        """Test LivyConnection initialization."""
        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="local",
            spark_config={"name": "test-session"},
        )

        mock_livy_session = MagicMock()
        mock_livy_session.session_id = "test-session-id"

        connection = LivyConnection(credentials, mock_livy_session)

        assert connection.session_id == "test-session-id"
        assert connection.get_session_id() == "test-session-id"

    def test_cursor_returns_livy_cursor(self):
        """Test that cursor() returns a LivyCursor instance."""
        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="local",
            spark_config={"name": "test-session"},
        )

        mock_livy_session = MagicMock()
        mock_livy_session.session_id = "test-session-id"

        connection = LivyConnection(credentials, mock_livy_session)
        cursor = connection.cursor()

        assert isinstance(cursor, LivyCursor)


class TestSessionFileManagement:
    """Tests for session ID file read/write functions."""

    def test_read_session_id_from_nonexistent_file(self):
        """Test reading from a file that doesn't exist returns None."""
        result = read_session_id_from_file("/nonexistent/path/session.txt")
        assert result is None

    def test_read_session_id_from_empty_file(self):
        """Test reading from an empty file returns None."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write("")
            temp_path = f.name

        try:
            result = read_session_id_from_file(temp_path)
            assert result is None
        finally:
            os.unlink(temp_path)

    def test_read_session_id_from_valid_file(self):
        """Test reading a valid session ID from file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write("123")
            temp_path = f.name

        try:
            result = read_session_id_from_file(temp_path)
            assert result == "123"
        finally:
            os.unlink(temp_path)

    def test_read_session_id_strips_whitespace(self):
        """Test that whitespace is stripped from session ID."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write("  456  \n")
            temp_path = f.name

        try:
            result = read_session_id_from_file(temp_path)
            assert result == "456"
        finally:
            os.unlink(temp_path)

    def test_write_session_id_to_file(self):
        """Test writing session ID to a file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "session.txt")

            result = write_session_id_to_file(file_path, "789")

            assert result is True
            with open(file_path, "r") as f:
                assert f.read() == "789"

    def test_write_session_id_creates_directory(self):
        """Test that write creates parent directories if needed."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "subdir", "nested", "session.txt")

            result = write_session_id_to_file(file_path, "999")

            assert result is True
            assert os.path.exists(file_path)
            with open(file_path, "r") as f:
                assert f.read() == "999"


class TestCredentialsSessionFile:
    """Tests for session_id_file credential property."""

    def test_default_session_file_path(self):
        """Test default session file path when not specified."""
        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="local",
            spark_config={"name": "test-session"},
        )

        expected_path = os.path.join(os.getcwd(), "livy-session-id.txt")
        assert credentials.resolved_session_id_file == expected_path

    def test_custom_session_file_path(self):
        """Test custom session file path when specified."""
        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="local",
            spark_config={"name": "test-session"},
            session_id_file="/custom/path/my-session.txt",
        )
        assert credentials.resolved_session_id_file == "/custom/path/my-session.txt"


def _make_fabric_credentials(reuse_session=False, session_id_file=None):
    """Helper to create Fabric mode credentials for tests."""
    return FabricSparkCredentials(
        method="livy",
        livy_mode="fabric",
        authentication="CLI",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        lakehouse="tests",
        endpoint="https://api.fabric.microsoft.com/v1",
        spark_config={"name": "test-session"},
        reuse_session=reuse_session,
        session_id_file=session_id_file,
    )


class TestFabricSessionReuseMode:
    """Tests for Fabric session reuse vs non-reuse mode routing and session file behavior."""

    def setup_method(self):
        """Reset global session state before each test."""
        LivySessionManager.livy_global_session = None

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.create_session")
    def test_fresh_mode_does_not_persist_session_file(self, mock_create, mock_headers):
        """In non-reuse mode, session ID should NOT be written to file."""
        mock_headers.return_value = {"Content-Type": "application/json"}
        mock_create.return_value = None

        with tempfile.TemporaryDirectory() as tmp:
            session_file = os.path.join(tmp, "session.txt")
            credentials = _make_fabric_credentials(
                reuse_session=False, session_id_file=session_file
            )

            LivySessionManager._connect_fabric_fresh(credentials, {"name": "test"})

            assert not os.path.exists(session_file), (
                "Session file should not be created in non-reuse mode"
            )

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.create_session")
    def test_reuse_mode_persists_session_file(self, mock_create, mock_headers):
        """In reuse mode, session ID should be written to file after creation."""
        mock_headers.return_value = {"Content-Type": "application/json"}
        mock_create.return_value = None

        with tempfile.TemporaryDirectory() as tmp:
            session_file = os.path.join(tmp, "session.txt")
            credentials = _make_fabric_credentials(
                reuse_session=True, session_id_file=session_file
            )

            # Simulate session creation setting a session ID
            def set_session_id(config):
                LivySessionManager.livy_global_session.session_id = "42"

            mock_create.side_effect = set_session_id

            LivySessionManager._connect_fabric_reuse(credentials, {"name": "test"})

            assert os.path.exists(session_file), "Session file should be created in reuse mode"
            assert read_session_id_from_file(session_file) == "42"

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.create_session")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.is_valid_session", return_value=True)
    def test_reuse_mode_reads_session_from_file(self, mock_valid, mock_create, mock_headers):
        """In reuse mode, should reattach to a session ID read from file."""
        mock_headers.return_value = {"Content-Type": "application/json"}

        with tempfile.TemporaryDirectory() as tmp:
            session_file = os.path.join(tmp, "session.txt")
            write_session_id_to_file(session_file, "99")

            credentials = _make_fabric_credentials(
                reuse_session=True, session_id_file=session_file
            )

            # Pre-create a session marked as needing a new session
            session = LivySession(credentials)
            session.is_new_session_required = True
            LivySessionManager.livy_global_session = session

            with patch.object(session, "try_reuse_session", return_value=True) as mock_reuse:
                LivySessionManager._connect_fabric_reuse(credentials, {"name": "test"})
                mock_reuse.assert_called_once_with("99")

            # create_session should NOT have been called — we reused
            mock_create.assert_not_called()

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.create_session")
    @patch("dbt.adapters.fabricspark.livysession.LivySession.is_valid_session", return_value=True)
    def test_reuse_mode_creates_new_session_when_file_session_invalid(
        self, mock_valid, mock_create, mock_headers
    ):
        """In reuse mode, if the persisted session is invalid, create a new one."""
        mock_headers.return_value = {"Content-Type": "application/json"}

        with tempfile.TemporaryDirectory() as tmp:
            session_file = os.path.join(tmp, "session.txt")
            write_session_id_to_file(session_file, "dead-session")

            credentials = _make_fabric_credentials(
                reuse_session=True, session_id_file=session_file
            )

            session = LivySession(credentials)
            session.is_new_session_required = True
            LivySessionManager.livy_global_session = session

            def set_session_id(config):
                LivySessionManager.livy_global_session.session_id = "new-100"

            mock_create.side_effect = set_session_id

            with patch.object(session, "try_reuse_session", return_value=False):
                LivySessionManager._connect_fabric_reuse(credentials, {"name": "test"})

            # Should have created a new session
            mock_create.assert_called_once()
            # File should be updated with the new session ID
            assert read_session_id_from_file(session_file) == "new-100"

    def test_disconnect_deletes_session_in_non_reuse_mode(self):
        """In non-reuse mode, disconnect should delete the Livy session."""
        credentials = _make_fabric_credentials(reuse_session=False)
        session = MagicMock()
        session.is_local_mode = False
        session.credential = credentials
        session.session_id = "to-delete"
        LivySessionManager.livy_global_session = session

        LivySessionManager.disconnect()

        session.delete_session.assert_called_once()
        assert LivySessionManager.livy_global_session is None

    def test_disconnect_keeps_session_in_reuse_mode(self):
        """In reuse mode, disconnect should NOT delete the Livy session."""
        credentials = _make_fabric_credentials(reuse_session=True)
        session = MagicMock()
        session.is_local_mode = False
        session.credential = credentials
        session.session_id = "keep-alive"
        LivySessionManager.livy_global_session = session

        LivySessionManager.disconnect()

        session.delete_session.assert_not_called()
        assert LivySessionManager.livy_global_session is None

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    def test_connect_fabric_routes_to_reuse_when_flag_set(self, mock_headers):
        """_connect_fabric should route to _connect_fabric_reuse when reuse_session=True."""
        credentials = _make_fabric_credentials(reuse_session=True)

        with (
            patch.object(LivySessionManager, "_connect_fabric_reuse") as mock_reuse,
            patch.object(LivySessionManager, "_connect_fabric_fresh") as mock_fresh,
        ):
            LivySessionManager._connect_fabric(credentials, {"name": "test"})
            mock_reuse.assert_called_once_with(credentials, {"name": "test"})
            mock_fresh.assert_not_called()

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    def test_connect_fabric_routes_to_fresh_when_flag_not_set(self, mock_headers):
        """_connect_fabric should route to _connect_fabric_fresh when reuse_session=False."""
        credentials = _make_fabric_credentials(reuse_session=False)

        with (
            patch.object(LivySessionManager, "_connect_fabric_reuse") as mock_reuse,
            patch.object(LivySessionManager, "_connect_fabric_fresh") as mock_fresh,
        ):
            LivySessionManager._connect_fabric(credentials, {"name": "test"})
            mock_fresh.assert_called_once_with(credentials, {"name": "test"})
            mock_reuse.assert_not_called()


class TestFixBinding:
    """Tests for LivySessionConnectionWrapper._fix_binding."""

    def test_string_without_quotes(self):
        """Plain strings are wrapped in single quotes."""
        assert LivySessionConnectionWrapper._fix_binding("hello") == "'hello'"

    def test_string_with_single_quote(self):
        """Single quotes inside string values are escaped with a backslash."""
        result = LivySessionConnectionWrapper._fix_binding("Cote d'Ivoire")
        assert result == "'Cote d\\'Ivoire'"

    def test_string_with_multiple_single_quotes(self):
        """Multiple single quotes are all escaped."""
        result = LivySessionConnectionWrapper._fix_binding("it's a 'test'")
        assert result == "'it\\'s a \\'test\\''"

    def test_none_returns_empty_string_literal(self):
        assert LivySessionConnectionWrapper._fix_binding(None) == "''"

    def test_integer(self):
        assert LivySessionConnectionWrapper._fix_binding(42) == 42.0

    def test_float(self):
        assert LivySessionConnectionWrapper._fix_binding(3.14) == 3.14

    def test_decimal(self):
        assert LivySessionConnectionWrapper._fix_binding(Decimal("1.5")) == 1.5

    def test_datetime(self):
        value = dt.datetime(2024, 1, 15, 10, 30, 45, 123456)
        result = LivySessionConnectionWrapper._fix_binding(value)
        assert result == "'2024-01-15 10:30:45.123'"

    def test_seed_insert_with_single_quote_produces_valid_sql(self):
        """End-to-end check: bindings containing single quotes produce
        syntactically valid SQL when substituted into an INSERT template."""
        sql_template = (
            "insert into db.schema.sample values "
            "(cast(%s as bigint),cast(%s as string)),(cast(%s as bigint),cast(%s as string))"
        )
        raw_bindings = [1.0, "Cote d'Ivoire", 2.0, "Tonga"]
        bindings = tuple(LivySessionConnectionWrapper._fix_binding(b) for b in raw_bindings)
        sql = sql_template % bindings
        # The generated SQL must NOT have an unescaped inner quote that would
        # break parsing. The value should be: 'Cote d\'Ivoire'
        assert "Cote d\\'Ivoire" in sql
        assert sql == (
            "insert into db.schema.sample values "
            "(cast(1.0 as bigint),cast('Cote d\\'Ivoire' as string)),"
            "(cast(2.0 as bigint),cast('Tonga' as string))"
        )


class TestFetchmany:
    """Tests for LivyCursor.fetchmany and LivySessionConnectionWrapper.fetchmany."""

    def _make_cursor(self, rows):
        """Helper: return a LivyCursor with ``_rows`` pre-set."""
        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="local",
            spark_config={"name": "test-session"},
        )
        mock_session = MagicMock()
        mock_session.session_id = "s1"
        cursor = LivyCursor(credentials, mock_session)
        cursor._rows = rows
        return cursor

    def test_fetchmany_size_less_than_total(self):
        """fetchmany(2) returns first 2 rows from a 3-row result."""
        rows = [(1,), (2,), (3,)]
        cursor = self._make_cursor(rows)
        assert cursor.fetchmany(2) == [(1,), (2,)]

    def test_fetchmany_size_greater_than_total(self):
        """fetchmany(10) returns all rows when fewer than 10 exist."""
        rows = [(1,), (2,)]
        cursor = self._make_cursor(rows)
        assert cursor.fetchmany(10) == [(1,), (2,)]

    def test_fetchmany_size_none_returns_all(self):
        """fetchmany(None) returns all rows (same as fetchall)."""
        rows = [(1,), (2,), (3,)]
        cursor = self._make_cursor(rows)
        assert cursor.fetchmany(None) == rows

    def test_fetchmany_size_zero(self):
        """fetchmany(0) returns an empty list."""
        rows = [(1,), (2,)]
        cursor = self._make_cursor(rows)
        assert cursor.fetchmany(0) == []

    def test_fetchmany_rows_none_returns_none(self):
        """fetchmany returns None when the cursor has no result set."""
        cursor = self._make_cursor(None)
        assert cursor.fetchmany(5) is None

    def test_wrapper_fetchmany_delegates_to_cursor(self):
        """LivySessionConnectionWrapper.fetchmany delegates to the inner cursor."""
        mock_cursor = MagicMock()
        mock_cursor.fetchmany.return_value = [(1,), (2,)]
        mock_handle = MagicMock()
        mock_handle.cursor.return_value = mock_cursor

        wrapper = LivySessionConnectionWrapper(mock_handle)
        wrapper.cursor()  # sets self._cursor
        result = wrapper.fetchmany(2)

        mock_cursor.fetchmany.assert_called_once_with(2)
        assert result == [(1,), (2,)]

    def test_wrapper_fetchone_delegates_to_cursor(self):
        """LivySessionConnectionWrapper.fetchone delegates to the inner cursor."""
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1, "a")
        mock_handle = MagicMock()
        mock_handle.cursor.return_value = mock_cursor

        wrapper = LivySessionConnectionWrapper(mock_handle)
        wrapper.cursor()  # sets self._cursor
        result = wrapper.fetchone()

        mock_cursor.fetchone.assert_called_once_with()
        assert result == (1, "a")

    def test_fetchone_resets_on_new_execute(self):
        """After execute() is called, _fetch_index resets so fetchone() returns
        row 0 even if it was previously advanced."""
        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="local",
            spark_config={"name": "test-session"},
        )
        mock_session = MagicMock()
        mock_session.session_id = "s1"
        mock_session.is_new_session_required = False
        cursor = LivyCursor(credentials, mock_session)

        # Simulate a first execute result: two rows
        first_rows = [("a",), ("b",)]
        # Manually set _rows and advance the index (as if fetchone was called once)
        cursor._rows = first_rows
        cursor._fetch_index = 1

        # Now simulate a second execute (single-row result) via patching
        # the internal helpers that make remote calls.
        second_rows = [("c",)]
        second_result = {
            "output": {
                "status": "ok",
                "data": {
                    "application/json": {
                        "data": second_rows,
                        "schema": {"fields": []},
                    }
                },
            }
        }

        with (
            patch.object(cursor, "_getLivySQL", return_value="SELECT 1"),
            patch.object(cursor, "_submitLivyCode", return_value=MagicMock()),
            patch.object(cursor, "_getLivyResult", return_value=second_result),
        ):
            cursor.execute("SELECT 1")

        # After execute(), _fetch_index must have been reset to 0
        assert cursor._fetch_index == 0
        # fetchone() should return the first (only) row of the new result
        assert cursor.fetchone() == ("c",)
