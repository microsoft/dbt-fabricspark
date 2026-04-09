"""Tests for livysession module, focusing on local vs Fabric mode routing."""
import os
import tempfile
from unittest.mock import MagicMock, patch

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
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("")
            temp_path = f.name

        try:
            result = read_session_id_from_file(temp_path)
            assert result is None
        finally:
            os.unlink(temp_path)

    def test_read_session_id_from_valid_file(self):
        """Test reading a valid session ID from file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("123")
            temp_path = f.name

        try:
            result = read_session_id_from_file(temp_path)
            assert result == "123"
        finally:
            os.unlink(temp_path)

    def test_read_session_id_strips_whitespace(self):
        """Test that whitespace is stripped from session ID."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
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
            with open(file_path, 'r') as f:
                assert f.read() == "789"

    def test_write_session_id_creates_directory(self):
        """Test that write creates parent directories if needed."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "subdir", "nested", "session.txt")

            result = write_session_id_to_file(file_path, "999")

            assert result is True
            assert os.path.exists(file_path)
            with open(file_path, 'r') as f:
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

            assert not os.path.exists(session_file), "Session file should not be created in non-reuse mode"

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

        with patch.object(LivySessionManager, "_connect_fabric_reuse") as mock_reuse, \
             patch.object(LivySessionManager, "_connect_fabric_fresh") as mock_fresh:
            LivySessionManager._connect_fabric(credentials, {"name": "test"})
            mock_reuse.assert_called_once_with(credentials, {"name": "test"})
            mock_fresh.assert_not_called()

    @patch("dbt.adapters.fabricspark.livysession.get_headers")
    def test_connect_fabric_routes_to_fresh_when_flag_not_set(self, mock_headers):
        """_connect_fabric should route to _connect_fabric_fresh when reuse_session=False."""
        credentials = _make_fabric_credentials(reuse_session=False)

        with patch.object(LivySessionManager, "_connect_fabric_reuse") as mock_reuse, \
             patch.object(LivySessionManager, "_connect_fabric_fresh") as mock_fresh:
            LivySessionManager._connect_fabric(credentials, {"name": "test"})
            mock_fresh.assert_called_once_with(credentials, {"name": "test"})
            mock_reuse.assert_not_called()
