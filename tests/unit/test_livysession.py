"""Tests for livysession module, focusing on local vs Fabric mode routing."""
import os
import tempfile
import pytest
from unittest import mock
from unittest.mock import MagicMock, patch

from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.livysession import (
    get_headers,
    LivySession,
    LivyCursor,
    LivyConnection,
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
            schema="default",
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
            schema="default",
            workspaceid="workspace-guid",
            lakehouseid="lakehouse-guid",
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
            schema="default",
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
            schema="default",
            workspaceid="workspace-guid",
            lakehouseid="lakehouse-guid",
            endpoint="https://api.fabric.microsoft.com/v1",
            spark_config={"name": "test-session"},
        )
        
        session = LivySession(credentials)
        
        assert session.is_local_mode is False
        assert "workspaces/workspace-guid" in session.connect_url
        assert "lakehouses/lakehouse-guid" in session.connect_url


class TestLivyCursor:
    """Tests for the LivyCursor class."""

    def test_init_local_mode(self):
        """Test LivyCursor initialization in local mode."""
        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="local",
            schema="default",
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
            schema="default",
            workspaceid="workspace-guid",
            lakehouseid="lakehouse-guid",
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
            schema="default",
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
            schema="default",
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
            schema="default",
            spark_config={"name": "test-session"},
        )
        
        expected_path = os.path.join(os.getcwd(), "livy-session-id.txt")
        assert credentials.resolved_session_id_file == expected_path

    def test_custom_session_file_path(self):
        """Test custom session file path when specified."""
        credentials = FabricSparkCredentials(
            method="livy",
            livy_mode="local",
            schema="default",
            spark_config={"name": "test-session"},
            session_id_file="/custom/path/my-session.txt",
        )
        
        assert credentials.resolved_session_id_file == "/custom/path/my-session.txt"