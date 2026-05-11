"""Unit tests for the FabricClient REST helper used by functional tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from tests.functional.fabric_client import FabricClient, StaticTokenProvider


def _make_client() -> FabricClient:
    return FabricClient(
        workspace_id="ws-1",
        api_endpoint="https://api.fabric.microsoft.com/v1",
        token_provider=StaticTokenProvider("fake-token"),
    )


def _mock_response(status_code: int, json_body: dict) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body
    resp.text = ""
    resp.raise_for_status.return_value = None
    return resp


class TestListLivySessions:
    """Tests for FabricClient.list_livy_sessions used by the regression guard."""

    def test_active_only_filters_terminal_states(self):
        client = _make_client()
        body = {
            "sessions": [
                {"id": "1", "state": "idle"},
                {"id": "2", "state": "busy"},
                {"id": "3", "state": "dead"},
                {"id": "4", "state": "killed"},
                {"id": "5", "state": "shutting_down"},
                {"id": "6", "state": "error"},
                {"id": "7", "state": "success"},
                {"id": "8", "state": "starting"},
            ]
        }
        with patch.object(
            client._session, "request", return_value=_mock_response(200, body)
        ) as mock_req:
            sessions = client.list_livy_sessions("lh-1")

        assert {s["id"] for s in sessions} == {"1", "2", "8"}
        called_url = mock_req.call_args.args[1]
        assert called_url == (
            "https://api.fabric.microsoft.com/v1/workspaces/ws-1/"
            "lakehouses/lh-1/livyApi/versions/2023-12-01/sessions"
        )

    def test_active_only_false_returns_all(self):
        client = _make_client()
        body = {"sessions": [{"id": "1", "state": "idle"}, {"id": "2", "state": "dead"}]}
        with patch.object(client._session, "request", return_value=_mock_response(200, body)):
            sessions = client.list_livy_sessions("lh-1", active_only=False)
        assert {s["id"] for s in sessions} == {"1", "2"}

    def test_handles_missing_sessions_key(self):
        client = _make_client()
        with patch.object(client._session, "request", return_value=_mock_response(200, {})):
            assert client.list_livy_sessions("lh-1") == []

    def test_handles_null_sessions_value(self):
        client = _make_client()
        with patch.object(
            client._session, "request", return_value=_mock_response(200, {"sessions": None})
        ):
            assert client.list_livy_sessions("lh-1") == []

    def test_state_comparison_is_case_insensitive(self):
        client = _make_client()
        body = {"sessions": [{"id": "1", "state": "DEAD"}, {"id": "2", "state": "Idle"}]}
        with patch.object(client._session, "request", return_value=_mock_response(200, body)):
            sessions = client.list_livy_sessions("lh-1")
        assert {s["id"] for s in sessions} == {"2"}

    def test_propagates_http_errors(self):
        client = _make_client()
        resp = MagicMock()
        resp.status_code = 500
        resp.text = "boom"
        resp.raise_for_status.side_effect = Exception("HTTP 500")
        with patch.object(client._session, "request", return_value=resp):
            with pytest.raises(Exception, match="HTTP 500"):
                client.list_livy_sessions("lh-1")

    def test_uses_custom_livy_api_version(self):
        client = _make_client()
        body = {"sessions": []}
        with patch.object(
            client._session, "request", return_value=_mock_response(200, body)
        ) as mock_req:
            client.list_livy_sessions("lh-1", livy_api_version="2024-01-01")
        called_url = mock_req.call_args.args[1]
        assert "/livyApi/versions/2024-01-01/sessions" in called_url
