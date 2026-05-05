"""Tests for defensive parsing of the Fabric Livy SessionResponse.

Per the official Fabric Livy SessionResponse swagger contract, every
state-related field (``state``, ``livyInfo``, ``fabricSessionStateInfo``,
``id``, ``errorInfo``) is *optional*. These tests exercise the request
paths that previously assumed those fields were always present and would
raise an opaque ``KeyError`` (or silently time out) when they weren't.
"""
from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.exceptions import FailedToConnectError
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.livysession import LivySession


def _fabric_credentials(**overrides) -> FabricSparkCredentials:
    """Build a Fabric-mode credential instance suitable for tests."""
    base = dict(
        method="livy",
        livy_mode="fabric",
        authentication="CLI",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        lakehouse="tests",
        spark_config={"name": "test-session"},
        # Keep timeouts tiny so tests stay fast under the polling loop.
        session_start_timeout=2,
        poll_wait=0,
    )
    base.update(overrides)
    return FabricSparkCredentials(**base)


def _http_response(status: int, body: dict | None = None, text: str = "") -> MagicMock:
    """Build a mock requests.Response with the given status_code/json()."""
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = body if body is not None else {}
    resp.text = text
    return resp


# ---------------------------------------------------------------------------
# create_session — HTTP 429 / 430 rate-limit handling
# ---------------------------------------------------------------------------


@patch("dbt.adapters.fabricspark.livysession.requests.post")
@patch("dbt.adapters.fabricspark.livysession.get_headers")
def test_create_session_clear_error_on_http_430(mock_headers, mock_post):
    """Fabric returns HTTP 430 (non-standard) on Spark capacity overflow.
    The body is an ErrorResponse, not a SessionResponse, so the legacy
    ``response.json()["id"]`` lookup raised an opaque KeyError. We expect a
    clear FailedToConnectError that names the rate limit.
    """
    mock_headers.return_value = {"Authorization": "Bearer t"}
    mock_post.return_value = _http_response(430, {"errorCode": "TooManyRequests"})

    session = LivySession(_fabric_credentials())
    with pytest.raises(FailedToConnectError, match="rate limit.*HTTP 430"):
        session.create_session({"name": "test"})


@patch("dbt.adapters.fabricspark.livysession.requests.post")
@patch("dbt.adapters.fabricspark.livysession.get_headers")
def test_create_session_clear_error_on_http_429(mock_headers, mock_post):
    """Same path as 430, but for the standard 429 status."""
    mock_headers.return_value = {"Authorization": "Bearer t"}
    mock_post.return_value = _http_response(429, {"errorCode": "TooManyRequests"})

    session = LivySession(_fabric_credentials())
    with pytest.raises(FailedToConnectError, match="rate limit.*HTTP 429"):
        session.create_session({"name": "test"})


# ---------------------------------------------------------------------------
# create_session — defensive 'id' extraction (id is optional per swagger)
# ---------------------------------------------------------------------------


@patch("dbt.adapters.fabricspark.livysession.requests.post")
@patch("dbt.adapters.fabricspark.livysession.get_headers")
def test_create_session_clear_error_when_response_missing_id(mock_headers, mock_post):
    """SessionResponse.id is optional per the swagger contract. When Fabric
    returns 200/201 with a body that omits ``id``, surface a clear error
    instead of letting ``KeyError('id')`` bubble up.
    """
    mock_headers.return_value = {"Authorization": "Bearer t"}
    mock_post.return_value = _http_response(
        201, {"livyInfo": {"currentState": "starting"}}
    )

    session = LivySession(_fabric_credentials())
    with pytest.raises(FailedToConnectError, match="no 'id' field"):
        session.create_session({"name": "test"})


# ---------------------------------------------------------------------------
# wait_for_session_start — non-2xx response is ErrorResponse, not SessionResponse
# ---------------------------------------------------------------------------


@patch("dbt.adapters.fabricspark.livysession.requests.get")
@patch("dbt.adapters.fabricspark.livysession.get_headers")
def test_wait_for_session_start_fails_fast_on_auth_terminal_status(
    mock_headers, mock_get
):
    """On terminal auth/permission codes (401, 403, 410) the body is an
    ErrorResponse (errorCode + message); retry cannot help. Verify the
    HTTP status is checked first and the errorCode/message are surfaced
    immediately (no polling).
    """
    mock_headers.return_value = {"Authorization": "Bearer t"}
    mock_get.return_value = _http_response(
        401, {"errorCode": "Unauthorized", "message": "Token expired"}
    )

    session = LivySession(_fabric_credentials())
    session.session_id = "1"
    with pytest.raises(FailedToConnectError, match="Unauthorized.*Token expired"):
        session.wait_for_session_start()


@patch("dbt.adapters.fabricspark.livysession.requests.get")
@patch("dbt.adapters.fabricspark.livysession.get_headers")
def test_wait_for_session_start_tolerates_transient_4xx(mock_headers, mock_get):
    """A transient 404 on the session endpoint (Fabric infra blip during
    early acquisition) must NOT hard-fail — the patch tolerates it the same
    way the legacy code did, by logging and continuing the polling loop.
    Verify by returning 404 once then a successful "idle" response.
    """
    mock_headers.return_value = {"Authorization": "Bearer t"}
    mock_get.side_effect = [
        _http_response(404, {"errorCode": "NotFound", "message": "session not yet visible"}),
        _http_response(200, {"livyInfo": {"currentState": "idle"}}),
    ]

    session = LivySession(_fabric_credentials())
    session.session_id = "1"
    # Should reach idle on the second poll, not raise.
    session.wait_for_session_start()
    assert session.is_new_session_required is False
    assert mock_get.call_count == 2


@patch("dbt.adapters.fabricspark.livysession.requests.get")
@patch("dbt.adapters.fabricspark.livysession.get_headers")
def test_wait_for_session_start_tolerates_transient_5xx(mock_headers, mock_get):
    """A transient 503 from Fabric infra blip must not hard-fail — same
    tolerance class as 404.
    """
    mock_headers.return_value = {"Authorization": "Bearer t"}
    mock_get.side_effect = [
        _http_response(503, {"errorCode": "ServiceUnavailable"}),
        _http_response(200, {"livyInfo": {"currentState": "idle"}}),
    ]

    session = LivySession(_fabric_credentials())
    session.session_id = "1"
    session.wait_for_session_start()
    assert mock_get.call_count == 2


@patch("dbt.adapters.fabricspark.livysession.requests.get")
@patch("dbt.adapters.fabricspark.livysession.get_headers")
def test_wait_for_session_start_includes_shutting_down_as_terminal(
    mock_headers, mock_get
):
    """Per the canonical terminal-state set accepted in PR #65,
    ``shutting_down`` is a terminal state. Treating it as transient would
    cause the poller to burn the full ``session_start_timeout`` window
    waiting for a session that's being torn down.
    """
    mock_headers.return_value = {"Authorization": "Bearer t"}
    mock_get.return_value = _http_response(
        200,
        {
            "state": "shutting_down",
            "livyInfo": {"currentState": "shutting_down"},
            "errorInfo": [
                {"errorCode": "SessionShuttingDown", "message": "Session terminated"}
            ],
        },
    )

    session = LivySession(_fabric_credentials())
    session.session_id = "1"
    with pytest.raises(FailedToConnectError, match="Session terminated"):
        session.wait_for_session_start()


# ---------------------------------------------------------------------------
# wait_for_session_start — fabricSessionStateInfo is the earliest failure signal
# ---------------------------------------------------------------------------


@patch("dbt.adapters.fabricspark.livysession.requests.get")
@patch("dbt.adapters.fabricspark.livysession.get_headers")
def test_wait_for_session_start_detects_fabric_state_error(mock_headers, mock_get):
    """During Fabric-side acquisition, only fabricSessionStateInfo is
    populated; state and livyInfo are absent. Without checking
    fabricSessionStateInfo, capacity-rejection failures cause a silent
    multi-minute timeout.
    """
    mock_headers.return_value = {"Authorization": "Bearer t"}
    mock_get.return_value = _http_response(
        200,
        {"fabricSessionStateInfo": {"state": "error"}},
    )

    session = LivySession(_fabric_credentials())
    session.session_id = "1"
    with pytest.raises(
        FailedToConnectError,
        match="fabricSessionStateInfo.state=error",
    ):
        session.wait_for_session_start()


@patch("dbt.adapters.fabricspark.livysession.requests.get")
@patch("dbt.adapters.fabricspark.livysession.get_headers")
def test_wait_for_session_start_detects_fabric_state_cancelled(mock_headers, mock_get):
    """``cancelled`` is the second terminal value Fabric uses for
    ``fabricSessionStateInfo.state`` (e.g. when a parallel run cancels the
    acquisition request).
    """
    mock_headers.return_value = {"Authorization": "Bearer t"}
    mock_get.return_value = _http_response(
        200,
        {"fabricSessionStateInfo": {"state": "cancelled"}},
    )

    session = LivySession(_fabric_credentials())
    session.session_id = "1"
    with pytest.raises(
        FailedToConnectError,
        match="fabricSessionStateInfo.state=cancelled",
    ):
        session.wait_for_session_start()


# ---------------------------------------------------------------------------
# wait_for_session_start — errorInfo[] surfaces concrete failure reasons
# ---------------------------------------------------------------------------


@patch("dbt.adapters.fabricspark.livysession.requests.get")
@patch("dbt.adapters.fabricspark.livysession.get_headers")
def test_wait_for_session_start_surfaces_errorInfo_on_dead(mock_headers, mock_get):
    """When Livy reports state=dead, Fabric's errorInfo[] often carries the
    real reason (e.g. ``LIVY_JOB_TIMED_OUT``). Surface it instead of the
    generic 'failed to connect' message.
    """
    mock_headers.return_value = {"Authorization": "Bearer t"}
    mock_get.return_value = _http_response(
        200,
        {
            "state": "dead",
            "livyInfo": {"currentState": "dead"},
            "errorInfo": [
                {"errorCode": "LIVY_JOB_TIMED_OUT", "message": "Job timed out after 30m"}
            ],
        },
    )

    session = LivySession(_fabric_credentials())
    session.session_id = "1"
    with pytest.raises(FailedToConnectError, match="Job timed out after 30m"):
        session.wait_for_session_start()


# ---------------------------------------------------------------------------
# wait_for_session_start — defensive against missing optional fields
# ---------------------------------------------------------------------------


@patch("dbt.adapters.fabricspark.livysession.requests.get")
@patch("dbt.adapters.fabricspark.livysession.get_headers")
def test_wait_for_session_start_tolerates_response_missing_all_state_fields(
    mock_headers, mock_get
):
    """A Fabric SessionResponse with ``state``, ``livyInfo``, and
    ``fabricSessionStateInfo`` all absent must not raise — these are
    legitimately optional during the early acquisition window. The poller
    should simply continue waiting (and time out cleanly if nothing changes).
    """
    mock_headers.return_value = {"Authorization": "Bearer t"}
    mock_get.return_value = _http_response(200, {"id": "1"})

    session = LivySession(_fabric_credentials(session_start_timeout=1))
    session.session_id = "1"
    # The poller should hit the configured timeout, not a KeyError or other
    # unrelated exception.
    with pytest.raises(FailedToConnectError, match="Timeout"):
        session.wait_for_session_start()
