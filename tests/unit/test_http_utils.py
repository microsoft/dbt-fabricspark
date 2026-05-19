"""Unit tests for the shared HTTP helpers."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from dbt.adapters.fabricspark._http_utils import parse_retry_after


def _response(headers=None, body=None):
    resp = MagicMock()
    resp.headers = headers or {}
    if body is None:
        resp.json.side_effect = ValueError("no body")
    else:
        resp.json.return_value = body
    return resp


def test_returns_numeric_retry_after_header():
    assert parse_retry_after(_response(headers={"Retry-After": "7"})) == 7.0


def test_returns_zero_when_header_and_body_missing():
    assert parse_retry_after(_response()) == 0


def test_falls_back_to_body_until_timestamp_when_header_missing():
    target = (datetime.now(timezone.utc) + timedelta(seconds=12)).strftime("%m/%d/%Y %I:%M:%S %p")
    body = {"message": f"Too many requests; retry until: {target} (UTC)"}
    delta = parse_retry_after(_response(body=body))
    # Allow small clock drift between strftime and the function's own datetime.now() call.
    assert 10 <= delta <= 12


def test_clamps_negative_delta_to_zero_when_until_is_in_past():
    target = (datetime.now(timezone.utc) - timedelta(seconds=30)).strftime("%m/%d/%Y %I:%M:%S %p")
    body = {"message": f"Too many requests; retry until: {target} (UTC)"}
    assert parse_retry_after(_response(body=body)) == 0


def test_falls_back_to_body_when_header_is_not_numeric():
    target = (datetime.now(timezone.utc) + timedelta(seconds=5)).strftime("%m/%d/%Y %I:%M:%S %p")
    body = {"message": f"Too many requests; retry until: {target} (UTC)"}
    resp = _response(headers={"Retry-After": "not-a-number"}, body=body)
    delta = parse_retry_after(resp)
    assert 3 <= delta <= 5


def test_returns_zero_when_body_is_not_json():
    assert parse_retry_after(_response()) == 0


def test_returns_zero_when_until_pattern_is_malformed():
    body = {"message": "Too many requests; retry until: not-a-timestamp"}
    assert parse_retry_after(_response(body=body)) == 0


def test_returns_zero_when_message_has_no_until_clause():
    body = {"message": "Too many requests"}
    assert parse_retry_after(_response(body=body)) == 0
