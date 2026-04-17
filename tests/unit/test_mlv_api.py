"""Unit tests for the MLV REST API helper module."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from dbt.adapters.fabricspark.mlv_api import (
    MAX_RETRIES,
    MLVApiError,
    _base_url,
    _extract_error_detail,
    _job_instance_url,
    _lakehouse_id_cache,
    _request_with_retry,
    create_or_update_schedule,
    create_schedule,
    delete_schedule,
    get_job_instance,
    list_schedules,
    poll_job_instance_until_complete,
    resolve_lakehouse_id,
    run_on_demand_refresh,
    update_schedule,
)


@pytest.fixture
def mock_credentials():
    creds = MagicMock()
    creds.endpoint = "https://api.fabric.microsoft.com/v1"
    creds.workspaceid = "ws-1234"
    creds.lakehouseid = "lh-5678"
    creds.http_timeout = 120
    creds.poll_statement_wait = 1
    creds.statement_timeout = 10
    return creds


class TestBaseUrl:
    def test_uses_credentials_lakehouse(self, mock_credentials):
        url = _base_url(mock_credentials)
        assert url == (
            "https://api.fabric.microsoft.com/v1/workspaces/ws-1234"
            "/lakehouses/lh-5678/jobs/RefreshMaterializedLakeViews"
        )

    def test_uses_override_lakehouse(self, mock_credentials):
        url = _base_url(mock_credentials, lakehouse_id="lh-override")
        assert url == (
            "https://api.fabric.microsoft.com/v1/workspaces/ws-1234"
            "/lakehouses/lh-override/jobs/RefreshMaterializedLakeViews"
        )


class TestExtractErrorDetail:
    def test_extracts_fabric_error_format(self):
        resp = MagicMock()
        resp.json.return_value = {"error": {"code": "LakehouseNotFound", "message": "Not found"}}
        assert "[LakehouseNotFound] Not found" == _extract_error_detail(resp)

    def test_falls_back_to_text(self):
        resp = MagicMock()
        resp.json.side_effect = ValueError("not json")
        resp.text = "plain error text"
        assert "plain error text" == _extract_error_detail(resp)

    def test_handles_non_fabric_json(self):
        resp = MagicMock()
        resp.json.return_value = {"msg": "other format"}
        resp.text = "raw body"
        assert "raw body" == _extract_error_detail(resp)


class TestRequestWithRetry:
    @patch("dbt.adapters.fabricspark.mlv_api.time.sleep")
    @patch("dbt.adapters.fabricspark.mlv_api.requests.request")
    def test_retries_on_429(self, mock_request, mock_sleep):
        rate_limit_resp = MagicMock()
        rate_limit_resp.status_code = 429
        rate_limit_resp.headers = {"Retry-After": "1"}
        rate_limit_resp.json.return_value = {
            "error": {"code": "TooManyRequests", "message": "slow down"}
        }
        rate_limit_resp.text = "slow down"

        ok_resp = MagicMock()
        ok_resp.status_code = 200

        mock_request.side_effect = [rate_limit_resp, ok_resp]
        result = _request_with_retry("GET", "http://test", {}, "test-op", 30)
        assert result.status_code == 200
        assert mock_request.call_count == 2
        mock_sleep.assert_called_once()

    @patch("dbt.adapters.fabricspark.mlv_api.time.sleep")
    @patch("dbt.adapters.fabricspark.mlv_api.requests.request")
    def test_retries_on_500(self, mock_request, mock_sleep):
        err_resp = MagicMock()
        err_resp.status_code = 500
        err_resp.headers = {}
        err_resp.json.return_value = {"error": {"code": "InternalError", "message": "oops"}}
        err_resp.text = "oops"

        ok_resp = MagicMock()
        ok_resp.status_code = 200

        mock_request.side_effect = [err_resp, ok_resp]
        result = _request_with_retry("POST", "http://test", {}, "test-op", 30)
        assert result.status_code == 200

    @patch("dbt.adapters.fabricspark.mlv_api.time.sleep")
    @patch("dbt.adapters.fabricspark.mlv_api.requests.request")
    def test_raises_after_max_retries(self, mock_request, mock_sleep):
        err_resp = MagicMock()
        err_resp.status_code = 503
        err_resp.headers = {}
        err_resp.json.return_value = {"error": {"code": "ServiceUnavailable", "message": "down"}}
        err_resp.text = "down"

        mock_request.return_value = err_resp
        with pytest.raises(MLVApiError, match="ServiceUnavailable"):
            _request_with_retry("GET", "http://test", {}, "test-op", 30)
        assert mock_request.call_count == MAX_RETRIES

    @patch("dbt.adapters.fabricspark.mlv_api.requests.request")
    def test_raises_immediately_on_400(self, mock_request):
        err_resp = MagicMock()
        err_resp.status_code = 400
        err_resp.headers = {}
        err_resp.json.return_value = {"error": {"code": "BadRequest", "message": "invalid"}}
        err_resp.text = "invalid"

        mock_request.return_value = err_resp
        with pytest.raises(MLVApiError, match="BadRequest"):
            _request_with_retry("POST", "http://test", {}, "test-op", 30)
        assert mock_request.call_count == 1

    @patch("dbt.adapters.fabricspark.mlv_api.time.sleep")
    @patch("dbt.adapters.fabricspark.mlv_api.requests.request")
    def test_retries_on_connection_error(self, mock_request, mock_sleep):
        mock_request.side_effect = [
            requests.exceptions.ConnectionError("refused"),
            MagicMock(status_code=200),
        ]
        result = _request_with_retry("GET", "http://test", {}, "test-op", 30)
        assert result.status_code == 200
        assert mock_request.call_count == 2

    @patch("dbt.adapters.fabricspark.mlv_api.time.sleep")
    @patch("dbt.adapters.fabricspark.mlv_api.requests.request")
    def test_retries_on_timeout(self, mock_request, mock_sleep):
        mock_request.side_effect = [
            requests.exceptions.Timeout("timed out"),
            MagicMock(status_code=202),
        ]
        result = _request_with_retry("POST", "http://test", {}, "test-op", 30)
        assert result.status_code == 202


class TestJobInstanceUrl:
    def test_builds_url(self, mock_credentials):
        url = _job_instance_url(mock_credentials, None, "job-abc")
        assert url == (
            "https://api.fabric.microsoft.com/v1/workspaces/ws-1234"
            "/lakehouses/lh-5678/jobs/instances/job-abc"
        )

    def test_uses_override_lakehouse(self, mock_credentials):
        url = _job_instance_url(mock_credentials, "lh-override", "job-abc")
        assert url == (
            "https://api.fabric.microsoft.com/v1/workspaces/ws-1234"
            "/lakehouses/lh-override/jobs/instances/job-abc"
        )


class TestGetJobInstance:
    @patch("dbt.adapters.fabricspark.mlv_api.get_headers")
    @patch("dbt.adapters.fabricspark.mlv_api._request_with_retry")
    def test_returns_job_dict(self, mock_request, mock_headers, mock_credentials):
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "id": "job-123",
            "status": "Completed",
            "failureReason": None,
        }
        mock_request.return_value = mock_response

        result = get_job_instance(mock_credentials, "job-123")
        assert result["status"] == "Completed"
        mock_request.assert_called_once()


class TestPollJobInstanceUntilComplete:
    @patch("dbt.adapters.fabricspark.mlv_api.time.sleep")
    @patch("dbt.adapters.fabricspark.mlv_api.get_job_instance")
    def test_returns_on_completed(self, mock_get, mock_sleep, mock_credentials):
        mock_get.side_effect = [
            {"status": "InProgress", "failureReason": None},
            {"status": "Completed", "failureReason": None},
        ]
        result = poll_job_instance_until_complete(mock_credentials, "job-1")
        assert result["status"] == "Completed"
        assert mock_sleep.call_count == 1

    @patch("dbt.adapters.fabricspark.mlv_api.time.sleep")
    @patch("dbt.adapters.fabricspark.mlv_api.get_job_instance")
    def test_raises_on_failed(self, mock_get, mock_sleep, mock_credentials):
        mock_get.return_value = {"status": "Failed", "failureReason": "OOM error"}
        with pytest.raises(MLVApiError, match="OOM error"):
            poll_job_instance_until_complete(mock_credentials, "job-1")

    @patch("dbt.adapters.fabricspark.mlv_api.time.sleep")
    @patch("dbt.adapters.fabricspark.mlv_api.get_job_instance")
    def test_raises_on_cancelled(self, mock_get, mock_sleep, mock_credentials):
        mock_get.return_value = {"status": "Cancelled", "failureReason": None}
        with pytest.raises(MLVApiError, match="Cancelled"):
            poll_job_instance_until_complete(mock_credentials, "job-1")

    @patch("dbt.adapters.fabricspark.mlv_api.time.sleep")
    @patch("dbt.adapters.fabricspark.mlv_api.get_job_instance")
    def test_raises_on_timeout(self, mock_get, mock_sleep, mock_credentials):
        mock_credentials.statement_timeout = 2
        mock_credentials.poll_statement_wait = 1
        mock_get.return_value = {"status": "InProgress", "failureReason": None}
        with pytest.raises(MLVApiError, match="timed out"):
            poll_job_instance_until_complete(mock_credentials, "job-1")


class TestRunOnDemandRefresh:
    @patch("dbt.adapters.fabricspark.mlv_api.poll_job_instance_until_complete")
    @patch("dbt.adapters.fabricspark.mlv_api.get_headers")
    @patch("dbt.adapters.fabricspark.mlv_api._request_with_retry")
    def test_triggers_and_polls(self, mock_request, mock_headers, mock_poll, mock_credentials):
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_response = MagicMock()
        mock_response.status_code = 202
        mock_response.headers = {
            "Location": "https://api.fabric.microsoft.com/v1/.../instances/job-123",
        }
        mock_request.return_value = mock_response
        mock_poll.return_value = {"status": "Completed", "failureReason": None}

        result = run_on_demand_refresh(mock_credentials)

        mock_request.assert_called_once()
        mock_poll.assert_called_once_with(mock_credentials, "job-123", None)
        assert result["status"] == "Completed"

    @patch("dbt.adapters.fabricspark.mlv_api.get_headers")
    @patch("dbt.adapters.fabricspark.mlv_api._request_with_retry")
    def test_raises_when_no_location_header(self, mock_request, mock_headers, mock_credentials):
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_response = MagicMock()
        mock_response.headers = {"Location": ""}
        mock_request.return_value = mock_response

        with pytest.raises(MLVApiError, match="Could not extract job instance ID"):
            run_on_demand_refresh(mock_credentials)

    @patch("dbt.adapters.fabricspark.mlv_api.get_headers")
    @patch("dbt.adapters.fabricspark.mlv_api._request_with_retry")
    def test_propagates_api_error(self, mock_request, mock_headers, mock_credentials):
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_request.side_effect = MLVApiError("on-demand MLV refresh", "HTTP 403 — Forbidden")

        with pytest.raises(MLVApiError, match="Forbidden"):
            run_on_demand_refresh(mock_credentials)


class TestListSchedules:
    @patch("dbt.adapters.fabricspark.mlv_api.get_headers")
    @patch("dbt.adapters.fabricspark.mlv_api._request_with_retry")
    def test_returns_schedules(self, mock_request, mock_headers, mock_credentials):
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": [{"id": "sched-1", "enabled": True}]}
        mock_request.return_value = mock_response

        result = list_schedules(mock_credentials)

        assert len(result) == 1
        assert result[0]["id"] == "sched-1"

    @patch("dbt.adapters.fabricspark.mlv_api.get_headers")
    @patch("dbt.adapters.fabricspark.mlv_api._request_with_retry")
    def test_returns_empty_when_no_schedules(self, mock_request, mock_headers, mock_credentials):
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        mock_request.return_value = mock_response

        result = list_schedules(mock_credentials)
        assert result == []


class TestCreateSchedule:
    @patch("dbt.adapters.fabricspark.mlv_api.get_headers")
    @patch("dbt.adapters.fabricspark.mlv_api._request_with_retry")
    def test_creates_schedule(self, mock_request, mock_headers, mock_credentials):
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": "sched-new", "enabled": True}
        mock_request.return_value = mock_response

        config = {
            "enabled": True,
            "configuration": {
                "type": "Cron",
                "interval": 10,
                "startDateTime": "2026-04-10T00:00:00",
                "endDateTime": "2027-04-10T00:00:00",
                "localTimeZoneId": "Central Standard Time",
            },
        }
        result = create_schedule(mock_credentials, config)

        mock_request.assert_called_once()
        call_kwargs = mock_request.call_args
        assert call_kwargs.kwargs["json_body"] == config
        assert result["id"] == "sched-new"

    def test_raises_when_no_end_date(self, mock_credentials):
        config = {
            "enabled": True,
            "configuration": {
                "type": "Daily",
                "startDateTime": "2026-04-10T00:00:00",
                "times": ["06:00"],
            },
        }
        with pytest.raises(MLVApiError, match="endDateTime.*required"):
            create_schedule(mock_credentials, config)


class TestUpdateSchedule:
    @patch("dbt.adapters.fabricspark.mlv_api.get_headers")
    @patch("dbt.adapters.fabricspark.mlv_api._request_with_retry")
    def test_updates_schedule(self, mock_request, mock_headers, mock_credentials):
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_response = MagicMock()
        mock_response.json.return_value = {"id": "sched-1", "enabled": True}
        mock_request.return_value = mock_response

        config = {"enabled": True, "configuration": {"type": "Cron", "interval": 30}}
        result = update_schedule(mock_credentials, "sched-1", config)

        assert "sched-1" in mock_request.call_args.args[1]  # URL
        assert result["id"] == "sched-1"


class TestDeleteSchedule:
    @patch("dbt.adapters.fabricspark.mlv_api.get_headers")
    @patch("dbt.adapters.fabricspark.mlv_api._request_with_retry")
    def test_deletes_schedule(self, mock_request, mock_headers, mock_credentials):
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        delete_schedule(mock_credentials, "sched-1")

        mock_request.assert_called_once()
        assert "sched-1" in mock_request.call_args.args[1]


class TestCreateOrUpdateSchedule:
    @patch("dbt.adapters.fabricspark.mlv_api.create_schedule")
    @patch("dbt.adapters.fabricspark.mlv_api.list_schedules")
    def test_creates_when_no_existing(self, mock_list, mock_create, mock_credentials):
        mock_list.return_value = []
        mock_create.return_value = {"id": "sched-new"}

        config = {"enabled": True, "configuration": {"type": "Cron", "interval": 10}}
        result = create_or_update_schedule(mock_credentials, config)

        mock_create.assert_called_once()
        assert result["id"] == "sched-new"

    @patch("dbt.adapters.fabricspark.mlv_api.update_schedule")
    @patch("dbt.adapters.fabricspark.mlv_api.list_schedules")
    def test_updates_existing(self, mock_list, mock_update, mock_credentials):
        mock_list.return_value = [{"id": "sched-existing"}]
        mock_update.return_value = {"id": "sched-existing"}

        config = {"enabled": True, "configuration": {"type": "Cron", "interval": 30}}
        result = create_or_update_schedule(mock_credentials, config)

        mock_update.assert_called_once_with(mock_credentials, "sched-existing", config, None)
        assert result["id"] == "sched-existing"

    @patch("dbt.adapters.fabricspark.mlv_api.create_schedule")
    @patch("dbt.adapters.fabricspark.mlv_api.list_schedules")
    def test_propagates_create_error(self, mock_list, mock_create, mock_credentials):
        mock_list.return_value = []
        mock_create.side_effect = MLVApiError("create MLV schedule", "HTTP 400 — Bad config")

        config = {"enabled": True}
        with pytest.raises(MLVApiError, match="Bad config"):
            create_or_update_schedule(mock_credentials, config)


class TestResolveLakehouseId:
    def setup_method(self):
        """Clear the cache before each test."""
        _lakehouse_id_cache.clear()

    @patch("dbt.adapters.fabricspark.mlv_api.get_headers")
    @patch("dbt.adapters.fabricspark.mlv_api._request_with_retry")
    def test_resolves_by_name(self, mock_request, mock_headers, mock_credentials):
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "value": [
                {"id": "lh-bronze-id", "displayName": "bronze"},
                {"id": "lh-gold-id", "displayName": "gold"},
            ]
        }
        mock_request.return_value = mock_response

        result = resolve_lakehouse_id(mock_credentials, "gold")
        assert result == "lh-gold-id"

    @patch("dbt.adapters.fabricspark.mlv_api.get_headers")
    @patch("dbt.adapters.fabricspark.mlv_api._request_with_retry")
    def test_case_insensitive(self, mock_request, mock_headers, mock_credentials):
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": [{"id": "lh-gold-id", "displayName": "Gold"}]}
        mock_request.return_value = mock_response

        result = resolve_lakehouse_id(mock_credentials, "gold")
        assert result == "lh-gold-id"

    @patch("dbt.adapters.fabricspark.mlv_api.get_headers")
    @patch("dbt.adapters.fabricspark.mlv_api._request_with_retry")
    def test_raises_when_not_found(self, mock_request, mock_headers, mock_credentials):
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "value": [{"id": "lh-bronze-id", "displayName": "bronze"}]
        }
        mock_request.return_value = mock_response

        with pytest.raises(MLVApiError, match="not found in workspace"):
            resolve_lakehouse_id(mock_credentials, "gold")

    @patch("dbt.adapters.fabricspark.mlv_api.get_headers")
    @patch("dbt.adapters.fabricspark.mlv_api._request_with_retry")
    def test_uses_cache(self, mock_request, mock_headers, mock_credentials):
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": [{"id": "lh-gold-id", "displayName": "gold"}]}
        mock_request.return_value = mock_response

        # First call populates cache
        resolve_lakehouse_id(mock_credentials, "gold")
        # Second call uses cache — no additional API call
        result = resolve_lakehouse_id(mock_credentials, "gold")

        assert result == "lh-gold-id"
        assert mock_request.call_count == 1
