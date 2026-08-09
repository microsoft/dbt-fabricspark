"""Governor-coverage tests for the Fabric REST clients.

These lock in that the shortcut, MLV and lakehouse-property clients route their
outbound Fabric calls through the process-wide throttle governor. Before the
coverage fix these clients used raw ``requests`` and were invisible to the
governor, so their calls did not draw on the shared per-identity budget and
their 429s did not park the gate that slows every other Fabric client.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests

import dbt.adapters.fabricspark.livysession as livy_mod
import dbt.adapters.fabricspark.mlv_api as mlv_mod
import dbt.adapters.fabricspark.shortcuts as shortcuts_mod
import dbt.adapters.fabricspark.throttle as throttle_mod
from dbt.adapters.fabricspark.mlv_api import (
    MLVApiError,
    _request_with_retry,
    create_schedule,
    delete_schedule,
    get_job_instance,
    list_schedules,
    resolve_lakehouse_id,
    run_on_demand_refresh,
    update_schedule,
)
from dbt.adapters.fabricspark.shortcuts import Shortcut, ShortcutClient, TargetName
from dbt.adapters.fabricspark.throttle import (
    PRIORITY_BACKGROUND,
    PRIORITY_CRITICAL,
    PRIORITY_NORMAL,
    ThrottleGovernor,
    governed,
    governor_for_credentials,
    governor_key,
    reset_governors,
)


class FakeClock:
    """Monotonic clock whose only advance comes from sleeping (see test_throttle)."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += max(seconds, 0.0)
        if seconds <= 0:
            self.now += 0.001


def _fake_governor(clock: FakeClock, budget: int = 150) -> ThrottleGovernor:
    return ThrottleGovernor(budget, clock=clock.time, sleeper=clock.sleep, jitter=lambda a, b: 0.0)


def _fabric_creds(**overrides) -> SimpleNamespace:
    """Credentials-shaped stub carrying only what the governor + clients read."""
    base = dict(
        is_local_mode=False,
        endpoint="https://api.fabric.microsoft.com/v1",
        workspaceid="ws-1",
        lakehouseid="lh-1",
        throttle_identity="client-abc",
        api_calls_per_minute=150,
        http_timeout=120,
        statement_timeout=10,
        poll_statement_wait=1,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _response(status=429, headers=None, body=None, raises=False) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.headers = headers or {}
    if body is None:
        resp.json.side_effect = ValueError("no body")
    else:
        resp.json.return_value = body
    if raises:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(f"HTTP {status}")
    else:
        resp.raise_for_status.return_value = None
    return resp


def _shortcut() -> Shortcut:
    return Shortcut(
        path="path",
        shortcut_name="name",
        target=TargetName.onelake,
        source_path="source_path",
        source_workspace_id="source_workspace_id",
        source_item_id="source_item_id",
    )


def _inject_governor(creds, gov) -> None:
    """Register ``gov`` as the shared governor for ``creds`` (endpoint, principal)."""
    throttle_mod._governors[governor_key(creds.endpoint, creds.throttle_identity)] = gov


@pytest.fixture(autouse=True)
def _isolate():
    reset_governors()
    mlv_mod._lakehouse_id_cache.clear()
    livy_mod._lakehouse_props_cache.clear()
    yield
    reset_governors()
    mlv_mod._lakehouse_id_cache.clear()
    livy_mod._lakehouse_props_cache.clear()


# --------------------------------------------------------------------------- #
# Routing + priority: each newly governed call site goes through ``_governed``  #
# with the deliberately chosen priority class.                                  #
# --------------------------------------------------------------------------- #


class TestShortcutRouting:
    def test_check_exists_uses_governed_normal(self):
        client = ShortcutClient("token", "ws", "item")
        with patch.object(shortcuts_mod, "_governed", return_value=_response(404)) as g:
            assert client.check_if_exists_and_delete_shortcut(_shortcut()) is False
        g.assert_called_once()
        assert g.call_args.args[1] == PRIORITY_NORMAL
        assert g.call_args.args[2] is requests.get

    def test_create_uses_governed_background(self):
        client = ShortcutClient("token", "ws", "item")
        with patch.object(client, "check_if_exists_and_delete_shortcut", return_value=False):
            with patch.object(shortcuts_mod, "_governed", return_value=_response(200)) as g:
                client.create_shortcut(_shortcut())
        g.assert_called_once()
        assert g.call_args.args[1] == PRIORITY_BACKGROUND
        assert g.call_args.args[2] is requests.post

    def test_delete_uses_governed_critical(self):
        client = ShortcutClient("token", "ws", "item")
        with patch.object(shortcuts_mod.time, "sleep"):
            with patch.object(shortcuts_mod, "_governed", return_value=_response(200)) as g:
                client.delete_shortcut("path", "name")
        g.assert_called_once()
        assert g.call_args.args[1] == PRIORITY_CRITICAL
        assert g.call_args.args[2] is requests.delete


class TestMlvRouting:
    def _patch_governed(self, resp):
        return patch.object(mlv_mod, "_governed", return_value=resp)

    def test_resolve_lakehouse_uses_governed_normal(self):
        creds = _fabric_creds()
        resp = _response(200, body={"value": [{"displayName": "lh", "id": "lid"}]})
        with patch.object(mlv_mod, "get_headers", return_value={}):
            with self._patch_governed(resp) as g:
                assert resolve_lakehouse_id(creds, "lh") == "lid"
        assert g.call_args.args[1] == PRIORITY_NORMAL
        assert g.call_args.args[2] is requests.request
        assert g.call_args.args[3] == "GET"

    def test_get_job_instance_uses_governed_critical(self):
        creds = _fabric_creds()
        resp = _response(200, body={"status": "Completed"})
        with patch.object(mlv_mod, "get_headers", return_value={}):
            with self._patch_governed(resp) as g:
                get_job_instance(creds, "job-1")
        assert g.call_args.args[1] == PRIORITY_CRITICAL
        assert g.call_args.args[3] == "GET"

    def test_run_on_demand_refresh_uses_governed_background(self):
        creds = _fabric_creds()
        resp = _response(202, headers={"Location": "https://x/instances/job-1"})
        with patch.object(mlv_mod, "get_headers", return_value={}):
            with patch.object(
                mlv_mod, "poll_job_instance_until_complete", return_value={"status": "Completed"}
            ):
                with self._patch_governed(resp) as g:
                    run_on_demand_refresh(creds)
        # The POST that submits the refresh is the only governed call here.
        assert g.call_args.args[1] == PRIORITY_BACKGROUND
        assert g.call_args.args[3] == "POST"

    def test_list_schedules_uses_governed_normal(self):
        creds = _fabric_creds()
        resp = _response(200, body={"value": []})
        with patch.object(mlv_mod, "get_headers", return_value={}):
            with self._patch_governed(resp) as g:
                list_schedules(creds)
        assert g.call_args.args[1] == PRIORITY_NORMAL
        assert g.call_args.args[3] == "GET"

    def test_create_schedule_uses_governed_background(self):
        creds = _fabric_creds()
        cfg = {
            "enabled": True,
            "configuration": {
                "endDateTime": "2027-12-31T23:59:59",
                "type": "Cron",
                "interval": 10,
            },
        }
        resp = _response(200, body={"id": "sid"})
        with patch.object(mlv_mod, "get_headers", return_value={}):
            with self._patch_governed(resp) as g:
                create_schedule(creds, cfg)
        assert g.call_args.args[1] == PRIORITY_BACKGROUND
        assert g.call_args.args[3] == "POST"

    def test_update_schedule_uses_governed_background(self):
        creds = _fabric_creds()
        resp = _response(200, body={"id": "sid"})
        with patch.object(mlv_mod, "get_headers", return_value={}):
            with self._patch_governed(resp) as g:
                update_schedule(creds, "sid", {"enabled": True})
        assert g.call_args.args[1] == PRIORITY_BACKGROUND
        assert g.call_args.args[3] == "PATCH"

    def test_delete_schedule_uses_governed_critical(self):
        creds = _fabric_creds()
        resp = _response(200, body={})
        with patch.object(mlv_mod, "get_headers", return_value={}):
            with self._patch_governed(resp) as g:
                delete_schedule(creds, "sid")
        assert g.call_args.args[1] == PRIORITY_CRITICAL
        assert g.call_args.args[3] == "DELETE"


class TestLakehousePropertiesRouting:
    def test_uses_governed_normal(self):
        creds = _fabric_creds()
        resp = _response(200, body={"properties": {"defaultSchema": "s"}})
        with patch.object(livy_mod, "get_headers", return_value={}):
            with patch.object(livy_mod, "_governed", return_value=resp) as g:
                props = livy_mod.get_lakehouse_properties(creds)
        assert props == {"defaultSchema": "s"}
        g.assert_called_once()
        assert g.call_args.args[1] == PRIORITY_NORMAL
        assert g.call_args.args[2] is requests.get


# --------------------------------------------------------------------------- #
# Cross-client 429 propagation: a throttle seen by one of these clients parks   #
# the SHARED (endpoint, principal) governor, delaying every other client.       #
# --------------------------------------------------------------------------- #


class TestCrossClientPropagation:
    def test_same_endpoint_and_principal_share_one_governor(self):
        creds_a = _fabric_creds()
        creds_b = _fabric_creds()
        shared = governor_for_credentials(creds_a)
        assert governor_for_credentials(creds_b) is shared
        client = ShortcutClient("t", "w", "i", creds_a.endpoint, credentials=creds_a)
        assert client.governor is shared

    def test_shortcut_429_parks_shared_governor_and_delays_livy_call(self):
        clock = FakeClock()
        gov = _fake_governor(clock)
        creds = _fabric_creds()
        _inject_governor(creds, gov)

        client = ShortcutClient("token", "ws", "item", creds.endpoint, credentials=creds)
        assert client.governor is gov

        throttled = _response(429, headers={"Retry-After": "60"}, raises=True)
        with patch.object(shortcuts_mod.requests, "get", return_value=throttled):
            with pytest.raises(requests.exceptions.HTTPError):
                client.check_if_exists_and_delete_shortcut(_shortcut())

        assert gov.throttle_events == 1
        assert gov.snapshot()["gate_remaining"] > 0

        # A subsequent Livy-style call on the SAME governor must now wait.
        governed(gov, PRIORITY_NORMAL, lambda *a, **k: _response(200))
        assert clock.slept

    def test_mlv_429_parks_shared_governor_and_delays_livy_call(self):
        clock = FakeClock()
        gov = _fake_governor(clock)
        creds = _fabric_creds()
        _inject_governor(creds, gov)

        throttled = _response(429, headers={"Retry-After": "60"})
        with patch.object(mlv_mod.requests, "request", return_value=throttled):
            with pytest.raises(MLVApiError):
                _request_with_retry(
                    "GET",
                    "https://x",
                    {},
                    "op",
                    30,
                    max_retries=1,
                    credentials=creds,
                    priority=PRIORITY_CRITICAL,
                )

        assert gov.throttle_events == 1
        assert gov.snapshot()["gate_remaining"] > 0

        governed(gov, PRIORITY_NORMAL, lambda *a, **k: _response(200))
        assert clock.slept

    def test_lakehouse_properties_429_parks_shared_governor(self):
        clock = FakeClock()
        gov = _fake_governor(clock)
        creds = _fabric_creds()
        _inject_governor(creds, gov)

        throttled = _response(429, headers={"Retry-After": "60"})
        ok = _response(200, body={"properties": {"defaultSchema": "s"}})
        with patch.object(livy_mod, "get_headers", return_value={}):
            with patch.object(livy_mod.requests, "get", side_effect=[throttled, ok]):
                with patch.object(livy_mod.time, "sleep") as local_sleep:
                    props = livy_mod.get_lakehouse_properties(creds)

        assert props == {"defaultSchema": "s"}
        # The shared gate served the wait (FakeClock), not a duplicate local sleep.
        local_sleep.assert_not_called()
        assert clock.slept
        assert gov.throttle_events == 1


# --------------------------------------------------------------------------- #
# Duplicate-sleep removal + preserved backward-compat behaviour.               #
# --------------------------------------------------------------------------- #


class TestSleepSemantics:
    def test_governed_mlv_429_does_not_sleep_locally(self):
        clock = FakeClock()
        gov = _fake_governor(clock)
        creds = _fabric_creds()
        _inject_governor(creds, gov)

        throttled = _response(429, headers={"Retry-After": "1"})
        ok = _response(200)
        with patch.object(mlv_mod.requests, "request", side_effect=[throttled, ok]):
            with patch.object(mlv_mod.time, "sleep") as local_sleep:
                result = _request_with_retry(
                    "GET", "https://x", {}, "op", 30, credentials=creds, priority=PRIORITY_NORMAL
                )
        assert result.status_code == 200
        # Governed 429 waits behind the shared gate, never via mlv_api.time.sleep.
        local_sleep.assert_not_called()
        assert clock.slept

    def test_ungoverned_mlv_429_still_sleeps_locally(self):
        # Without credentials there is no governor, so the legacy local
        # Retry-After back-off must still run (keeps existing callers working).
        throttled = _response(429, headers={"Retry-After": "1"}, body={"error": {"code": "x"}})
        ok = _response(200)
        with patch.object(mlv_mod.requests, "request", side_effect=[throttled, ok]):
            with patch.object(mlv_mod.time, "sleep") as local_sleep:
                result = _request_with_retry("GET", "https://x", {}, "op", 30)
        assert result.status_code == 200
        local_sleep.assert_called_once()
