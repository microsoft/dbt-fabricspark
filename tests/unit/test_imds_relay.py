# Router cases adapted from openivm/ivm-bench@935e5f3d189c1a87fb409888cc357f676803581d
# (src/containers/dbt-server/tests/test_imds_relay_router.py), MIT License,
# Copyright (c) 2026 Raki; full notice in tools/scripts/ci/imds_relay_router.py.
import importlib.util
import json
import os
import signal
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
import requests

CI_DIR = Path(__file__).resolve().parents[2] / "tools" / "scripts" / "ci"


def _load(name):
    spec = importlib.util.spec_from_file_location(name, CI_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


router = _load("imds_relay_router")
bridge = _load("imds_relay")

SECRET_FRAGMENTS = ("relay.example", "sig=abc", "eyJ")
LEAKY_STDERR = (
    "ERROR: request to https://relay.example/hc?sb-hc-token=SharedAccessSignature%20sr&sig=abc"
    " failed\nToken eyJhbGciOiJ.eyJzdWIiOiJ.c2lnbmF0dXJl was rejected"
)


@pytest.fixture(autouse=True)
def fresh_cache(monkeypatch):
    monkeypatch.setattr(router, "_CACHE", router.TokenCache(300))


def _relay_response(status):
    response = requests.Response()
    response.status_code = status
    return response


def _handler(identity_header):
    handler = object.__new__(router.Handler)
    handler.headers = {"X-IDENTITY-HEADER": identity_header}
    handler._json = Mock()
    return handler


class TestRouter:
    def test_real_relay_errors_recover_and_warm_cache_avoids_relay(self, monkeypatch):
        calls = []
        authorization = []
        sender_query = "sb-hc-token=SharedAccessSignature%20sr%3Drelay%26sig%3Da%2Bb%252Fc%3D"

        class Relay(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                calls.append(self.path)
                authorization.append(self.headers.get("ServiceBusAuthorization"))
                payload = json.dumps(
                    {"access_token": "test-token", "expires_on": int(time.time()) + 3600}
                ).encode()
                self.send_response(500 if len(calls) < 4 else 200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Relay)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        session = requests.Session()
        session.trust_env = False
        sender = f"http://127.0.0.1:{server.server_port}/token?{sender_query}"
        monkeypatch.setattr(router, "RELAY_TEMPORARY_SAS_URL", sender)
        monkeypatch.setattr(router, "_relay_session", lambda: session)
        monkeypatch.setattr(router.random, "uniform", lambda *_: 0)
        try:
            token, error = router._fetch_token("https://scope/a?b=c&d=e", "client+id", "a")
            cached, _ = router._fetch_token("https://scope/a?b=c&d=e", "client+id", "a")
        finally:
            session.close()
            server.shutdown()
            server.server_close()
            thread.join()

        assert error == ""
        assert token == cached
        assert len(calls) == 4
        expected_sas = urllib.parse.parse_qs(sender_query)["sb-hc-token"]
        for path in calls:
            parsed = urllib.parse.urlparse(path)
            query = urllib.parse.parse_qs(parsed.query)
            assert parsed.path == "/token"
            assert query["sb-hc-token"] == expected_sas
            assert query["resource"] == ["https://scope/a?b=c&d=e"]
            assert query["client_id"] == ["client+id"]
        assert authorization == [None] * 4

    @pytest.mark.parametrize("path", ["/connection", "/connection/token/"])
    @pytest.mark.parametrize("client_id", [None, "client+id"])
    def test_sender_url_routes_to_token_and_merges_identity_query(
        self, monkeypatch, path, client_id
    ):
        token = "SharedAccessSignature sr=relay&sig=a+b%2Fc=&se=123"
        query = urllib.parse.urlencode(
            {"sb-hc-token": token, "resource": "stale", "client_id": "stale"}
        )
        monkeypatch.setattr(
            router, "RELAY_TEMPORARY_SAS_URL", f"https://example.invalid{path}?{query}"
        )
        session = MagicMock()
        response = session.get.return_value.__enter__.return_value
        response.status_code = 200
        response.json.return_value = {"access_token": "test-token"}
        monkeypatch.setattr(router, "_relay_session", lambda: session)

        router._relay_fetch("https://scope/?a=b&c=d", client_id)

        parsed = urllib.parse.urlsplit(session.get.call_args.args[0])
        forwarded = urllib.parse.parse_qs(parsed.query)
        assert parsed.path == "/connection/token"
        assert forwarded["sb-hc-token"] == [token]
        assert forwarded["resource"] == ["https://scope/?a=b&c=d"]
        assert forwarded.get("client_id") == ([client_id] if client_id else None)
        assert session.get.call_args.kwargs["allow_redirects"] is False

    def test_missing_sender_url_fails_without_contacting_relay(self, monkeypatch):
        monkeypatch.setattr(router, "IDENTITY_HEADER_VALUE", "header")
        monkeypatch.setattr(router, "RELAY_TEMPORARY_SAS_URL", "")
        fetch = Mock()
        monkeypatch.setattr(router, "_fetch_token", fetch)
        handler = _handler("header")

        handler._handle_token(urllib.parse.urlparse("/token?resource=a"))

        handler._json.assert_called_once_with(
            500, {"error": "IMDS_RELAY_TEMPORARY_SAS_URL not set"}
        )
        fetch.assert_not_called()

    @pytest.mark.parametrize(("configured", "presented"), [("header", "wrong"), ("", "")])
    def test_identity_header_mismatch_is_rejected(self, monkeypatch, configured, presented):
        monkeypatch.setattr(router, "IDENTITY_HEADER_VALUE", configured)
        monkeypatch.setattr(router, "RELAY_TEMPORARY_SAS_URL", "https://relay.invalid/hc")
        fetch = Mock()
        monkeypatch.setattr(router, "_fetch_token", fetch)
        handler = _handler(presented)

        handler._handle_token(urllib.parse.urlparse("/token?resource=a"))

        handler._json.assert_called_once_with(403, {"error": "Invalid X-IDENTITY-HEADER"})
        fetch.assert_not_called()

    def test_connect_loss_and_server_errors_recover_on_tenth_attempt(self, monkeypatch):
        failure = requests.HTTPError(response=_relay_response(500))
        token = {"access_token": "test-token", "expires_on": 10000}
        fetch = Mock(
            side_effect=[
                requests.ConnectTimeout(),
                requests.ConnectionError(),
                failure,
                requests.ReadTimeout(),
                *[failure] * 5,
                (token, ""),
            ]
        )
        monkeypatch.setattr(router, "_relay_fetch", fetch)
        monkeypatch.setattr(router.time, "sleep", lambda _: None)
        monkeypatch.setattr(router.time, "monotonic", lambda: 0)

        body, error = router._relay_retry("resource", None, 50, lambda *_: None)

        assert (body, error) == (token, "")
        assert fetch.call_count == 10

    def test_nontransient_failure_is_not_retried_or_exposed(self, monkeypatch):
        fetch = Mock(
            side_effect=requests.HTTPError(
                "private response content", response=_relay_response(403)
            )
        )
        monkeypatch.setattr(router, "_relay_fetch", fetch)

        body, error = router._relay_retry("resource", None, time.monotonic() + 50, lambda *_: None)

        assert body is None
        assert error == "Relay HTTP 403"
        assert fetch.call_count == 1

    def test_deadline_stops_retries_and_bounds_backoff(self, monkeypatch):
        fetch = Mock(side_effect=requests.ConnectTimeout())
        sleep = Mock()
        monkeypatch.setattr(router, "_relay_fetch", fetch)
        monkeypatch.setattr(router.time, "monotonic", Mock(side_effect=[0, 49.9, 50.1]))
        monkeypatch.setattr(router.random, "uniform", lambda *_: 2)
        monkeypatch.setattr(router.time, "sleep", sleep)

        body, error = router._relay_retry("resource", None, 50, lambda *_: None)

        assert body is None
        assert error == "Relay ConnectTimeout"
        assert fetch.call_count == 1
        assert sleep.call_args.args[0] == pytest.approx(0.1)

    def test_cache_separates_scopes_and_refreshes_before_expiry(self, monkeypatch):
        now = [1000]

        def mint(resource, client_id, deadline, log):
            return {"access_token": f"{resource}-{client_id}", "expires_on": now[0] + 1000}, ""

        fetch = Mock(side_effect=mint)
        monkeypatch.setattr(router, "_CACHE", router.TokenCache(300, now=lambda: now[0]))
        monkeypatch.setattr(router, "_relay_retry", fetch)

        first, _ = router._fetch_token("a", None, "a")
        assert router._fetch_token("a", None, "a")[0] == first
        router._fetch_token("b", None, "b")
        router._fetch_token("a", "client", "a|client_id=client")
        assert fetch.call_count == 3
        now[0] = 1700
        refreshed, _ = router._fetch_token("a", None, "a")
        assert fetch.call_count == 4
        assert refreshed["expires_on"] == 2700

    def test_concurrent_requests_for_one_scope_share_a_relay_call(self, monkeypatch):
        started = threading.Event()
        release = threading.Event()
        calls = []

        def slow_retry(resource, client_id, deadline, log):
            calls.append(resource)
            started.set()
            release.wait(5)
            return {"access_token": "test-token", "expires_on": int(time.time()) + 3600}, ""

        monkeypatch.setattr(router, "_relay_retry", slow_retry)
        results = []
        threads = [
            threading.Thread(target=lambda: results.append(router._fetch_token("c", None, "c")))
            for _ in range(4)
        ]
        threads[0].start()
        assert started.wait(5)
        for thread in threads[1:]:
            thread.start()
        time.sleep(0.2)
        release.set()
        for thread in threads:
            thread.join(5)

        assert calls == ["c"]
        assert [entry["access_token"] for entry, _ in results] == ["test-token"] * 4


@pytest.fixture
def ci_env(monkeypatch, tmp_path):
    github_env = tmp_path / "github_env"
    github_env.touch()
    monkeypatch.setenv("GITHUB_ENV", str(github_env))
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    monkeypatch.setenv(bridge.SAS_ENV, "https://relay.invalid/hc?sb-hc-token=placeholder")
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("UAMI_CLIENT_ID", raising=False)
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    real_sleep = time.sleep
    monkeypatch.setattr(bridge.time, "sleep", lambda seconds: real_sleep(min(seconds, 0.05)))
    spawned = []
    real_spawn = bridge.spawn

    def recording_spawn(*args):
        spawned.append(real_spawn(*args))
        return spawned[-1]

    monkeypatch.setattr(bridge, "spawn", recording_spawn)
    yield github_env, spawned
    signal.signal(signal.SIGTERM, previous_sigterm)
    for proc in spawned:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def _exported(github_env):
    lines = github_env.read_text(encoding="utf-8").splitlines()
    return dict(line.split("=", 1) for line in lines)


class TestBridgeLifecycle:
    @pytest.mark.parametrize(
        "sas_url", ["", "http://relay.example/hc?sb-hc-token=x", "relay.example/hc"]
    )
    def test_start_rejects_non_https_sender_before_creating_state(
        self, ci_env, monkeypatch, tmp_path, sas_url
    ):
        github_env, spawned = ci_env
        monkeypatch.setenv(bridge.SAS_ENV, sas_url)

        assert bridge.main(["start"]) == 1

        assert spawned == []
        assert not list(tmp_path.glob("imds-relay-*"))
        assert github_env.read_text(encoding="utf-8") == ""

    def test_start_exports_identity_env_and_stop_removes_owned_state(self, ci_env, monkeypatch):
        github_env, spawned = ci_env
        calls = []

        def fake_az(args, env):
            calls.append((args, env))
            return 0, "", ""

        monkeypatch.setattr(bridge, "run_az", fake_az)

        assert bridge.main(["start"]) == 0

        exported = _exported(github_env)
        state = Path(exported[bridge.STATE_ENV])
        port = int((state / "router.port").read_text(encoding="utf-8"))
        assert exported["IDENTITY_ENDPOINT"] == (
            f"http://127.0.0.1:{port}/metadata/identity/oauth2/token"
        )
        assert exported["AZURE_CONFIG_DIR"] == str(state / "azure")
        assert len(exported["IDENTITY_HEADER"]) >= 32
        assert [args[:4] for args, _ in calls] == [
            ["login", "--identity", "--allow-no-subscriptions", "--output"],
            ["account", "get-access-token", "--scope", bridge.POWERBI_SCOPE],
            ["account", "get-access-token", "--resource", bridge.STORAGE_RESOURCE],
        ]
        for _, env in calls:
            assert bridge.SAS_ENV not in env
            assert env["IDENTITY_HEADER"] == exported["IDENTITY_HEADER"]
            assert env["AZURE_CONFIG_DIR"] == exported["AZURE_CONFIG_DIR"]

        with requests.Session() as session:
            session.trust_env = False
            base = f"http://127.0.0.1:{port}"
            assert session.get(f"{base}/healthz", timeout=5).status_code == 200
            rejected = session.get(
                exported["IDENTITY_ENDPOINT"],
                params={"resource": bridge.STORAGE_RESOURCE},
                headers={"X-IDENTITY-HEADER": "wrong"},
                timeout=5,
            )
            assert rejected.status_code == 403

        monkeypatch.setenv(bridge.STATE_ENV, str(state))
        assert bridge.main(["stop"]) == 0

        assert [proc.wait(timeout=10) is not None for proc in spawned] == [True, True]
        assert not state.exists()

    @pytest.mark.parametrize(
        ("stage", "client_id"),
        [("login", None), ("login", "11111111-2222-3333-4444-555555555555"), ("prewarm", None)],
    )
    def test_start_failure_cleans_up_without_exporting_identity(
        self, ci_env, monkeypatch, tmp_path, capsys, stage, client_id
    ):
        github_env, spawned = ci_env
        if client_id:
            monkeypatch.setenv("UAMI_CLIENT_ID", client_id)
        monkeypatch.setattr(bridge, "LOGIN_ATTEMPTS", 2)
        calls = []

        def fake_az(args, env):
            calls.append(args)
            if args[0] == "login" and stage == "prewarm":
                return 0, "", ""
            return 1, "", LEAKY_STDERR

        monkeypatch.setattr(bridge, "run_az", fake_az)

        assert bridge.main(["start"]) == 1

        assert set(_exported(github_env)) == {bridge.STATE_ENV}
        assert not list(tmp_path.glob("imds-relay-*"))
        assert [proc.wait(timeout=10) is not None for proc in spawned] == [True]
        logins = [args for args in calls if args[0] == "login"]
        assert len(logins) == (2 if stage == "login" else 1)
        for args in logins:
            assert ("--client-id" in args) == bool(client_id)
            if client_id:
                assert args[args.index("--client-id") + 1] == client_id
        output = capsys.readouterr()
        assert "ERROR:" in output.out
        for fragment in SECRET_FRAGMENTS:
            assert fragment not in output.out + output.err

    def test_keepwarm_failures_are_reported_when_stopping(self, monkeypatch, tmp_path, capsys):
        state = tmp_path / "imds-relay-test"
        state.mkdir()
        marker = state / bridge.OWNER_MARKER
        marker.touch()
        sleeps = []

        def sleep(_):
            sleeps.append(1)
            if len(sleeps) == 2:
                marker.unlink()

        monkeypatch.setattr(bridge.time, "sleep", sleep)
        monkeypatch.setattr(bridge, "run_az", lambda *_: (1, "", LEAKY_STDERR))
        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)

        bridge.keepwarm(Mock(state_dir=state))

        assert (state / "keepwarm.failures").read_text(encoding="utf-8") == "2"
        (state / "keepwarm.log").write_text(capsys.readouterr().out, encoding="utf-8")
        marker.touch()
        assert bridge.main(["stop", "--state-dir", str(state)]) == 0
        output = capsys.readouterr().out
        assert "WARNING: IMDS relay keep-warm failed 2 time(s)" in output
        for fragment in SECRET_FRAGMENTS:
            assert fragment not in output
        assert not state.exists()

    def test_stop_never_signals_processes_it_does_not_own(self, monkeypatch, tmp_path):
        state = tmp_path / "imds-relay-test"
        state.mkdir()
        (state / bridge.OWNER_MARKER).touch()
        (state / "router.pid").write_text(str(os.getpid()), encoding="utf-8")
        killpg = Mock()
        monkeypatch.setattr(bridge.os, "killpg", killpg)

        assert bridge.main(["stop", "--state-dir", str(state)]) == 0

        killpg.assert_not_called()
        assert not state.exists()


class TestPreflight:
    def test_reports_status_and_error_code_without_leaking_token(self, monkeypatch, capsys):
        token = "eyJhbGciOiJ.eyJzdWIiOiJ.c2lnbmF0dXJl"
        monkeypatch.setattr(bridge, "run_az", lambda *_: (0, f"{token}\n", ""))
        denied = _relay_response(403)
        denied._content = b'{"errorCode": "InsufficientPrivileges", "message": "private detail"}'
        responses = iter([_relay_response(200), denied])
        requests_made = []

        def fake_get(self, url, **kwargs):
            requests_made.append((url, kwargs))
            return next(responses)

        monkeypatch.setattr(bridge.requests.Session, "get", fake_get)
        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)

        exit_code = bridge.main(
            [
                "preflight",
                "--api",
                "https://api.example/v1/",
                "--workspace",
                "w1",
                "--workspace",
                "w2",
            ]
        )

        assert exit_code == 1
        assert [url for url, _ in requests_made] == [
            "https://api.example/v1/workspaces/w1",
            "https://api.example/v1/workspaces/w2",
        ]
        for _, kwargs in requests_made:
            assert kwargs["allow_redirects"] is False
            assert kwargs["headers"]["Authorization"] == f"Bearer {token}"
        output = capsys.readouterr().out
        assert "Workspace 1: HTTP 200" in output
        assert "Workspace 2: HTTP 403 InsufficientPrivileges" in output
        assert "private detail" not in output
        assert "eyJ" not in output

    @pytest.mark.parametrize(
        ("api", "workspace"),
        [
            ("http://api.fabric.microsoft.com/v1", "w1"),
            ("https://api.fabric.microsoft.com/v1", ""),
        ],
    )
    def test_rejects_unsafe_input_before_acquiring_a_token(self, monkeypatch, api, workspace):
        run_az = Mock()
        monkeypatch.setattr(bridge, "run_az", run_az)

        assert bridge.main(["preflight", "--api", api, "--workspace", workspace]) == 1

        run_az.assert_not_called()
