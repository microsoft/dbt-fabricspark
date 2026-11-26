# Copyright (c) 2026 Raki
#
# Ported from openivm/ivm-bench@935e5f3d189c1a87fb409888cc357f676803581d
# (src/containers/imds-router/imds_relay_router.py) under the MIT License:
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
"""Serve the App Service managed-identity token contract on loopback.

Token requests are forwarded to an Azure Relay hybrid connection through a
temporary HTTPS sender URL, so ``az login --identity`` works on a runner that
has no managed identity of its own.
"""

import argparse
import hmac
import json
import os
import random
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests
from urllib3.util import Timeout

LISTEN_HOST = os.environ.get("IMDS_ROUTER_HOST", "127.0.0.1")
LISTEN_PORT = int(os.environ.get("IMDS_ROUTER_PORT", "0"))
RELAY_TEMPORARY_SAS_URL = os.environ.get("IMDS_RELAY_TEMPORARY_SAS_URL", "")
IDENTITY_HEADER_VALUE = os.environ.get("IDENTITY_HEADER", "")
TOKEN_MAX_ATTEMPTS = int(os.environ.get("IMDS_TOKEN_MAX_ATTEMPTS", "10"))
TOKEN_REQUEST_BUDGET_SEC = 50
TOKEN_CONNECT_TIMEOUT_SEC = 5
TOKEN_READ_TIMEOUT_SEC = 30
TOKEN_EXPIRY_SKEW_SEC = int(os.environ.get("IMDS_TOKEN_EXPIRY_SKEW_SEC", "300"))
# Larger than the serve-time skew so the background refresher re-mints tokens
# before a synchronous `az` call would find them stale.
TOKEN_REFRESH_LEAD_SEC = int(os.environ.get("IMDS_TOKEN_REFRESH_LEAD_SEC", "900"))
REFRESHER_INTERVAL_SEC = int(os.environ.get("IMDS_REFRESHER_INTERVAL_SEC", "60"))


class TokenCache:
    """Token cache keyed by resource and client id; near-expiry entries are stale."""

    def __init__(self, skew_sec, now=lambda: int(time.time())):
        self._entries = {}
        self._skew_sec = skew_sec
        self._now = now
        self._lock = threading.Lock()

    def is_fresh(self, entry):
        return entry["expires_on"] - self._now() > self._skew_sec

    def get(self, key):
        with self._lock:
            hit = self._entries.get(key)
            if hit is None:
                return None
            if self.is_fresh(hit):
                return hit
            del self._entries[key]
            return None

    def set(self, key, entry):
        with self._lock:
            self._entries[key] = entry

    def snapshot(self):
        with self._lock:
            return list(self._entries.items())


def _parse_expires_on(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


_CACHE = TokenCache(TOKEN_EXPIRY_SKEW_SEC)

_fetch_locks = {}
_fetch_locks_guard = threading.Lock()


def _fetch_lock(cache_key):
    with _fetch_locks_guard:
        lock = _fetch_locks.get(cache_key)
        if lock is None:
            lock = threading.Lock()
            _fetch_locks[cache_key] = lock
        return lock


_sessions = threading.local()


def _relay_session():
    if not hasattr(_sessions, "session"):
        _sessions.session = requests.Session()
    return _sessions.session


def is_https_url(url):
    parts = urllib.parse.urlsplit(url)
    return parts.scheme == "https" and bool(parts.hostname)


def _relay_fetch(resource, client_id, remaining=TOKEN_REQUEST_BUDGET_SEC):
    sender = urllib.parse.urlsplit(RELAY_TEMPORARY_SAS_URL)
    path = sender.path.rstrip("/")
    if not path.endswith("/token"):
        path += "/token"
    query = dict(urllib.parse.parse_qsl(sender.query, keep_blank_values=True))
    query["resource"] = resource
    query.pop("client_id", None)
    if client_id:
        query["client_id"] = client_id
    relay_uri = urllib.parse.urlunsplit(
        sender._replace(path=path, query=urllib.parse.urlencode(query))
    )
    with _relay_session().get(
        relay_uri,
        timeout=Timeout(
            total=remaining,
            connect=min(TOKEN_CONNECT_TIMEOUT_SEC, remaining),
            read=min(TOKEN_READ_TIMEOUT_SEC, remaining),
        ),
        allow_redirects=False,
    ) as resp:
        resp.raise_for_status()
        if resp.status_code != 200:
            return None, f"Unexpected relay HTTP {resp.status_code}"
        body = resp.json()
    if not isinstance(body, dict) or not body.get("access_token"):
        return None, "relay returned empty access_token"
    return body, ""


def _relay_retry(resource, client_id, deadline, log):
    last_error = "Token request deadline exceeded"
    for attempt in range(1, TOKEN_MAX_ATTEMPTS + 1):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            return _relay_fetch(resource, client_id, remaining)
        except requests.HTTPError as e:
            status = e.response.status_code
            last_error = f"Relay HTTP {status}"
            if status < 500:
                return None, last_error
        except (requests.ConnectionError, requests.Timeout) as e:
            last_error = f"Relay {type(e).__name__}"
        except (ValueError, requests.RequestException) as e:
            return None, f"Invalid relay response: {type(e).__name__}"
        log("Token attempt %d/%d failed: %s", attempt, TOKEN_MAX_ATTEMPTS, last_error)
        if attempt < TOKEN_MAX_ATTEMPTS:
            delay = random.uniform(0, min(0.5 * 2 ** (attempt - 1), 2))
            time.sleep(min(delay, max(0, deadline - time.monotonic())))
    return None, last_error


def _fetch_token(resource, client_id, cache_key, log=lambda *a: None):
    deadline = time.monotonic() + TOKEN_REQUEST_BUDGET_SEC
    lock = _fetch_lock(cache_key)
    if not lock.acquire(timeout=TOKEN_REQUEST_BUDGET_SEC):
        return None, "Token request deadline exceeded waiting for refresh"
    try:
        cached = _CACHE.get(cache_key)
        if cached:
            return cached, ""
        body, err = _relay_retry(resource, client_id, deadline, log)
        if body is None:
            return None, err
        expires_on = _parse_expires_on(body.get("expires_on"))
        log("Relay token acquired (expires_on=%s)", expires_on)
        entry = {
            "access_token": body["access_token"],
            "expires_on": expires_on if expires_on is not None else 0,
            "resource": resource,
            "client_id": client_id,
        }
        if expires_on is not None:
            _CACHE.set(cache_key, entry)
        return entry, ""
    finally:
        lock.release()


def _log(fmt, *args):
    print(f"[imds-router] {fmt % args}", flush=True)


def _refresher_loop():
    while True:
        time.sleep(REFRESHER_INTERVAL_SEC)
        now = int(time.time())
        for cache_key, entry in _CACHE.snapshot():
            if entry.get("expires_on", 0) - now > TOKEN_REFRESH_LEAD_SEC:
                continue
            with _fetch_lock(cache_key):
                still = _CACHE.get(cache_key)
                if still and still["expires_on"] - int(time.time()) > TOKEN_REFRESH_LEAD_SEC:
                    continue
                body, err = _relay_retry(
                    entry["resource"],
                    entry.get("client_id"),
                    time.monotonic() + TOKEN_REQUEST_BUDGET_SEC,
                    _log,
                )
                if body is None:
                    _log("refresh failed for %s: %s", cache_key, err)
                    continue
                expires_on = _parse_expires_on(body.get("expires_on"))
                if expires_on is not None:
                    _CACHE.set(
                        cache_key,
                        {
                            "access_token": body["access_token"],
                            "expires_on": expires_on,
                            "resource": entry["resource"],
                            "client_id": entry.get("client_id"),
                        },
                    )
                    _log("refreshed %s (expires_on=%s)", cache_key, expires_on)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        _log(fmt, *args)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/healthz":
            return self._json(200, {"Healthy": True})
        if parsed.path in ("/metadata/identity/oauth2/token", "/token"):
            return self._handle_token(parsed)
        if parsed.path.startswith("/metadata/instance"):
            return self._json(
                200,
                {
                    "compute": {
                        "subscriptionId": os.environ.get(
                            "IMDS_SUBSCRIPTION_ID", "00000000-0000-0000-0000-000000000000"
                        ),
                        "resourceGroupName": "dbt-fabricspark-ci",
                        "name": "dbt-fabricspark-ci",
                    }
                },
            )
        self._json(404, {"error": "Not found"})

    def _handle_token(self, parsed):
        presented = self.headers.get("X-IDENTITY-HEADER", "")
        if not IDENTITY_HEADER_VALUE or not hmac.compare_digest(
            presented.encode(), IDENTITY_HEADER_VALUE.encode()
        ):
            return self._json(403, {"error": "Invalid X-IDENTITY-HEADER"})

        qs = urllib.parse.parse_qs(parsed.query)
        resource = qs.get("resource", ["https://management.azure.com/"])[0]

        if not RELAY_TEMPORARY_SAS_URL:
            return self._json(500, {"error": "IMDS_RELAY_TEMPORARY_SAS_URL not set"})

        client_id = qs.get("client_id", [None])[0]
        cache_key = resource if not client_id else f"{resource}|client_id={client_id}"

        entry = _CACHE.get(cache_key)
        if entry:
            self.log_message("Cache hit: %s (expires_on=%s)", cache_key, entry["expires_on"])
        else:
            entry, err = _fetch_token(resource, client_id, cache_key, self.log_message)
        if entry is not None:
            return self._json(
                200,
                {
                    "access_token": entry["access_token"],
                    "expires_on": str(entry["expires_on"]),
                    "resource": resource,
                    "token_type": "Bearer",
                },
            )
        self._json(502, {"error": "Relay token request failed", "detail": err[:500]})

    def _json(self, status: int, body: dict):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ready-file", type=Path, required=True)
    args = parser.parse_args(argv)
    if not IDENTITY_HEADER_VALUE:
        print("[imds-router] IDENTITY_HEADER must be set", file=sys.stderr, flush=True)
        return 2
    if not is_https_url(RELAY_TEMPORARY_SAS_URL):
        print(
            "[imds-router] IMDS_RELAY_TEMPORARY_SAS_URL must be an https sender URL",
            file=sys.stderr,
            flush=True,
        )
        return 2
    threading.Thread(target=_refresher_loop, name="token-refresher", daemon=True).start()
    server = ThreadingHTTPServer((LISTEN_HOST, LISTEN_PORT), Handler)
    server.daemon_threads = True
    port = server.server_address[1]
    pending = args.ready_file.with_name(args.ready_file.name + ".tmp")
    pending.write_text(str(port), encoding="utf-8")
    pending.replace(args.ready_file)
    _log("Listening on %s:%d (refresh lead %ds)", LISTEN_HOST, port, TOKEN_REFRESH_LEAD_SEC)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
