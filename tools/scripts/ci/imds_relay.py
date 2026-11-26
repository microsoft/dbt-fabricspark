# Copyright (c) 2026 Raki
#
# Login and keep-warm flow adapted from openivm/ivm-bench@935e5f3d189c1a87fb409888cc357f676803581d
# (src/containers/dbt-server/services/fabric.py) under the MIT License; the full
# notice is in imds_relay_router.py.
"""Run the IMDS relay bridge for a CI job.

start      Start the router, run ``az login --identity`` through it, pre-warm tokens
           and export the managed-identity environment to later steps via $GITHUB_ENV.
preflight  Check read access to Fabric workspaces with the relayed identity.
stop       Stop the processes started by ``start`` and remove its state.
"""

import argparse
import os
import re
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.parse
from pathlib import Path

import requests

ROUTER = Path(__file__).resolve().with_name("imds_relay_router.py")
SAS_ENV = "IMDS_RELAY_TEMPORARY_SAS_URL"
STATE_ENV = "IMDS_RELAY_STATE_DIR"
OWNER_MARKER = ".imds-relay-owned"
POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
STORAGE_RESOURCE = "https://storage.azure.com"
WARM_TARGETS = (("--scope", POWERBI_SCOPE), ("--resource", STORAGE_RESOURCE))
LOGIN_ATTEMPTS = 5
AZ_TIMEOUT_SEC = 120
READY_TIMEOUT_SEC = 30
KEEPWARM_INTERVAL_SEC = 30
STOP_GRACE_SEC = 5
HTTP_TIMEOUT_SEC = 30

_REDACTIONS = (
    (re.compile(r"[a-z][a-z0-9+.-]*://\S+", re.IGNORECASE), "<url>"),
    (re.compile(r"eyJ[\w-]+\.[\w-]+\.[\w-]*"), "<token>"),
    (re.compile(r"SharedAccessSignature\S*", re.IGNORECASE), "<sas>"),
    (re.compile(r"\b(sig|sb-hc-token|access_token)=\S+", re.IGNORECASE), r"\1=<redacted>"),
)


class BridgeError(RuntimeError):
    pass


def annotate(level, message):
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::{level}::{message}", flush=True)
    else:
        print(f"{level.upper()}: {message}", flush=True)


def sanitize(text, limit=500):
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    message = " | ".join(lines[-3:])
    for pattern, replacement in _REDACTIONS:
        message = pattern.sub(replacement, message)
    return message[:limit]


def is_https_url(url):
    parts = urllib.parse.urlsplit(url or "")
    return parts.scheme == "https" and bool(parts.hostname)


def az_executable():
    search = os.pathsep.join([str(Path(sys.executable).parent), os.environ.get("PATH", "")])
    found = shutil.which("az", path=search)
    if not found:
        raise BridgeError("Azure CLI (az) was not found")
    return found


def run_az(args, env):
    try:
        result = subprocess.run(
            [az_executable(), *args],
            env=env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=AZ_TIMEOUT_SEC,
        )
    except subprocess.TimeoutExpired:
        return 124, "", f"az {' '.join(args[:2])} timed out after {AZ_TIMEOUT_SEC}s"
    return result.returncode, result.stdout, result.stderr


def identity_env(state, port, header):
    base = f"http://127.0.0.1:{port}"
    return {
        "IDENTITY_ENDPOINT": f"{base}/metadata/identity/oauth2/token",
        "IMDS_METADATA_ENDPOINT": f"{base}/metadata/instance?api-version=2021-02-01",
        "IDENTITY_HEADER": header,
        "AZURE_CONFIG_DIR": str(state / "azure"),
    }


def append_env(path, values):
    with open(path, "a", encoding="utf-8") as env_file:
        for key, value in values.items():
            env_file.write(f"{key}={value}\n")


def spawn(state, name, cmd, env):
    with open(state / f"{name}.log", "ab") as log:
        proc = subprocess.Popen(
            cmd,
            env=env,
            cwd=state,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    (state / f"{name}.pid").write_text(str(proc.pid), encoding="utf-8")
    return proc


def healthy(port):
    try:
        with requests.Session() as session:
            session.trust_env = False
            return session.get(f"http://127.0.0.1:{port}/healthz", timeout=2).status_code == 200
    except requests.RequestException:
        return False


def read_log(state, name):
    try:
        return (state / f"{name}.log").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def spawn_router(state, sas_url, header):
    ready = state / "router.port"
    env = {**os.environ, SAS_ENV: sas_url, "IDENTITY_HEADER": header, "IMDS_ROUTER_PORT": "0"}
    proc = spawn(state, "router", [sys.executable, str(ROUTER), "--ready-file", str(ready)], env)
    deadline = time.monotonic() + READY_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            detail = sanitize(read_log(state, "router"))
            raise BridgeError(f"IMDS router exited with code {proc.returncode}: {detail}")
        if ready.is_file():
            port = int(ready.read_text(encoding="utf-8"))
            if healthy(port):
                return port
        time.sleep(0.2)
    raise BridgeError(f"IMDS router was not healthy within {READY_TIMEOUT_SEC}s")


def login(env):
    args = ["login", "--identity", "--allow-no-subscriptions", "--output", "none"]
    client_id = os.environ.get("UAMI_CLIENT_ID", "").strip()
    if client_id:
        args += ["--client-id", client_id]
    detail = ""
    for attempt in range(1, LOGIN_ATTEMPTS + 1):
        code, _, stderr = run_az(args, env)
        if code == 0:
            return
        detail = sanitize(stderr)
        print(
            f"az login --identity attempt {attempt}/{LOGIN_ATTEMPTS} failed: {detail}", flush=True
        )
        if attempt < LOGIN_ATTEMPTS:
            time.sleep(5 * 2 ** (attempt - 1))
    raise BridgeError(f"az login --identity failed after {LOGIN_ATTEMPTS} attempts: {detail}")


def warm(env):
    failures = []
    for flag, audience in WARM_TARGETS:
        code, _, stderr = run_az(
            ["account", "get-access-token", flag, audience, "-o", "none"], env
        )
        if code != 0:
            failures.append(f"{audience}: {sanitize(stderr)}")
    return failures


def start(_args):
    sas_url = os.environ.get(SAS_ENV, "")
    if not is_https_url(sas_url):
        raise BridgeError(f"{SAS_ENV} must be set to an https Azure Relay sender URL")
    github_env = os.environ.get("GITHUB_ENV")
    if not github_env:
        raise BridgeError("GITHUB_ENV must name the file that receives the bridge environment")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    state = Path(tempfile.mkdtemp(prefix="imds-relay-", dir=os.environ.get("RUNNER_TEMP") or None))
    (state / OWNER_MARKER).touch()
    append_env(github_env, {STATE_ENV: str(state)})
    try:
        header = secrets.token_urlsafe(32)
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print(f"::add-mask::{header}", flush=True)
        port = spawn_router(state, sas_url, header)
        exported = identity_env(state, port, header)
        env = {key: value for key, value in os.environ.items() if key != SAS_ENV}
        env.update(exported)
        login(env)
        failures = warm(env)
        if failures:
            raise BridgeError("Token pre-warm failed: " + "; ".join(failures))
        keepwarm_cmd = [sys.executable, str(Path(__file__).resolve()), "keepwarm"]
        spawn(state, "keepwarm", [*keepwarm_cmd, "--state-dir", str(state)], env)
        append_env(github_env, exported)
    except BaseException:
        stop_state(state)
        raise
    print(f"IMDS relay bridge ready on 127.0.0.1:{port}", flush=True)


def keepwarm(args):
    state = args.state_dir
    failures = 0
    while (state / OWNER_MARKER).is_file():
        time.sleep(KEEPWARM_INTERVAL_SEC)
        if not (state / OWNER_MARKER).is_file():
            break
        for problem in warm(os.environ.copy()):
            failures += 1
            (state / "keepwarm.failures").write_text(str(failures), encoding="utf-8")
            stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            print(f"{stamp} keep-warm failed: {problem}", flush=True)


def preflight(args):
    if not is_https_url(args.api):
        raise BridgeError("--api must be an https Fabric API URL")
    if not all(args.workspace):
        raise BridgeError("Every --workspace must be a non-empty workspace id")
    token_args = ["account", "get-access-token", "--scope", POWERBI_SCOPE]
    code, stdout, stderr = run_az([*token_args, "--query", "accessToken", "-o", "tsv"], None)
    if code != 0:
        raise BridgeError(f"Could not get a Fabric token: {sanitize(stderr)}")
    headers = {"Authorization": f"Bearer {stdout.strip()}"}
    failed = 0
    with requests.Session() as session:
        for index, workspace in enumerate(args.workspace, 1):
            url = f"{args.api.rstrip('/')}/workspaces/{urllib.parse.quote(workspace, safe='')}"
            try:
                resp = session.get(
                    url, headers=headers, timeout=HTTP_TIMEOUT_SEC, allow_redirects=False
                )
            except requests.RequestException as e:
                failed += 1
                print(f"Workspace {index}: {type(e).__name__}", flush=True)
                continue
            if resp.status_code == 200:
                print(f"Workspace {index}: HTTP 200", flush=True)
                continue
            failed += 1
            print(
                f"Workspace {index}: HTTP {resp.status_code} {error_code(resp)}".rstrip(),
                flush=True,
            )
    if failed:
        raise BridgeError(f"{failed} of {len(args.workspace)} Fabric workspace check(s) failed")


def error_code(resp):
    try:
        body = resp.json()
    except ValueError:
        return ""
    code = body.get("errorCode", "") if isinstance(body, dict) else ""
    return code if isinstance(code, str) and re.fullmatch(r"[\w.-]{1,100}", code) else ""


def owned(pid, state):
    try:
        cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        return os.getpgid(pid) == pid and any(str(state).encode() in arg for arg in cmdline)
    except OSError:
        return False


def stop_process(state, name):
    try:
        pid = int((state / f"{name}.pid").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    for sig in (signal.SIGTERM, signal.SIGKILL):
        if not owned(pid, state):
            return
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            return
        deadline = time.monotonic() + STOP_GRACE_SEC
        while time.monotonic() < deadline and owned(pid, state):
            time.sleep(0.1)


def report_failures(state):
    try:
        count = int((state / "keepwarm.failures").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        count = 0
    if count:
        last = sanitize(read_log(state, "keepwarm"))
        annotate("warning", f"IMDS relay keep-warm failed {count} time(s); last: {last}")
    refresh_failures = read_log(state, "router").count("refresh failed")
    if refresh_failures:
        annotate("warning", f"IMDS router token refresh failed {refresh_failures} time(s)")


def stop_state(state):
    report_failures(state)
    (state / OWNER_MARKER).unlink(missing_ok=True)
    for name in ("keepwarm", "router"):
        stop_process(state, name)
    shutil.rmtree(state, ignore_errors=True)


def stop(args):
    state = args.state_dir or (Path(os.environ[STATE_ENV]) if os.environ.get(STATE_ENV) else None)
    if state is None or not (state / OWNER_MARKER).is_file():
        print("No IMDS relay bridge state to clean up", flush=True)
        return
    stop_state(state)
    print("IMDS relay bridge stopped", flush=True)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("start")
    check = commands.add_parser("preflight")
    check.add_argument("--api", required=True)
    check.add_argument("--workspace", action="append", required=True)
    warmer = commands.add_parser("keepwarm")
    warmer.add_argument("--state-dir", type=Path, required=True)
    stopper = commands.add_parser("stop")
    stopper.add_argument("--state-dir", type=Path)
    args = parser.parse_args(argv)
    handlers = {"start": start, "preflight": preflight, "keepwarm": keepwarm, "stop": stop}
    try:
        handlers[args.command](args)
    except BridgeError as e:
        annotate("error", str(e))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
