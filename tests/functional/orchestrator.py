from __future__ import annotations

import argparse
import logging
import os
import sys
import time

from dotenv import load_dotenv

load_dotenv("test.env", override=False)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("orchestrator")

SHARED_ENV_FILE = "logs/test-runs/.env.shared"


def _load_shared_env() -> None:
    """Load the shared env file into os.environ."""
    if os.path.isfile(SHARED_ENV_FILE):
        with open(SHARED_ENV_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if line and "=" in line and not line.startswith("#"):
                    key, val = line.split("=", 1)
                    os.environ[key] = val


def _append_shared_env(key: str, value: str) -> None:
    """Append a key=value to the shared env file."""
    os.makedirs(os.path.dirname(SHARED_ENV_FILE), exist_ok=True)
    with open(SHARED_ENV_FILE, "a") as f:
        f.write(f"{key}={value}\n")
    os.environ[key] = value


def _make_client():
    from tests.functional.fabric_client import (
        AzureCliTokenProvider,
        FabricClient,
        StaticTokenProvider,
    )

    workspace_id = os.environ.get("WORKSPACE_ID")
    api_endpoint = os.environ.get("LIVY_ENDPOINT", "https://api.fabric.microsoft.com/v1")
    if not workspace_id:
        logger.error("WORKSPACE_ID not set")
        sys.exit(1)
    token = os.environ.get("FABRIC_INTEGRATION_TESTS_TOKEN")
    provider = StaticTokenProvider(token) if token else AzureCliTokenProvider()
    return FabricClient(
        workspace_id=workspace_id, api_endpoint=api_endpoint, token_provider=provider
    )


def _current_branch_hash() -> str:
    """Return the short branch hash for the current CI branch."""
    from tests.functional.nuke import branch_hash

    branch = os.environ.get("GITHUB_HEAD_REF") or os.environ.get("GITHUB_REF_NAME", "unknown")
    return branch_hash(branch)


def cmd_nuke() -> None:
    """Delete items from the workspace that match this branch or are stale (>24h)."""
    from tests.functional.nuke import nuke_workspace

    client = _make_client()
    bhash = _current_branch_hash()
    logger.info("Nuking workspace items for branch hash '%s' and stale items", bhash)
    nuke_workspace(client, bhash)


def cmd_provision() -> None:
    """Create a lakehouse and write its details to the shared env file."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-mode", required=True, choices=("no_schema", "with_schema"))
    args = parser.parse_args(sys.argv[2:])

    client = _make_client()
    enable_schemas = args.schema_mode == "with_schema"
    ts = int(time.time())
    bhash = _current_branch_hash()
    name = f"dbt_{bhash}_{ts}_{args.schema_mode}"

    logger.info("Creating lakehouse '%s' (schemas=%s)...", name, enable_schemas)
    lh = client.create_lakehouse(name, enable_schemas=enable_schemas)
    logger.info("Created: %s (id=%s)", lh.name, lh.id)

    prefix = args.schema_mode.upper()
    _append_shared_env(f"{prefix}_LAKEHOUSE_ID", lh.id)
    _append_shared_env(f"{prefix}_LAKEHOUSE_NAME", lh.name)
    logger.info("Written to %s", SHARED_ENV_FILE)


def cmd_create_session() -> None:
    """
    Pre-create N Livy sessions for a lakehouse and write each ID to its own file.

    Multiple sessions let xdist workers shard across independent Spark clusters
    so server-side statement execution is parallelised beyond a single session's
    capacity. Each worker deterministically picks one session by worker index,
    so ``loadscope`` still pins a class's tests to a single session.
    """
    import json
    from concurrent.futures import ThreadPoolExecutor, as_completed

    import requests

    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-mode", required=True, choices=("no_schema", "with_schema"))
    parser.add_argument(
        "--count",
        type=int,
        default=int(os.environ.get("LIVY_SESSION_COUNT", "2")),
        help="Number of Livy sessions to create (default: 2, overridable via LIVY_SESSION_COUNT).",
    )
    args = parser.parse_args(sys.argv[2:])

    _load_shared_env()

    prefix = args.schema_mode.upper()
    lakehouse_id = os.environ.get(f"{prefix}_LAKEHOUSE_ID")
    lakehouse_name = os.environ.get(f"{prefix}_LAKEHOUSE_NAME")
    workspace_id = os.environ.get("WORKSPACE_ID")
    api_endpoint = os.environ.get("LIVY_ENDPOINT", "https://api.fabric.microsoft.com/v1")

    if not all([lakehouse_id, lakehouse_name, workspace_id]):
        logger.error("Missing lakehouse or workspace details for %s", args.schema_mode)
        sys.exit(1)

    token_str = os.environ.get("FABRIC_INTEGRATION_TESTS_TOKEN")
    if not token_str:
        from azure.identity import AzureCliCredential

        token_str = (
            AzureCliCredential(process_timeout=30)
            .get_token("https://analysis.windows.net/powerbi/api/.default")
            .token
        )

    livy_url = (
        f"{api_endpoint}/workspaces/{workspace_id}"
        f"/lakehouses/{lakehouse_id}/livyapi/versions/2023-12-01"
    )

    headers = {"Authorization": f"Bearer {token_str}", "Content-Type": "application/json"}

    MAX_RETRIES = 3
    POLL_TIMEOUT = 600  # 10 min per attempt before considering it stale
    POLL_INTERVAL = 10

    def _create_one(idx: int) -> str:
        last_err: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return _try_create_session(idx, attempt)
            except (RuntimeError, TimeoutError, requests.RequestException) as exc:
                last_err = exc
                logger.warning(
                    "[shard %d] Attempt %d/%d failed: %s", idx, attempt, MAX_RETRIES, exc
                )
                if attempt < MAX_RETRIES:
                    backoff = 10 * attempt
                    logger.info("[shard %d] Retrying in %ds...", idx, backoff)
                    time.sleep(backoff)
        raise RuntimeError(
            f"[shard {idx}] All {MAX_RETRIES} attempts to create a Livy session failed. "
            f"Last error: {last_err}"
        )

    def _try_create_session(idx: int, attempt: int) -> str:
        spark_config = {
            "name": f"dbt-test-{lakehouse_name}-{idx}",
            "conf": {"spark.livy.session.idle.timeout": "60m"},
            "tags": {"project": f"dbt-test-{lakehouse_name}", "shard": str(idx)},
        }
        logger.info(
            "[shard %d] Creating Livy session for %s at %s (attempt %d)...",
            idx,
            lakehouse_name,
            livy_url,
            attempt,
        )
        resp = requests.post(
            f"{livy_url}/sessions",
            data=json.dumps(spark_config),
            headers=headers,
            timeout=120,
        )
        resp.raise_for_status()
        sid = str(resp.json()["id"])
        logger.info("[shard %d] Livy session initiated: %s (waiting for idle...)", idx, sid)

        deadline = time.monotonic() + POLL_TIMEOUT
        while time.monotonic() < deadline:
            time.sleep(POLL_INTERVAL)
            status_resp = requests.get(
                f"{livy_url}/sessions/{sid}",
                headers=headers,
                timeout=120,
            )
            if status_resp.ok:
                data = status_resp.json()
                top_state = data.get("state", "")
                livy_state = data.get("livyInfo", {}).get("currentState", "")
                logger.info(
                    "[shard %d] Session %s: top=%s livy=%s", idx, sid, top_state, livy_state
                )
                if livy_state == "idle":
                    return sid
                if livy_state in ("dead", "error", "killed") or top_state in ("dead", "error"):
                    raise RuntimeError(f"[shard {idx}] Session failed to start: {data}")
        raise TimeoutError(
            f"[shard {idx}] Session {sid} did not become idle within "
            f"{POLL_TIMEOUT}s (attempt {attempt})"
        )

    count = max(1, args.count)
    os.makedirs("logs/test-runs", exist_ok=True)
    session_ids: list[str] = [""] * count
    with ThreadPoolExecutor(max_workers=count) as pool:
        futures = {pool.submit(_create_one, i): i for i in range(count)}
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                session_ids[idx] = fut.result()
            except Exception as exc:
                logger.error("Session creation for shard %d failed: %s", idx, exc)
                sys.exit(1)

    # Write per-shard files and a combined list. The combined file drives xdist
    # worker sharding (conftest.py picks one line by worker index).
    shard_files: list[str] = []
    for i, sid in enumerate(session_ids):
        path = f"logs/test-runs/livy-session-{args.schema_mode}.{i}.txt"
        with open(path, "w") as f:
            f.write(sid)
        shard_files.append(os.path.abspath(path))

    # Legacy single-file path (first shard) — kept so anything that reads the
    # old `_SESSION_FILE` env var still gets a valid session id.
    legacy = f"logs/test-runs/livy-session-{args.schema_mode}.txt"
    with open(legacy, "w") as f:
        f.write(session_ids[0])

    combined = f"logs/test-runs/livy-sessions-{args.schema_mode}.txt"
    with open(combined, "w") as f:
        f.write("\n".join(shard_files) + "\n")

    _append_shared_env(f"{prefix}_SESSION_FILE", os.path.abspath(legacy))
    _append_shared_env(f"{prefix}_SESSION_FILES", os.path.abspath(combined))
    logger.info("Created %d Livy session(s): %s", count, session_ids)
    logger.info("Combined shard list written to %s", combined)


def cmd_run_tests() -> None:
    """Run pytest for a specific schema mode, loading lakehouse details from shared env."""
    import pytest

    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-mode", required=True, choices=("no_schema", "with_schema"))
    parser.add_argument("extra_args", nargs="*", default=[])
    args = parser.parse_args(sys.argv[2:])

    _load_shared_env()

    prefix = args.schema_mode.upper()
    lakehouse_id = os.environ.get(f"{prefix}_LAKEHOUSE_ID")
    lakehouse_name = os.environ.get(f"{prefix}_LAKEHOUSE_NAME")
    session_file = os.environ.get(f"{prefix}_SESSION_FILE", "")
    session_files_list = os.environ.get(f"{prefix}_SESSION_FILES", "")

    if not lakehouse_id or not lakehouse_name:
        logger.error("Lakehouse details not found in shared env for %s", args.schema_mode)
        sys.exit(1)

    os.environ["LAKEHOUSE_ID"] = lakehouse_id
    os.environ["LAKEHOUSE_NAME"] = lakehouse_name
    if args.schema_mode == "with_schema":
        os.environ["SCHEMA_NAME"] = "dbo"
    else:
        os.environ["SCHEMA_NAME"] = lakehouse_name

    parallelism_args = ["-n", "auto", "--dist=loadscope"]

    pytest_args = [
        "tests/functional",
        "--tb=short",
        "--maxfail=3",
        f"--schema-mode={args.schema_mode}",
        "--profile=az_cli",
        *parallelism_args,
        f"--fail-fast-sentinel=logs/test-runs/fail-fast-sentinel-{args.schema_mode}.json",
    ]

    if session_files_list:
        pytest_args.append(f"--session-id-files={session_files_list}")
    elif session_file:
        pytest_args.append(f"--session-id-file={session_file}")

    pytest_args.extend(args.extra_args)

    os.environ.setdefault("FABRIC_SKIP_DEBUG_QUERY", "1")
    os.environ.setdefault("DBT_SPARK_VERSION", "3.5")

    logger.info("pytest.main(%s)", pytest_args)
    exit_code = pytest.main(pytest_args)
    sys.exit(int(exit_code))


COMMANDS = {
    "nuke": cmd_nuke,
    "provision": cmd_provision,
    "create-session": cmd_create_session,
    "run-tests": cmd_run_tests,
}


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(f"Usage: python -m tests.functional.orchestrator <{'|'.join(COMMANDS)}>")
        sys.exit(1)
    COMMANDS[sys.argv[1]]()


if __name__ == "__main__":
    main()
