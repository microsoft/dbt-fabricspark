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


def cmd_nuke() -> None:
    """Delete ALL items from the workspace."""
    from tests.functional.nuke import nuke_workspace

    client = _make_client()
    nuke_workspace(client)


def cmd_provision() -> None:
    """Create a lakehouse and write its details to the shared env file."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-mode", required=True, choices=("no_schema", "with_schema"))
    args = parser.parse_args(sys.argv[2:])

    client = _make_client()
    enable_schemas = args.schema_mode == "with_schema"
    ts = int(time.time())
    name = f"dbt_{ts}_{args.schema_mode}"

    logger.info("Creating lakehouse '%s' (schemas=%s)...", name, enable_schemas)
    lh = client.create_lakehouse(name, enable_schemas=enable_schemas)
    logger.info("Created: %s (id=%s)", lh.name, lh.id)

    prefix = args.schema_mode.upper()
    _append_shared_env(f"{prefix}_LAKEHOUSE_ID", lh.id)
    _append_shared_env(f"{prefix}_LAKEHOUSE_NAME", lh.name)
    logger.info("Written to %s", SHARED_ENV_FILE)


def cmd_create_session() -> None:
    """
    Pre-create a Livy session for a lakehouse and write its ID to a file.
    """
    import json

    import requests

    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-mode", required=True, choices=("no_schema", "with_schema"))
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

    spark_config = {
        "name": f"dbt-test-{lakehouse_name}",
        "conf": {
            "spark.livy.session.idle.timeout": "60m",
        },
        "tags": {"project": f"dbt-test-{lakehouse_name}"},
    }

    headers = {"Authorization": f"Bearer {token_str}", "Content-Type": "application/json"}

    logger.info("Creating Livy session for %s at %s...", lakehouse_name, livy_url)
    resp = requests.post(
        f"{livy_url}/sessions",
        data=json.dumps(spark_config),
        headers=headers,
        timeout=120,
    )
    resp.raise_for_status()
    session_id = str(resp.json()["id"])
    logger.info("Livy session initiated: %s (waiting for idle...)", session_id)

    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        time.sleep(10)
        status_resp = requests.get(
            f"{livy_url}/sessions/{session_id}",
            headers=headers,
            timeout=30,
        )
        if status_resp.ok:
            data = status_resp.json()
            top_state = data.get("state", "")
            livy_state = data.get("livyInfo", {}).get("currentState", "")
            logger.info("  Session %s: top=%s livy=%s", session_id, top_state, livy_state)
            if livy_state == "idle":
                break
            if livy_state in ("dead", "error", "killed") or top_state in ("dead", "error"):
                logger.error("Session failed to start: %s", data)
                sys.exit(1)
    else:
        logger.error("Session did not become idle within 10 minutes")
        sys.exit(1)

    session_file = f"logs/test-runs/livy-session-{args.schema_mode}.txt"
    os.makedirs(os.path.dirname(session_file), exist_ok=True)
    with open(session_file, "w") as f:
        f.write(session_id)

    _append_shared_env(f"{prefix}_SESSION_FILE", os.path.abspath(session_file))
    logger.info("Session %s ready, written to %s", session_id, session_file)


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

    if not lakehouse_id or not lakehouse_name:
        logger.error("Lakehouse details not found in shared env for %s", args.schema_mode)
        sys.exit(1)

    os.environ["LAKEHOUSE_ID"] = lakehouse_id
    os.environ["LAKEHOUSE_NAME"] = lakehouse_name
    if args.schema_mode == "with_schema":
        os.environ["SCHEMA_NAME"] = "dbo"
    else:
        os.environ["SCHEMA_NAME"] = lakehouse_name

    pytest_args = [
        "tests/functional",
        "-v",
        "--tb=short",
        "-x",
        f"--schema-mode={args.schema_mode}",
        "--profile=az_cli",
        "-n",
        "auto",
        "--dist=load",
    ]

    if session_file:
        pytest_args.append(f"--session-id-file={session_file}")

    pytest_args.extend(args.extra_args)

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
