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
        f"--schema-mode={args.schema_mode}",
        "--profile=az_cli",
        *args.extra_args,
    ]

    logger.info("pytest.main(%s)", pytest_args)
    exit_code = pytest.main(pytest_args)
    sys.exit(int(exit_code))


COMMANDS = {
    "nuke": cmd_nuke,
    "provision": cmd_provision,
    "run-tests": cmd_run_tests,
}


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(f"Usage: python -m tests.functional.orchestrator <{'|'.join(COMMANDS)}>")
        sys.exit(1)
    COMMANDS[sys.argv[1]]()


if __name__ == "__main__":
    main()
