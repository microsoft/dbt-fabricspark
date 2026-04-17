from __future__ import annotations

import copy
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import pytest

logger = logging.getLogger("functional_conftest")

_fail_fast_sentinel: Optional[Path] = None


def _write_fail_sentinel(nodeid: str) -> None:
    """Write the fail-fast sentinel file (exclusive create, first writer wins)."""
    if _fail_fast_sentinel is None or _fail_fast_sentinel.exists():
        return
    try:
        _fail_fast_sentinel.parent.mkdir(parents=True, exist_ok=True)
        with open(_fail_fast_sentinel, "x") as f:
            json.dump(
                {
                    "test": nodeid,
                    "worker": os.environ.get("PYTEST_XDIST_WORKER", "controller"),
                    "timestamp": time.time(),
                },
                f,
            )
    except FileExistsError:
        pass


def pytest_addoption(parser):
    parser.addoption(
        "--schema-mode",
        action="store",
        default="no_schema",
        choices=("no_schema", "with_schema"),
        help="Lakehouse schema mode: no_schema or with_schema",
    )
    parser.addoption(
        "--session-id-file",
        action="store",
        default=None,
        help="Path to shared Livy session ID file for session reuse across xdist workers",
    )
    parser.addoption(
        "--fail-fast-sentinel",
        action="store",
        default=None,
        help="Path to shared fail-fast sentinel file for cross-session abort on first failure",
    )


def pytest_configure(config):
    global _fail_fast_sentinel
    sentinel = config.getoption("--fail-fast-sentinel", default=None)
    if sentinel:
        _fail_fast_sentinel = Path(sentinel).resolve()


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    """Abort test session if a prior test has failed (cross-worker, cross-session)."""
    if _fail_fast_sentinel is not None and _fail_fast_sentinel.exists():
        try:
            info = _fail_fast_sentinel.read_text()
        except OSError:
            info = "(unable to read sentinel)"
        pytest.exit(f"fail-fast: aborting — a prior test failed.\n{info}", returncode=1)


def pytest_runtest_logreport(report):
    """Write fail-fast sentinel on first test failure or setup error."""
    if report.failed and report.when != "teardown":
        _write_fail_sentinel(report.nodeid)


def pytest_collectreport(report):
    """Write fail-fast sentinel on collection errors (e.g. import failures)."""
    if report.failed:
        _write_fail_sentinel(report.nodeid)


@pytest.fixture(scope="session")
def workspace_id():
    wid = os.getenv("WORKSPACE_ID")
    if not wid:
        pytest.skip("WORKSPACE_ID not set — cannot run functional tests")
    return wid


@pytest.fixture(scope="session")
def api_endpoint():
    return os.getenv("LIVY_ENDPOINT", "https://api.fabric.microsoft.com/v1")


@pytest.fixture(scope="session")
def schema_mode(request):
    return request.config.getoption("--schema-mode")


@pytest.fixture(scope="session")
def is_schema_enabled(schema_mode):
    """Whether the current lakehouse has schemas enabled."""
    return schema_mode == "with_schema"


@pytest.fixture(scope="session")
def dbt_profile_target(request, workspace_id, api_endpoint, schema_mode):
    """Build a dbt profile target from orchestrator-provided env vars.

    Enables ``reuse_session=True`` and ``session_id_file`` so all xdist
    workers share a single Livy session per lakehouse.
    """
    lakehouse_id = os.getenv("LAKEHOUSE_ID")
    lakehouse_name = os.getenv("LAKEHOUSE_NAME")
    schema_name = os.getenv("SCHEMA_NAME")

    if not all([lakehouse_id, lakehouse_name]):
        pytest.skip("LAKEHOUSE_ID / LAKEHOUSE_NAME not set — run via orchestrator or set manually")

    profile_type = request.config.getoption("--profile", default="az_cli")
    session_id_file = request.config.getoption("--session-id-file", default=None)

    base = {
        "type": "fabricspark",
        "method": "livy",
        "connect_retries": 2,
        "connect_timeout": 10,
        "endpoint": api_endpoint,
        "workspaceid": workspace_id,
        "lakehouseid": lakehouse_id,
        "lakehouse": lakehouse_name,
        "schema": schema_name or lakehouse_name,
        "retry_all": True,
        "create_shortcuts": False,
        "shortcuts_json_str": os.getenv("SHORTCUTS_JSON_STR"),
        "environmentId": os.getenv("FABRIC_ENVIRONMENT_ID"),
        "livy_mode": os.getenv("LIVY_MODE", "fabric"),
        "reuse_session": True,
        "session_idle_timeout": "60m",
        "spark_config": {
            "name": f"dbt-test-{lakehouse_name}",
            "tags": {
                "project": f"dbt-test-{lakehouse_name}",
            },
        },
    }

    if session_id_file:
        base["session_id_file"] = session_id_file

    if profile_type == "int_tests":
        base["authentication"] = "int_tests"
        base["accessToken"] = os.getenv("FABRIC_INTEGRATION_TESTS_TOKEN")
    else:
        base["authentication"] = "CLI"

    return base


@pytest.fixture(scope="class")
def dbt_profile_data(unique_schema, dbt_profile_target, profiles_config_update, is_schema_enabled):
    """Build profile data with per-class schema isolation.

    Overrides root conftest's ``dbt_profile_data`` to:
    1. Use ``is_schema_enabled`` directly — no Fabric API call needed.
    2. Deep-copy the session-scoped target to prevent cross-class mutation.
    """
    target = copy.deepcopy(dbt_profile_target)

    if is_schema_enabled:
        target["schema"] = unique_schema
    else:
        target["schema"] = target.get("lakehouse")

    profile = {
        "test": {
            "outputs": {
                "default": target,
            },
            "target": "default",
        },
    }

    if profiles_config_update:
        profile.update(profiles_config_update)
    return profile
