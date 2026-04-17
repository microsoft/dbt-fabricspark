from __future__ import annotations

import copy
import logging
import os

import pytest

logger = logging.getLogger("functional_conftest")


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
