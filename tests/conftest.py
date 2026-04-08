import functools
import os

import pytest

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="az_cli", type=str)


# Using @pytest.mark.skip_profile('apache_spark') uses the 'skip_by_profile_type'
# autouse fixture below
def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "skip_profile(profile): skip test for the given profile",
    )


@pytest.fixture(scope="session")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    if profile_type == "az_cli":
        target = _profile_azure_cli_target()
    elif profile_type == "azure_spn":
        target = _profile_azure_spn_target()
    elif profile_type == "int_tests":
        target = _profile_int_tests_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
    return target


def _is_schema_enabled_lakehouse():
    """Detect whether the target lakehouse has schemas enabled via the Fabric REST API.

    Calls GET /v1/workspaces/{workspaceId}/lakehouses/{lakehouseId} and checks
    for the ``defaultSchema`` property in the response, which indicates a
    schema-enabled lakehouse.

    Returns False when required env vars are missing (e.g. local dev) or
    when the API call fails. Result is cached for the process lifetime.
    """
    return _is_schema_enabled_lakehouse_cached()


@functools.lru_cache(maxsize=1)
def _is_schema_enabled_lakehouse_cached():
    workspace_id = os.getenv("WORKSPACE_ID")
    lakehouse_id = os.getenv("LAKEHOUSE_ID")
    endpoint = os.getenv("LIVY_ENDPOINT", "https://api.fabric.microsoft.com/v1")

    if not all([workspace_id, lakehouse_id]):
        return False

    import requests

    # Use explicit token if available (CI), otherwise fall back to Azure CLI (local dev).
    token = os.getenv("FABRIC_INTEGRATION_TESTS_TOKEN")
    if not token:
        try:
            from azure.identity import AzureCliCredential

            credential = AzureCliCredential()
            token = credential.get_token("https://analysis.windows.net/powerbi/api").token
        except Exception:
            return False

    url = f"{endpoint}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
    try:
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return "defaultSchema" in resp.json().get("properties", {})
    except Exception:
        return False


@pytest.fixture(scope="class")
def dbt_profile_data(unique_schema, dbt_profile_target, profiles_config_update):
    """Build profile data for both schema-enabled and non-schema lakehouses.

    - Non-schema lakehouse: schema = lakehouse name (only valid namespace)
    - Schema-enabled lakehouse: schema = unique_schema (test isolation via
      dbt-core's per-class unique schema)
    """
    profile = {
        "test": {
            "outputs": {
                "default": {},
            },
            "target": "default",
        },
    }
    target = dbt_profile_target
    if _is_schema_enabled_lakehouse():
        target["schema"] = unique_schema
    else:
        target["schema"] = target.get("lakehouse")
    profile["test"]["outputs"]["default"] = target

    if profiles_config_update:
        profile.update(profiles_config_update)
    return profile


def _all_profiles_base():
    return {
        "type": "fabricspark",
        "method": "livy",
        "connect_retries": 2,
        "connect_timeout": 10,
        "endpoint": os.getenv("LIVY_ENDPOINT", "https://api.fabric.microsoft.com/v1"),
        "workspaceid": os.getenv("WORKSPACE_ID"),
        "lakehouseid": os.getenv("LAKEHOUSE_ID"),
        "lakehouse": os.getenv("LAKEHOUSE_NAME"),
        "schema": os.getenv("SCHEMA_NAME"),
        "retry_all": True,
        "create_shortcuts": False,
        "shortcuts_json_str": os.getenv("SHORTCUTS_JSON_STR"),
        "environmentId": os.getenv("FABRIC_ENVIRONMENT_ID"),
        "livy_mode": os.getenv("LIVY_MODE", "fabric"),
    }


def _profile_azure_cli_target():
    spark_config = {
        "name": os.getenv("SESSION_NAME", "example-session"),
        "tags": {
            "project": os.getenv("SESSION_NAME", "example-session"),
            "user": "pvenkat@microsoft.com",
        },
    }
    return {**_all_profiles_base(), **{"authentication": "CLI"}, **{"spark_config": spark_config}}


def _profile_azure_spn_target():
    spark_config = {
        "name": os.getenv("SESSION_NAME", "example-session"),
        "tags": {
            "project": os.getenv("SESSION_NAME", "example-session"),
            "user": "pvenkat@microsoft.com",
        },
    }
    return {
        **_all_profiles_base(),
        **{
            "authentication": "SPN",
            "client_id": os.getenv("DBT_AZURE_SP_NAME"),
            "client_secret": os.getenv("DBT_AZURE_SP_SECRET"),
            "tenant_id": os.getenv("DBT_AZURE_TENANT"),
        },
        **{"spark_config": spark_config},
    }


def _profile_int_tests_target():
    spark_config = {
        "name": os.getenv("SESSION_NAME", "example-session"),
        "tags": {
            "project": os.getenv("SESSION_NAME", "example-session"),
            "user": "pvenkat@microsoft.com",
        },
    }
    return {
        **_all_profiles_base(),
        **{
            "authentication": "int_tests",
            "accessToken": os.getenv("FABRIC_INTEGRATION_TESTS_TOKEN"),
        },
        **{"spark_config": spark_config},
    }


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip(f"skipped on '{profile_type}' profile")
