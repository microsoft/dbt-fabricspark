import pytest
import os

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
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
    return target


def _all_profiles_base():
    return {
        "type": "fabricspark",
        "method": "livy",
        "connect_retries": 2,
        "connect_timeout": 10,
        "endpoint": "https://msitapi.fabric.microsoft.com/v1",
        "workspaceid": os.getenv("DBT_FABRIC_SPARK_WORKSPACE_ID"),
        "lakehouseid": os.getenv("DBT_FABRIC_SPARK_LAKEHOUSE_ID"),
        "lakehouse": os.getenv("DBT_FABRIC_SPARK_LAKEHOUSE_NAME"),
        "schema": os.getenv("DBT_FABRIC_SPARK_LAKEHOUSE_NAME"),
        "retry_all": True,
        "create_shortcuts": True,
    }


def _profile_azure_cli_target():
    return {**_all_profiles_base(), **{"authentication": "CLI"}}


def _profile_azure_spn_target():
    return {
        **_all_profiles_base(),
        **{
            "authentication": "SPN",
            "client_id": os.getenv("DBT_FABRIC_SPARK_CLIENT_ID"),
            "client_secret": os.getenv("DBT_FABRIC_SPARK_CLIENT_SECRET"),
            "tenant_id": os.getenv("DBT_FABRIC_SPARK_TENANT_ID"),
        },
    }


@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip(f"skipped on '{profile_type}' profile")
