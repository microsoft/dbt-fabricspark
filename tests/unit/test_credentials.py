import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.fabricspark import FabricSparkCredentials


def test_credentials_server_side_parameters_keys_and_values_are_strings() -> None:
    credentials = FabricSparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="tests",
        schema="tests",
        workspaceid="test-workspace-id",
        lakehouseid="test-lakehouse-id",
        spark_config={"name": "test-session"},
    )
    assert credentials.schema == "tests"


def test_credentials_fabric_mode_default() -> None:
    """Test that Fabric mode is the default and requires workspace/lakehouse IDs."""
    credentials = FabricSparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="tests",
        schema="tests",
        workspaceid="test-workspace-id",
        lakehouseid="test-lakehouse-id",
        spark_config={"name": "test-session"},
    )
    assert credentials.livy_mode == "fabric"
    assert credentials.is_local_mode is False


def test_credentials_local_mode() -> None:
    """Test local mode credentials without workspace/lakehouse IDs."""
    credentials = FabricSparkCredentials(
        method="livy",
        livy_mode="local",
        livy_url="http://localhost:8998",
        schema="default",
        spark_config={"name": "test-session"},
    )
    assert credentials.livy_mode == "local"
    assert credentials.is_local_mode is True
    assert credentials.lakehouse_endpoint == "http://localhost:8998"


def test_credentials_local_mode_custom_url() -> None:
    """Test local mode with custom Livy URL."""
    credentials = FabricSparkCredentials(
        method="livy",
        livy_mode="local",
        livy_url="http://custom-host:9999",
        schema="default",
        spark_config={"name": "test-session"},
    )
    assert credentials.lakehouse_endpoint == "http://custom-host:9999"


def test_credentials_fabric_mode_endpoint() -> None:
    """Test Fabric mode generates correct endpoint."""
    credentials = FabricSparkCredentials(
        method="livy",
        livy_mode="fabric",
        authentication="CLI",
        lakehouse="tests",
        schema="tests",
        workspaceid="workspace-guid",
        lakehouseid="lakehouse-guid",
        endpoint="https://api.fabric.microsoft.com/v1",
        spark_config={"name": "test-session"},
    )
    expected_endpoint = "https://api.fabric.microsoft.com/v1/workspaces/workspace-guid/lakehouses/lakehouse-guid/livyapi/versions/2023-12-01"
    assert credentials.lakehouse_endpoint == expected_endpoint


def test_credentials_fabric_mode_requires_workspaceid() -> None:
    """Test that Fabric mode raises error without workspaceid."""
    with pytest.raises(DbtRuntimeError, match="workspace guid"):
        FabricSparkCredentials(
            method="livy",
            livy_mode="fabric",
            lakehouseid="lakehouse-guid",
            schema="tests",
            spark_config={"name": "test-session"},
        )


def test_credentials_fabric_mode_requires_lakehouseid() -> None:
    """Test that Fabric mode raises error without lakehouseid."""
    with pytest.raises(DbtRuntimeError, match="lakehouse guid"):
        FabricSparkCredentials(
            method="livy",
            livy_mode="fabric",
            workspaceid="workspace-guid",
            schema="tests",
            spark_config={"name": "test-session"},
        )


def test_credentials_local_mode_no_workspace_required() -> None:
    """Test that local mode doesn't require workspace/lakehouse IDs."""
    # Should not raise any error
    credentials = FabricSparkCredentials(
        method="livy",
        livy_mode="local",
        schema="default",
        spark_config={"name": "test-session"},
    )
    assert credentials.workspaceid is None
    assert credentials.lakehouseid is None


def test_credentials_type() -> None:
    """Test that type property returns 'fabricspark'."""
    credentials = FabricSparkCredentials(
        method="livy",
        livy_mode="local",
        schema="default",
        spark_config={"name": "test-session"},
    )
    assert credentials.type == "fabricspark"
