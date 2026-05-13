import pytest
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

from dbt.adapters.fabricspark import FabricSparkCredentials
from dbt.adapters.fabricspark.connections import _is_permanent_error, _is_retryable_error


def test_credentials_fabric_mode_defaults_schema_to_lakehouse() -> None:
    credentials = FabricSparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="tests",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        spark_config={"name": "test-session"},
    )
    assert credentials.schema == "tests"


def test_credentials_fabric_mode_default() -> None:
    """Test that Fabric mode is the default and requires workspace/lakehouse IDs."""
    credentials = FabricSparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="tests",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
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
        spark_config={"name": "test-session"},
    )
    assert credentials.livy_mode == "local"
    assert credentials.is_local_mode is True
    assert credentials.lakehouse_endpoint == "http://localhost:8998"
    assert credentials.schema == "default"
    assert credentials.database == "default"


def test_credentials_local_mode_custom_url() -> None:
    """Test local mode with custom Livy URL."""
    credentials = FabricSparkCredentials(
        method="livy",
        livy_mode="local",
        livy_url="http://custom-host:9999",
        spark_config={"name": "test-session"},
    )
    assert credentials.lakehouse_endpoint == "http://custom-host:9999"
    assert credentials.schema == "default"
    assert credentials.database == "default"


def test_credentials_fabric_mode_endpoint() -> None:
    """Test Fabric mode generates correct endpoint."""
    credentials = FabricSparkCredentials(
        method="livy",
        livy_mode="fabric",
        authentication="CLI",
        lakehouse="tests",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        endpoint="https://api.fabric.microsoft.com/v1",
        spark_config={"name": "test-session"},
    )
    expected_endpoint = "https://api.fabric.microsoft.com/v1/workspaces/1de8390c-9aca-4790-bee8-72049109c0f4/lakehouses/8c5bc260-bc3a-4898-9ada-01e433d461ba/livyapi/versions/2023-12-01"
    assert credentials.lakehouse_endpoint == expected_endpoint


def test_credentials_fabric_mode_requires_workspaceid() -> None:
    """Test that Fabric mode raises error without workspaceid."""
    with pytest.raises(DbtRuntimeError, match="workspaceid"):
        FabricSparkCredentials(
            method="livy",
            livy_mode="fabric",
            lakehouse="tests",
            lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
            spark_config={"name": "test-session"},
        )


def test_credentials_fabric_mode_requires_lakehouseid() -> None:
    """Test that Fabric mode raises error without lakehouseid."""
    with pytest.raises(DbtRuntimeError, match="lakehouseid"):
        FabricSparkCredentials(
            method="livy",
            livy_mode="fabric",
            lakehouse="tests",
            workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
            spark_config={"name": "test-session"},
        )


def test_credentials_fabric_mode_requires_lakehouse() -> None:
    """Test that Fabric mode raises error without lakehouse."""
    with pytest.raises(DbtRuntimeError, match="lakehouse"):
        FabricSparkCredentials(
            method="livy",
            livy_mode="fabric",
            workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
            lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
            spark_config={"name": "test-session"},
        )


def test_credentials_local_mode_no_workspace_required() -> None:
    """Test that local mode doesn't require workspace/lakehouse IDs."""
    credentials = FabricSparkCredentials(
        method="livy",
        livy_mode="local",
        spark_config={"name": "test-session"},
    )
    assert credentials.workspaceid is None
    assert credentials.lakehouseid is None
    assert credentials.schema == "default"
    assert credentials.database == "default"


def test_credentials_fabric_mode_requires_endpoint() -> None:
    """Test that Fabric mode raises error without endpoint."""
    with pytest.raises(DbtRuntimeError, match="endpoint"):
        FabricSparkCredentials(
            method="livy",
            livy_mode="fabric",
            lakehouse="tests",
            workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
            lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
            endpoint=None,
            spark_config={"name": "test-session"},
        )


def test_credentials_local_mode_no_endpoint_required() -> None:
    """Test that local mode doesn't require endpoint."""
    credentials = FabricSparkCredentials(
        method="livy",
        livy_mode="local",
        endpoint=None,
        spark_config={"name": "test-session"},
    )
    assert credentials.endpoint is None
    assert credentials.schema == "default"
    assert credentials.database == "default"


def test_credentials_type() -> None:
    """Test that type property returns 'fabricspark'."""
    credentials = FabricSparkCredentials(
        method="livy",
        livy_mode="local",
        spark_config={"name": "test-session"},
    )
    assert credentials.type == "fabricspark"


def test_credentials_database_defaults_to_lakehouse() -> None:
    """Test that database is always derived from lakehouse name."""
    credentials = FabricSparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="my_lakehouse",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        spark_config={"name": "test-session"},
    )
    assert credentials.database == "my_lakehouse"
    assert credentials.schema == "my_lakehouse"


def test_credentials_custom_schema() -> None:
    """Test that user can provide a custom schema name for schema-enabled lakehouses."""
    credentials = FabricSparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="my_lakehouse",
        schema="custom_schema",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        spark_config={"name": "test-session"},
    )
    assert credentials.schema == "custom_schema"
    # database is always set to lakehouse name; include_policy controls rendering
    assert credentials.database == "my_lakehouse"


def test_apply_lakehouse_properties_schemas_enabled() -> None:
    """Test that schema-enabled lakehouse allows custom schema."""
    credentials = FabricSparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="my_lakehouse",
        schema="custom_schema",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        spark_config={"name": "test-session"},
    )
    credentials.apply_lakehouse_properties({"defaultSchema": "dbo", "oneLakeTablesPath": "..."})
    assert credentials.lakehouse_schemas_enabled is True
    assert credentials.schema == "custom_schema"
    assert credentials.database == "my_lakehouse"


def test_apply_lakehouse_properties_schemas_enabled_rejects_default_schema() -> None:
    """Test that schema-enabled lakehouse rejects schema == lakehouse (user must pick a real schema)."""
    credentials = FabricSparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="my_lakehouse",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        spark_config={"name": "test-session"},
    )
    # schema defaults to lakehouse name — should be rejected for schema-enabled lakehouse
    with pytest.raises(DbtRuntimeError, match="schemas enabled.*schema.*other than"):
        credentials.apply_lakehouse_properties(
            {"defaultSchema": "dbo", "oneLakeTablesPath": "..."}
        )


def test_apply_lakehouse_properties_no_schemas() -> None:
    """Test that non-schema lakehouse sets schema to lakehouse name."""
    credentials = FabricSparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="my_lakehouse",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        spark_config={"name": "test-session"},
    )
    credentials.apply_lakehouse_properties({"oneLakeTablesPath": "..."})
    assert credentials.lakehouse_schemas_enabled is False
    assert credentials.schema == "my_lakehouse"
    assert credentials.database == "my_lakehouse"


def test_apply_lakehouse_properties_overrides_mismatched_schema() -> None:
    """Test that non-schema lakehouse silently overrides schema to lakehouse name."""
    credentials = FabricSparkCredentials(
        method="livy",
        authentication="CLI",
        lakehouse="my_lakehouse",
        schema="different_name",
        workspaceid="1de8390c-9aca-4790-bee8-72049109c0f4",
        lakehouseid="8c5bc260-bc3a-4898-9ada-01e433d461ba",
        spark_config={"name": "test-session"},
    )
    credentials.apply_lakehouse_properties({"oneLakeTablesPath": "..."})
    assert credentials.schema == "my_lakehouse"


# --- Tests for _is_retryable_error and statement_timeout defaults ---


def test_default_statement_timeout_is_12_hours() -> None:
    """Default statement_timeout should be 43200s (12 hours)."""
    credentials = FabricSparkCredentials(
        method="livy",
        livy_mode="local",
        spark_config={"name": "test-session"},
    )
    assert credentials.statement_timeout == 43200


def test_statement_timeout_error_is_not_retryable() -> None:
    """Client-side statement polling timeouts must NOT be retried.

    Retrying re-submits the SQL while the original statement may still be
    running on the Spark cluster, causing overlapping statements.
    """
    exc = DbtDatabaseError(
        "Timeout (43200s) waiting for statement 42 to complete. "
        "Increase `statement_timeout` in profiles.yml."
    )
    assert _is_retryable_error(exc) == ""


def test_transient_timeout_is_still_retryable() -> None:
    """Generic 'timeout' errors (e.g. HTTP timeouts) should still be retried."""
    exc = DbtDatabaseError("Connection timeout while reaching the server")
    assert _is_retryable_error(exc) != ""


def test_other_retryable_keywords_still_work() -> None:
    """Sanity check that other retryable keywords are unaffected."""
    for keyword in ["throttling", "service busy", "rate limit", "unavailable"]:
        exc = Exception(f"The server returned: {keyword}")
        assert _is_retryable_error(exc) != "", f"Expected '{keyword}' to be retryable"


# --- Tests for _is_permanent_error (SCHEMA_NOT_FOUND retry-storm regression) ---


def test_schema_not_found_is_permanent_error() -> None:
    """[SCHEMA_NOT_FOUND] must be classified as a permanent error.

    Regression: dbt docs generate fans out catalog queries to foreign source
    schemas. When Spark returns [SCHEMA_NOT_FOUND] and retry_all:true is set,
    the pre-fix adapter retried the query 24 times (~120s per schema) before
    finally returning [].  _is_permanent_error must short-circuit that storm.
    """
    exc = DbtDatabaseError(
        "Error while executing query: [SCHEMA_NOT_FOUND] "
        "The schema `nonexistent_foreign_schema` cannot be found. "
        "Verify the spelling and correctness of the schema and catalog."
    )
    assert _is_permanent_error(exc) is True


def test_table_or_view_not_found_is_permanent_error() -> None:
    """[TABLE_OR_VIEW_NOT_FOUND] must also be classified as permanent."""
    exc = DbtDatabaseError(
        "Error while executing query: [TABLE_OR_VIEW_NOT_FOUND] "
        "The table or view `my_table` cannot be found."
    )
    assert _is_permanent_error(exc) is True


def test_schema_not_found_is_not_retryable() -> None:
    """[SCHEMA_NOT_FOUND] must not match retryable keywords either."""
    exc = DbtDatabaseError(
        "Error while executing query: [SCHEMA_NOT_FOUND] The schema `foo_db` cannot be found."
    )
    assert _is_retryable_error(exc) == ""


def test_transient_error_is_not_permanent() -> None:
    """Regular transient errors must not be falsely classified as permanent."""
    for msg in ["service busy", "throttling", "gateway timeout", "connection reset"]:
        exc = Exception(f"The server returned: {msg}")
        assert _is_permanent_error(exc) is False, f"'{msg}' should not be permanent"


def test_permanent_error_case_insensitive() -> None:
    """_is_permanent_error matching is case-insensitive."""
    exc = DbtRuntimeError(
        "Error while executing query: [schema_not_found] The schema `foo` cannot be found."
    )
    assert _is_permanent_error(exc) is True


# --- Tests for token_credential auth ---


def _base_fabric_kwargs() -> dict:
    return {
        "method": "livy",
        "lakehouse": "tests",
        "workspaceid": "00000000-0000-0000-0000-000000000000",
        "lakehouseid": "00000000-0000-0000-0000-000000000001",
        "spark_config": {"name": "test-session"},
    }


def test_token_credential_requires_credential_class() -> None:
    """authentication=token_credential without credential_class must raise."""
    with pytest.raises(DbtRuntimeError, match="credential_class"):
        FabricSparkCredentials(
            authentication="token_credential",
            **_base_fabric_kwargs(),
        )


def test_token_credential_accepts_dotted_path() -> None:
    credentials = FabricSparkCredentials(
        authentication="token_credential",
        credential_class="my_pkg.auth.ExternalTokenCredential",
        credential_kwargs={"token_url": "https://broker.internal/token", "user_id": "alice"},
        **_base_fabric_kwargs(),
    )
    assert credentials.credential_class == "my_pkg.auth.ExternalTokenCredential"
    assert credentials.credential_kwargs == {
        "token_url": "https://broker.internal/token",
        "user_id": "alice",
    }


def test_token_credential_case_insensitive() -> None:
    """authentication value should be matched case-insensitively."""
    credentials = FabricSparkCredentials(
        authentication="Token_Credential",
        credential_class="my_pkg.auth.Cred",
        **_base_fabric_kwargs(),
    )
    assert credentials.credential_class == "my_pkg.auth.Cred"


def test_credential_class_with_non_token_auth_raises() -> None:
    """credential_class set but authentication != token_credential must raise."""
    with pytest.raises(DbtRuntimeError, match="token_credential"):
        FabricSparkCredentials(
            authentication="CLI",
            credential_class="my_pkg.auth.Cred",
            **_base_fabric_kwargs(),
        )


def test_credential_kwargs_with_non_token_auth_raises() -> None:
    """credential_kwargs set but authentication != token_credential must raise."""
    with pytest.raises(DbtRuntimeError, match="token_credential"):
        FabricSparkCredentials(
            authentication="CLI",
            credential_kwargs={"token_url": "https://x"},
            **_base_fabric_kwargs(),
        )


def test_repr_masks_credential_kwargs_values() -> None:
    """__repr__ must not leak credential_kwargs values (broker URLs, user ids)."""
    secret_value = "secret-broker.internal/token"
    credentials = FabricSparkCredentials(
        authentication="token_credential",
        credential_class="my_pkg.auth.Cred",
        # Two value shapes: a URL-like string and a plain scalar. We want
        # both kinds masked so the redaction isn't accidentally only
        # matching things that look like URLs.
        credential_kwargs={"token_url": secret_value, "user_id": "user-123"},
        **_base_fabric_kwargs(),
    )
    rendered = repr(credentials)
    assert secret_value not in rendered
    assert "user-123" not in rendered
    # Keys should still appear so operators can debug shape mismatches.
    assert "token_url" in rendered
    assert "user_id" in rendered
    assert "my_pkg.auth.Cred" in rendered
