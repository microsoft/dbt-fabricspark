import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional, Tuple
from urllib.parse import urlparse

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("fabricspark")

_UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)

_ALLOWED_FABRIC_DOMAINS = [
    r"\.fabric\.microsoft\.com$",
    r"\.pbidedicated\.windows\.net$",
    r"\.analysis\.windows\.net$",
    r"\.microsoftfabric\.com$",
]

# Mode types for Livy connection
LivyMode = Literal["fabric", "local"]

# Default session ID file name
DEFAULT_SESSION_ID_FILENAME = "livy-session-id.txt"


@dataclass
class FabricSparkCredentials(Credentials):
    # schema: user-provided from profiles.yml. Defaults to lakehouse name.
    # For schema-enabled lakehouses, user can set this to a specific schema name.
    schema: Optional[str] = None  # type: ignore
    method: str = "livy"
    livy_mode: LivyMode = "fabric"  # "fabric" or "local"
    workspaceid: Optional[str] = None
    # database is internal — always derived from lakehouse name.
    # Not a user input. init=False excludes it from deserialization.
    database: Optional[str] = field(default=None, init=False)  # type: ignore
    lakehouse: Optional[str] = None
    lakehouseid: Optional[str] = None
    endpoint: Optional[str] = "https://api.fabric.microsoft.com/v1"
    livy_url: str = "http://localhost:8998"  # Local Livy URL
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    tenant_id: Optional[str] = None
    authentication: str = "CLI"
    connect_retries: int = 1
    connect_timeout: int = 10
    create_shortcuts: Optional[bool] = False
    retry_all: bool = False
    shortcuts_json_str: Optional[str] = None
    # Auto-detected at connection time via Fabric REST API; not user-configurable.
    # init=False ensures this is never populated from profile YAML.
    lakehouse_schemas_enabled: bool = field(default=False, init=False)
    identifier_prefix: Optional[str] = ""
    accessToken: Optional[str] = None
    spark_config: Dict[str, Any] = field(default_factory=dict)
    environmentId: Optional[str] = None
    session_id_file: Optional[str] = None
    reuse_session: bool = False  # When True, Fabric sessions are kept alive and reused across runs
    session_idle_timeout: str = "30m"  # Livy session idle timeout (e.g. "30m", "1h")

    # Livy session stability settings
    http_timeout: int = 120  # seconds for each HTTP request to Fabric API
    session_start_timeout: int = 600  # max seconds to wait for session start (10 min)
    statement_timeout: int = 3600  # max seconds to wait for a statement result (1 hour)
    poll_wait: int = 10  # seconds between polls for session start
    poll_statement_wait: float = 0.5  # seconds between polls for statement result

    def __repr__(self) -> str:
        """Mask sensitive fields in repr to prevent credential leakage in logs/tracebacks."""
        return (
            f"FabricSparkCredentials("
            f"workspaceid={self.workspaceid!r}, "
            f"lakehouseid={self.lakehouseid!r}, "
            f"endpoint={self.endpoint!r}, "
            f"authentication={self.authentication!r}, "
            f"client_id={self.client_id!r}, "
            f"client_secret='***', "
            f"accessToken='***')"
        )

    @classmethod
    def __pre_deserialize__(cls, data: Any) -> Any:
        data = super().__pre_deserialize__(data)
        if "lakehouse" not in data:
            data["lakehouse"] = None
        # database is init=False (derived from lakehouse) — strip if someone passes it.
        if "database" in data:
            del data["database"]
        # lakehouse_schemas_enabled is init=False — strip if someone passes it.
        if "lakehouse_schemas_enabled" in data:
            del data["lakehouse_schemas_enabled"]
        return data

    @property
    def is_local_mode(self) -> bool:
        return self.livy_mode == "local"

    @property
    def resolved_session_id_file(self) -> str:
        if self.session_id_file:
            return self.session_id_file
        return os.path.join(os.getcwd(), DEFAULT_SESSION_ID_FILENAME)

    @property
    def lakehouse_endpoint(self) -> str:
        if self.is_local_mode:
            return self.livy_url
        return f"{self.endpoint}/workspaces/{self.workspaceid}/lakehouses/{self.lakehouseid}/livyapi/versions/2023-12-01"

    def __post_init__(self) -> None:
        if self.method is None:
            raise DbtRuntimeError("Must specify `method` in profile")

        # Fabric-specific validations
        if not self.is_local_mode:
            if self.endpoint is None:
                raise DbtRuntimeError("Must specify `endpoint` in profile for Fabric mode")
            if self.workspaceid is None:
                raise DbtRuntimeError("Must specify `workspaceid` in profile for Fabric mode")
            if self.lakehouseid is None:
                raise DbtRuntimeError("Must specify `lakehouseid` in profile for Fabric mode")
            if self.lakehouse is None:
                raise DbtRuntimeError("Must specify `lakehouse` in profile for Fabric mode")

        # schema defaults to lakehouse name if not provided by user.
        # For schema-enabled lakehouses, user can override this in profiles.yml.
        # For local mode without lakehouse, defaults to "default" (Spark's default database).
        if self.schema is None:
            if self.lakehouse is not None:
                self.schema = self.lakehouse
            elif self.is_local_mode:
                self.schema = "default"

        # database is always set to lakehouse name for relation rendering.
        # In non-schema mode, include_policy.database=False excludes it from SQL.
        # In schema-enabled mode, include_policy.database=True renders three-part names.
        # For local mode without lakehouse, defaults to "default".
        if self.lakehouse is not None:
            self.database = self.lakehouse
        elif self.is_local_mode:
            self.database = "default"

        # Security validations (Fabric mode only)
        if not self.is_local_mode:
            self._validate_uuid(self.workspaceid, "workspaceid")
            self._validate_uuid(self.lakehouseid, "lakehouseid")
            self._validate_endpoint()

        # Validate spark_config
        required_keys = ["name"]
        for key in required_keys:
            if key not in self.spark_config:
                raise ValueError(f"Missing required key: {key}")

    def apply_lakehouse_properties(self, lakehouse_properties: dict) -> None:
        """Apply lakehouse properties after fetching them from the Fabric REST API.

        Detects whether the lakehouse has schemas enabled by checking for the
        ``defaultSchema`` property in the API response.

        For schema-enabled lakehouses:
          - schema can differ from lakehouse (user picks a custom schema name)
        For non-schema lakehouses:
          - schema must equal lakehouse name
        """
        self.lakehouse_schemas_enabled = "defaultSchema" in lakehouse_properties
        logger.debug(f"Lakehouse schemas enabled: {self.lakehouse_schemas_enabled}")

        if self.lakehouse_schemas_enabled:
            if (
                self.schema is not None
                and self.lakehouse is not None
                and self.schema == self.lakehouse
            ):
                raise DbtRuntimeError(
                    f"Lakehouse '{self.lakehouse}' has schemas enabled. "
                    f"Please set `schema` in profiles.yml to a schema name other than "
                    f"the lakehouse name (e.g. 'dbo')."
                )
        else:
            if (
                self.schema is not None
                and self.lakehouse is not None
                and self.schema != self.lakehouse
            ):
                logger.debug(
                    f"Non-schema lakehouse: overriding schema '{self.schema}' "
                    f"to lakehouse name '{self.lakehouse}'"
                )
                self.schema = self.lakehouse

    @property
    def type(self) -> str:
        return "fabricspark"

    @property
    def unique_field(self) -> str:
        if self.is_local_mode:
            return self.livy_url
        return self.lakehouseid

    def _validate_endpoint(self) -> None:
        """Validate the endpoint uses HTTPS and points to a known Fabric domain."""
        if not self.endpoint:
            raise DbtRuntimeError("Must specify `endpoint` in profile")

        parsed = urlparse(self.endpoint)
        if parsed.scheme != "https":
            raise DbtRuntimeError(f"endpoint must use HTTPS, got: {self.endpoint}")

        hostname = parsed.hostname or ""
        is_known_domain = any(re.search(pattern, hostname) for pattern in _ALLOWED_FABRIC_DOMAINS)
        if not is_known_domain:
            logger.warning(
                f"Security warning: endpoint '{self.endpoint}' does not match any known "
                f"Microsoft Fabric domain ({', '.join(_ALLOWED_FABRIC_DOMAINS)}). "
                f"Bearer tokens will be sent to this host. "
                f"Ensure this is a trusted endpoint."
            )

    def _validate_uuid(self, value: Optional[str], field_name: str) -> None:
        """Validate that a field value is a proper UUID to prevent path traversal."""
        if value is not None and value != "" and not _UUID_PATTERN.match(value):
            raise DbtRuntimeError(
                f"{field_name} must be a valid UUID (got: {value!r}). "
                f"Check your profiles.yml configuration."
            )

    def _connection_keys(self) -> Tuple[str, ...]:
        # Intentionally excludes client_secret, accessToken, tenant_id
        return "workspaceid", "lakehouseid", "lakehouse", "endpoint", "schema"
