import re
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple
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


@dataclass
class FabricSparkCredentials(Credentials):
    schema: Optional[str] = None  # type: ignore
    method: str = "livy"
    workspaceid: Optional[str] = None
    database: Optional[str] = None  # type: ignore
    lakehouse: Optional[str] = None
    lakehouseid: Optional[str] = None
    endpoint: str = "https://msitapi.fabric.microsoft.com/v1"
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    tenant_id: Optional[str] = None
    authentication: str = "az_cli"
    connect_retries: int = 1
    connect_timeout: int = 10
    create_shortcuts: Optional[bool] = False
    retry_all: bool = False
    shortcuts_json_str: Optional[str] = None
    lakehouse_schemas_enabled: bool = False
    accessToken: Optional[str] = None
    spark_config: Dict[str, Any] = field(default_factory=dict)

    # Livy session stability settings
    http_timeout: int = 120  # seconds for each HTTP request to Fabric API
    session_start_timeout: int = 600  # max seconds to wait for session start (10 min)
    statement_timeout: int = 3600  # max seconds to wait for a statement result (1 hour)
    poll_wait: int = 10  # seconds between polls for session start
    poll_statement_wait: int = 5  # seconds between polls for statement result

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
        return data

    @property
    def lakehouse_endpoint(self) -> str:
        return f"{self.endpoint}/workspaces/{self.workspaceid}/lakehouses/{self.lakehouseid}/livyapi/versions/2023-12-01"

    def _validate_endpoint(self) -> None:
        """Validate the endpoint uses HTTPS and points to a known Fabric domain."""
        if not self.endpoint:
            raise DbtRuntimeError("Must specify `endpoint` in profile")

        parsed = urlparse(self.endpoint)
        if parsed.scheme != "https":
            raise DbtRuntimeError(
                f"endpoint must use HTTPS, got: {self.endpoint}"
            )

        hostname = parsed.hostname or ""
        is_known_domain = any(
            re.search(pattern, hostname) for pattern in _ALLOWED_FABRIC_DOMAINS
        )
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

    def __post_init__(self) -> None:
        if self.method is None:
            raise DbtRuntimeError("Must specify `method` in profile")
        if self.workspaceid is None:
            raise DbtRuntimeError("Must specify `workspace guid` in profile")
        if self.lakehouseid is None:
            raise DbtRuntimeError("Must specify `lakehouse guid` in profile")
        if self.schema is None:
            raise DbtRuntimeError("Must specify `schema` in profile")
        if self.database is not None:
            raise DbtRuntimeError(
                "database property is not supported by adapter. Set database as none and use lakehouse instead."
            )
        if self.lakehouse_schemas_enabled and self.schema is None:
            raise DbtRuntimeError(
                "Please provide a schema name because you enabled lakehouse schemas"
            )

        if not self.lakehouse_schemas_enabled and self.lakehouse is not None:
            self.schema = self.lakehouse

        # Security validations
        self._validate_uuid(self.workspaceid, "workspaceid")
        self._validate_uuid(self.lakehouseid, "lakehouseid")
        self._validate_endpoint()

        """ Validate spark_config fields manually. """
        # other keys - "archives", "conf", "tags", "driverMemory", "driverCores", "executorMemory", "executorCores", "numExecutors"
        required_keys = ["name"]

        for key in required_keys:
            if key not in self.spark_config:
                raise ValueError(f"Missing required key: {key}")

    @property
    def type(self) -> str:
        return "fabricspark"

    @property
    def unique_field(self) -> str:
        return self.lakehouseid

    def _connection_keys(self) -> Tuple[str, ...]:
        # Intentionally excludes client_secret, accessToken, tenant_id
        return "workspaceid", "lakehouseid", "lakehouse", "endpoint", "schema", "file_format"
