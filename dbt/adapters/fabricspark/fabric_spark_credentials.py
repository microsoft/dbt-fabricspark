from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.events.logging import AdapterLogger
from typing import Any, Dict, Optional, Tuple
from dataclasses import dataclass, field
from dbt_common.exceptions import DbtRuntimeError

logger = AdapterLogger("fabricspark")


@dataclass
class SparkCredentials(Credentials):
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
    livy_session_parameters: Dict[str, Any] = field(default_factory=dict)
    retry_all: bool = False
    accessToken: Optional[str] = None

    @classmethod
    def __pre_deserialize__(cls, data: Any) -> Any:
        data = super().__pre_deserialize__(data)
        if "lakehouse" not in data:
            data["lakehouse"] = None
        return data

    @property
    def lakehouse_endpoint(self) -> str:
        # TODO: Construct Endpoint of the lakehouse from the
        return f"{self.endpoint}/workspaces/{self.workspaceid}/lakehouses/{self.lakehouseid}/livyapi/versions/2023-12-01"

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

    @property
    def type(self) -> str:
        return "fabricspark"

    @property
    def unique_field(self) -> str:
        return self.lakehouseid

    def _connection_keys(self) -> Tuple[str, ...]:
        return "workspaceid", "lakehouseid", "lakehouse", "endpoint", "schema"
