from dbt.adapters.contracts.connection import Credentials
from typing import Any, Dict, Optional, Tuple
from dataclasses import dataclass, field
import dbt.exceptions

@dataclass
class SparkCredentials(Credentials):
    schema: Optional[str] = None  # type: ignore
    method: str = "livy"
    workspaceid: str = None
    database: Optional[str] = None
    lakehouse: str = None
    lakehouseid: str = None  # type: ignore    
    endpoint: Optional[str] = "https://api.fabric.microsoft.com/v1"
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    tenant_id: Optional[str] = None
    authentication: str= "CLI"
    connect_retries: int = 1
    connect_timeout: int = 10
    livy_session_parameters: Dict[str, Any] = field(default_factory=dict)
    retry_all: bool = False
    shortcuts_json_path: Optional[str] = None

    @classmethod
    def __pre_deserialize__(cls, data: Any) -> Any:
        data = super().__pre_deserialize__(data)
        if "lakehouse" not in data:
            data["lakehouse"] = None
        return data

    @property
    def lakehouse_endpoint(self) -> str:
        # TODO: Construct Endpoint of the lakehouse from the 
        return f'{self.endpoint}/workspaces/{self.workspaceid}/lakehouses/{self.lakehouseid}/livyapi/versions/2023-12-01'

    def __post_init__(self) -> None:        
        
        if self.method is None:
            raise dbt.exceptions.DbtRuntimeError("Must specify `method` in profile")
        if self.workspaceid is None:
            raise dbt.exceptions.DbtRuntimeError("Must specify `workspace guid` in profile")
        if self.lakehouseid is None:
            raise dbt.exceptions.DbtRuntimeError("Must specify `lakehouse guid` in profile")
        if self.schema is None:
            raise dbt.exceptions.DbtRuntimeError("Must specify `schema` in profile")
        if self.database is not None:
            raise dbt.exceptions.DbtRuntimeError("database property is not supported by adapter. Set database as none and use lakehouse instead.")
        

        # spark classifies database and schema as the same thing
        if self.lakehouse is not None and self.lakehouse != self.schema and self.schema is not None:
            # raise dbt.exceptions.DbtRuntimeError(
            #     f"    schema: {self.schema} \n"
            #     f"    lakehouse: {self.lakehouse} \n"
            #     f"On Spark, lakehouse must be omitted or have the same value as"
            # #     f" schema."
            # # )
            self.schema = f"{self.lakehouse}.{self.schema}"    # Support Fabric Lakehouse schema
            # self.schema = self.lakehouse

    @property
    def type(self) -> str:
        return "fabricspark"

    @property
    def unique_field(self) -> str:
        return self.lakehouseid  # type: ignore

    def _connection_keys(self) -> Tuple[str, ...]:
        return "workspaceid", "lakehouseid", "lakehouse", "endpoint", "schema"

