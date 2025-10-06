from dataclasses import dataclass, field
from typing import Optional, TypeVar, Type

from dbt_common.exceptions import DbtRuntimeError
from dbt_common.dataclass_schema import StrEnum
from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.utils import classproperty

logger = AdapterLogger("fabricspark")

class FabricSparkRelationType(StrEnum):
    Table = "table"
    View = "view"
    CTE = "cte"
    MaterializedView = "materialized_view"

@dataclass
class FabricSparkQuotePolicy(Policy):
    database: bool = False
    schema: bool = False
    identifier: bool = False


@dataclass
class FabricSparkIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class FabricSparkRelation(BaseRelation):
    type: Optional[FabricSparkRelationType] = None  # type: ignore
    quote_policy: Policy = field(default_factory=lambda: FabricSparkQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: FabricSparkIncludePolicy())
    quote_character: str = "`"
    is_delta: Optional[bool] = None
    # TODO: make this a dict everywhere
    information: Optional[str] = None

    @classmethod
    def __pre_deserialize__(cls, data: dict) -> dict:
        data = super().__pre_deserialize__(data)
        # Defensive: set type to "table" if None or "Undefined"
        if "type" not in data or data["type"] in (None, "Undefined"):
            data["type"] = FabricSparkRelationType.Table
        return data

    @classproperty
    def get_relation_type(cls) -> Type[FabricSparkRelationType]:  # noqa
        return FabricSparkRelationType
      
    def __post_init__(self) -> None:
        
        if self.database != self.schema and self.database:
            raise DbtRuntimeError("Cannot set database in spark!")

    def render(self) -> str:
        if self.include_policy.database and self.include_policy.schema:
            raise DbtRuntimeError(
                "Got a spark relation with schema and database set to "
                "include, but only one can be set"
            )
        return super().render()
