from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Type, TypeVar

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("fabricspark")

Self = TypeVar("Self", bound="BaseRelation")

# Valid RelationType values
_VALID_RELATION_TYPES = {t.value for t in RelationType}


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
    quote_policy: Policy = field(default_factory=lambda: FabricSparkQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: FabricSparkIncludePolicy())
    quote_character: str = "`"
    is_delta: Optional[bool] = None
    # TODO: make this a dict everywhere
    information: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FabricSparkRelation":
        # Sanitize 'type' field: Jinja Undefined or invalid strings become None
        if "type" in data and data["type"] is not None:
            type_val = data["type"]
            if not isinstance(type_val, RelationType):
                type_str = str(type_val)
                if type_str not in _VALID_RELATION_TYPES:
                    logger.debug(
                        f"Replacing invalid relation type '{type_str}' with None"
                    )
                    data = dict(data)
                    data["type"] = None
        return super().from_dict(data)

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

