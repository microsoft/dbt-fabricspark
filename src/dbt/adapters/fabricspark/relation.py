from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, Optional, TypeVar

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("fabricspark")

Self = TypeVar("Self", bound="BaseRelation")

# Valid RelationType values
_VALID_RELATION_TYPES = {t.value for t in RelationType}


@dataclass
class FabricSparkQuotePolicy(Policy):
    # ``database`` is unquoted so that dotted database values render as
    # multiple native identifiers — required for cross-workspace queries.
    #
    # When ``database = True``, ``BaseRelation.render()`` wraps the
    # database value in backticks. A profile setting
    # ``database: "ws.lh"`` then becomes ``` `ws.lh`.dbo.t ``` — Spark
    # treats that as a single backtick-quoted identifier with a literal
    # dot, **not** as the four-part name needed to address ``lh`` in a
    # different Fabric workspace. With ``database = False``, the same
    # value renders as ``ws.lh.dbo.t`` (four native unquoted parts) and
    # the cross-workspace query resolves correctly.
    #
    # Single-lakehouse profiles (``database: "lh"``) continue to render
    # as expected (``lh.dbo.t``) — only the no-longer-quoted style.
    #
    # Precondition: workspace, lakehouse, and schema names should be
    # lowercase. ``_make_match_kwargs`` lowercases unquoted identifiers
    # internally, so a mixed-case lakehouse (e.g. ``DBTTest``) would
    # silently mismatch its catalog entry through dbt's relation cache
    # under unquoted policy. This matches the de-facto convention in
    # most Fabric environments.
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
    # Class-level flag set once by the connection manager after detecting schema support.
    # Controls the default include_policy for all relations created after it is set:
    #   True  → three-part naming: database.schema.identifier (lakehouse.schema.table)
    #   False → two-part naming:   schema.identifier (lakehouse.table)
    #
    # Macros can still override per-relation via .include(database=false, schema=false)
    # for temporary views which require unqualified identifiers.
    _schemas_enabled: ClassVar[bool] = False
    _identifier_prefix: ClassVar[str] = ""

    quote_policy: Policy = field(default_factory=lambda: FabricSparkQuotePolicy())
    include_policy: Policy = field(
        default_factory=lambda: FabricSparkIncludePolicy(
            database=FabricSparkRelation._schemas_enabled,
            schema=True,
            identifier=True,
        )
    )
    quote_character: str = "`"
    is_delta: Optional[bool] = None
    # TODO: make this a dict everywhere
    information: Optional[str] = None

    @classmethod
    def create(cls, database=None, schema=None, identifier=None, **kwargs):
        skip_prefix = kwargs.pop("_skip_prefix", False)
        prefix = cls._identifier_prefix
        if prefix and identifier and not skip_prefix:
            # Never prefix CTE identifiers — ephemeral models are inlined as
            # WITH clauses and must keep their dbt-generated __dbt__cte__ name.
            rel_type = kwargs.get("type")
            is_cte = (rel_type == RelationType.CTE) or (
                isinstance(identifier, str) and "__dbt__cte__" in identifier
            )
            if not is_cte and not identifier.startswith(prefix):
                identifier = f"{prefix}{identifier}"
        return super().create(database=database, schema=schema, identifier=identifier, **kwargs)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FabricSparkRelation":
        # Sanitize 'type' field: Jinja Undefined or invalid strings become None
        if "type" in data and data["type"] is not None:
            type_val = data["type"]
            if not isinstance(type_val, RelationType):
                type_str = str(type_val)
                if type_str not in _VALID_RELATION_TYPES:
                    logger.debug(f"Replacing invalid relation type '{type_str}' with None")
                    data = dict(data)
                    data["type"] = None
        return super().from_dict(data)
