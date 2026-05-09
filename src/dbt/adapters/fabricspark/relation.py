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
    # database must be quoted (True) so that dbt preserves original casing
    # through _make_match_kwargs; otherwise mixed-case lakehouse names like
    # 'DBTTest' get lowered to 'dbttest' and trigger ApproximateMatchError.
    # Fabric Lakehouse stores names with their original casing, so case-sensitive
    # matching is required for correct relation resolution.
    database: bool = True
    schema: bool = True
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

    # Cross-workspace 4-part naming. When set, ``render()`` prepends the
    # workspace as a backtick-quoted prefix so the SQL becomes:
    #   `workspace`.`lakehouse`.`schema`.identifier
    # When None (default), rendering is unchanged (2-part or 3-part).
    #
    # Fabric Livy supports 4-part names *only* against schema-enabled lakehouses.
    # The macro layer (``fabricspark__generate_database_name``) raises a
    # ``DbtRuntimeError`` at parse time if a model sets ``workspace_name`` while
    # the target lakehouse is non-schema-enabled.
    workspace: Optional[str] = None

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
    def create_from(cls, quoting, relation_config, **kwargs):
        """Pull ``workspace_name`` from the model's ``config()`` into the relation.

        ``relation_config.config`` is a ``MaterializationConfig`` mapping that
        carries adapter-specific keys (registered on ``FabricSparkConfig``).
        For nodes whose config sets ``workspace_name`` (top-level config key),
        we forward it as the ``workspace`` field on the resulting relation so
        ``render()`` emits a 4-part name.
        """
        if "workspace" not in kwargs:
            ws_name = None
            cfg = getattr(relation_config, "config", None)
            if cfg is not None:
                try:
                    ws_name = cfg.get("workspace_name")
                except Exception:
                    ws_name = None
            if ws_name:
                kwargs["workspace"] = ws_name
        return super().create_from(quoting, relation_config, **kwargs)

    def render(self) -> str:
        base = super().render()
        # Only emit the workspace prefix when the database segment is also
        # included. This automatically excludes the workspace from temp views
        # and CTEs which use ``.include(database=false, schema=false)`` to
        # render as bare identifiers (those need to stay session-scoped).
        # Workspace_name is only valid against schema-enabled lakehouses where
        # ``include_policy.database`` defaults to True, so this check is a
        # no-op in the supported configuration.
        if self.workspace and self.include_policy.database:
            # Workspace casing must be preserved (mixed-case workspace names
            # like 'dbt Fabric Spark 1' are common). Quote it iff the database
            # quote policy says so — they share the same quote rationale.
            if self.quote_policy.database:
                quoted_ws = self.quoted(self.workspace)
            else:
                quoted_ws = self.workspace
            return f"{quoted_ws}.{base}" if base else quoted_ws
        return base

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
