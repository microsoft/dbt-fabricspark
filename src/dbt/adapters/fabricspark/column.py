from dataclasses import dataclass
from typing import Any, Dict, Optional, TypeVar, Union

from dbt_common.dataclass_schema import dbtClassMixin

from dbt.adapters.base.column import Column

Self = TypeVar("Self", bound="FabricSparkColumn")


@dataclass
class FabricSparkColumn(dbtClassMixin, Column):
    table_database: Optional[str] = None
    table_schema: Optional[str] = None
    table_name: Optional[str] = None
    table_type: Optional[str] = None
    table_owner: Optional[str] = None
    table_stats: Optional[Dict[str, Any]] = None
    column_index: Optional[int] = None

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        return dtype

    def can_expand_to(self: Self, other_column: Self) -> bool:
        """returns True if both columns are strings"""
        return self.is_string() and other_column.is_string()

    def literal(self, value: Any) -> str:
        return "cast({} as {})".format(value, self.dtype)

    @property
    def quoted(self) -> str:
        return "`{}`".format(self.column)

    @property
    def data_type(self) -> str:
        return self.dtype

    @classmethod
    def numeric_type(cls, dtype: str, precision: Any, scale: Any) -> str:
        # SparkSQL does not support 'numeric' or 'number', only 'decimal'
        if precision is None or scale is None:
            return "decimal"
        else:
            return "{}({},{})".format("decimal", precision, scale)

    def __repr__(self) -> str:
        return "<FabricSpark {} ({})>".format(self.name, self.data_type)

    @staticmethod
    def convert_table_stats(raw_stats: Optional[str]) -> Dict[str, Any]:
        table_stats: Dict[str, Union[int, str, bool]] = {}
        if raw_stats:
            # format: 1109049927 bytes, 14093476 rows
            stats = {
                stats.split(" ")[1]: int(stats.split(" ")[0]) for stats in raw_stats.split(", ")
            }
            for key, val in stats.items():
                table_stats[f"stats:{key}:label"] = key
                table_stats[f"stats:{key}:value"] = val
                table_stats[f"stats:{key}:description"] = ""
                table_stats[f"stats:{key}:include"] = True
        return table_stats

    def to_column_dict(self, omit_none: bool = True, validate: bool = False) -> Dict[str, Any]:
        original_dict = self.to_dict(omit_none=omit_none)
        # If there are stats, merge them into the root of the dict
        original_stats = original_dict.pop("table_stats", None)
        if original_stats:
            original_dict.update(original_stats)
        return original_dict
