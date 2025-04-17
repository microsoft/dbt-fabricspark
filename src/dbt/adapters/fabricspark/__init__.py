from dbt.adapters.fabricspark.connections import FabricSparkConnectionManager
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.relation import FabricSparkRelation  # noqa
from dbt.adapters.fabricspark.column import FabricSparkColumn  # noqa
from dbt.adapters.fabricspark.impl import FabricSparkAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import fabricspark

Plugin = AdapterPlugin(
    adapter=FabricSparkAdapter, credentials=FabricSparkCredentials, include_path=fabricspark.PACKAGE_PATH  # type: ignore
)
