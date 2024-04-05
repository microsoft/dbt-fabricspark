from dbt.adapters.fabricspark.connections import SparkConnectionManager
from dbt.adapters.fabricspark.fabric_spark_credentials import SparkCredentials
from dbt.adapters.fabricspark.relation import SparkRelation  # noqa
from dbt.adapters.fabricspark.column import SparkColumn  # noqa
from dbt.adapters.fabricspark.impl import SparkAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import fabricspark

Plugin = AdapterPlugin(
    adapter=SparkAdapter, credentials=SparkCredentials, include_path=fabricspark.PACKAGE_PATH  # type: ignore
)
