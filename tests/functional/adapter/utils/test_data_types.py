import pytest
from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_float import (
    BaseTypeFloat,
    seeds__expected_csv as seeds__float_expected_csv,
)
from dbt.tests.adapter.utils.data_types.test_type_int import (
    BaseTypeInt,
    seeds__expected_csv as seeds__int_expected_csv,
)
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_timestamp import BaseTypeTimestamp
from dbt.tests.adapter.utils.data_types.test_type_boolean import BaseTypeBoolean


class TestTypeBigInt(BaseTypeBigInt):
    pass


# need to explicitly cast this to avoid it being inferred/loaded as a DOUBLE on Spark
# in SparkSQL, the two are equivalent for `=` comparison, but distinct for EXCEPT comparison
seeds__float_expected_yml = """
version: 2
seeds:
  - name: expected
    config:
      materialized: seed
      pre-hook: "drop view if exists {{ target.schema }}.expected"
      column_types:
        float_col: float
"""
# need to explicitly cast this to avoid it being inferred/loaded as a BIGINT on Spark
seeds__int_expected_yml = """
version: 2
seeds:
  - name: expected
    config:
      column_types:
        int_col: int
"""


seeds__numeric_expected_yml = """
version: 2
seeds:
  - name: expected
    config:
      materialized: seed
      column_types:
        numeric_col: {}
"""

seeds__numeric_expected_csv = """numeric_col
1.2345
""".lstrip()

seeds__string_expected_csv = """string_col
"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
""".lstrip()

seeds__string_expected_yml = """
version: 2
seeds:
  - name: expected
    config:
      materialized: seed
      column_types:
        string_col: {}
"""

seeds__bool_expected_csv = """boolean_col
True
""".lstrip()

seeds__bool_expected_yml = """
version: 2
seeds:
  - name: expected
    config:
      materialized: seed
      column_types:
        boolean_col: boolean
"""

seeds__timestamp_expected_csv = """timestamp_col
2021-01-01 01:01:01
""".lstrip()

# need to explicitly cast this to avoid it being a DATETIME on BigQuery
# (but - should it actually be a DATETIME, for consistency with other dbs?)
seeds__timestamp_expected_yml = """
version: 2
seeds:
  - name: expected
    config:
      materialized: seed
      column_types:
        timestamp_col: timestamp
"""


class TestTypeTimestamp(BaseTypeTimestamp):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": seeds__timestamp_expected_csv,
            "expected.yml": seeds__timestamp_expected_yml,
        }


class TestTypeBoolean(BaseTypeBoolean):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": seeds__bool_expected_csv,
            "expected.yml": seeds__bool_expected_yml,
        }


class TestTypeFloat(BaseTypeFloat):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": seeds__float_expected_csv,
            "expected.yml": seeds__float_expected_yml,
        }


class TestTypeInt(BaseTypeInt):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": seeds__int_expected_csv,
            "expected.yml": seeds__int_expected_yml,
        }


class TestTypeNumeric(BaseTypeNumeric):
    def numeric_fixture_type(self):
        return "decimal(28,6)"

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": seeds__numeric_expected_csv,
            "expected.yml": seeds__numeric_expected_yml.format(self.numeric_fixture_type()),
        }


class TestTypeString(BaseTypeString):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": seeds__string_expected_csv,
            "expected.yml": seeds__string_expected_yml,
        }
