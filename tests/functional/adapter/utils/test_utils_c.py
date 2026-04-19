import pytest

from dbt.tests.adapter.utils.fixture_listagg import models__test_listagg_yml
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesBackslash
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from tests.functional.adapter.utils.fixture_listagg import models__test_listagg_no_order_by_sql

seeds__data_last_day_csv = """date_day,date_part,result
2018-01-02,month,2018-01-31
2018-01-02,quarter,2018-03-31
2018-01-02,year,2018-12-31
"""


class TestEscapeSingleQuotes(BaseEscapeSingleQuotesBackslash):
    pass


class TestExcept(BaseExcept):
    pass


@pytest.mark.skip_profile("spark_session")
class TestHash(BaseHash):
    pass


class TestIntersect(BaseIntersect):
    pass


@pytest.mark.skip_profile("spark_session")
class TestLastDay(BaseLastDay):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_last_day.csv": seeds__data_last_day_csv}


class TestLength(BaseLength):
    pass


class TestListagg(BaseListagg):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                models__test_listagg_no_order_by_sql, "listagg"
            ),
        }
