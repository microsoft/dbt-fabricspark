import pytest

from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampNaive
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesBackslash
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.util import relation_from_name, run_dbt
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral
from datetime import datetime, timedelta

# requires modification
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.fixture_listagg import models__test_listagg_yml
from tests.functional.adapter.utils.fixture_listagg import models__test_listagg_no_order_by_sql

seeds__data_split_part_csv = """parts,split_on,result_1,result_2,result_3,result_4
a|b|c,|,a,b,c,c
1|2|3,|,1,2,3,3
EMPTY|EMPTY|EMPTY,|,EMPTY,EMPTY,EMPTY,EMPTY
"""

seeds__data_last_day_csv = """date_day,date_part,result
2018-01-02,month,2018-01-31
2018-01-02,quarter,2018-03-31
2018-01-02,year,2018-12-31
"""


# skipped: ,month,


class TestAnyValue(BaseAnyValue):
    pass


class TestArrayAppend(BaseArrayAppend):
    pass


class TestArrayConcat(BaseArrayConcat):
    pass


class TestArrayConstruct(BaseArrayConstruct):
    pass


class TestBoolOr(BaseBoolOr):
    pass


class TestCastBoolToText(BaseCastBoolToText):
    pass


@pytest.mark.skip_profile("spark_session")
class TestConcat(BaseConcat):
    pass


# Use either BaseCurrentTimestampAware or BaseCurrentTimestampNaive but not both
class TestCurrentTimestamp(BaseCurrentTimestampNaive):
    @pytest.fixture(scope="class")
    def current_timestamp(self, project):
        run_dbt(["build"])
        relation = relation_from_name(project.adapter, "current_ts")
        result = project.run_sql(f"select current_ts_column from {relation}", fetch="one")
        sql_timestamp = result[0] if result is not None else None
        # Parse the string into a datetime object
        return datetime.fromisoformat(sql_timestamp) if sql_timestamp else None

    def test_current_timestamp_matches_utc(self, current_timestamp):
        sql_timestamp = current_timestamp
        now_utc = self.utcnow_matching_type(sql_timestamp)
        # Plenty of wiggle room if clocks aren't perfectly sync'd, etc
        # The clock on the macos image appears to be a few minutes slow in GHA, causing false negatives
        tolerance = timedelta(minutes=10)
        assert (sql_timestamp > (now_utc - tolerance)) and (
            sql_timestamp < (now_utc + tolerance)
        ), f"SQL timestamp {sql_timestamp.isoformat()} is not close enough to Python UTC {now_utc.isoformat()}"

    def test_current_timestamp_type(self, current_timestamp):
        assert current_timestamp


class TestDateAdd(BaseDateAdd):
    pass


# this generates too much SQL to run successfully in our testing environments :(
@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestDateDiff(BaseDateDiff):
    pass


class TestDateTrunc(BaseDateTrunc):
    pass


class TestEscapeSingleQuotes(BaseEscapeSingleQuotesBackslash):
    pass


class TestExcept(BaseExcept):
    pass


@pytest.mark.skip_profile("spark_session")
class TestHash(BaseHash):
    pass


class TestIntersect(BaseIntersect):
    pass


@pytest.mark.skip_profile("spark_session")  # spark session crashes in CI
class TestLastDay(BaseLastDay):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_last_day.csv": seeds__data_last_day_csv}


class TestLength(BaseLength):
    pass


# SparkSQL does not support 'order by' for its 'listagg' equivalent
# the argument is ignored, so let's ignore those fields when checking equivalency
class TestListagg(BaseListagg):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                models__test_listagg_no_order_by_sql, "listagg"
            ),
        }


class TestPosition(BasePosition):
    pass


@pytest.mark.skip_profile("spark_session")
class TestReplace(BaseReplace):
    pass


@pytest.mark.skip_profile("spark_session")
class TestRight(BaseRight):
    pass


class TestSafeCast(BaseSafeCast):
    pass


class TestSplitPart(BaseSplitPart):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_split_part.csv": seeds__data_split_part_csv}


class TestStringLiteral(BaseStringLiteral):
    pass
