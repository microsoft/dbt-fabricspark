from datetime import datetime, timedelta

import pytest

from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampNaive
from dbt.tests.adapter.utils.test_date import BaseDate
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.util import relation_from_name, run_dbt


@pytest.mark.skip_profile("spark_session")
class TestConcat(BaseConcat):
    pass


class TestCurrentTimestamp(BaseCurrentTimestampNaive):
    @pytest.fixture(scope="class")
    def current_timestamp(self, project):
        run_dbt(["build"])
        relation = relation_from_name(project.adapter, "current_ts")
        result = project.run_sql(f"select current_ts_column from {relation}", fetch="one")
        sql_timestamp = result[0] if result is not None else None
        if not sql_timestamp:
            return None
        if isinstance(sql_timestamp, str):
            sql_timestamp = sql_timestamp.replace("Z", "+00:00")
            return datetime.fromisoformat(sql_timestamp)
        return sql_timestamp

    def test_current_timestamp_matches_utc(self, current_timestamp):
        sql_timestamp = current_timestamp
        now_utc = self.utcnow_matching_type(sql_timestamp)
        tolerance = timedelta(minutes=10)
        assert (sql_timestamp > (now_utc - tolerance)) and (
            sql_timestamp < (now_utc + tolerance)
        ), (
            f"SQL timestamp {sql_timestamp.isoformat()} is not close enough to Python UTC {now_utc.isoformat()}"
        )

    def test_current_timestamp_type(self, current_timestamp):
        assert current_timestamp


class TestDate(BaseDate):
    pass


class TestDateAdd(BaseDateAdd):
    pass


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestDateDiff(BaseDateDiff):
    pass


class TestDateTrunc(BaseDateTrunc):
    pass
