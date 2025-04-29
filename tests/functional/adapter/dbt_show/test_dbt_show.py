from dbt.tests.adapter.dbt_show.test_dbt_show import (
    BaseShowDoesNotHandleDoubleLimit,
    BaseShowLimit,
    BaseShowSqlHeader,
)


class TestSparkShowLimit(BaseShowLimit):
    pass


class TestSparkShowSqlHeader(BaseShowSqlHeader):
    pass


class TestSparkShowDoesNotHandleDoubleLimit(BaseShowDoesNotHandleDoubleLimit):
    DATABASE_ERROR_MESSAGE = "limit"
