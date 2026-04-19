import pytest

from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral

seeds__data_split_part_csv = """parts,split_on,result_1,result_2,result_3,result_4
a|b|c,|,a,b,c,c
1|2|3,|,1,2,3,3
EMPTY|EMPTY|EMPTY,|,EMPTY,EMPTY,EMPTY,EMPTY
"""


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
