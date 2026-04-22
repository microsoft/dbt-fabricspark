from tests.functional.adapter.basic._snapshot_check_cols_base import (
    SnapshotCheckColsSplitBase,
)


class TestSnapshotCheckColsDate(SnapshotCheckColsSplitBase):
    snapshot_name = "cc_date_snapshot"
    # date strategy sees timestamp updates but not name updates
    expected_counts = (10, 20, 30, 30)
