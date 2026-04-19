from tests.functional.adapter.basic._snapshot_check_cols_base import (
    SnapshotCheckColsSplitBase,
)


class TestSnapshotCheckColsAll(SnapshotCheckColsSplitBase):
    snapshot_name = "cc_all_snapshot"
    expected_counts = (10, 20, 30, 40)
