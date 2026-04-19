from tests.functional.adapter.basic._snapshot_check_cols_base import (
    SnapshotCheckColsSplitBase,
)


class TestSnapshotCheckColsName(SnapshotCheckColsSplitBase):
    snapshot_name = "cc_name_snapshot"
    # name strategy does not see timestamp-only updates (count stays at 20 at step 3)
    expected_counts = (10, 20, 20, 30)
