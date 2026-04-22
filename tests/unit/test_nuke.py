"""Unit tests for the branch-aware nuke logic."""

from __future__ import annotations

import time

from tests.functional.nuke import (
    STALE_THRESHOLD_SECONDS,
    _should_delete,
    branch_hash,
)


class TestBranchHash:
    """Tests for branch_hash()."""

    def test_deterministic(self) -> None:
        """Same branch name always produces the same hash."""
        assert branch_hash("feature/foo") == branch_hash("feature/foo")

    def test_different_branches_differ(self) -> None:
        """Different branch names produce different hashes."""
        assert branch_hash("feature/foo") != branch_hash("feature/bar")

    def test_length_is_eight(self) -> None:
        """Hash is always exactly 8 hex characters."""
        h = branch_hash("main")
        assert len(h) == 8
        assert all(c in "0123456789abcdef" for c in h)


class TestShouldDelete:
    """Tests for _should_delete()."""

    def _make_name(self, bhash: str, ts: int, mode: str = "no_schema") -> str:
        return f"dbt_{bhash}_{ts}_{mode}"

    def test_same_branch_always_deleted(self) -> None:
        """Items from the same branch are always deleted."""
        now = time.time()
        current = branch_hash("my-branch")
        name = self._make_name(current, int(now) - 60)  # 60s ago, same branch
        assert _should_delete(name, current, now) is True

    def test_different_branch_recent_not_deleted(self) -> None:
        """Recent items from other branches are kept."""
        now = time.time()
        current = branch_hash("my-branch")
        other = branch_hash("other-branch")
        name = self._make_name(other, int(now) - 60)  # 60s ago, different branch
        assert _should_delete(name, current, now) is False

    def test_different_branch_stale_deleted(self) -> None:
        """Old items from any branch are deleted (>24h)."""
        now = time.time()
        current = branch_hash("my-branch")
        other = branch_hash("other-branch")
        old_ts = int(now) - STALE_THRESHOLD_SECONDS - 1
        name = self._make_name(other, old_ts)
        assert _should_delete(name, current, now) is True

    def test_non_matching_pattern_never_deleted(self) -> None:
        """Items that don't match the naming pattern are never deleted."""
        now = time.time()
        current = branch_hash("my-branch")
        assert _should_delete("manual_lakehouse", current, now) is False
        assert _should_delete("my_other_lh", current, now) is False

    def test_old_format_without_hash_not_matched(self) -> None:
        """Old naming format dbt_{ts}_{mode} does not match the new pattern.

        The regex requires ``dbt_{hex}_{digits}_`` — the old format
        ``dbt_1714000000_no_schema`` has no second numeric segment after the
        first underscore-delimited group, so the regex does not match and the
        item is left untouched.  This is the desired behaviour: unknown items
        are never deleted.
        """
        now = time.time()
        current = branch_hash("my-branch")
        name = "dbt_1714000000_no_schema"
        assert _should_delete(name, current, now) is False

    def test_exactly_at_threshold_not_deleted(self) -> None:
        """An item exactly at the 24-hour boundary is NOT deleted (> not >=)."""
        now = float(int(time.time()))  # integer-valued float to avoid fractional drift
        current = branch_hash("my-branch")
        other = branch_hash("other-branch")
        boundary_ts = int(now) - STALE_THRESHOLD_SECONDS
        name = self._make_name(other, boundary_ts)
        assert _should_delete(name, current, now) is False

    def test_with_schema_mode_suffix(self) -> None:
        """The pattern works with both schema mode suffixes."""
        now = time.time()
        current = branch_hash("my-branch")
        name_no = self._make_name(current, int(now), "no_schema")
        name_ws = self._make_name(current, int(now), "with_schema")
        assert _should_delete(name_no, current, now) is True
        assert _should_delete(name_ws, current, now) is True
