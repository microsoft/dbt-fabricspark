"""Unit tests for the branch-aware nuke logic."""

from __future__ import annotations

import os
import time
from unittest.mock import patch

from tests.functional.nuke import (
    STALE_THRESHOLD_SECONDS,
    _git_branch,
    _should_delete,
    branch_hash,
    current_branch_hash,
    current_run_id,
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

    def _make_name(self, bhash: str, run_id: str, ts: int, mode: str = "NoSchema") -> str:
        """Build a lakehouse name: dbt_{hash}_r{run_id}_{ts}_{mode}."""
        return f"dbt_{bhash}_r{run_id}_{ts}_{mode}"

    def test_same_run_deleted(self) -> None:
        """Items from the same branch AND run are deleted."""
        now = time.time()
        current = branch_hash("my-branch")
        name = self._make_name(current, "12345", int(now) - 60)
        assert _should_delete(name, current, now, "12345") is True

    def test_same_branch_different_run_not_deleted(self) -> None:
        """Items from the same branch but a different run are NOT deleted.

        This is the key test: concurrent CI runs for the same branch must not
        delete each other's lakehouses.
        """
        now = time.time()
        current = branch_hash("my-branch")
        name = self._make_name(current, "99999", int(now) - 60)
        assert _should_delete(name, current, now, "12345") is False

    def test_different_branch_not_deleted(self) -> None:
        """Items from a different branch are not deleted (unless stale)."""
        now = time.time()
        current = branch_hash("my-branch")
        other = branch_hash("other-branch")
        name = self._make_name(other, "12345", int(now) - 60)
        assert _should_delete(name, current, now, "12345") is False

    def test_stale_always_deleted(self) -> None:
        """Items older than 24h are always deleted (garbage collection)."""
        now = time.time()
        current = branch_hash("my-branch")
        other = branch_hash("other-branch")
        old_ts = int(now) - STALE_THRESHOLD_SECONDS - 1
        name = self._make_name(other, "99999", old_ts)
        assert _should_delete(name, current, now, "12345") is True

    def test_same_run_no_run_id_arg_not_deleted(self) -> None:
        """Items are NOT deleted by hash match alone when run_id arg is empty."""
        now = time.time()
        current = branch_hash("my-branch")
        name = self._make_name(current, "12345", int(now) - 60)
        # Caller didn't pass run_id → safe default: don't delete unless stale.
        assert _should_delete(name, current, now) is False

    def test_non_matching_pattern_never_deleted(self) -> None:
        """Items that don't match the naming pattern are never deleted."""
        now = time.time()
        current = branch_hash("my-branch")
        assert _should_delete("manual_lakehouse", current, now) is False
        assert _should_delete("my_other_lh", current, now) is False

    def test_exactly_at_threshold_not_deleted(self) -> None:
        """An item exactly at the 24-hour boundary is NOT deleted (> not >=)."""
        now = float(int(time.time()))
        current = branch_hash("my-branch")
        other = branch_hash("other-branch")
        boundary_ts = int(now) - STALE_THRESHOLD_SECONDS
        name = self._make_name(other, "99999", boundary_ts)
        assert _should_delete(name, current, now, "12345") is False

    def test_mixed_case_suffixes(self) -> None:
        """Naming works with both mixed-case suffix variants."""
        now = time.time()
        current = branch_hash("my-branch")
        name_no = self._make_name(current, "12345", int(now), "NoSchema")
        name_ws = self._make_name(current, "12345", int(now), "WithSchema")
        assert _should_delete(name_no, current, now, "12345") is True
        assert _should_delete(name_ws, current, now, "12345") is True

    def test_legacy_v1_format_ignored(self) -> None:
        """Old V1 format names (no run_id) are not matched and never deleted."""
        now = time.time()
        current = branch_hash("my-branch")
        # V1 format: dbt_{hash}_{ts}_{mode} — no 'r' prefix segment
        name = f"dbt_{current}_{int(now) - 60}_no_schema"
        assert _should_delete(name, current, now, "12345") is False


class TestCurrentRunId:
    """Tests for current_run_id()."""

    def test_reads_github_run_id(self) -> None:
        """Returns GITHUB_RUN_ID when set."""
        with patch.dict("os.environ", {"GITHUB_RUN_ID": "98765"}, clear=False):
            assert current_run_id() == "98765"

    def test_defaults_to_zero_locally(self) -> None:
        """Returns '0' when GITHUB_RUN_ID is not set (local dev)."""
        with patch.dict("os.environ", {}, clear=False):
            os.environ.pop("GITHUB_RUN_ID", None)
            assert current_run_id() == "0"


class TestCurrentBranchHash:
    """Tests for current_branch_hash() and _git_branch()."""

    def test_prefers_github_head_ref(self) -> None:
        """GITHUB_HEAD_REF takes priority over GITHUB_REF_NAME and git."""
        env = {"GITHUB_HEAD_REF": "pr-branch", "GITHUB_REF_NAME": "main"}
        with patch.dict("os.environ", env, clear=False):
            assert current_branch_hash() == branch_hash("pr-branch")

    def test_falls_back_to_github_ref_name(self) -> None:
        """GITHUB_REF_NAME is used when GITHUB_HEAD_REF is empty."""
        env = {"GITHUB_HEAD_REF": "", "GITHUB_REF_NAME": "main"}
        with patch.dict("os.environ", env, clear=False):
            assert current_branch_hash() == branch_hash("main")

    def test_falls_back_to_git_branch(self) -> None:
        """git rev-parse is used when no GitHub env vars are set."""
        env = {"GITHUB_HEAD_REF": "", "GITHUB_REF_NAME": ""}
        with (
            patch.dict("os.environ", env, clear=False),
            patch("tests.functional.nuke._git_branch", return_value="local-dev"),
        ):
            assert current_branch_hash() == branch_hash("local-dev")

    def test_falls_back_to_unknown(self) -> None:
        """Falls back to 'unknown' when all detection methods fail."""
        env = {"GITHUB_HEAD_REF": "", "GITHUB_REF_NAME": ""}
        with (
            patch.dict("os.environ", env, clear=False),
            patch("tests.functional.nuke._git_branch", return_value=None),
        ):
            assert current_branch_hash() == branch_hash("unknown")

    def test_git_branch_returns_string(self) -> None:
        """_git_branch() returns a string or None without raising."""
        result = _git_branch()
        assert result is None or isinstance(result, str)
