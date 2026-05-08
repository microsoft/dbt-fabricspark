from __future__ import annotations

import hashlib
import logging
import os
import re
import subprocess
import time
from typing import Any

logger = logging.getLogger("nuke")

_ITEM_TYPES = [
    "lakehouses",
    "environments",
]

# Naming convention: dbt_{8hex}_r{run_id}_{ts}_{rest}
# The 'r' prefix on the run segment makes the format unambiguous.
_NAME_PATTERN = re.compile(r"^dbt_([a-f0-9]{8})_r(\d+)_(\d+)_")

# Items older than this (in seconds) are considered stale and always deleted.
STALE_THRESHOLD_SECONDS = 24 * 60 * 60  # 24 hours


def branch_hash(branch: str) -> str:
    """Return an 8-character deterministic hash for a branch name."""
    return hashlib.sha256(branch.encode()).hexdigest()[:8]


def current_run_id() -> str:
    """Return the GitHub Actions run ID for this CI invocation, or '0' locally."""
    return os.environ.get("GITHUB_RUN_ID", "0")


def _should_delete(item_name: str, current_hash: str, now: float, run_id: str = "") -> bool:
    """Decide whether a workspace item should be deleted.

    Items matching ``dbt_{hash}_r{run_id}_{ts}_…`` are deleted when:
    - the hash AND run_id both match the current run (same-branch, same-run
      cleanup), **or**
    - the timestamp is older than ``STALE_THRESHOLD_SECONDS``.

    This ensures that two concurrent runs for the same branch never delete
    each other's lakehouses: each run's nuke only removes its own items.

    Non-matching names are never deleted.
    """
    m = _NAME_PATTERN.match(item_name)
    if not m:
        return False

    item_hash = m.group(1)
    item_run_id = m.group(2)
    item_ts = int(m.group(3))

    if item_hash == current_hash and run_id and item_run_id == run_id:
        return True

    return (now - item_ts) > STALE_THRESHOLD_SECONDS


def _git_branch() -> str | None:
    """Best-effort ``git rev-parse --abbrev-ref HEAD``, or *None* on failure."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
        )
        branch = result.stdout.strip()
        if result.returncode == 0 and branch and branch != "HEAD":
            return branch
    except Exception:
        pass
    return None


def current_branch_hash() -> str:
    """Return the branch hash for the current CI run.

    Reads ``GITHUB_HEAD_REF`` (set on PR builds) or ``GITHUB_REF_NAME``
    (set on push builds).  On local machines where neither variable is set,
    falls back to ``git rev-parse --abbrev-ref HEAD`` so that each
    developer's branch gets its own hash.  If that also fails, falls back
    to ``"unknown"``.
    """
    branch = (
        os.environ.get("GITHUB_HEAD_REF")
        or os.environ.get("GITHUB_REF_NAME")
        or _git_branch()
        or "unknown"
    )
    return branch_hash(branch)


def nuke_workspace_task(shared_state: dict[str, Any]) -> None:
    """Scheduler-compatible entry point — creates a FabricClient from env and nukes."""
    from tests.functional.fabric_client import (
        AzureCliTokenProvider,
        FabricClient,
        StaticTokenProvider,
    )

    workspace_id = os.environ.get("WORKSPACE_ID_1")
    if not workspace_id:
        raise RuntimeError(
            "WORKSPACE_ID_1 must be set in test.env or the environment for nuke_workspace_task."
        )
    api_endpoint = os.environ.get("LIVY_ENDPOINT", "https://api.fabric.microsoft.com/v1")

    token = os.environ.get("FABRIC_INTEGRATION_TESTS_TOKEN")
    provider = StaticTokenProvider(token) if token else AzureCliTokenProvider()

    client = FabricClient(
        workspace_id=workspace_id,
        api_endpoint=api_endpoint,
        token_provider=provider,
    )

    nuke_workspace(client, current_branch_hash(), current_run_id())


def nuke_workspace(client: Any, current_hash: str, run_id: str = "") -> None:
    """Delete lakehouses and environments that belong to this branch/run or are stale.

    Items using the naming convention (``dbt_{hash}_r{run_id}_{ts}_…``) are
    deleted only when *both* the hash and the run_id match (same run) or the
    timestamp is older than 24 hours.  This prevents one concurrent CI run
    from deleting the lakehouses of a sibling run sharing the same branch hash.

    Non-matching names are never deleted.
    """
    now = time.time()

    for item_type in _ITEM_TYPES:
        try:
            resp = client._request(
                "GET",
                item_type,
                expected_status=(200,),
            )
            items = resp.json().get("value", [])
            logger.info("Found %d %s in workspace", len(items), item_type)

            for item in items:
                item_id = item["id"]
                item_name = item.get("displayName", "<unknown>")

                if not _should_delete(item_name, current_hash, now, run_id):
                    logger.info(
                        "Skipping %s: %s (%s) — not matching branch/run or stale threshold",
                        item_type,
                        item_name,
                        item_id,
                    )
                    continue

                logger.info("Deleting %s: %s (%s)", item_type, item_name, item_id)
                try:
                    client._request(
                        "DELETE",
                        f"{item_type}/{item_id}",
                        expected_status=(200, 204),
                    )
                except Exception:
                    logger.warning(
                        "Failed to delete %s %s (%s)",
                        item_type,
                        item_name,
                        item_id,
                        exc_info=True,
                    )
        except Exception:
            logger.warning("Failed to list %s", item_type, exc_info=True)

    logger.info("Workspace nuke complete")
