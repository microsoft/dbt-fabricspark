from __future__ import annotations

import hashlib
import logging
import re
import subprocess
import time
from typing import Any

logger = logging.getLogger("nuke")

_ITEM_TYPES = [
    "lakehouses",
    "environments",
]

# Matches the new naming pattern: dbt_{8-char-hex-hash}_{timestamp}_{rest}
_NAME_PATTERN = re.compile(r"^dbt_([a-f0-9]{8})_(\d+)_")

# Items older than this (in seconds) are considered stale and always deleted.
STALE_THRESHOLD_SECONDS = 24 * 60 * 60  # 24 hours


def branch_hash(branch: str) -> str:
    """Return an 8-character deterministic hash for a branch name."""
    return hashlib.sha256(branch.encode()).hexdigest()[:8]


def _should_delete(item_name: str, current_hash: str, now: float) -> bool:
    """Decide whether a workspace item should be deleted.

    Returns True when the name matches ``dbt_{hash}_{ts}_…`` and either the
    hash equals *current_hash* (same-branch cleanup) or the timestamp is older
    than ``STALE_THRESHOLD_SECONDS`` (stale-infra garbage collection).

    Non-matching names are never deleted.
    """
    m = _NAME_PATTERN.match(item_name)
    if not m:
        return False

    item_hash = m.group(1)
    item_ts = int(m.group(2))

    if item_hash == current_hash:
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
    import os

    branch = (
        os.environ.get("GITHUB_HEAD_REF")
        or os.environ.get("GITHUB_REF_NAME")
        or _git_branch()
        or "unknown"
    )
    return branch_hash(branch)


def nuke_workspace_task(shared_state: dict[str, Any]) -> None:
    """Scheduler-compatible entry point — creates a FabricClient from env and nukes."""
    import os

    from tests.functional.fabric_client import (
        AzureCliTokenProvider,
        FabricClient,
        StaticTokenProvider,
    )

    workspace_id = os.environ.get("WORKSPACE_ID")
    api_endpoint = os.environ.get("LIVY_ENDPOINT", "https://api.fabric.microsoft.com/v1")

    if not workspace_id:
        logger.warning("WORKSPACE_ID not set, skipping nuke")
        return

    token = os.environ.get("FABRIC_INTEGRATION_TESTS_TOKEN")
    provider = StaticTokenProvider(token) if token else AzureCliTokenProvider()

    client = FabricClient(
        workspace_id=workspace_id,
        api_endpoint=api_endpoint,
        token_provider=provider,
    )

    nuke_workspace(client, current_branch_hash())


def nuke_workspace(client: Any, current_hash: str) -> None:
    """Delete lakehouses and environments that belong to this branch or are stale.

    Items whose ``displayName`` matches ``dbt_{hash}_{ts}_…`` are deleted when
    the hash equals *current_hash* (same branch) or the timestamp is older than
    24 hours (stale). Non-matching names are never deleted.
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

                if not _should_delete(item_name, current_hash, now):
                    logger.info(
                        "Skipping %s: %s (%s) — not matching branch or stale threshold",
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
