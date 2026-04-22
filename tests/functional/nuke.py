from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("nuke")

_ITEM_TYPES = [
    "lakehouses",
    "environments",
]


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
    nuke_workspace(client)


def nuke_workspace(client: Any) -> None:
    """Delete ALL lakehouses and environments in the workspace.

    This is a brute-force cleanup — it deletes everything it can find.
    Only use on dedicated test workspaces.
    """
    for item_type in _ITEM_TYPES:
        try:
            resp = client._request(
                "GET",
                item_type,
                expected_status=(200,),
            )
            items = resp.json().get("value", [])
            logger.info("Found %d %s to delete", len(items), item_type)

            for item in items:
                item_id = item["id"]
                item_name = item.get("displayName", "<unknown>")
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
