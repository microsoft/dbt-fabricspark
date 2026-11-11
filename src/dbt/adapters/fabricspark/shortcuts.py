import json
import time
from typing import Any

import requests

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabricspark.shortcut import Shortcut, TargetName

logger = AdapterLogger("Microsoft Fabric-Spark")
DEFAULT_POLL_WAIT = 30
DETERMINISTIC_FAILURE_STATUSES = {400, 403, 404}


class ShortcutClient:
    def __init__(
        self,
        token: str,
        workspace_id: str,
        item_id: str,
        endpoint: str = "https://api.fabric.microsoft.com/v1",
        retry_wait_seconds: float = 1.0,
    ):
        """Initializes a ShortcutClient object."""
        self.token = token
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.endpoint = endpoint.rstrip("/")
        self.retry_wait_seconds = retry_wait_seconds

    def parse_json(self, json_str: str) -> list[Shortcut]:
        """Parses a JSON string into Shortcut objects."""
        shortcuts = []
        try:
            parsed_json = json.loads(json_str)
            for shortcut in parsed_json["shortcuts"]:
                shortcut["target"] = TargetName(shortcut["target"])
                try:
                    shortcuts.append(Shortcut(**shortcut))
                except Exception as exc:
                    raise ValueError(f"Could not parse shortcut: {shortcut} with error: {exc}")
            return shortcuts
        except Exception as exc:
            raise ValueError(f"Could not parse JSON: {json_str} with error: {exc}")

    def create_shortcuts(self, shortcuts_json_str: str, max_retries: int = 3) -> None:
        """Creates configured shortcuts, retrying transient failures per shortcut."""
        if shortcuts_json_str is not None or shortcuts_json_str == "":
            json_str = shortcuts_json_str
        else:
            with open("shortcuts.json", "r") as f:
                json_str = f.read()
            logger.debug("Read from shortcuts.json file")

        shortcuts = self.parse_json(json_str)
        if max_retries < 1:
            raise ValueError("max_retries must be greater than zero")

        for shortcut in shortcuts:
            logger.debug(f"Creating a shortcut: {shortcut}")
            for attempt in range(1, max_retries + 1):
                try:
                    self.create_shortcut(shortcut)
                    break
                except Exception as exc:
                    response = getattr(exc, "response", None)
                    status = getattr(response, "status_code", None)
                    body = getattr(response, "text", "") if response is not None else ""
                    cause = f"HTTP {status}: {body or exc}" if status is not None else str(exc)
                    if status in DETERMINISTIC_FAILURE_STATUSES or attempt == max_retries:
                        attempt_label = "attempt" if attempt == 1 else "attempts"
                        raise ValueError(
                            f"Failed to create shortcut '{shortcut.shortcut_name}' after "
                            f"{attempt} {attempt_label}: {cause}"
                        ) from exc
                    logger.debug(
                        f"Failed to create shortcut {shortcut.shortcut_name} on attempt {attempt}: {cause}. Retrying..."
                    )

                    time.sleep(self.retry_wait_seconds)

    def check_if_exists_and_delete_shortcut(self, shortcut: Shortcut) -> bool:
        """Return whether the existing shortcut already has the configured target."""
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        shortcut_url = (
            f"{self.endpoint}/workspaces/{self.workspace_id}/items/{self.item_id}"
            f"/shortcuts/{shortcut.path}/{shortcut.shortcut_name}"
        )
        response = requests.get(shortcut_url, headers=headers)
        if response.status_code == 404:
            return False
        response.raise_for_status()

        response_json = response.json()
        response_target = response_json.get("target", {})
        target_body = shortcut.get_target_body()
        if not self._targets_match(response_target, target_body):
            logger.debug(
                f"Shortcut {shortcut} already exists with a different target; deleting and recreating it."
            )

            self.delete_shortcut(response_json["path"], response_json["name"])
            return False
        return True

    @staticmethod
    def _targets_match(response_target: dict[str, Any], requested_target: dict[str, Any]) -> bool:
        """Compare only the supported OneLake target fields, independent of casing."""
        if not isinstance(response_target, dict) or not isinstance(requested_target, dict):
            return False
        if (
            str(response_target.get("type", "")).casefold()
            != str(requested_target.get("type", "")).casefold()
        ):
            return False

        response_onelake = next(
            (value for key, value in response_target.items() if key.casefold() == "onelake"),
            None,
        )
        requested_onelake = next(
            (value for key, value in requested_target.items() if key.casefold() == "onelake"),
            None,
        )
        if not isinstance(response_onelake, dict) or not isinstance(requested_onelake, dict):
            return False

        def normalize_path(value: Any) -> str:
            return str(value or "").strip("/")

        for field in ("workspaceId", "itemId"):
            if (
                str(response_onelake.get(field, "")).casefold()
                != str(requested_onelake.get(field, "")).casefold()
            ):
                return False
        return normalize_path(response_onelake.get("path")) == normalize_path(
            requested_onelake.get("path")
        )

    def delete_shortcut(self, shortcut_path: str, shortcut_name: str, wait: bool = True) -> None:
        """Deletes a shortcut, optionally waiting before a replacement is created."""
        shortcut_url = (
            f"{self.endpoint}/workspaces/{self.workspace_id}/items/{self.item_id}"
            f"/shortcuts/{shortcut_path}/{shortcut_name}"
        )
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        logger.debug(
            f"Deleting shortcut {shortcut_name} at {shortcut_path} from workspace {self.workspace_id} and item {self.item_id}"
        )

        response = requests.delete(shortcut_url, headers=headers)
        response.raise_for_status()
        if wait:
            time.sleep(DEFAULT_POLL_WAIT)

    def prune_shortcuts(self, shortcuts_json_str: str) -> None:
        """Delete unconfigured shortcuts only from parent paths in the configuration."""
        shortcuts = self.parse_json(shortcuts_json_str)
        desired_by_parent: dict[str, set[str]] = {}
        for shortcut in shortcuts:
            desired_by_parent.setdefault(shortcut.path or "", set()).add(
                shortcut.shortcut_name or ""
            )

        if not desired_by_parent:
            return

        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        shortcuts_url = (
            f"{self.endpoint}/workspaces/{self.workspace_id}/items/{self.item_id}/shortcuts"
        )
        for parent_path, configured_names in desired_by_parent.items():
            continuation_token = None
            while True:
                params = {"parentPath": parent_path}
                if continuation_token:
                    params["continuationToken"] = continuation_token
                response = requests.get(shortcuts_url, headers=headers, params=params)
                response.raise_for_status()
                page = response.json()
                for existing in page.get("value", []):
                    existing_path = str(existing.get("path") or "").strip("/")
                    if existing_path != parent_path.strip("/"):
                        continue
                    name = existing.get("name")
                    if not name or name in configured_names:
                        continue
                    self.delete_shortcut(parent_path, name, wait=False)
                    logger.info(f"Pruned shortcut {name} at {parent_path}")
                continuation_token = page.get("continuationToken")
                if not continuation_token:
                    break

    def create_shortcut(self, shortcut: Shortcut) -> None:
        """Creates a shortcut unless it already exists with the configured target."""
        if self.check_if_exists_and_delete_shortcut(shortcut):
            logger.debug(f"Shortcut {shortcut} already exists, skipping...")
            return
        shortcuts_url = (
            f"{self.endpoint}/workspaces/{self.workspace_id}/items/{self.item_id}/shortcuts"
        )
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        body = {
            "path": shortcut.path,
            "name": shortcut.shortcut_name,
            "target": shortcut.get_target_body(),
        }
        response = requests.post(shortcuts_url, headers=headers, data=json.dumps(body))
        response.raise_for_status()


def create_and_prune_shortcuts(
    shortcut_client: ShortcutClient,
    shortcuts_json_str: str | None,
    prune: bool = False,
) -> None:
    """Create configured shortcuts before optionally pruning their configured parents."""
    if shortcuts_json_str is None:
        with open("shortcuts.json", "r") as f:
            shortcuts_json_str = f.read()
    shortcut_client.create_shortcuts(shortcuts_json_str)
    if prune:
        shortcut_client.prune_shortcuts(shortcuts_json_str)
