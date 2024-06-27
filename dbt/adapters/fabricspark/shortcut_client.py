import requests
import json
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabricspark.shortcut import Shortcut, TargetName

logger = AdapterLogger("Microsoft Fabric-Spark")


class ShortcutClient:
    def __init__(
        self, token: str, workspace_id: str, item_id: str, endpoint: str, shortcuts: list
    ):
        """
        Initializes a ShortcutClient object.
        Args:
            token (str): The API token to use for creating shortcuts.
            workspace_id (str): The workspace ID to use for creating shortcuts.
            item_id (str): The item ID to use for creating shortcuts.
            endpoint (str): Base URL of fabric api
        """
        self.token = token
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.endpoint = endpoint
        self.shortcuts = shortcuts

    def connect_url(self, shortcut: Shortcut):
        """
        Returns the connect URL for the shortcut.
        """
        return f"{self.endpoint}/workspaces/{shortcut.source_workspace_id}/items/{shortcut.source_item_id}/shortcuts/{shortcut.source_path}/{shortcut.shortcut_name}"

    def get_target_body(shortcut: Shortcut):
        """
        Returns the target body for the shortcut based on the target attribute.
        """
        if shortcut.target == TargetName.onelake:
            return {
                shortcut.target.value: {
                    "workspaceId": shortcut.source_workspace_id,
                    "itemId": shortcut.source_item_id,
                    "path": shortcut.source_path,
                }
            }

    def create_shortcuts(self, max_retries: int = 3):
        """
        Creates shortcuts from a JSON file.
        Args:
            retry (bool): Whether to retry creating shortcuts if there is an error (default: True).
        """
        for shortcut in self.shortcuts:
            logger.debug(f"Creating shortcut: {shortcut}")
            while max_retries > 0:
                try:
                    self.create_shortcut(shortcut)
                    break
                except Exception as e:
                    logger.debug(
                        f"Failed to create shortcut: {shortcut} with error: {e}. Retrying..."
                    )
                    max_retries -= 1
            if max_retries == 0:
                raise f"Failed to create shortcut: {shortcut} after {max_retries} retries, failing..."

    def check_exists(self, shortcut: Shortcut):
        """
        Checks if a shortcut exists.
        Args:
            shortcut (Shortcut): The shortcut to check.
        """
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        response = requests.get(shortcut.connect_url(), headers=headers)
        # check if the error is ItemNotFound
        if response.status_code == 404:
            return False
        response.raise_for_status()  # raise an exception if there are any other errors
        # else, check that the target body of the existing shortcut matches the target body of the shortcut they want to create
        response_json = response.json()
        response_target = response_json["target"]
        target_body = shortcut.get_target_body()
        if response_target != target_body:
            # if the response target does not match the target body, delete the existing shortcut, then return False so we can create the new shortcut
            logger.debug(
                f"Shortcut {shortcut} already exists with different source path, workspace ID, and/or item ID. Deleting exisiting shortcut and recreating based on JSON."
            )
            self.delete_shortcut(response_json["path"], response_json["name"])
            return False
        return True

    def delete_shortcut(self, shortcut_path: str, shortcut_name: str):
        """
        Deletes a shortcut.
        Args:
            shortcut_path (str): The path where the shortcut is located.
            shortcut_name (str): The name of the shortcut.
        """
        connect_url = f"{self.endpoint}/workspaces/{self.workspace_id}/items/{self.item_id}/shortcuts/{shortcut_path}/{shortcut_name}"
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        logger.debug(
            f"Deleting shortcut {shortcut_name} at {shortcut_path} from workspace {self.workspace_id} and item {self.item_id}"
        )
        response = requests.delete(connect_url, headers=headers)
        response.raise_for_status()

    def create_shortcut(self, shortcut: Shortcut):
        """
        Creates a shortcut.
        Args:
            shortcut (Shortcut): The shortcut to create.
        """
        if self.check_exists(shortcut):
            logger.debug(f"Shortcut {shortcut} already exists, skipping...")
            return
        connect_url = (
            f"{self.endpoint}/workspaces/{self.workspace_id}/items/{self.item_id}/shortcuts"
        )
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        target_body = self.get_target_body(shortcut)
        body = {"path": shortcut.path, "name": shortcut.shortcut_name, "target": target_body}
        response = requests.post(connect_url, headers=headers, data=json.dumps(body))
        response.raise_for_status()
