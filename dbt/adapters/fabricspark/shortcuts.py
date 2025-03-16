import time
import requests
import json
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabricspark.shortcut import Shortcut, TargetName

logger = AdapterLogger("Microsoft Fabric-Spark")
DEFAULT_POLL_WAIT = 30


class ShortcutClient:
    def __init__(
        self,
        token: str,
        workspace_id: str,
        item_id: str,
        endpoint: str = "https://api.fabric.microsoft.com/v1",
    ):
        """
        Initializes a ShortcutClient object.

        Args:
            token (str): The API token to use for creating shortcuts.
            workspace_id (str): The workspace ID to use for creating shortcuts.
            item_id (str): The item ID to use for creating shortcuts.
        """
        self.token = token
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.endpoint = endpoint

    def parse_json(self, json_str: str) -> list:
        """
        Parses a JSON string into a list of Shortcut objects.

        Args:
            json_str (str): The JSON string to parse.
        """
        shortcuts = []
        try:
            parsed_json = json.loads(json_str)
            for shortcut in parsed_json["shortcuts"]:
                # convert string target to TargetName enum
                shortcut["target"] = TargetName(shortcut["target"])
                try:
                    shortcut_obj = Shortcut(**shortcut)
                except Exception as e:
                    raise ValueError(f"Could not parse shortcut: {shortcut} with error: {e}")
                shortcuts.append(shortcut_obj)
            return shortcuts
        except Exception as e:
            raise ValueError(f"Could not parse JSON: {json_str} with error: {e}")

    def create_shortcuts(self, shortcuts_json_str: str, max_retries: int = 3) -> None:
        """
        Creates shortcuts from a profile.yaml configuration.

        Args:
            json_path (str): The path to the JSON file containing the shortcuts.
            retry (bool): Whether to retry creating shortcuts if there is an error (default: True).
        """

        json_str = None
        if shortcuts_json_str is not None or shortcuts_json_str == "":
            json_str = shortcuts_json_str
        else:
            with open("shortcuts.json", "r") as f:
                json_str = f.read()
            logger.debug("Read from shortcuts.json file")
        shortcuts = self.parse_json(json_str)

        for shortcut in shortcuts:
            logger.debug(f"Creating a shortcut: {shortcut}")
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
                raise ValueError(
                    f"Failed to create shortcut: {shortcut} after {max_retries} retries, failing..."
                )

    def check_if_exists_and_delete_shortcut(self, shortcut: Shortcut) -> bool:
        """
        Checks if a shortcut exists.

        Args:
            shortcut (Shortcut): The shortcut to check.
        """
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        shortcut_url = f"{self.endpoint}/workspaces/{self.workspace_id}/items/{self.item_id}/shortcuts/{shortcut.path}/{shortcut.shortcut_name}"
        response = requests.get(shortcut_url, headers=headers)
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

    def delete_shortcut(self, shortcut_path: str, shortcut_name: str) -> None:
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
        time.sleep(DEFAULT_POLL_WAIT)
        response.raise_for_status()

    def create_shortcut(self, shortcut: Shortcut) -> None:
        """
        Creates a shortcut.

        Args:
            shortcut (Shortcut): The shortcut to create.
        """
        if self.check_if_exists_and_delete_shortcut(shortcut):
            logger.debug(f"Shortcut {shortcut} already exists, skipping...")
            return
        connect_url = (
            f"{self.endpoint}/workspaces/{self.workspace_id}/items/{self.item_id}/shortcuts"
        )
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        target_body = shortcut.get_target_body()
        body = {"path": shortcut.path, "name": shortcut.shortcut_name, "target": target_body}
        response = requests.post(connect_url, headers=headers, data=json.dumps(body))
        response.raise_for_status()
