import requests
import json
from dataclasses import dataclass
from typing import Optional
from enum import Enum
from dbt.events import AdapterLogger

logger = AdapterLogger("Microsoft Fabric-Spark")

class TargetName(Enum):
    onelake = "onelake"

@dataclass
class Shortcut:
    """
    A shortcut that can be created in different target systems.

    Attributes:
        path (str): The path where the shortcut will be created.
        name (str): The name of the shortcut.
        target (TargetName): The target system where the shortcut will be created -- only 'onelake' is supported for now.
        source_path (Optional[str]): The source path for the shortcut ('onelake' target).
        source_workspace_id (Optional[str]): The source workspace ID for the shortcut ('onelake' target).
        source_item_id (Optional[str]): The source item ID for the shortcut ('onelake' target).
        location (Optional[str]): The location for the shortcut ('amazonS3' and 'adlsGen2' targets).
        subpath (Optional[str]): The subpath for the shortcut ('amazonS3' and 'adlsGen2' targets).
        connection_id (Optional[str]): The connection ID for the shortcut ('amazonS3', 'adlsGen2', and 'dataverse' targets).
        delta_lake_folder (Optional[str]): The delta lake folder for the shortcut ('dataverse' target).
        environment_domain (Optional[str]): The environment domain for the shortcut ('dataverse' target).
        table_name (Optional[str]): The table name for the shortcut ('dataverse' target).
    """
    # the path where the shortcut will be created
    path: str = None
    shortcut_name: str = None
    target: TargetName = None
    # onelake specific
    source_path: Optional[str] = None
    source_workspace_id: Optional[str] = None
    source_item_id: Optional[str] = None

    def __post_init__(self):
        if self.path is None:
            raise ValueError("destination_path is required")
        if self.shortcut_name is None:
            raise ValueError("name is required")
        if self.target not in TargetName:
            raise ValueError("target must be one of 'onelake', 'amazonS3', 'adlsGen2', or 'dataverse'")
        
        if self.target == TargetName.onelake:
            if self.source_path is None:
                raise ValueError(f"source_path is required for {self.target}")
            if self.source_workspace_id is None:
                raise ValueError(f"source_workspace_id is required for {self.target}")
            if self.source_item_id is None:
                raise ValueError(f"source_item_id is required for {self.target}")
    
    def __str__(self):
        """
        Returns a string representation of the Shortcut object.
        """
        return f"Shortcut: {self.shortcut_name} from {self.source_path} to {self.path}"
    
    def connect_url(self):
        """
        Returns the connect URL for the shortcut.
        """
        return f"https://api.fabric.microsoft.com/v1/workspaces/{self.source_workspace_id}/items/{self.source_item_id}/shortcuts"
    
    def get_target_body(self):
        """
        Returns the target body for the shortcut based on the target attribute.
        """
        if self.target == TargetName.onelake:
            return {
                self.target.value: {
                    "workspaceId": self.source_workspace_id,
                    "itemId": self.source_item_id,
                    "path": self.source_path
                }
            }


class ShortcutClient:
    def __init__(self, token: str, workspace_id: str, item_id: str):
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

    def parse_json(self, json_str: str):
        """
        Parses a JSON string into a list of Shortcut objects.

        Args:
            json_str (str): The JSON string to parse.
        """
        shortcuts = []
        for shortcut in json.loads(json_str):
            # convert string target to TargetName enum
            shortcut["target"] = TargetName(shortcut["target"])
            try:
                shortcut_obj = Shortcut(**shortcut)
            except Exception as e:
                raise ValueError(f"Could not parse shortcut: {shortcut} with error: {e}")
            shortcuts.append(shortcut_obj)
        return shortcuts
    
    def create_shortcuts(self, json_path: str, max_retries: int = 3):
        """
        Creates shortcuts from a JSON file.

        Args:
            json_path (str): The path to the JSON file containing the shortcuts.
            retry (bool): Whether to retry creating shortcuts if there is an error (default: True).
        """
        json_str = None
        with open(json_path, "r") as f:
            json_str = f.read()
        if json_str is None:
            raise ValueError(f"Could not read JSON file at {json_path}")
        for shortcut in self.parse_json(json_str):
            logger.debug(f"Creating shortcut: {shortcut}")
            while max_retries > 0:
                try:
                    self.create_shortcut(shortcut)
                    break
                except Exception as e:
                    logger.debug(f"Failed to create shortcut: {shortcut} with error: {e}. Retrying...")
                    max_retries -= 1
            if max_retries == 0:
                raise f"Failed to create shortcut: {shortcut} after {max_retries} retries, failing..."
            
    def check_exists(self, shortcut: Shortcut):
        """
        Checks if a shortcut exists.

        Args:
            shortcut (Shortcut): The shortcut to check.
        """
        connect_url = f"https://api.fabric.microsoft.com/v1/workspaces/{self.workspace_id}/items/{self.item_id}/shortcuts/{shortcut.path}/{shortcut.shortcut_name}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        response = requests.get(connect_url, headers=headers)
        # check if the error is ItemNotFound
        if response.status_code == 404:
            return False
        response.raise_for_status() # raise an exception if there are any other errors
        # else, check that the target body of the existing shortcut matches the target body of the shortcut they want to create
        response_json = response.json()
        response_target = response_json["target"]
        target_body = shortcut.get_target_body()
        if response_target != target_body:
            # if the response target does not match the target body, delete the existing shortcut, then return False so we can create the new shortcut
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
        connect_url = f"https://api.fabric.microsoft.com/v1/workspaces/{self.workspace_id}/items/{self.item_id}/shortcuts/{shortcut_path}/{shortcut_name}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
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
        connect_url = f"https://api.fabric.microsoft.com/v1/workspaces/{self.workspace_id}/items/{self.item_id}/shortcuts"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        target_body = shortcut.get_target_body()
        body = {
            "path": shortcut.path,
            "name": shortcut.shortcut_name,
            "target": target_body
        }
        response = requests.post(connect_url, headers=headers, data=json.dumps(body))
        response.raise_for_status()