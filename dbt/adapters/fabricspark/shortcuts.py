import requests
import json
from dataclasses import dataclass
from typing import Optional
from enum import Enum
from dbt.events import AdapterLogger

logger = AdapterLogger("Microsoft Fabric-Spark")

class TargetName(Enum):
    onelake = "onelake"
    amazonS3 = "amazonS3"
    adlsGen2 = "adlsGen2"
    dataverse = "dataverse"

@dataclass
class Shortcut:
    """
    A shortcut that can be created in different target systems.

    Attributes:
        path (str): The path where the shortcut will be created.
        name (str): The name of the shortcut.
        target (TargetName): The target system where the shortcut will be created -- one of 'onelake', 'amazonS3', 'adlsGen2', or 'dataverse'.
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
    # amazonS3/adlsGen2 specific
    location: Optional[str] = None
    subpath: Optional[str] = None
    connection_id: Optional[str] = None # also used for dataverse
    # dataverse specific
    delta_lake_folder: Optional[str] = None
    environment_domain: Optional[str] = None
    table_name: Optional[str] = None

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
        elif self.target == TargetName.amazonS3 or self.target == TargetName.adlsGen2:
            if self.location is None:
                raise ValueError(f"location is required for {self.target}")
            if self.subpath is None:
                raise ValueError(f"subpath is required for {self.target}")
            if self.connection_id is None:
                raise ValueError(f"connection_id is required for {self.target}")
        elif self.target == TargetName.dataverse:
            if self.connection_id is None:
                raise ValueError(f"connection_id is required for {self.target}")
            if self.delta_lake_folder is None:
                raise ValueError(f"delta_lake_folder is required for {self.target}")
            if self.environment_domain is None:
                raise ValueError("environment_domain is required for {self.target}")
            if self.table_name is None:
                raise ValueError(f"table_name is required for {self.target}")
    
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
        elif self.target == TargetName.amazonS3 or self.target == TargetName.adlsGen2:
            return {
                self.target.value: {
                    "location": self.location,
                    "subpath": self.subpath,
                    "connectionId": self.connection_id
                }
            }
        elif self.target == TargetName.dataverse:
            return {
                self.target.value: {
                    "connectionId": self.connection_id,
                    "deltaLakeFolder": self.delta_lake_folder,
                    "environmentDomain": self.environment_domain,
                    "tableName": self.table_name
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
                print(f"Could not create shortcut object: {e}, skipping...")
            shortcuts.append(shortcut_obj)
        return shortcuts
    
    def create_shortcuts(self, json_path: str, retry: bool = True):
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
            try:
                self.create_shortcut(shortcut)
            except Exception as e:
                if retry:
                    logger.debug(f"Failed to create shortcut: {shortcut} with error: {e}. Retrying...")
                    try:
                        self.create_shortcut(shortcut)
                    except Exception as e:
                        raise "Could not create shortcut {shortcut} after retrying, ending run."
                else:
                    logger.debug(f"Failed to create shortcut: {shortcut}, skipping...")
            
    def create_shortcut(self, shortcut: Shortcut):
        """
        Creates a shortcut.

        Args:
            shortcut (Shortcut): The shortcut to create.
        """
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