import requests
import json
from dataclasses import dataclass
from typing import Optional
from enum import Enum

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
        target_name (TargetName): The target system where the shortcut will be created -- one of 'onelake', 'amazonS3', 'adlsGen2', or 'dataverse'.
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
    target_name: TargetName = None
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
        if self.target_name not in TargetName:
            raise ValueError("target_name must be one of 'onelake', 'amazonS3', 'adlsGen2', or 'dataverse'")
        
        if self.target_name == TargetName.onelake:
            if self.source_path is None:
                raise ValueError(f"source_path is required for {self.target_name}")
            if self.source_workspace_id is None:
                raise ValueError(f"source_workspace_id is required for {self.target_name}")
            if self.source_item_id is None:
                raise ValueError(f"source_item_id is required for {self.target_name}")
        elif self.target_name == TargetName.amazonS3 or self.target_name == TargetName.adlsGen2:
            if self.location is None:
                raise ValueError(f"location is required for {self.target_name}")
            if self.subpath is None:
                raise ValueError(f"subpath is required for {self.target_name}")
            if self.connection_id is None:
                raise ValueError(f"connection_id is required for {self.target_name}")
        elif self.target_name == TargetName.dataverse:
            if self.connection_id is None:
                raise ValueError(f"connection_id is required for {self.target_name}")
            if self.delta_lake_folder is None:
                raise ValueError(f"delta_lake_folder is required for {self.target_name}")
            if self.environment_domain is None:
                raise ValueError("environment_domain is required for {self.target_name}")
            if self.table_name is None:
                raise ValueError(f"table_name is required for {self.target_name}")
    
    def __str__(self):
        """
        Returns a string representation of the Shortcut object.
        """
        return f"Shortcut: {self.shortcut_name} from {self.source_path} to {self.path}"
    
    def connect_url(self):
        """
        Returns the connect URL for the shortcut.
        """
        return f"api.fabric.microsoft.com/v1/workspaces/{self.source_workspace_id}/items/{self.source_item_id}/shortcuts"
    
    def get_target_body(self):
        """
        Returns the target body for the shortcut based on the target_name attribute.
        """
        if self.target_name == TargetName.onelake:
            return {
                self.target_name.value: {
                    "workspaceId": self.source_workspace_id,
                    "itemId": self.source_item_id,
                    "path": self.source_path
                }
            }
        elif self.target_name == TargetName.amazonS3 or self.target_name == TargetName.adlsGen2:
            return {
                self.target_name.value: {
                    "location": self.location,
                    "subpath": self.subpath,
                    "connectionId": self.connection_id
                }
            }
        elif self.target_name == TargetName.dataverse:
            return {
                self.target_name.value: {
                    "connectionId": self.connection_id,
                    "deltaLakeFolder": self.delta_lake_folder,
                    "environmentDomain": self.environment_domain,
                    "tableName": self.table_name
                }
            }


class ShortcutClient:
    def __init__(self, token: str):
        self.token = token

    def parse_json_str(self, json_str: str):
        for shortcut in json.loads(json_str):
            # convert string target_name to TargetName enum
            shortcut["target_name"] = TargetName(shortcut["target_name"])
            yield Shortcut(**shortcut)
    
    def create_shortcuts(self, json_path, retry: bool = True):
        json_str = None
        with open(json_path, "r") as f:
            json_str = f.read()
        if json_str is None:
            raise ValueError(f"Could not read JSON file at {json_path}")
        for shortcut in self.parse_json_str(json_str):
            try:
                self.create_shortcut(shortcut)
            except Exception as e:
                if retry:
                    print(f"Failed to create shortcut: {shortcut}. Retrying...")
                    try:
                        self.create_shortcut(shortcut)
                    except Exception as e:
                        raise "Could not create shortcut {shortcut} after retrying, ending run."
                else:
                    print(f"Failed to create shortcut: {shortcut}. Skipping...")
            
    def create_shortcut(self, shortcut: Shortcut):
        shortcut.__post_init__()
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        body = {
            "path": shortcut.destination_path,
            "name": shortcut.name,
            "target": {
                shortcut.get_target_body()
            }
        }
        response = requests.post(shortcut.connect_url, headers=headers, data=json.dumps(body))
        response.raise_for_status()