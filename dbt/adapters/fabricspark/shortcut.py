from dataclasses import dataclass
from typing import Optional
from enum import Enum

class TargetName(Enum):
    onelake = "onelake"

@dataclass
class Shortcut:
    """
    A shortcut that can be created for different target systems (onelake).

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
    endpoint: str = None
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