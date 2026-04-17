"""
All custom exceptions.
"""

from ..common.constants import (
    VALID_MODES,
    VALID_STRATEGIES,
)


class InvalidStrategyError(Exception):
    """
    Raise when strategy defined is not valid
    """

    def __init__(self, strategy: str):
        self.message = f"Strategy '{strategy}' not found in valid strategies. Valid values: {VALID_STRATEGIES}"
        super().__init__(self.message)

    def __str__(self):
        return self.message


class InvalidModeError(Exception):
    """
    Raise when mode defined is not valid
    """

    def __init__(self, mode: str):
        self.message = f"Mode '{mode}' not found in valid modes. Valid values: {VALID_MODES}"
        super().__init__(self.message)

    def __str__(self):
        return self.message


class MissingTaskParameterError(Exception):
    """
    Raise when param is missing in a task
    """

    def __init__(self, parameter: str):
        self.message = f"Configuration error: '{parameter}' not provided for a task."
        super().__init__(self.message)

    def __str__(self):
        return self.message


class InvalidGroupTypeError(Exception):
    """
    Raise when Group is not valid in a task
    """

    def __init__(self, group: str, groups: dict, task: str):
        keys = ",".join(groups.keys())
        self.message = (
            f"Configuration error: Invalid group '{group}' for task '{task}'. Valid values: {keys}"
        )
        super().__init__(self.message)

    def __str__(self):
        return self.message
