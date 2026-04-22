import re

import yaml

from tests.functional.scheduler.common.constants import (
    VALID_MODES,
    VALID_STRATEGIES,
)
from tests.functional.scheduler.common.exceptions import (
    InvalidGroupTypeError,
    InvalidModeError,
    InvalidStrategyError,
    MissingTaskParameterError,
)
from tests.functional.scheduler.common.helper import ConsoleLogger


class Configuration:
    """
    Manages Configuration of the scheduler
    """

    def __init__(self, configfile: str, desiredTasks: str):
        """
        Load the config parameters from config file
        """

        with open(configfile, "r") as ymlfile:
            config = self._yamlLoad(ymlfile)

        self.desiredTasks = desiredTasks.split()
        self.mode = config["mode"]
        self.tasks = config["tasks"]
        self.groupOrder = []
        self.groups = {}

        self._parseGroups(config["groups"])
        self._parseTaskGroup(config["tasks"])

        self.validateConfig()

    def _failAndExitOnBadStrategy(self, strategy: str):
        """
        Declare failure due to bad strategy and exit
        """

        error = InvalidStrategyError(strategy)
        ConsoleLogger.logFailure(error)
        raise error

    def _failAndExitOnBadMode(self, mode: str):
        """
        Declare failure due to bad mode and exit
        """
        error = InvalidModeError(mode)

        ConsoleLogger.logFailure(error)
        raise error

    def _failAndExitOnAbsentTaskParameter(self, parameter: str):
        """
        Declare failure due to absent task parameters and exit
        """

        error = MissingTaskParameterError(parameter)

        ConsoleLogger.logFailure(error)
        raise error

    def _failAndExitOnBadGroupType(self, task: dict):
        """
        Declare failure due to bad group type and exit
        """

        error = InvalidGroupTypeError(task["Group"], self.groups, task["Name"])

        ConsoleLogger.logFailure(error)
        raise error

    def _inDesiredTasks(self, task: dict):
        """
        Check whether this task is in desired tasks set
        """

        for i in self.desiredTasks:
            if re.fullmatch(i, task["Command"]):
                return True
        return False

    def _parseGroups(self, groups: dict):
        """
        Create a dictionary of the define groupings and their strategy
        """

        for group in groups:
            groupName = group["Group"]

            if group["Strategy"] not in VALID_STRATEGIES:
                self._failAndExitOnBadStrategy(group["Strategy"])

            strategy = group["Strategy"]

            self.groups[groupName] = {}
            self.groups[groupName]["Strategy"] = strategy
            self.groups[groupName]["Tasks"] = []

            if "Mode" in group:
                mode = group["Mode"]
                if mode not in VALID_MODES:
                    self._failAndExitOnBadMode(group["Mode"])

                self.groups[groupName]["Mode"] = mode

            self.groupOrder.append(groupName)

    def _parseTaskGroup(self, tasks: dict):
        """
        Assign each task to their validation group
        """

        for task in tasks:
            taskGroup = task["Group"]

            if taskGroup not in self.groupOrder:
                self._failAndExitOnBadGroupType(task)

            if self.desiredTasks[0] == "all" or self._inDesiredTasks(task):
                self.groups[taskGroup]["Tasks"].append(task)

    def _yamlLoad(self, file: str):
        """
        Wrapper function for yaml load, loader fix is needed for new version (5.1+).
        Check for the attribute to ensure backwards compatibility
        """
        return yaml.full_load(file)

    def validateConfig(self):
        """
        Check validity of the config parameter values
        """

        if self.mode not in VALID_MODES:
            self._failAndExitOnBadMode()

        for task in self.tasks:
            if "Command" not in task:
                self._failAndExitOnAbsentTaskParameter("Command")
            if "Name" not in task:
                self._failAndExitOnAbsentTaskParameter("Name")
            if task["Group"] not in self.groups.keys():
                self._failAndExitOnBadGroupType(task)
