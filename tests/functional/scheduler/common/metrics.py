import collections
from threading import Lock

from ..common.helper import ConsoleLogger


class Metrics(object):
    DURATION_HEADERS = ["task", "duration"]

    """
    Class for storing metrics about tasks ran by the validation framework
    """

    def __init__(self):
        """
        Initialize the storage
        """
        self.taskMetrics = collections.OrderedDict()
        self.metricsLock = Lock()

    def _convertToMinutesAndSeconds(self, seconds):
        """
        Convert seconds to minutes and seconds
        """
        return divmod(int(seconds), 60)

    def addTaskMetrics(self, taskName, durationInSeconds, status, cleanupDuration=None):
        """
        Record a task duration
        """
        self.metricsLock.acquire()

        self.taskMetrics[taskName] = {
            "taskDuration": durationInSeconds,
            "status": status,
        }

        if cleanupDuration is not None:
            self.taskMetrics[taskName]["taskCleanup"] = cleanupDuration

        self.metricsLock.release()

    def printTaskSummary(self):
        """
        Print the summary for each task ran
        """
        self.metricsLock.acquire()

        ConsoleLogger.logInfo(" Tasks Summary ".center(70, "="))
        ConsoleLogger.logInfo("{:^44}| {:^15}| {:^4}".format("Task", "Duration", "Status"))
        ConsoleLogger.logInfo("".center(70, "="))

        for task, info in self.taskMetrics.items():
            self._printTaskInfo(task, info["taskDuration"], info["status"])

            if "taskCleanup" in info:
                self._printTaskInfo(task + " cleanup", info["taskCleanup"], "-")

        ConsoleLogger.logInfo("".center(70, "="))

        self.metricsLock.release()

    def _printTaskInfo(self, taskName, durationInSeconds, status):
        """
        Format and print the task metrics
        """
        minutes, seconds = self._convertToMinutesAndSeconds(durationInSeconds)

        ConsoleLogger.logInfo(
            "{:<44}| {:>3} min {:>2} sec |  {:<8}".format(taskName, minutes, seconds, status)
        )
