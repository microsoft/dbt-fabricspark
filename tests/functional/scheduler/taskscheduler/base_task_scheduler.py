import os

from ..common.helper import ConsoleLogger
from ..common.metrics import Metrics


class BaseTaskScheduler(object):
    """
    Basic class for ParallelScheduler and SequentialScheduler
    """

    def __init__(self, mode: str, metrics: Metrics):
        """
        Initilize mode property (failfast/waitcurrent or None)
        """

        self.mode = mode
        self.metrics = metrics

    def _printFailedTaskLog(self):
        """
        Print logs from failed tasks
        """

        for item in self.failedTaskLog:
            logfile = item[0]
            taskId = item[1]
            taskname = item[2]
            errMsg = item[3]

            ConsoleLogger.logFailure(
                "######################### Failure log for %s ########################" % taskname
            )

            if os.path.isfile(logfile):
                with open(logfile, "r") as reader:
                    lines = reader.readlines()

                    for line in lines:
                        ConsoleLogger.logFailure(line.strip())

            ConsoleLogger.logFailure("[%d] %s failed: %s" % (taskId, taskname, errMsg))
