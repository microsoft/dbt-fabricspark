#!/usr/bin/env python3
# coding=utf-8
from tests.functional.scheduler.taskdriver.task_driver_factory import TaskDriverFactory

from ..common.constants import (
    SEQUENTIAL_STRATEGY,
    STATUS_FAIL,
    STATUS_PASS,
    WAITALL,
)
from ..common.helper import ConsoleLogger
from ..taskscheduler.base_task_scheduler import BaseTaskScheduler


class SequentialScheduler(BaseTaskScheduler):
    """
    Schedule tasks sequentially.
    If the scheduler's mode is NOT set to waitall, the
    scheduler will fail as soon as a test fails
    """

    def __call__(self, tasks: dict):
        """
        Running multiple tasks in sequential
        """
        success = True

        for task in tasks:
            task["Strategy"] = SEQUENTIAL_STRATEGY
            driver = TaskDriverFactory.createTaskDriver(task, self.mode)

            result = driver()

            self.metrics.addTaskMetrics(
                result["name"],
                result["taskDuration"],
                result["status"],
                result["cleanupDuration"],
            )
            if result["status"] == STATUS_PASS:
                ConsoleLogger.logSuccess(result["message"])
            elif result["status"] == STATUS_FAIL:
                self.failedTaskLog = [
                    [task["logfile"], task["id"], task["Name"], result["message"]]
                ]
                self._printFailedTaskLog()

                success = False

                # Return immediately if we are not in waitall
                #
                if self.mode != WAITALL:
                    return success
        return success
