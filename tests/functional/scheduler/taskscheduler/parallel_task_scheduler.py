#!/usr/bin/env python3
# coding=utf-8

import multiprocessing

import psutil

from tests.functional.scheduler.taskdriver.task_driver_factory import TaskDriverFactory

from ..common.constants import (
    FAILFAST,
    PARALLEL_STRATEGY,
    STATUS_FAIL,
    STATUS_PASS,
)
from ..common.helper import ConsoleLogger
from ..taskscheduler.base_task_scheduler import BaseTaskScheduler


class ParallelScheduler(BaseTaskScheduler):
    """
    Schedule tasks in parallel with given mode
    """

    def _killChildProcessTrees(self):
        """
        Kill child process trees spawned by task drivers to prevent orphans.
        """
        for task_id, pid in self.childPids.items():
            try:
                parent = psutil.Process(pid)
                children = parent.children(recursive=True)
                for child in children:
                    try:
                        child.kill()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                parent.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

    def _taskCallback(self, result):
        """
        Callback function for succeeded tasks
        """

        # Only record the duration if the test wasn't skipped
        #
        self.metrics.addTaskMetrics(
            result["name"],
            result["taskDuration"],
            result["status"],
            result["cleanupDuration"],
        )

        if result["status"] == STATUS_PASS:
            ConsoleLogger.logSuccess(result["message"])
        elif result["status"] == STATUS_FAIL:
            # if failfast was set, we kill all processes within the same process group
            #
            if self.mode == FAILFAST:
                self._killChildProcessTrees()
                self.processPool.terminate()

    def __call__(self, tasks: dict):
        """
        Running multiple tasks in parallel
        """

        numProcess = max(1, multiprocessing.cpu_count() // 2)

        self.processPool = multiprocessing.Pool(numProcess)
        processManager = multiprocessing.Manager()

        # When a task failed, the process will set this event and other process will know
        # a failure happened and won't schedule new tasks
        #
        self.failedTaskEvent = processManager.Event()

        # Shared variable between processes to get the failed task's log file
        #
        self.failedTaskLog = processManager.list()

        # Shared dict for child process PIDs — used to kill orphans on failfast
        #
        self.childPids = processManager.dict()

        # The conflict tasks cannot parallelly run with some existed tasks and would be saved to sequentially schedule at the end.
        #
        conflictTasks = []

        for task in tasks:
            task["Strategy"] = PARALLEL_STRATEGY
            driver = TaskDriverFactory.createTaskDriver(task, self.mode)
            driver.setSyncVariable(self.failedTaskEvent, self.failedTaskLog, self.childPids)
            self.processPool.apply_async(driver, args=(), callback=self._taskCallback)

        self.processPool.close()
        self.processPool.join()

        if not self.failedTaskLog:
            # Sequentially running conflicting tasks if parallel tasks passed.
            #
            if conflictTasks:
                ConsoleLogger.logInfo("Scheduling the remaining tasks sequentially.")
                from ..taskscheduler.task_scheduler_factory import TaskSchedulerFactory

                scheduler = TaskSchedulerFactory.createTaskScheduler("sequential", None)
                return scheduler(conflictTasks)

            return True
        else:
            ConsoleLogger.logFailure("Logs from the failed task(s) are as follows:")
            self._printFailedTaskLog()
            return False
