#!/usr/bin/env python3
# coding=utf-8
import os
import sys
import time
import traceback

import psutil

from ..common.constants import (
    DEFAULT_TASK_TIMEOUT_IN_MINUTES,
    SEQUENTIAL_STRATEGY,
    STATUS_FAIL,
    STATUS_PASS,
    STATUS_SKIP,
    WAITALL,
)
from ..common.helper import ConsoleLogger, Global


class TaskDriver:
    """
    Class for taskdrivers
    """

    def __init__(self, task: dict):
        self.task = task
        self.taskId = task["id"]

        self.cmd = task["Command"]
        self.name = task["Name"]
        self.logfile = Global.LOG_DIR + task["Name"].replace(" ", "_") + "_log"
        task["logfile"] = self.logfile
        self.strategy = task["Strategy"]
        self.mode = task["Mode"]

        task["result"] = {}
        task["result"]["name"] = "[%d] %s" % (self.taskId, self.name)
        task["result"]["status"] = STATUS_FAIL
        task["result"]["taskDuration"] = 0
        task["result"]["cleanupDuration"] = None

        # Change working directory to Linux HE root
        #
        self.wdir = Global.WORKING_DIR

        # Init sync variables for parallel run
        #
        self.failedTaskEvent = None
        self.failedTaskLog = None

    def __call__(self):
        """
        Make this class to be callable, which is used in multiprocessing module
        """

        if self.mode == WAITALL or not self.failedTaskEvent or not self.failedTaskEvent.is_set():
            try:
                self._startProcess()
                self._waitForStatus()

                self.task["result"]["message"] = "[%d] %s execution completed." % (
                    self.taskId,
                    self.name,
                )
                self.task["result"]["status"] = STATUS_PASS

            except Exception as e:
                ConsoleLogger.logInfo(e)

                endTime = time.time()
                self._duration = endTime - self._startTime
                self.task["result"]["message"] = str(e)

                # Append the exception message and full stack trace into task logfile.
                #
                with open(self.logfile, "a") as log:
                    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
                    traceback.print_exception(
                        exceptionType, exceptionValue, exceptionTraceback, file=log
                    )

                # Set failed task event and log to sync up with other processes
                #
                if self.failedTaskEvent:
                    self.failedTaskEvent.set()
                    self.failedTaskLog.append([self.logfile, self.taskId, self.name, str(e)])

        else:
            self.task["result"]["message"] = "[%d] %s skipped." % (
                self.taskId,
                self.name,
            )
            self.task["result"]["status"] = STATUS_SKIP

        self.task["result"]["taskDuration"] = self._duration

        return self.task["result"]

    def _startProcess(self):
        """
        Execute the corresponding command
        """

        ConsoleLogger.logInfo("[%d]" % self.taskId, "Executing %s..." % self.name)

        self._startTime = time.time()

        if self.strategy == SEQUENTIAL_STRATEGY:
            self.process = psutil.Popen(self.cmd, shell=True)
        else:
            with open(self.logfile, "w") as log:
                self.process = psutil.Popen(
                    self.cmd,
                    shell=True,
                    stdout=log,
                    stderr=log,
                )

    def _waitForStatus(self):
        """
        Wait for the spawned shell script to finished.
        Return true if shell script executed successfully else return False
        """
        task_timeout = self.task.get("TimeoutInMinutes", DEFAULT_TASK_TIMEOUT_IN_MINUTES)
        task_timeout = task_timeout * 60
        try:
            self.process.wait(timeout=task_timeout)
        except psutil.TimeoutExpired:
            ConsoleLogger.logFailure(f"Task {self.name} timed out after {task_timeout} seconds")
            self.kill_proc_tree(self.process.pid, recursive=True)
            self.run_command_on_timeout()

        status = self.process.returncode
        endTime = time.time()

        self._duration = endTime - self._startTime

        if status != 0:
            raise Exception('Failure(s) occurred in running command "%s"' % (self.cmd))

    def run_command_on_timeout(self):
        if "RunCommandOnTimeout" not in self.task:
            ConsoleLogger.logInfo(f"No RunCommandOnTimeout specified for task {self.task['Name']}")
            return

        run_command = self.task["RunCommandOnTimeout"]
        ConsoleLogger.logInfo(f"Running command on timeout: {run_command}")
        process = psutil.Popen(run_command, shell=True)
        process.wait()
        result = process.returncode

        if result != 0:
            ConsoleLogger.logFailure(f"Command {run_command} failed with exit code {result}")
            return

        ConsoleLogger.logInfo(f"Command {run_command} executed successfully")

    def kill_proc_tree(self, pid, include_parent=False, recursive=True):
        """
        Kill a process tree (including grandchildren) with signal sig (default is SIGTERM).
        If include_parent is True, include the parent process in the kill.
        If recursive is True, kill the entire process tree rooted at pid, else kill just the children of pid.
        """
        assert pid != os.getpid(), "won't kill myself"
        parent = psutil.Process(pid)
        children = parent.children(recursive=recursive)
        if include_parent:
            children.append(parent)
        print("children: ", children)
        for p in children:
            try:
                if p.status() == psutil.STATUS_RUNNING or p.status() == psutil.STATUS_SLEEPING:
                    print("killing process: ", p)
                    p.kill()
            except psutil.TimeoutExpired as e:
                print("Error: ", e)
                pass

    def setSyncVariable(self, failedTaskEvent, failedTaskLog):
        """
        Set the sync variables for the task
        """

        self.failedTaskEvent = failedTaskEvent
        self.failedTaskLog = failedTaskLog
