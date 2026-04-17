#!/usr/bin/env python3
# coding=utf-8

import argparse
import os
import shutil
import sys
import threading

from tests.functional.scheduler.common.constants import (
    NUM_SECONDS_PER_HOUR,
    RUNALWAYS,
    STREAMING_REACTIVE_SECONDS,
    WAITALL,
)
from tests.functional.scheduler.common.helper import ConsoleLogger, Global
from tests.functional.scheduler.common.metrics import Metrics
from tests.functional.scheduler.configuration import Configuration
from tests.functional.scheduler.taskscheduler.task_scheduler_factory import TaskSchedulerFactory


def failAndExit():
    """
    Define failure handler
    """

    ConsoleLogger.logFailure("Pre-checkin validation failed.")
    raise RuntimeError


def parseArguments():
    """
    Parse program arguments
    """

    choiceDescription = """
        config: Defines the task config yaml file

        tasks: A list of tasks or regex of tasks you want to run.
    """

    argParser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter, epilog=choiceDescription
    )

    argParser.add_argument(
        "-c",
        "--config",
        type=str,
        default="config.yaml",
        help="config file outlining the tasks to run",
    )

    argParser.add_argument(
        "-t",
        "--tasks",
        type=str,
        default="all",
        help="run desired task sets, it can be regex",
    )

    return argParser.parse_args()


def runTasks(tasks: dict, strategy: str, mode: str, metrics: Metrics):
    """
    Creates tasks scheduler with given strategy
    """

    scheduler = TaskSchedulerFactory.createTaskScheduler(strategy, mode, metrics)
    result = scheduler(tasks)

    return result


def execGroups(config: Configuration, metrics: Metrics) -> bool:
    """
    Execute groups in the order defined in the yaml
    """

    ConsoleLogger.logInfo("Starting pre-check in validation...")

    configMode = os.getenv("VALIDATION_MODE") or config.mode
    ConsoleLogger.logInfo("Mode is: " + configMode)
    executionPassed = True
    groupExecuted = 0
    for group in config.groupOrder:
        taskGroup = config.groups[group]

        mode = taskGroup["Mode"] if "Mode" in taskGroup else configMode

        result = runTasks(taskGroup["Tasks"], taskGroup["Strategy"], mode, metrics)
        executionPassed = executionPassed and result

        if not result and mode != WAITALL:
            break

        groupExecuted += 1

    for index in range(groupExecuted + 1, len(config.groupOrder)):
        group = config.groupOrder[index]
        taskGroup = config.groups[group]

        if "Mode" in taskGroup and taskGroup["Mode"] == RUNALWAYS:
            result = runTasks(
                taskGroup["Tasks"], taskGroup["Strategy"], taskGroup["Mode"], metrics
            )
            executionPassed = executionPassed and result

    current_thread = threading.current_thread()
    current_thread.executionPassed = executionPassed
    return executionPassed


def runAndMonitor(config: Configuration, metrics: Metrics):
    """
    This function kicks off the run of all tasks and monitor whether the run has finished.
    It is also regularly printing messages to tell that the tasks are still running.
    """
    run_thread = threading.Thread(target=execGroups, args=(config, metrics))
    run_thread.executionPassed = False
    timer = 0
    run_thread.start()
    while run_thread.is_alive():
        if timer > 0:
            ConsoleLogger.logInfo(
                "The tasks are still running. It has been running for about {0} hours.".format(
                    timer / NUM_SECONDS_PER_HOUR
                )
            )
        run_thread.join(STREAMING_REACTIVE_SECONDS)
        timer = timer + STREAMING_REACTIVE_SECONDS

    metrics.printTaskSummary()

    if not run_thread.executionPassed:
        failAndExit()
    ConsoleLogger.logSuccess("Pre-checkin validation passed.")


def setupGlobalVariables():
    """
    This function is called at the beginning to set the global variables
    """

    Global.CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))

    if sys.__stdin__.isatty():
        ConsoleLogger.RED = "\033[1;31m"
        ConsoleLogger.GREEN = "\033[0;32m"
        ConsoleLogger.NOCOLOR = "\033[0m"

    Global.LOG_DIR = Global.WORKING_DIR + "/logs/"
    if os.path.exists(Global.LOG_DIR):
        shutil.rmtree(Global.LOG_DIR)
    os.makedirs(Global.LOG_DIR)


def run(configfile: str, tasks: str = "all"):
    """
    Main function to run all the tasks defined
    """

    if not os.path.exists(configfile):
        ConsoleLogger.logFailure("YAML file does not exists...")
        raise RuntimeError

    setupGlobalVariables()
    config = Configuration(configfile, tasks)
    metrics = Metrics()
    runAndMonitor(config, metrics)


if __name__ == "__main__":
    try:
        args = parseArguments()
        configfile = args.config
        run(configfile=configfile, tasks=args.tasks)

    except:
        raise
