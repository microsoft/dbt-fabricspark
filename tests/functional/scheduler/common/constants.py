#!/usr/bin/env python3
"""
Default constants used for Scheduler
"""

"""
STRATEGIES WHICH SCHEDULER SUPPORTS
"""
SEQUENTIAL_STRATEGY = "sequential"

PARALLEL_STRATEGY = "parallel"

"""
MODES IN WHICH SCHEDULER WORKS
"""
# Schedules all tasks, ignoring failures. At the end will exit with error code if any tasks failed.
WAITALL = "waitall"
# If a task fails, wait for all the running tasks to finish, but don't schedule new tasks.
WAITCURRENT = "waitcurrent"
# Immediately exit when a task fails.
FAILFAST = "failfast"
# Always execute irrespective of failures
RUNALWAYS = "runalways"

"""
VALID STRATEGIES AND MODES
"""
VALID_STRATEGIES = [SEQUENTIAL_STRATEGY, PARALLEL_STRATEGY]
VALID_MODES = [FAILFAST, WAITALL, WAITCURRENT, RUNALWAYS]

"""
STATUS CONSTANTS
"""
STATUS_PASS = "Pass"
STATUS_FAIL = "Fail"
STATUS_SKIP = "Skip"

"""
TIMING CONSTANTS
"""
STREAMING_REACTIVE_SECONDS = 3600
NUM_SECONDS_PER_HOUR = 3600
DEFAULT_TASK_TIMEOUT_IN_MINUTES = 5
DEFAULT_WAIT_TIME_IN_MINUTES = 5
