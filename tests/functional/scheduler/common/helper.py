#!/usr/bin/env python3
# coding=utf-8
import os
import subprocess
import sys


class ConsoleLogger(object):
    """
    Console logger class
    """

    # Coloring scheme
    #
    RED = ""
    GREEN = ""
    NOCOLOR = ""

    @classmethod
    def logFailure(cls, farg, *args):
        sys.stdout.write(cls.RED)
        print(farg, " ".join(map(str, args)))
        sys.stdout.write(cls.NOCOLOR)
        sys.stdout.flush()

    @classmethod
    def logSuccess(cls, farg, *args):
        sys.stdout.write(cls.GREEN)
        print(farg, " ".join(map(str, args)))
        sys.stdout.write(cls.NOCOLOR)
        sys.stdout.flush()

    @classmethod
    def logInfo(cls, farg, *args):
        sys.stdout.write(cls.NOCOLOR)
        print(farg, " ".join(map(str, args)))
        sys.stdout.flush()


class Helper(object):
    """
    Some helper functions
    """

    @classmethod
    def isVerboseMode(self):
        """
        Check if verbose mode is set in the environment
        """

        verbose = os.environ.get("V")

        if verbose is None:
            return False
        elif verbose == 0:
            return False
        else:
            return True

    @classmethod
    def downloadDeps(self, yamlfile: str):
        """
        Download dependencies for given yaml file
        """

        ConsoleLogger.logInfo("Download dependencies: " + yamlfile)
        process = subprocess.Popen(
            Global.DEPS_CMD + " " + yamlfile, shell=True, cwd=Global.WORKING_DIR
        )
        ret = process.wait()
        if ret != 0:
            ConsoleLogger.logFailure("Failed to download dependencies: " + yamlfile)
            return False

        return True


class Global(object):
    """
    Define the global variables for validation
    """

    CURRENT_PATH = "./"
    WORKING_DIR = "./"
    LOG_DIR = "./logs/"
    FLOAT_EPS = 1e-5
    """
    BUILD_ROOT = os.environ['BUILD_ROOT']

    DEPS_CMD = BUILD_ROOT + "/build/buildtool/buildtool deps get --full-template-sub"
    TEST_DEPS_YAML = BUILD_ROOT + "/build/config/test_deps.yaml"
    JOB_TEST_DEPS_YAML = BUILD_ROOT + "/build/config/job_test_deps.yaml"
    """
