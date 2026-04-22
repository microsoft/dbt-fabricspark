from .task_driver import TaskDriver


class TaskDriverFactory(object):
    """
    Factory class to create a taskdriver instance
    """

    taskCounter = 0

    @staticmethod
    def createTaskDriver(task: dict, mode: str):
        """
        Create a task driver for the given task
        """

        task["id"] = TaskDriverFactory.taskCounter
        TaskDriverFactory.taskCounter += 1
        task["Mode"] = mode
        driver = None
        driver = TaskDriver(task)
        return driver
