import time

import logging
from pywaveai.task import TaskResult, TaskInfo
from functools import wraps


logger = logging.getLogger(__name__)


def apply_extantion(task_info: TaskInfo, func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"Starting task {task_info.task.type} {task_info.task.id}")
        start_time = time.time()
        try:
            res: TaskResult = func(*args, **kwargs)
            logger.info(f"Finished task {task_info.task.type} {task_info.task.id} in {time.time() - start_time:.2f}s")
            res.statistics['d'] = time.time() - start_time
            return res
        
        except Exception:
            logger.exception(f"Task {task_info.task.type} {task_info.task.id} failed in {time.time() - start_time:.2f}s")
            raise
    return wrapper

