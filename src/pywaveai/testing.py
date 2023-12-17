from .task import Task
import json
import os
import logging
logger = logging.getLogger(__name__)
from testfixtures import compare




def load_json(path: str) -> dict:
    if not os.path.exists(path):
        return None
    with open(path, 'r') as f:
        return json.load(f)

def _test_processor(task: Task, task_dir: str) -> bool:
    """
    :param task: Task object
    :param task_dir: Task directory
    :return: True if test passed, False otherwise
    """

    result = load_json(os.path.join(task_dir, "result.json"))

    error = load_json(os.path.join(task_dir, "error.json"))

    expected_result = load_json(os.path.join(task_dir, "expected.json"))

    
    if error is not None:
        logger.error(f"Task {task.id} failed with error: {error}, task.options: {task.options}")
        return False

    if expected_result is None:
            logger.warning(f"Task {task.id} has no expected result file (expected.json)")
            return True
    else:
        try:
            if 'statistics' in result:
                del result['statistics']
            if 'statistics' in expected_result:
                del expected_result['statistics']
            compare(result, expected_result)
            return True
        except Exception as e:
            logger.error(f"Task {task.id} due to result mismatch: {e}, task.options: {task.options}")
            return False
    
    

def set_test_result_processor(test_processor: callable):
    """
    :param test_processor: function that takes two arguments: task and task_dir
    :return: None
    """
    global _current_test_processor
    _current_test_processor = test_processor

def process_test_result(task: Task, task_dir: str) -> bool:
    """
    :param task: Task object
    :param task_dir: Task directory
    :return: True if test passed, False otherwise
    """
    global _current_test_processor
    return _current_test_processor(task, task_dir)


_current_test_processor = _test_processor

    