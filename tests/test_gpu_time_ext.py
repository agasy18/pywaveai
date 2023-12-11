from pywaveai.ext.gpu_time_ext import apply_extantion
from pywaveai.task import TaskResult
import pytest

class MockTaskResult(TaskResult):
    pass
    
def test_apply_extantion():
    # Mock task_info and func
    class TaskInfo:
        def __init__(self, task_type, task_id):
            self.task_type = task_type
            self.task = MockTask(task_id)
    
    class MockTask:
        def __init__(self, task_id):
            self.id = task_id
    
    def mock_func(input):
        assert input == "input"
        return MockTaskResult()
    
    # Call the function
    task_info = TaskInfo("test_type", "test_id")
    wrapped_func = apply_extantion(task_info, mock_func)
    res = wrapped_func("input")

    # Check the result
    assert isinstance(res, MockTaskResult)
    assert res.statistics['d'] > 0
    