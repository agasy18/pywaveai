import time
from functools import wraps
import pytest
from pywaveai.task import TaskResult

from pywaveai.ext.gpu_lock_ext import apply_extantion, GPULockSettings




@pytest.fixture
def func():
    def mock_func(*args, **kwargs):
        assert args == ("input",)
        return TaskResult()
    return mock_func

def test_apply_extantion(func):

    func = apply_extantion(None, func)

    res = func("input")

    assert isinstance(res, TaskResult)
    assert res.statistics['ld'] > 0

    