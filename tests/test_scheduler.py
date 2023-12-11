import asyncio
import pytest
from unittest.mock import MagicMock


from pywaveai.scheduler import Scheduler, TaskInfo, TaskSource, TaskExectionInfo
from pywaveai.task import TaskResult, TaskOptions, TaskIOManager, Task
from pywaveai.image_io import BasicImageFileResolver
from pywaveai.ext import gpu_time_ext


class MockTaskIOManager(TaskIOManager):
    def __init__(self):
        self.download_bytes = MagicMock()
        self.upload_bytes = MagicMock()
        self.mark_task_failed = MagicMock()
        self.mark_task_completed = MagicMock()


    async def download_bytes(self, task: Task, name: str, url: str) -> tuple[str, bytes]:
        return self.download_bytes(task, name, url)
    
    async def upload_bytes(self, task: Task, name: str, filename: str, byte_array: bytes) -> tuple[str, str]:
        return self.upload_bytes(task, name, filename, byte_array)
    

    async def mark_task_failed(self, task: Task, error: Exception):
        return self.mark_task_failed(task, error)

    async def mark_task_completed(self, task: Task, result):
        return self.mark_task_completed(task, result)

class MockTaskSource(TaskSource):
    async def fetch_task(self):
        return TaskInfo(task=None, execution_info=None, task_io_manager=None)

def func(options: TaskOptions) -> TaskResult:
    return TaskResult()

@pytest.fixture
def scheduler():
    supported_tasks = [
        TaskExectionInfo(
            type_name="test_type",
            func=func, 
            resource_resolver=BasicImageFileResolver,
        )
    ]
    sources = [MockTaskSource]
    executor_extensions = [
        gpu_time_ext.apply_extantion,
    ]

    return Scheduler(supported_tasks, sources, executor_extensions)


@pytest.mark.asyncio
async def test_scheduler(scheduler):
    await scheduler.start()
    await asyncio.sleep(1)
    await scheduler.stop()



