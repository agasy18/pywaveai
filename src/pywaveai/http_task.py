from enum import Enum
from typing import Optional
from pywaveai.task import Task as BaseTask, TaskResult
from pywaveai.task_io_manager import TaskIOManager
import logging

logger = logging.getLogger(__name__)


from httpx import AsyncClient


import os


class TaskStatus(str, Enum):
    draft = "draft"
    scheduled = "scheduled"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"


class Task(BaseTask):
    task_url: str
    error: Optional[str] = None
    result: Optional[dict] = None
    status: TaskStatus = TaskStatus.draft

    def set_completed(self, result):
        self.result = result
        self.status = TaskStatus.completed

    def set_failed(self, error):
        self.error = error
        self.status = TaskStatus.failed


class HTTPTaskIOManager(TaskIOManager):
    def __init__(self, result_dir: str='/tmp/http_api/results') -> None:
        super().__init__()
        self.result_dir = result_dir
        self.download_client = AsyncClient()

    def load_inital_tasks(self) -> list[(str, str, dict)]:
        return []

    def path_for_file(self, task: Task, filename: str) -> str:
        return f'{self.result_dir}/{task.id}/{filename}'

    async def mark_task_failed(self, task: Task, error: Exception):
        logger.error(f"Task {task.type} {task.id} failed")
        task.set_failed(str(error))


    async def mark_task_completed(self, task: Task, result: TaskResult):
        logger.info(f"Task {task.type} {task.id} completed")
        task.set_completed(result)


    async def download_bytes(self, task: Task, url: str) -> bytes:
        if url.startswith('http'):
            response = await self.download_client.get(url)
            response.raise_for_status()
            return await response.aread()
        else:
            with open(f'{self.result_dir}/{task.id}/{url}', 'rb') as f:
                return f.read()

    async def upload_bytes(self, task: Task, filename: str, byte_array: bytes) -> str:
        tmp_dir = f'{self.result_dir}/{task.id}'
        with open(f'{tmp_dir}/result_{filename}', 'wb') as f:
            f.write(byte_array)

        return f'{task.task_url}/file/result_{filename}'

    async def create_task(self, task: Task):
        os.makedirs(f'{self.result_dir}/{task.id}', exist_ok=True)