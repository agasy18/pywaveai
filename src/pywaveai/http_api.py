import os
from typing import Optional
from httpx import AsyncClient
from pywaveai.task import Task, TaskIOManager, TaskType, TaskSource, TaskInfo

from logging import getLogger
from httpx import AsyncClient
from fastapi import APIRouter
from enum import Enum

logger = getLogger(__name__)


class TaskStatus(str, Enum):
    draft = "draft"
    scheduled = "scheduled"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"


class HTTPTaskInfo(TaskInfo):
    def __init__(self, task: Task, task_type: TaskType, task_io_manager: TaskIOManager) -> None:
        super().__init__(task, task_type, task_io_manager)
        self.status = TaskStatus.draft
        self.result = None
        self.error = None

    def set_completed(self, result):
        self.status = TaskStatus.completed
        self.result = result

    def set_failed(self, error: str):
        self.status = TaskStatus.failed
        self.error = error


result_dir = f'/tmp/http_api/results'


class AICTaskIOManager(TaskIOManager):
    def __init__(self) -> None:
        super().__init__()

        self.tasks: dict[str, TaskInfo] = {}
        self.download_client = AsyncClient()

    async def mark_task_failed(self, task: Task, error: Exception):
        logger.error(f"Task {task.type} {task.id} failed")
        logger.exception(error)
        self.tasks[task.id].set_failed(str(error))

    async def mark_task_completed(self, task: Task, result):
        logger.info(f"Task {task.type} {task.id} completed")
        self.tasks[task.id].set_completed(result)

    async def download_bytes(self, task: Task,  name: str, url: str) -> tuple[str, bytes]:
        response = await self.download_client.get(url)
        response.raise_for_status()
        return name, await response.aread()

    async def upload_bytes(self, task: Task, name: str, filename: str, byte_array: bytes) -> tuple[str, str]:
        tmp_dir = f'{result_dir}/{task.id}'
        os.makedirs(tmp_dir, exist_ok=True)

        with open(f'{tmp_dir}/{filename}', 'wb') as f:
            f.write(byte_array)

        return name, filename


class HTTPTaskSource(TaskSource):
    def __init__(self, supported_tasks: list[TaskType], **kwargs) -> None:
        super().__init__(supported_tasks, **kwargs)
        self.task_io_manager = AICTaskIOManager()
        self.supported_tasks_dict = {
            task.type_name: task for task in supported_tasks}

    async def fetch_task(self) -> Optional[TaskInfo]:
        for task in self.task_io_manager.tasks.values():
            if not task.status == TaskStatus.scheduled:
                task.status = TaskStatus.in_progress
                return task
        return None

    def build_task_router(self) -> APIRouter:
        router = APIRouter('/task')

    def build_api_router(self) -> APIRouter:
        router = APIRouter()
        router.add_api_route('/task', self.build_task_router())

        @router.get('/api.json')
        async def api_json():
            tasks = []
            for task_type in self.supported_tasks:
                tasks.append({
                    "name": task_type.type_name,
                    "options": task_type.options.schema(),
                    "options_class": task_type.options.__name__,
                    "result": task_type.result.schema(),
                    "result_class": task_type.result.__name__,
                    "doc": task_type.func.__doc__ or f'No documentation for {task_type.type_name}'
                })
            return {
                "tasks": tasks
            }

        return router
