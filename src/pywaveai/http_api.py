import os
from typing import Optional, Dict
from httpx import AsyncClient
from pywaveai.task import Task as BaseTask, TaskIOManager, TaskExectionInfo, TaskSource, TaskInfo

from logging import getLogger
from httpx import AsyncClient
from fastapi import APIRouter, HTTPException, File, Request
from fastapi.responses import FileResponse
from enum import Enum
from pydantic import BaseModel


from pywaveai.worker_api import worker_api

logger = getLogger(__name__)


class TaskStatus(str, Enum):
    draft = "draft"
    scheduled = "scheduled"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"


class TaskStatusResponse(BaseModel):
    id: str
    status: TaskStatus


class FileInfo(BaseModel):
    filename: str


class Task(BaseTask):
    error: Optional[str] = None
    result: Optional[dict] = None
    status: TaskStatus = TaskStatus.draft

    def set_completed(self, result):
        self.result = result
        self.status = TaskStatus.completed

    def set_failed(self, error):
        self.error = error
        self.status = TaskStatus.failed


result_dir = f'/tmp/http_api/results'


class AICTaskIOManager(TaskIOManager):
    def __init__(self) -> None:
        super().__init__()

        self.download_client = AsyncClient()

    async def mark_task_failed(self, task: Task, error: Exception):
        logger.error(f"Task {task.type} {task.id} failed")
        task.set_failed(str(error))

    async def mark_task_completed(self, task: Task, result):
        logger.info(f"Task {task.type} {task.id} completed")
        task.set_completed(result)

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
    def __init__(self, supported_tasks: list[TaskExectionInfo], **kwargs) -> None:
        super().__init__(supported_tasks, **kwargs)
        self.task_io_manager = AICTaskIOManager()
        self.supported_tasks_dict = {
            task.type_name: task for task in supported_tasks}

        self.tasks: Dict[str, Task] = {}

    async def fetch_task(self) -> Optional[TaskInfo]:
        for task in self.tasks.values():
            if task.status == TaskStatus.scheduled:
                task.status = TaskStatus.in_progress
                return TaskInfo(task=task,
                                execution_info=self.supported_tasks_dict[task.type],
                                task_io_manager=self.task_io_manager)
        return None

    def build_api_for_task_type(self, task_type: TaskExectionInfo) -> APIRouter:
        router = APIRouter()

        @router.post('/create')
        async def create_task(queue_name: str = 'default'):
            id = str(len(self.tasks))
            self.tasks[id] = Task(
                id=id,
                type=task_type.type_name,
            )

            return TaskStatusResponse(id=id, status=TaskStatus.draft)

        @router.post('/{id}/file/new')
        async def new_file(id: str, file_info: FileInfo, request: Request):
            url = str(request.url).replace('/new', file_info.filename)
            return {
                "url": url
            }

        @router.put('/{id}/file/{filename}')
        async def upload_file(id: str, filename: str, file: bytes = File(...)):
            os.makedirs(f'{result_dir}/{id}', exist_ok=True)
            with open(f'{result_dir}/{id}/{filename}', 'wb') as f:
                f.write(file)

        @router.get('/{id}/file/{filename}')
        async def download_file(id: str, filename: str):
            return FileResponse(f'{result_dir}/{id}/{filename}')

        
        @router.post('/{id}/schedule', response_model=TaskStatusResponse)
        async def schedule_task(id: str, options: task_type.options_type):
            if id not in self.tasks:
                raise HTTPException(status_code=404, detail="Task not found")
            task = self.tasks[id]
            
            if task.status != TaskStatus.draft:
                raise HTTPException(detail='Task is not in draft', status_code=403)
            
            task.options = options
            task.status = TaskStatus.scheduled

            return TaskStatusResponse(id=id, status=TaskStatus.scheduled)

        @router.get('/{id}', response_model=TaskStatusResponse)
        async def get_task_status(id: str):
            if id not in self.tasks:
                raise HTTPException(status_code=404, detail="Task not found")
            task = self.tasks[id]
            if task.status == TaskStatus.failed:
                raise HTTPException(detail=task.error, status_code=509)
            return TaskStatusResponse(id=id, status=task.status)

        @router.delete('/{id}')
        async def delete_task(id: str):
            if id not in self.tasks:
                raise HTTPException(status_code=404, detail="Task not found")

            self.tasks[id].status = TaskStatus.failed
            self.tasks[id].error = 'Task was deleted'

            return {'status': 'ok'}

        class TaskResultResponse(TaskStatusResponse):
            result: task_type.result_type

        @router.get('/{id}/result', response_model=TaskResultResponse)
        async def get_task_result(id: str):
            if id not in self.tasks:
                raise HTTPException(status_code=404, detail="Task not found")

            task = self.tasks[id]

            if task.status == TaskStatus.failed:
                raise HTTPException(detail=task.error, status_code=500)

            if task.status != TaskStatus.completed:
                raise HTTPException(detail='Task is not completed', status_code=403)

            return TaskResultResponse(id=id, status=task.status, result=task.result)

        return router

    def build_task_router(self) -> APIRouter:
        router = APIRouter()

        for task_type in self.supported_tasks_dict.values():
            router.include_router(self.build_api_for_task_type(
                task_type), prefix=f'/{task_type.type_name}')

        return router

    def build_api_router(self) -> APIRouter:
        router = APIRouter()
        router.include_router(self.build_task_router(), prefix='/task')

        @router.get('/api.json')
        async def api_json():
            return worker_api(self.supported_tasks)

        return router
