from typing import Optional, Dict
from pywaveai.http_task import HTTPTaskIOManager, Task, TaskStatus
from pywaveai.runtime import TaskInfo
from pywaveai.runtime import TaskExectionInfo, TaskSource

from logging import getLogger
from fastapi import APIRouter, HTTPException, File, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel


from pywaveai.worker_api import worker_api

logger = getLogger(__name__)


class TaskStatusResponse(BaseModel):
    id: str
    status: TaskStatus


class FileInfo(BaseModel):
    filename: str


class HTTPTaskSource(TaskSource):
    def __init__(self, supported_tasks: list[TaskExectionInfo], task_io_manager: HTTPTaskIOManager=None, **kwargs) -> None:
        super().__init__(supported_tasks, **kwargs)
        self.task_io_manager = task_io_manager or HTTPTaskIOManager()
        
        self.supported_tasks_dict = {
            task.task_name: task for task in supported_tasks}
        
        logger.info(f"Supported tasks: {list(self.supported_tasks_dict.keys())}")

        self.tasks: Dict[str, Task] = {}

        for ttype, tid, opt in self.task_io_manager.load_inital_tasks():
            self.tasks[tid] = Task(
                id=tid,
                type=ttype,
                options=self.supported_tasks_dict[ttype].options_type.model_validate(opt),
                status=TaskStatus.scheduled,
                task_url=f'http://localhost:8000/task/{ttype}/{tid}'
            )


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
        async def create_task(request: Request, queue_name: str = 'default'):
            id = str(len(self.tasks))
            task = Task(
                id=id,
                type=task_type.task_name,
                task_url=f'{request.base_url}task/{task_type.task_name}/{id}'
            )
            await self.task_io_manager.create_task(task)
            self.tasks[id] = task
            return TaskStatusResponse(id=id, status=TaskStatus.draft)

        @router.post('/{id}/file/new')
        async def new_file(id: str, file_info: FileInfo, request: Request):
            url = str(request.url).replace('/new', '/' + file_info.filename)
            return {
                "url": url
            }

        @router.put('/{id}/file/{filename}')
        async def upload_file(id: str, filename: str, file: bytes = File(...)):
            if id not in self.tasks:
                raise HTTPException(status_code=404, detail="Task not found")
            with open(self.task_io_manager.path_for_file(self.tasks[id], filename), 'wb') as f:
                f.write(file)

        @router.get('/{id}/file/{filename}')
        async def download_file(id: str, filename: str):
            if id not in self.tasks:
                raise HTTPException(status_code=404, detail="Task not found")
            return FileResponse(self.task_io_manager.path_for_file(self.tasks[id], filename))
        
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
                task_type), prefix=f'/{task_type.task_name}', tags=[task_type.task_name])

        return router

    def build_api_router(self) -> APIRouter:
        router = APIRouter()
        router.include_router(self.build_task_router(), prefix='/task')

        @router.get('/api.json')
        async def api_json():
            return worker_api(self.supported_tasks)

        return router
