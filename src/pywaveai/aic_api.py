from typing import Any, Optional
from httpx import AsyncClient
from pydantic_settings import BaseSettings
from os import getenv
from pywaveai.task_io_manager import TaskIOManager
from pywaveai.runtime import TaskInfo
from pywaveai.runtime import TaskExectionInfo, TaskSource
from pywaveai.image_io import bytes2image
from pywaveai.task import Task as TaskBase, TaskResult

import asyncio
from logging import getLogger
from httpx import AsyncClient, RequestError

logger = getLogger(__name__)



class AICAPISettings(BaseSettings):
    API_BASE_URL: Optional[str] = None
    API_KEY: Optional[str] = None
    NODE_ID: Optional[str] = None

    class Config:
        env_file = getenv("AIC_API_CONFIG_FILE", ".env")

settings = AICAPISettings()

class Task(TaskBase):
    new_file: str
    error: str
    result: str

class AICAPIError(Exception):
    def __init__(self, status_code: int, message: str, inner: Exception = None):
        self.status_code = status_code
        self.message = message
        self.inner = inner
        super().__init__(message)

    def __str__(self):
        if self.inner is None:
            return f"AICAPIError {self.status_code}: {self.message}"
        return f"AICAPIError {self.status_code}: {self.message} {type(self.inner).__name__}: {self.inner}"

def raise_for_status(response):
    try:
        response.raise_for_status()
    except Exception as e:
        try:
            js = response.json()
            logger.error(f"Erorr - {js} with status code {response.status_code}")
        except Exception:
            pass
        raise e


def _try_pase_error_core(e: Exception, default: int = 508) -> int:
    status_code = getattr(e, 'status_code', 
        getattr(e, 'code', None), None)
    if isinstance(status_code, int):
        return status_code
    return default
        
class AICTaskIOManager(TaskIOManager):
    def __init__(self) -> None:
        self.init_download_client()
        self.init_upload_client()
        self.init_api_client()


    def init_upload_client(self):
        self.upload_client = AsyncClient()

    def init_download_client(self):
        self.download_client = AsyncClient()

    def init_api_client(self):
        self.api_client = AsyncClient(headers={
            "X-API-KEY": settings.API_KEY
        },
        base_url=settings.API_BASE_URL, http2=True)


    async def mark_task_failed(self, task: Task, error: Exception):
        logger.error(f"Task {task.type} {task.id} failed")

        for retry in range(3):
            try:
                response = await self.api_client.post(task.error, json={
                    "code": _try_pase_error_core(error),
                    "message": f"{type(error).__name__}: {error}"
                })

                raise_for_status(response)
            except RequestError as e:
                self.init_api_client()
                logger.error(f"Failed to report an error for task {task.type} {task.id} retrying in 1 second")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Failed to report an error for task {task.type} {task.id}")
                logger.exception(e)
                await asyncio.sleep(1)
            else:
                break
        else:
            logger.error(f"Failed to report an error for task {task.type} {task.id} after 3 retries")

    async def mark_task_completed(self, task: Task, result: TaskResult):
        last_error = None
        for retry in range(3):
            try:
                js = result.model_dump()
                if 'statistics' in js:
                    js['st'] = js.pop('statistics')
                response = await self.api_client.post(task.result, json=js)
                raise_for_status(response)
            except RequestError as e:
                self.init_api_client()
                logger.error(f"Failed to report a result for task {task.type} {task.id} retrying in 1 second")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Failed to report a result for task {task.type} {task.id}")
                logger.exception(e)
                last_error = e
                await asyncio.sleep(1)
            else:
                break
        else:
            logger.error(f"Failed to report a result for task {task.type} {task.id} after 3 retries")

            if last_error is not None:
                await self.mark_task_failed(task, last_error)

    async def download_bytes(self, task: Task, url: str) -> bytes:
        last_error = None
        for retry in range(3):
            try:
                response = await self.download_client.get(url)
                raise_for_status(response)
                bytes_array = await response.aread()
                return bytes_array
            except Exception as e:
                logger.error(f"Failed to download {url}")
                logger.exception(e)
                self.init_download_client()
                last_error = e
                await asyncio.sleep(1)
        assert last_error is not None
        raise AICAPIError(500, f"Failed to download {url} after 3 retries", last_error)
        

    async def upload_bytes(self, task: Task, filename: str, byte_array: bytes) -> str:
        response = await self.api_client.post(task.new_file, json={
            "filename": filename,
        })
        raise_for_status(response)
        file_url = response.json()['url']
        
        logger.debug(f"Uploading {filename} to {file_url}")
        last_error = None
        for retry in range(3):
            try:
                response = await self.upload_client.put(file_url, data=byte_array, timeout=120)
                raise_for_status(response)
            except Exception as e:
                logger.error(f"Failed to upload {filename} to {file_url}")
                logger.exception(e)
                last_error = e
                self.init_upload_client()
                await asyncio.sleep(1)
            else:
                break
        if last_error is not None:
            raise AICAPIError(500, f"Failed to upload {filename} to {file_url} after 3 retries", last_error)
        return filename
    
    async def fetch_new_task(self, supported_tasks: list[TaskExectionInfo]) -> Task:
        supported_tasks_dict = {task.task_name: task for task in supported_tasks}
        task_types = list(supported_tasks_dict.keys())
        
        next_url = f"{settings.API_BASE_URL}/node/{settings.NODE_ID}/tasks/next?retry_seconds=40"
        response = await self.api_client.post(next_url, json={
            "task_types": task_types
        }, timeout=60)
        raise_for_status(response)
        if response.status_code == 204:
            return None
        js = response.json()

        task = Task.model_validate(js)

        tx = supported_tasks_dict[task.type]
        task.options = tx.options_type.model_validate(js['options'])
        return task



class AICTaskSource(TaskSource):
    def __init__(self, supported_tasks: list[TaskExectionInfo], **kwargs) -> None:
        super().__init__(supported_tasks, **kwargs)
        self.task_io_manager = AICTaskIOManager()
        self.supported_tasks_dict = {task.task_name: task for task in supported_tasks}

    async def fetch_task(self) -> Optional[TaskInfo]:
        task = await self.task_io_manager.fetch_new_task(self.supported_tasks)
        if task is None:
            return None
        
        task_type = self.supported_tasks_dict[task.type]
        
        return TaskInfo(task=task, execution_info=task_type, task_io_manager=self.task_io_manager)
