from typing import Any, Optional
from httpx import AsyncClient
from pydantic_settings import BaseSettings
from os import getenv
from pywaveai.image_io import bytes2image
from pywaveai.task import Task, TaskIOManager, TaskType, TaskSource, TaskInfo

import asyncio
from logging import getLogger
from httpx import AsyncClient, RequestError

logger = getLogger(__name__)



class AICAPISettings(BaseSettings):
    API_BASE_URL: str = None
    API_KEY: str = None
    NODE_ID: str = None

    class Config:
        env_file = getenv("AIC_API_CONFIG_FILE", ".env")

settings = AICAPISettings()



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
        logger.exception(error)

        for retry in range(3):
            try:
                response = await self.api_client.post(task.error, json={
                    "code": 508,
                    "message": str(error)
                })

                response.raise_for_status()
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

    async def mark_task_completed(self, task: Task, result):
        for retry in range(3):
            try:
                response = await self.api_client.post(task.result, json=result)
                response.raise_for_status()
            except RequestError as e:
                self.init_api_client()
                logger.error(f"Failed to report a result for task {task.type} {task.id} retrying in 1 second")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Failed to report a result for task {task.type} {task.id}")
                logger.exception(e)
                await asyncio.sleep(1)
            else:
                break
        else:
            logger.error(f"Failed to report a result for task {task.type} {task.id} after 3 retries")

    async def download_bytes(self, task: Task,  name: str, url: str) -> tuple[str, bytes]:
        response = await self.download_client.get(url)
        response.raise_for_status()
        bytes_array = await response.aread()
        return name, bytes_array

    async def upload_bytes(self, task: Task, name: str, filename: str, byte_array: bytes) -> tuple[str, str]:
        
        response = await self.api_client.post(task.new_file, json={
            "filename": filename,
        })
        response.raise_for_status()
        file_url = response.json()['url']
            
        logger.debug(f"Uploading {filename} to {file_url}")
        response = await self.upload_client.put(file_url, data=byte_array)
        response.raise_for_status()
        return name, filename
    
    async def fetch_new_task(self, supported_tasks: list[TaskType]) -> Task:
        task_types = list(supported_tasks.keys())
        
        next_url = f"{settings.API_BASE_URL}/node/{settings.NODE_ID}/tasks/next?retry_seconds=40"
        response = await self.api_client.post(next_url, json={
            "task_types": task_types
        }, timeout=60)
        response.raise_for_status()
        if response.status_code == 204:
            return None
        return Task.validate(response.json())



class AICTaskSource(TaskSource):
    def __init__(self, supported_tasks: list[TaskType], **kwargs) -> None:
        super().__init__(supported_tasks, **kwargs)
        self.task_io_manager = AICTaskIOManager()
        self.supported_tasks_dict = {task.type_name: task for task in supported_tasks}

    async def fetch_task(self) -> Optional[TaskInfo]:
        task = await self.task_io_manager.fetch_new_task(self.supported_tasks)
        if task is None:
            return None
        
        task_type = self.supported_tasks_dict[task.type]
        
        return TaskInfo(task=task, task_type=task_type, task_io_manager=self.task_io_manager)
