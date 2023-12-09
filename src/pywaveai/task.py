from pydantic import BaseModel, Field
from typing import List, Optional, Union, Dict, Any
from collections import namedtuple
import json
from abc import ABC
from dataclasses import dataclass
import tempfile
import asyncio
import os

class TaskOptions(BaseModel):
    files = Field(default_factory=dict)


class TaskResult(BaseModel):
    files = Field(default_factory=dict)
    statistics = Field(default_factory=dict)



class Task(BaseModel):
    id: str
    type: str
    options: TaskOptions
    result: TaskResult
    error: str
    task_status: str
    new_file: str




class TaskIOManager(ABC):
    async def mark_task_failed(self, task: Task, error: Exception):
        raise NotImplementedError()

    async def mark_task_completed(self, task: Task, result):
        raise NotImplementedError()

    async def download_bytes(self, task: Task, name: str, url: str) -> tuple[str, bytes]:
        raise NotImplementedError()

    async def upload_bytes(self, task: Task, name: str, filename: str, byte_array: bytes) -> tuple[str, str]:
        raise NotImplementedError()

    async def download_file(self, task: Task, name: str, url: str) -> tuple[str, str]:
        name, byte_array = await self.download_bytes(task, name, url)
        with tempfile.NamedTemporaryFile(delete=False, suffix=url.split('.')[-1]) as f:
            await asyncio.to_thread(f.write, byte_array)
        
        return name, f.name

    async def remove_file(self, task: Task, name: str, file_path: str):
        os.remove(file_path)

    async def upload_file(self, task: Task, name: str, file_path: str) -> tuple[str, str]:
        with open(file_path, 'rb') as f:
            byte_array = await asyncio.to_thread(f.read)
        return await self.upload_bytes(task, name, name + os.path.splitext(file_path)[-1], byte_array)
    


class TaskResourceResolver(ABC):
    async def _encode_to_bytes(self, task: Task, name: str, resource: object) -> tuple[str, str, bytes]:
        raise NotImplementedError()

    async def _decode_from_bytes(self, task: Task, name: str, byte_array: bytes) -> tuple[str, object]:
        raise NotImplementedError()

    async def download_files(self, task: Task, task_io_manager: TaskIOManager):
        files = task.options.files

        for name, url in files.items():
            name, byte_array =  await task_io_manager.download_bytes(task, name, url)
            name, obj = await self._decode_from_bytes(name, byte_array)
            task.files[name] = obj

    async def upload_files(self, task: Task, task_io_manager: TaskIOManager):
        files = task.result.files

        for name, obj in files.items():
            name, byte_array = await self._encode_to_bytes(name, obj)
            name, url = await task_io_manager.upload_bytes(task, name, name, byte_array)
            task.files[name] = url

    


class TaskType(object):
    def __init__(self, type_name: str, 
                 func, 
                 options_type, 
                 result_type, 
                 resource_resolver: TaskResourceResolver):
        self.type_name = type_name
        self.func = func
        assert issubclass(options_type, TaskOptions)
        assert issubclass(result_type, TaskResult)
        self.options_type = options_type
        self.result_type = result_type
        self.resource_resolver = resource_resolver


@dataclass
class TaskInfo(object):
    task: Task
    task_type: TaskType
    task_io_manager: TaskIOManager

    @property
    def resource_resolver(self):
        return self.task_type.resource_resolver


class TaskSource(ABC):
    def __init__(self, supported_tasks: List[TaskType], **kwargs) -> None:
        self.supported_tasks = supported_tasks

    async def fetch_task(self) -> Optional[TaskInfo]:
        raise NotImplementedError()

    def build_api_router(self):
        """
        Build a FastAPI router for this task source
        """
        return None
