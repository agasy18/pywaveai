from pydantic import BaseModel, Field
from typing import List, Optional, Union, Dict, Any
from collections import namedtuple
import json
from abc import ABC
from dataclasses import dataclass


class Task(BaseModel):
    id: str
    type: str
    options: dict
    result: str
    error: str
    task_status: str
    new_file: str


class TaskResourceResolver(ABC):
    async def encode_to_bytes(self, name: str, resource: object) -> tuple[str, str, bytes]:
        raise NotImplementedError()

    async def decode_from_bytes(self, name: str, byte_array: bytes) -> tuple[str, object]:
        raise NotImplementedError()


class TaskIOManager(ABC):
    async def mark_task_failed(self, task: Task, error: Exception):
        raise NotImplementedError()
    
    async def mark_task_completed(self, task: Task, result):
        raise NotImplementedError()

    async def download_bytes(self, task: Task, name: str, url: str) -> tuple[str, bytes]:
        raise NotImplementedError()

    async def upload_bytes(self, task:Task, name: str, filename: str, byte_array: bytes) -> tuple[str, str]:
        raise NotImplementedError()


class TaskType(object):
    def __init__(self, type_name, func, options_type, result_type, resource_resolver: TaskResourceResolver):
        self.type_name = type_name
        self.func = func
        self.options_type = options_type
        self.result_type = result_type
        self.resource_resolver = resource_resolver

@dataclass
class TaskInfo(object):
    task: Task
    task_type: TaskType
    task_io_manager: TaskIOManager


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