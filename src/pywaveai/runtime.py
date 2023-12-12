from abc import ABC
from dataclasses import dataclass

from typing import List, Optional
from pywaveai.task_io_manager import TaskIOManager

from pywaveai.task import Task


@dataclass
class TaskExectionInfo(object):
    task_name: str
    func: callable
    options_type: type
    result_type: type
    documantation: str
    preprocessing_func: callable = None
    postprocessing_func: callable = None


@dataclass
class TaskInfo(object):
    task: Task
    execution_info: TaskExectionInfo
    task_io_manager: TaskIOManager

    @property
    def resource_resolver(self):
        return self.execution_info.resource_resolver


class TaskSource(ABC):
    def __init__(self, supported_tasks: List[TaskExectionInfo], **kwargs) -> None:
        self.supported_tasks = supported_tasks

    async def fetch_task(self) -> Optional[TaskInfo]:
        raise NotImplementedError()

    def build_api_router(self):
        """
        Build a FastAPI router for this task source
        """
        return None


class RestartRequest(SystemExit):
    def __init__(self, exception: Exception, code: int=1):
        super().__init__(code)
        self.exception = exception