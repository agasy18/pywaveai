from pywaveai.http_task import HTTPTaskIOManager
from pywaveai.http_task import Task
from pywaveai.task import TaskResult
from pywaveai.testing import process_test_result
from pywaveai.runtime import RestartRequest
import json
import os
import logging
import sys

logger = logging.getLogger(__name__)

class TestFialdException(RestartRequest):
    def __init__(self, exception: Exception) -> None:
        super().__init__(exception=exception, code=18)


class TestIOManager(HTTPTaskIOManager):
    def __init__(self, result_dir: str) -> None:
        if not os.path.exists(result_dir):
            raise Exception(f"Test directory {result_dir} does not exist")

        self.tasks_to_complete = []
        super().__init__(result_dir)

    def load_inital_tasks(self) -> list[str, str, dict]:
        logger.info(f"Loading tasks from {self.result_dir}")
        dirs = os.listdir(self.result_dir)
        dirs = [d for d in dirs if os.path.isdir(f'{self.result_dir}/{d}')]
        logger.info(f"Loading {len(dirs)} tasks from {self.result_dir}")
        tasks = []
        for d in dirs:
            try:
                with open(f'{self.result_dir}/{d}/options.json') as f:
                    options = json.load(f)
            except Exception as e:
                logger.exception(e)
                logger.error(f"Failed to load options for task {d}")
                TestFialdException(e)
            tid = d
            task_type = d.split('-')[0]
            task = (task_type, tid, options)
            tasks.append(task)
            self.tasks_to_complete.append(tid)
            self._create_task(tid)
        
        return tasks

    async def mark_task_completed(self, task: Task, result: TaskResult):
        with open(f'{self.result_dir}/{task.id}/result.json', 'w') as f:
            f.write(result.model_dump_json(indent=2))
        
        r = await super().mark_task_completed(task, result)
        self._check_for_status(task)
        return r

    def _check_for_status(self, task: Task):
        if not process_test_result(task, f'{self.result_dir}/{task.id}'):
            raise TestFialdException(Exception(f"Test {task.id} failed"))
        self.tasks_to_complete.remove(task.id)
        if len(self.tasks_to_complete) == 0:
            logger.info("All tests passed")
            sys.exit(0)

    async def mark_task_failed(self, task: Task, error: Exception):
        with open(f'{self.result_dir}/{task.id}/error.json', 'w') as f:
            f.write(json.dumps({
                'detail': str(error)
            }))

        r = await super().mark_task_failed(task, error)
        self._check_for_status(task)
        return r
    
    def _create_task(self, task_id: str):
        self._rm_file(f'{self.result_dir}/{task_id}/result.json')
        self._rm_file(f'{self.result_dir}/{task_id}/error.json')

    async def create_task(self, task: Task):
        await super().create_task(task)
        self._create_task(task.id)
        


    def _rm_file(self, f):
        if os.path.exists(f):
            os.remove(f)
        