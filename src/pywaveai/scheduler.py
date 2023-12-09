import asyncio

from filelock import FileLock
from lockfile import LockTimeout
from pywaveai.task import TaskType, TaskSource, TaskInfo
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from os import getenv
from contextlib import contextmanager
from functools import partial
from pywaveai.ext import apply_extantions

import sys

import time

import logging

logger = logging.getLogger(__name__)


class SchedulerSettings(BaseSettings):
    GPU_LOCK_CHECK_INTERVAL: float = 0.1

    GPU_LOCK_TIMEOUT: float = 3 * 60

    USE_GPU_LOCK: bool = True

    PROCESSING_QUEUE_SIZE: int = 1

    GPU_LOCK_FILE: str = "/var/run/lock/gpu/0.lock"


    class Config:
        env_file = getenv("SCHEDULER_CONFIG_FILE", ".env")

settings = SchedulerSettings()

class RestartRequest(SystemExit):
    def __init__(self, exception: Exception, code: int=1):
        super().__init__(code)
        self.exception = exception


@contextmanager
def wait_for_gpu():
    if not settings.USE_GPU_LOCK:
        yield
        return
    # 100ms due to inner loop will be 10ms, this is not actual timeout, but a delay
    lock = FileLock(settings.GPU_LOCK_FILE, timeout=settings.GPU_LOCK_CHECK_INTERVAL)
    time0 = time.time()
    while True:
        if time.time() - time0 > settings.GPU_LOCK_TIMEOUT:
            logger.warning(f"Failed to acquire GPU lock in {settings.GPU_LOCK_TIMEOUT} seconds, breaking the lock")
            lock.break_lock()
        try:
            # give it some time to release the lock in order to avoid busy loop
            time.sleep(settings.GPU_LOCK_CHECK_INTERVAL * 2)
            with lock:
                yield
                break
        except LockTimeout:
            continue


class Scheduler(object):
    def __init__(self, supported_tasks: list[TaskType], sources: list[TaskSource], executor_extensions: list[callable]):
        self.supported_tasks = supported_tasks
        self.sources = sources
        self.is_running = False
        self.executor_extensions = executor_extensions
        self.running_queue_tasks: asyncio.Task | None = None
        self.running_sources_tasks: dict[TaskSource, asyncio.Task] = {source: None for source in sources}
        
    
    async def fetch_from_sources(self, source: TaskSource):
        task_info = await source.fetch_task()
        if task_info is None:
            return None
        task = task_info.task
        task_type = task_info.task_type
        task_io_manager = task_info.task_io_manager
        try:
            logger.info(f"Got new task: {task.type} {task.id}")
            options_type: BaseModel = task_type.options
            task.options = options_type.model_validate(task.task.options)
            await task_info.resource_resolver.download_files(task, task_io_manager)
            return task_info
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.exception(e)
            self.report_an_error_in_bg(task_info, e)
        
        return None

    def start_fetching(self):
        for source, ftask in self.running_sources_tasks.items():
            if ftask is not None and ftask.done():
                # wait for the task to be processed
                return
        for source, ftask in self.running_sources_tasks.items():
            if ftask is None:
                self.running_sources_tasks[source] = asyncio.create_task(self.fetch_from_sources(source))

    async def fetch_once(self):
        for source, ftask in self.running_sources_tasks.items():
            if ftask is None:
                continue

            if ftask.done():
                self.running_sources_tasks[source] = None
                
                task_info = await ftask
                if task_info is None:
                    continue

                return task_info
        return None
    

    async def fetch_task(self):
        for retry in range(3):
            try:
                while True:
                        self.start_fetching()
                        task_info = await self.fetch_once()
                        if task_info is not None:
                            return task_info
                        await asyncio.sleep(0.001)
            except (KeyboardInterrupt, SystemExit, asyncio.CancelledError) as e:
                raise
            except Exception as e:
                logger.exception(e)
                await asyncio.sleep(5)
        else:
            logger.error("Failed to fetch task from sources, restarting")
            sys.exit(1)

    
    async def _execute_task(self, task_info: TaskInfo):
        call_f = apply_extantions(task_info, task_info.task_type.func, self.executor_extensions)
       
        result = await asyncio.to_thread(call_f, task_info.task.options)
        return result
    
    async def report_an_error(self, task_info: TaskInfo, e: Exception):
        return await task_info.task_io_manager.mark_task_failed(task_info.task, e)

    def report_an_error_in_bg(self, task_info: TaskInfo, e: Exception):
        asyncio.create_task(self.report_an_error(task_info, e))
        
    async def run_processing_queue(self):
        while True: 
            task_info = await self.fetch_task()
            try:
                assert task_info is not None
                self.start_fetching()
                result = await self._execute_task(task_info)
                asyncio.create_task(self.complete_task(task_info, result))
            except (KeyboardInterrupt, SystemExit, asyncio.CancelledError) as e:
                await self.report_an_error(task_info, e)
                raise
            except Exception as e:
                logger.exception(e)
                self.report_an_error_in_bg(task_info, e)
                await asyncio.sleep(1)

    async def complete_task(self, task_info: TaskInfo, result):
        if not isinstance(result, task_info.task_type.result_type):
            raise TypeError(f"Result is not of type {task_info.task_type.result} but {type(result)}")

    async def start(self):
        self.is_running = True
        self.running_queue_tasks = asyncio.gather(*[
            self.run_processing_queue() for _ in range(settings.PROCESSING_QUEUE_SIZE)
        ])

    async def stop(self):
        if self.running_queue_tasks is not None:
            self.running_queue_tasks.cancel()
            await self.running_queue_tasks




