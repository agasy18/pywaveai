from pywaveai.scheduler import Scheduler
from pywaveai.runtime import TaskSource
from pywaveai.aic_api import AICTaskSource, settings as aic_settings
from pywaveai.http_api import HTTPTaskSource
from pywaveai.runtime import TaskExectionInfo
from fastapi import FastAPI
import uvicorn
from pydantic_settings import BaseSettings
from os import getenv
from pywaveai.ext import default_extantions
from contextlib import asynccontextmanager
from inspect import signature
from pywaveai.task import TaskResult, TaskOptions
from .worker_api import worker_api

import logging

level = getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=level)

logger = logging.getLogger(__name__)


class WaveWorkerSettings(BaseSettings):
    WORKER_HOST: str = "0.0.0.0"
    WORKER_PORT: int = 8000
    WORKER_LOG_LEVEL: str = "info"
    WORKER_RELOAD: bool = True
    WORKER_TITLE: str = aic_settings.NODE_ID or "WaveAI Worker"

    DEPLOYMENT_POSTFIX: str = '_dev'

    class Config:
        env_file = getenv("WAVE_WORKER_CONFIG_FILE", ".env")


settings = WaveWorkerSettings()


_default_sources = [
    HTTPTaskSource
]

if aic_settings.API_BASE_URL and aic_settings.API_KEY and aic_settings.NODE_ID:
    _default_sources.append(AICTaskSource)
else:
    logger.warning("AIC API is not configured, AICTaskSource will not be used")


DefaultScheduler = Scheduler


class WaveWorker(object):
    def __init__(self,
                 scheduler_class=DefaultScheduler,
                 sources: list[TaskSource] = _default_sources,
                 extensions: list = default_extantions):
        self.scheduler_class = scheduler_class
        self.sources = sources
        self.extensions = extensions
        self.supported_tasks = []
        
                

    @asynccontextmanager
    async def _lifespan(self, app):

        loaded_sources = [source(self.supported_tasks)
                                for source in self.sources]
        for source in loaded_sources:
            router = source.build_api_router()
            if router:
                app.include_router(router)

        scheduler = self.scheduler_class(
            self.supported_tasks, loaded_sources, self.extensions)
        
        await scheduler.start()
        yield
        await scheduler.stop()

    def build_http_api(self):
        app = FastAPI(title=settings.WORKER_TITLE,
                                   lifespan=self._lifespan)
        return app


    def register_task(self, name: str,
                      func: callable,
                      options_type: type = None,
                      result_type: type = None,
                      documantation: str = None,
                      preprocessing_func: callable = None,
                      postprocessing_func: callable = None):

        if not callable(func):
            raise TypeError(
                f"Task function must be callable instead of {func}")

        if result_type is None:
            result_type = signature(func).return_annotation

        if not (issubclass(result_type, TaskResult) or result_type == TaskResult):
            raise TypeError(
                f"Task result type must be a subclass of TaskResult instead of {result_type}")

        if len(signature(func).parameters) != 1:
            raise TypeError("Task function must have exactly one parameter")

        if options_type is None:
            options_type = next(
                iter(signature(func).parameters.values())).annotation

        if not (issubclass(options_type, TaskOptions) or options_type == TaskOptions):
            raise TypeError(
                f"Task options type must be a subclass of TaskOptions instead of {options_type}")

        if hasattr(options_type, '__call__') and preprocessing_func is None:
            preprocessing_func = options_type.__call__

        if hasattr(result_type, '__call__') and postprocessing_func is None:
            postprocessing_func = result_type.__call__

        if preprocessing_func is not None and not callable(preprocessing_func):
            raise TypeError(
                f"Task preprocessing function must be async callable instead of {preprocessing_func}")

        if postprocessing_func is not None and not callable(postprocessing_func):
            raise TypeError(
                f"Task postprocessing function must be async callable instead of {postprocessing_func}")

        if documantation is None:
            if func.__doc__ is None:
                documantation = f'No documentation for {func.__name__}'
            else:
                documantation = func.__doc__

        task_type = TaskExectionInfo(
            task_name=name + settings.DEPLOYMENT_POSTFIX, 
            func=func,
            options_type=options_type,
            result_type=result_type,
            documantation=documantation,
            preprocessing_func=preprocessing_func,
            postprocessing_func=postprocessing_func
        )
                                     
        self.supported_tasks.append(task_type)

        
    def run(self, app_path: str = None):
        """
        Run the worker
        if app_path is None, the worker will run with uvicorn with reload option 
        
        if app_path is provided, the worker will run with uvicorn without reload option
        Example:
        # app/main.py
        app = worker.build_http_api()
        worker.run('app.main:app')
        """
        
        if app_path is None:
            logger.info("Starting worker, without reloading option, read worker.run() docs for more info")
            app = self.build_http_api()
            uvicorn.run(app,
                        host=settings.WORKER_HOST,
                        port=settings.WORKER_PORT,
                        log_level=settings.WORKER_LOG_LEVEL)

        else:
            logger.info("Starting worker, with reloading option")
            uvicorn.run(app_path,
                        host=settings.WORKER_HOST,
                        port=settings.WORKER_PORT,
                        log_level=settings.WORKER_LOG_LEVEL,
                        reload=settings.WORKER_RELOAD)


    def generate_api(self):
        return worker_api(self.supported_tasks)
