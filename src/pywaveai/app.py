from pywaveai.scheduler import Scheduler
from pywaveai.task import TaskSource
from pywaveai.aic_api import AICTaskSource, settings as aic_settings
from pywaveai.http_api import HTTPTaskSource
from pywaveai.task import TaskExectionInfo, TaskResourceResolver
from fastapi import FastAPI
import uvicorn
from pydantic_settings import BaseSettings
from os import getenv
from pywaveai.ext import default_extantions
from contextlib import asynccontextmanager

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
        self.fastapi_app = FastAPI(title=settings.WORKER_TITLE,
                                   lifespan=self._lifespan)
        self.scheduler = None

    @asynccontextmanager
    async def _lifespan(self, app):
        self.build()
        await self.scheduler.start()
        yield
        await self.scheduler.stop()

    def register_task(self, name: str, func, resource_resolver: TaskResourceResolver):
        task_type = TaskExectionInfo(name + settings.DEPLOYMENT_POSTFIX,
                             func, resource_resolver)
        self.supported_tasks.append(task_type)

    def build(self):
        loaded_sources = [source(self.supported_tasks)
                                for source in self.sources]

        for source in loaded_sources:
            router = source.build_api_router()
            if router:
                self.fastapi_app.include_router(router)

        self.scheduler = self.scheduler_class(
            self.supported_tasks, loaded_sources, self.extensions)

    def run(self):
        uvicorn.run(self.fastapi_app,
                    host=settings.WORKER_HOST,
                    port=settings.WORKER_PORT,
                    log_level=settings.WORKER_LOG_LEVEL)
