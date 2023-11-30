from pywaveai.scheduler import Scheduler
from pywaveai.task_scheduler import TaskSource
from pywaveai.aic_api import AICTaskSource, settings as aic_settings
from pywaveai.http_api import HTTPTaskSource
from pywaveai.task import TaskType
from fastapi import FastAPI
import uvicorn
from pydantic_settings import BaseSettings
from os import getenv

import logging

level = getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=level)

logger = logging.getLogger(__name__)

class WaveWorkerSettings(BaseSettings):
    WORKER_HOST: str = "0.0.0.0"
    WORKER_PORT: int = 8000
    WORKER_LOG_LEVEL: str = "info"
    WORKER_RELOAD: bool = True
    WORKER_TITLE: str = aic_settings.NODE_ID | "WaveAI Worker"

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
                 sources: list[TaskSource] = _default_sources):
        self.scheduler_class = scheduler_class
        self.sources = sources
        self.supported_tasks = []
        self.fastapi_app = FastAPI(title=settings.WORKER_TITLE,
                                    lifespan=self._lifespan)
        self.scheduler = None

    async def _lifespan(self):
        await self.scheduler.start()
        yield
        await self.scheduler.stop()


    def register_task(self, task_type: TaskType):
        self.supported_tasks.append(task_type)

    def run(self):
        loaded_sources = [source(self.supported_tasks) for source in self.sources]
        
        for source in loaded_sources:
            router = source.build_api_router()
            if router:
                self.fastapi_app.include_router(router)

        self.scheduler = self.scheduler_class(self.supported_tasks, loaded_sources)

        uvicorn.run(self.fastapi_app, 
                    host=settings.WORKER_HOST, 
                    port=settings.WORKER_PORT, 
                    log_level=settings.WORKER_LOG_LEVEL, 
                    reload=settings.WORKER_RELOAD)

