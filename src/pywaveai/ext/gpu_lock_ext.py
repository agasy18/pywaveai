
import time
import logging
from pywaveai.task import TaskResult
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from os import getenv
from contextlib import contextmanager
from filelock import FileLock, Timeout as LockTimeout
from functools import wraps

logger = logging.getLogger(__name__)



class GPULockSettings(BaseSettings):
    GPU_LOCK_CHECK_INTERVAL: float = 0.1

    GPU_LOCK_TIMEOUT: float = 3 * 60

    USE_GPU_LOCK: bool = True

    GPU_LOCK_FILE: str = "/var/run/lock/gpu/0.lock"


    class Config:
        env_file = getenv("GPULOCK_CONFIG_FILE", ".env")

settings = GPULockSettings()


@contextmanager
def wait_for_gpu(settings):
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


def apply_extantion(task_info, func, settings=settings):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        with wait_for_gpu(settings=settings):
            end_time = time.time()
            res: TaskResult = func(*args, **kwargs)
        res.statistics['ld'] = end_time - start_time
        return res
    return wrapper

