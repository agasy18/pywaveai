from pywaveai.aic_api import fetch_new_task, get_downloader_client, get_uploader_client
from pywaveai.image_io import image2bytes
from .task import Task, TaskType
import asyncio
from logging import getLogger
from pydantic import BaseModel
from httpx import AsyncClient, RequestError
import os
import sys
from ext.torch_ext import TorchMemoryTracker
from ext.automatic1111_ext import cleanup_memory


from lockfile import FileLock, LockTimeout
import time

from io import BytesIO
# from prometheus_client import Summary



logger = getLogger(__name__)

async def run_preload_queue(processing_queue: asyncio.Queue, supported_tasks: dict[str, TaskType]):
    logger.info(f"Starting preload queue for {list(supported_tasks.keys())}")

    while True:
        try:
            task = await fetch_new_task(supported_tasks)
            if task is None:
                continue
            try:
                logger.info(f"Got new task: {task.type} {task.id}")
                options_type: BaseModel = supported_tasks[task.type].options
                task.options = options_type.validate(task.options)
                files = getattr(task.options, 'files', None)
                if isinstance(files, dict):
                    logger.info(f"Loading files for task {task.type} {task.id} into memory files: {list(files.keys())}")
                    
                    async def load_image_from_url(name, url):
                        response = await get_downloader_client().get(url)
                        response.raise_for_status()

                        img = open_image(BytesIO(response.content))
                        await asyncio.to_thread(img.load)

                        logger.info(f"Loaded {name} - {img}")

                        return name, img

                    task.options.files = dict(await asyncio.gather(*[load_image_from_url(name, url) for name, url in files.items()]))

            except Exception as e:
                await report_an_error(task, e)            
                continue
            await processing_queue.put(task)
        except RequestError as e:
            http_clents.clear()
            logger.exception(e)
            await asyncio.sleep(1)
        except Exception as e:
            logger.exception(e)
            await asyncio.sleep(1)
        

gpu_lock = FileLock("/var/run/lock/gpu/0.lock", timeout=0.1) # 100ms due to inner loop will be 10ms, this is not actual timeout, but a delay
GPU_TIMEOUT = int(os.environ.get("GPU_TIMEOUT", 120)) # 120 seconds
async def run_processing_queue(processing_queue: asyncio.Queue, upload_queue: asyncio.Queue, supported_tasks: dict[str, TaskType]):
    def func(*args, **kwargs):
        tl0 = time.time()
        while True:
            for i in range(GPU_TIMEOUT * 10): # total timeout is 120 seconds
                try:
                    time.sleep(0.02)
                    with gpu_lock:
                        torch_tracker = TorchMemoryTracker()
                        torch_tracker.reset_peak_memory_stats()
                        tl1 = time.time()
                        logger.info(f"Acquired GPU lock in {tl1-tl0} seconds")
                        t0 = time.time()
                        res = supported_tasks[task.type].func(*args, **kwargs)
                        t1 = time.time()
                        logger.info(f"Task {task.type} {task.id} took {t1-t0} seconds")
                        res = res.dict()
                        res['st'] = {
                            **torch_tracker.get_memory_stats(),
                            'ld': tl1-tl0,
                            'd': t1-t0,
                        }

                        logger.info(f"Stats: lock duration {res['st']['ld']}, duration {res['st']['d']}, max memory {res['st'].get('mx')}, min memory {res['st'].get('mn')}")
                        return res
                except LockTimeout:
                    continue
            else:
                logger.error(f"Failed to acquire GPU lock for task {task.type} {task.id}, breaking lock")
                gpu_lock.break_lock()
                

    while True:
        task = await processing_queue.get()
        try:
            result = await asyncio.to_thread(func, task.options)
        except Exception as e:
            if 'CUDA out of memor' in str(e):
                logger.exception(e)
                cleanup_memory()
                try:
                    # try again after garbage collection
                    result = await asyncio.to_thread(func, task.options)
                except Exception as e:
                    logger.exception(e)
                    if 'CUDA out of memor' in str(e):
                        # we need to report an error and restart the service
                        try:
                            await report_an_error(task, e)
                        finally:
                            logger.error("CUDA out of memory, restarting")
                            sys.exit(1)
            await report_an_error(task, e)
            continue
        await upload_queue.put((task, result))

async def run_upload_queue(upload_queue: asyncio.Queue):
    while True:
        task, result = await upload_queue.get()
        for retry in range(3):
            try:
                files = {}
                for name, image in result["files"].items():
                    extension = '.jpg'
                    
                    file_name = name+extension
                    response = await get_api_client().post(task.new_file, json={
                        "filename": name+extension,
                    })

                    response.raise_for_status()
                    file_url = response.json()['url']
                        
                    image_bytes = await image2bytes(image, extension=extension)
                    logger.debug(f"Uploading {file_name} to {file_url}")
                    response = await get_uploader_client().put(file_url, data=image_bytes)
                    response.raise_for_status()
                    files[name] = file_name

                        
                result["files"] = files
                break
            except RequestError as e:
                http_clents.clear()
                logger.exception(e)
                logger.warning(f"Retrying task {task.type} {task.id}")
                await asyncio.sleep(1)
            except Exception as e:
                await report_an_error(task, e)
        else:
            logger.error(f"Failed to upload files for task {task.type} {task.id} after 3 retries")
            continue
        for retry in range(3):
            try:
                response = await get_api_client().post(task.result, json=result)
                response.raise_for_status()
                break
            except RequestError as e:
                http_clents.clear()
                logger.exception(e)
                logger.warning(f"Retrying task {task.type} {task.id}")
                await asyncio.sleep(1)
            except Exception as e:
                await report_an_error(task, e)
                break
        else:
            logger.error(f"Failed to report result for task {task.type} {task.id} after 3 retries")
            continue
        logger.info(f"Task {task.type} {task.id} completed")


async def schedule_tasks(supported_tasks: dict[str, TaskType]):
    processing_queue = asyncio.Queue(int(os.environ.get("PROCESSING_QUEUE_SIZE", 1)))
    upload_queue = asyncio.Queue()

    queues = asyncio.gather(
        run_preload_queue(processing_queue, supported_tasks),
        run_processing_queue(processing_queue, upload_queue, supported_tasks),
        run_upload_queue(upload_queue),
    )

    