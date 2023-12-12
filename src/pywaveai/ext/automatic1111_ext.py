import asyncio
import logging
import sys
from pywaveai.runtime import RestartRequest
logger = logging.getLogger(__name__)
from functools import wraps




try:
    from modules import devices
except ImportError:
    logger.warning('modules.devices not found, automatic1111_ext will not work')
    devices = None



def apply_extantion(task_info, func):
    if devices is None:
        return func
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        
        last_error = None
        for retry in range(3):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if 'CUDA out of memor' in str(e):
                    last_error = e
                    logger.exception(e)
                    logger.error(f"CUDA out of memory, cleaning up memory and retrying task {task_info.task_type} {task_info.task.id} retry {retry}")
                    cleanup_memory()
                else:
                    raise
        else:
            logger.error("CUDA out of memory, restarting")
            raise RestartRequest(last_error)
    return wrapper
    

def cleanup_memory():
    if devices is not None:
        devices.torch_gc()
