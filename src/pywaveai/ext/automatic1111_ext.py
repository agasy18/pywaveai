import logging
logger = logging.getLogger(__name__)

try:
    from modules import devices
except ImportError:
    logger.warning('modules.devices not found, automatic1111_ext will not work')
    devices = None

def cleanup_memory():
    if devices is not None:
        devices.torch_gc()
