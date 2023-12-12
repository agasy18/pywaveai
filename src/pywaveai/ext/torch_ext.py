import logging
logger = logging.getLogger(__name__)
from functools import wraps



try:
    import torch
except ImportError:
    logger.warning('torch not found, TorchMemoryTracker will not work')
    torch = None




class TorchMemoryTracker(object):
    def reset_peak_memory_stats(self):
        if torch is not None:
            torch.cuda.reset_peak_memory_stats()

    def get_memory_stats(self):
        if torch is None:
            return {}
        
        return {
            'mx': torch.cuda.max_memory_reserved() // 1024 // 1024,
            'mn': torch.cuda.memory_reserved() // 1024 // 1024,
        }
    

def apply_extantion(task_info, func):
    if torch is None:
        return func

    @wraps(func)
    def wrapper(*args, **kwargs):
        
        torch_tracker = TorchMemoryTracker()
        torch_tracker.reset_peak_memory_stats()
        res = func(*args, **kwargs)
        res.statistics['mx'] = torch_tracker.get_memory_stats()['mx']
        res.statistics['mn'] = torch_tracker.get_memory_stats()['mn']
        return res
    return wrapper
    
    




        