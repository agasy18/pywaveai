from . import automatic1111_ext
from . import torch_ext
from . import gpu_lock_ext
from . import gpu_time_ext


extantions = [
    # this must be first
    gpu_time_ext.apply_extantion,

    gpu_lock_ext.apply_extantion,
    torch_ext.apply_extantion,
    
    # this must be last
    automatic1111_ext.apply_extantion,
]


def apply_extantions(task_info, func, extantions):
    for ext in extantions:
        func = ext(task_info, func)
    return func


    