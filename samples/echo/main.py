import pywaveai
from pydantic import BaseModel

class EchoTaskOptions(BaseModel):
    message: str
    files: dict

class EchoResult(BaseModel):
    message: str
    file_info: dict
    files: dict


worker =  pywaveai.WaveWorker()

def echo_task(task: EchoTaskOptions):

    return EchoResult(message=task.message, files=task.files, file_info={name: str(img) for name, img in task.files.items()})

worker.register_task(pywaveai.TaskType(
    task_type="echo",
    func=echo_task,
    options_type=EchoTaskOptions,
    result_type=EchoResult,
    resource_resolver=pywaveai.BasicImageFileResolver
))

if __name__ == "__main__":
    worker.run()