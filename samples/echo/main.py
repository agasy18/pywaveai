import pywaveai
from typing import Optional

class EchoTaskOptions(pywaveai.TaskOptions):
    message: str
    echo_error: Optional[str] = None


class EchoResult(pywaveai.TaskResult):
    message: str
    file_info: dict


worker =  pywaveai.WaveWorker()

def echo_task(task: EchoTaskOptions) -> EchoResult:
    if task.echo_error:
        raise Exception(task.echo_error)

    return EchoResult(message=task.message, files=task.files, file_info={name: str(img) for name, img in task.files.items()})

worker.register_task("echo", echo_task, resource_resolver=pywaveai.BasicImageFileResolver)

if __name__ == "__main__":
    worker.run()