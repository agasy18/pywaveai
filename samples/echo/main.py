import pywaveai
from typing import Any, Optional, Dict
from pydantic import Field
from PIL import Image
from pywaveai import download_image, upload_image

class EchoTaskOptions(pywaveai.TaskOptions):
    message: str
    echo_error: Optional[str] = None

    loaded_images: Dict[str, object] = Field(
        default_factory=dict, description="Images loaded by the task", exclude=True)


    async def __call__(self, task, task_io_manager):
        for name, url in self.files.items():
            if not url.endswith('.jpg') and not url.endswith('.jpeg') and not url.endswith('.png'):
                raise Exception("Only jpg, jpeg and png files are supported")
            img = await download_image(url, task, task_io_manager)
            self.loaded_images[name] = img

        return self



class EchoResult(pywaveai.TaskResult):
    message: str
    file_info: dict

    loaded_images: Dict[str, object] = Field(
        default_factory=dict, description="Images loaded by the task", exclude=True)

    async def __call__(self, task, task_io_manager):
        for name, img in self.loaded_images.items():
            source_url = task.options.files[name]
            ext = source_url.split('.')[-1]
            url = await upload_image('result_' + name + '.' + ext, img, task, task_io_manager)
            self.files[name] = url

        return self


worker =  pywaveai.WaveWorker()

def echo_task(task: EchoTaskOptions) -> EchoResult:
    if task.echo_error:
        raise Exception(task.echo_error)

    return EchoResult(message=task.message, loaded_images=task.loaded_images, file_info={name: str(img.size) for name, img in task.loaded_images.items()})

worker.register_task("echo", echo_task)

if __name__ == "__main__":
    worker.run()