from PIL.Image import Image
from PIL.Image import open as open_image
import asyncio
from tempfile import NamedTemporaryFile
from .task_io_manager import TaskIOManager
from .task import Task
from io import BytesIO
from os import path

async def image2bytes(img: Image, extension=".jpg"):
    with NamedTemporaryFile(suffix=extension, mode='w+b') as f:
        await asyncio.to_thread(img.save, f.name)
        f.seek(0)
        byte_array = f.read()
        return byte_array

async def bytes2image(byte_array: bytes) -> Image:
    stream = BytesIO(byte_array)
    img = open_image(stream)
    await asyncio.to_thread(img.load)
    return img

async def download_image(url: str, task: Task, task_io_manager: TaskIOManager) -> Image:
    byte_array = await task_io_manager.download_bytes(task, url)
    return await bytes2image(byte_array)


async def upload_image(filename: str, img: Image, task: Task, task_io_manager: TaskIOManager) -> str:
    extension = path.splitext(filename)[1]
    byte_array = await image2bytes(img, extension)
    return await task_io_manager.upload_bytes(task, filename, byte_array)

