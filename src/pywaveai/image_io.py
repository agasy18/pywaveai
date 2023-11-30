from PIL.Image import Image
from PIL.Image import open as open_image
import asyncio
from tempfile import NamedTemporaryFile
from .task import TaskResourceResolver


async def image2bytes(img: Image, extension=".jpg"):
    with NamedTemporaryFile(suffix=extension, mode='rb') as f:
        await asyncio.to_thread(img.save, f.name)
        byte_array = f.read()
        return byte_array
    

async def bytes2image(byte_array: bytes) -> Image:
    img = open_image(byte_array)
    return await asyncio.to_thread(img.load)


class BasicImageFileResolver(TaskResourceResolver):
    def __init__(self, extension=".jpg"):
        self.extension = extension

    async def encode_to_bytes(self, name: str, resource: object) -> tuple[str, str, bytes]:
        assert isinstance(resource, Image)
        image_bytes = await image2bytes(resource, extension=self.extension)
        return name, name+self.extension, image_bytes
    
    async def decode_from_bytes(self, name: str, byte_array: bytes) -> tuple[str, object]:
        img = await bytes2image(byte_array)
        return name, img
    
