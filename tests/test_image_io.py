import pytest
from PIL import Image

from  pywaveai.image_io import image2bytes, bytes2image, BasicImageFileResolver

@pytest.fixture
def red_image():
    return Image.new('RGB', (60, 30), color = 'red')

@pytest.fixture
def resolver():
    return BasicImageFileResolver()

@pytest.mark.asyncio
async def test_image2bytes(red_image):
    byte_array = await image2bytes(red_image)
    assert isinstance(byte_array, bytes)
    assert len(byte_array) > 0

@pytest.mark.asyncio
async def test_bytes2image(red_image):
    byte_array = await image2bytes(red_image)
    img = await bytes2image(byte_array)
    assert img.size == red_image.size

@pytest.mark.asyncio
async def test_encode_to_bytes(red_image, resolver):
    name, filename, byte_array = await resolver.encode_to_bytes(None, 'test', red_image)
    assert name == 'test'
    assert filename == 'test.jpg'
    assert isinstance(byte_array, bytes)

@pytest.mark.asyncio
async def test_decode_from_bytes(red_image, resolver):
    name, filename, byte_array = await resolver.encode_to_bytes(None, 'test', red_image)
    name, img = await resolver.decode_from_bytes(None, name, byte_array)
    assert name == 'test'
    assert img.size == red_image.size