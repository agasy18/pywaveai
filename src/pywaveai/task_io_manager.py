from pywaveai.task import Task, TaskResult


import asyncio
import os
import tempfile
from abc import ABC


class TaskIOManager(ABC):
    async def mark_task_failed(self, task: Task, error: Exception):
        raise NotImplementedError()

    async def mark_task_completed(self, task: Task, result: TaskResult):
        raise NotImplementedError()

    async def download_bytes(self, task: Task, url: str) -> bytes:
        raise NotImplementedError()

    async def upload_bytes(self, task: Task, filename: str, byte_array: bytes) -> str:
        raise NotImplementedError()

    async def download_file(self, task: Task, url: str) -> str:
        byte_array = await self.download_bytes(task, url)
        ext = ''
        parts = url.split('?')[0].split('.')
        if len(parts) > 1:
            ext = parts[-1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext as f:
            await asyncio.to_thread(f.write, byte_array)

        return f.name

    async def remove_file(self, task: Task, file_path: str):
        os.remove(file_path)

    async def upload_file(self, task: Task, filename: str, file_path: str) -> str:
        with open(file_path, 'rb') as f:
            byte_array = await asyncio.to_thread(f.read)
        return await self.upload_bytes(task, filename, byte_array)
