import asyncio

import aiohttp
from aiofiles.threadpool.binary import AsyncFileIO, AsyncBufferedIOBase


async def download_url(file: AsyncBufferedIOBase, url: str, total_size: int, callback=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as response:
            assert response.status == 200
            chunk_size = 1024
            current = 0
            chunk = await response.content.read(chunk_size)
            while chunk:
                await asyncio.sleep(1)
                await file.write(chunk)
                if callback:
                    current += len(chunk)
                    await callback(current, total_size)
                chunk = await response.content.read(chunk_size)


async def get_file_size(url: str):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0"
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.head(url) as response:
            total_size = response.headers.get('Content-Length')
            return total_size

