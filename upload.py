import asyncio
import math
import re
import time
from typing import *
from typing import IO

from aiodav.exceptions import OptionNotValid, RemoteParentNotFound
from aiodav.urn import Urn
from aiofiles.threadpool.binary import AsyncBufferedIOBase


async def upload_to(
        self,
        path: Union[str, "os.PathLike[str]"],
        buffer: Union[IO, AsyncGenerator[bytes, None]],
        buffer_size: Optional[int] = None,
        progress: Optional[Callable[[int, int, Tuple], None]] = None,
        progress_args: Optional[Tuple] = ()
) -> None:
    """
    Uploads file from buffer to remote path on WebDAV server.
    More information you can find by link http://webdav.org/specs/rfc4918.html#METHOD_PUT

    Parameters:
        path (``str``):
            The path to remote resource

        buffer (``IO``)
            IO like object to read the data or a asynchronous generator to get buffer data.
            In order do you select use a async generator `progress` callback cannot be called.

        progress (``callable``, *optional*):
            Pass a callback function to view the file transmission progress.
            The function must take *(current, total)* as positional arguments (look at Other Parameters below for a
            detailed description) and will be called back each time a new file chunk has been successfully
            transmitted.

        progress_args (``tuple``, *optional*):
            Extra custom arguments for the progress callback function.
            You can pass anything you need to be available in the progress callback scope.

    Other Parameters:
        current (``int``):
            The amount of bytes transmitted so far.

        total (``int``):
            The total size of the file.

        *args (``tuple``, *optional*):
            Extra custom arguments as defined in the ``progress_args`` parameter.
            You can either keep ``*args`` or add every single extra argument in your function signature.

    Example:
        .. code-block:: python

            ...
            # Keep track of the progress while uploading
            def progress(current, total):
                print(f"{current * 100 / total:.1f}%")

            async with aiofiles.open('file.zip', 'rb') as file:
                await client.upload_to('/path/to/file.zip', file, progress=progress)
            ...
    """

    urn = Urn(path)
    if urn.is_dir():
        raise OptionNotValid(name="path", value=path)

    if not (await self.exists(urn.parent())):
        raise RemoteParentNotFound(urn.path())

    headers = {}
    if callable(progress) and not asyncio.iscoroutinefunction(buffer):

        async def file_sender(buff: IO):
            current = 0

            if asyncio.iscoroutinefunction(progress):
                await progress(current, buffer_size, *progress_args)
            else:
                progress(current, buffer_size, *progress_args)

            while current < buffer_size:
                chunk = await buff.read(self._chunk_size) if isinstance(buff, AsyncBufferedIOBase) \
                    else buff.read(self._chunk_size)
                if not chunk:
                    break
                current += len(chunk)

                if asyncio.iscoroutinefunction(progress):
                    await progress(current, buffer_size, *progress_args)
                else:
                    progress(current, buffer_size, *progress_args)
                yield chunk
        chunk_count = int(math.ceil(float(buffer_size) / float(self._chunk_size)))
        transfer_id = int(time.time())
        chunk_index = 0
        async for chunk in file_sender(buffer):
            if chunk_count > 1:
                headers['OC-CHUNKED'] = '1'
                chunk_name = '%s-chunking-%s-%i-%i' % \
                             (urn.quote(), transfer_id, chunk_count,
                              chunk_index)
                chunk_index += 1
            else:
                chunk_name = urn.quote()
            try:
                await self._execute_request(action='upload', path=chunk_name, data=chunk, headers_ext=headers)
            except Exception as e:
                if re.match(r'Request to .+ failed with code (504) and message: .+', str(e)):
                    return
                raise e
