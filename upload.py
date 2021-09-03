import asyncio
import os
import time
import math
from typing import List
from threading import Thread


def _put_file_chunked(self, remote_path, local_source_file, callback=None, **kwargs):
    """Uploads a file using chunks. If the file is smaller than
    ``chunk_size`` it will be uploaded directly.

    :param remote_path: path to the target file. A target directory can
    also be specified instead by appending a "/"
    :param local_source_file: path to the local file to upload
    :param \*\*kwargs: optional arguments that ``put_file`` accepts
    :returns: True if the operation succeeded, False otherwise
    :raises: HTTPResponseError in case an HTTP error status was returned
    """
    chunk_size = kwargs.get('chunk_size', 1024 * 1024)
    result = True
    transfer_id = int(time.time())

    remote_path = self._normalize_path(remote_path)
    if remote_path.endswith('/'):
        remote_path += os.path.basename(local_source_file)

    stat_result = os.stat(local_source_file)

    file_handle = open(local_source_file, 'rb', 8192)
    file_handle.seek(0, os.SEEK_END)
    size = file_handle.tell()
    file_handle.seek(0)

    headers = {}
    if kwargs.get('keep_mtime', True):
        headers['X-OC-MTIME'] = str(int(stat_result.st_mtime))

    if size == 0:
        return self._make_dav_request(
            'PUT',
            remote_path,
            data='',
            headers=headers
        )

    chunk_count = int(math.ceil(float(size) / float(chunk_size)))

    if chunk_count > 1:
        headers['OC-CHUNKED'] = '1'
    progress_list: List[asyncio.Task] = []
    for chunk_index in range(0, int(chunk_count)):
        data = file_handle.read(chunk_size)
        if chunk_count > 1:
            chunk_name = '%s-chunking-%s-%i-%i' % \
                         (remote_path, transfer_id, chunk_count,
                          chunk_index)
        else:
            chunk_name = remote_path
        time.sleep(1)
        if not self._make_dav_request(
                'PUT',
                chunk_name,
                data=data,
                headers=headers
        ):
            result = False
            break
        hilo = Thread(target=callback, args=(min(chunk_size * (chunk_index + 1), size), size))
        hilo.start()
    file_handle.close()
    return result