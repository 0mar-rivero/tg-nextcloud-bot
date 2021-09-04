import asyncio
import datetime
import json
import logging
import os
from datetime import timezone, timedelta, datetime
from functools import partial
from pathlib import Path
from typing import *
from urllib import request
from zipfile import PyZipFile
from zipstream import AioZipStream

import aiofiles
import telethon
from aiodav import Client
from telethon.events import NewMessage
from telethon.tl.custom import Message

import upload
from download import download_url

logging.basicConfig(format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
                    level=logging.WARNING)

if __name__ == '__main__':

    loop = asyncio.get_event_loop()

    cloud: str
    admin_id: str
    api_id: int
    api_hash: str
    bot_token: str
    auth_users: dict
    zipping: dict


    async def load():
        global admin_id, api_id, api_hash, bot_token, cloud, auth_users
        if "env.json" in os.listdir('.'):
            with open('env.json', 'r') as envdoc:
                env: dict = json.load(envdoc)
            admin_id = env['ADMIN']
            api_id = int(env['API_ID'])
            api_hash = env['API_HASH']
            bot_token = env['BOT_TOKEN']
            cloud = env['CLOUD']
        else:
            admin_id = os.getenv('ADMIN')
            api_id = int(os.getenv('API_ID'))
            api_hash = os.getenv('API_HASH')
            bot_token = os.getenv('BOT_TOKEN')
            cloud = os.getenv('CLOUD')
        global auth_users
        async with telethon.TelegramClient('me', api_id, api_hash) as me:
            m: Message
            async for message in me.iter_messages(-525481046):
                m = message
                break
            file = await me.download_media(m, 'users/users.json')
            with open(file, 'r') as doc:
                auth_users = json.load(doc)


    loading = asyncio.get_event_loop().run_until_complete(load())
    bot = telethon.TelegramClient('bot', api_id=api_id, api_hash=api_hash).start(bot_token=bot_token)
    up_lock_dict = {}
    down_lock_dict = {}
    zipping = {}
    upload_path = '/TG Uploads/'
    slow_time = 5


    # region users

    @bot.on(NewMessage(pattern='/start'))
    async def start(event: NewMessage.Event):
        chatter = str(event.chat_id)
        if chatter not in auth_users.keys() and chatter != admin_id:
            return
        if 'username' not in auth_users[chatter].keys():
            await event.respond('Please type /login')
            return
        await event.respond('Send me a file and I will upload it to your owncloud server')


    @bot.on(NewMessage(pattern=r'/login'))
    async def login(event: Union[NewMessage, Message]):
        chatter = str(event.chat_id)
        if chatter not in auth_users.keys() and chatter != admin_id:
            return

        async with bot.conversation(event.chat_id) as conv:
            try:
                await conv.send_message('Please send your nextcloud username')
                resp: Message = await conv.get_response(timeout=60)
                auth_users[chatter]['username'] = resp.raw_text
                await conv.send_message('Now send your password please')
                resp: Message = await conv.get_response(timeout=60)
                auth_users[chatter]['password'] = resp.raw_text
                await save_auth_users()
                await conv.send_message('User saved correctly, you may start using the bot')
            except:
                await conv.send_message('Login failed')


    @bot.on(NewMessage())
    async def file_handler(event: Union[NewMessage.Event, Message]):
        chatter = str(event.chat_id)
        if not event.file or event.sticker or event.voice or zipping.get(chatter):
            return
        if chatter not in auth_users.keys():
            return
        if not auth_users[chatter]['username']:
            await event.respond('Please type /login')
            return
        reply: Message = await event.reply('File queued')
        async with get_down_lock(chatter):
            try:
                downloaded_file = await tg_download(event=event, reply=reply, download_path=get_down_path(chatter))
            except:
                return
        async with get_up_lock(chatter):
            try:
                await cloud_upload(downloaded_file, reply, event)
            except Exception as exc:
                raise exc


    @bot.on(NewMessage(pattern=r'/link\s([^\s]+)(?:\s+\|\s+)?([^\s].*)?'))
    async def link_handler(event: Union[NewMessage, Message]):
        chatter = str(event.chat_id)
        if chatter not in auth_users.keys() or zipping.get(chatter):
            raise
        if not auth_users[chatter]['username']:
            await event.respond('Please type /login')
            raise
        url = event.pattern_match.group(1)
        filename = None
        try:
            if not event.pattern_match.group(2).strip():
                filename = str(event.pattern_match.group(2)).strip()
        except:
            filename = None
        reply: Message = await event.respond(f'{filename if filename else url} queued')
        async with get_down_lock(chatter):
            filepath = await url_download(reply, url, filename, get_down_path(chatter))
        async with get_up_lock(chatter):
            await cloud_upload(filepath, reply, event)


    @bot.on(NewMessage(pattern=r'/zip\s(.+)'))
    async def zip_handler(event: Union[NewMessage.Event, Message]):
        global zipping
        chatter = str(event.chat_id)
        if chatter not in auth_users.keys() or zipping.get(chatter):
            raise
        if not auth_users[chatter]['username']:
            await event.respond('Please type /login')
            raise
        zip_name = event.pattern_match.group(1)
        folder_path: Path = get_down_path(chatter).joinpath(zip_name)
        zipping[chatter] = True
        try:
            async with bot.conversation(event.chat_id) as conv:
                r: Message = await conv.send_message('Start sending me files and i\'ll zip and upload them'
                                                     '\n/stop to start zipping\n/cancel to cancel', reply_to=event)
                m: Message = await conv.get_response()
                m_download_list: List[Message] = []
                while not m.raw_text.startswith(('/cancel', '/stop')):
                    if not m.file or m.sticker or m.voice:
                        m = await conv.get_response()
                        continue
                    m_download_list.append(m)
                    m = await conv.get_response()
                zipping[chatter] = False
                if m.raw_text.startswith('/cancel'):
                    await conv.send_message('Ok, cancelled', reply_to=m)
                    return
                await r.edit('Zip queued')
                files: List[{}] = []
                for mes in m_download_list:
                    if not mes.file.name:
                        filename = str(m_download_list.index(mes)) + mes.file.ext
                    else:
                        filename = mes.file.name
                    async with get_down_lock(chatter):
                        files.append({'file': await tg_download(mes, r, filename=filename,
                                                                download_path=folder_path)})
                zip_path = str(folder_path)+'.zip'
                await r.edit('Zipping...')
                await zip_async(zip_path, files, slow(slow_time)(partial(refresh_progress_status, zip_name, r, 'Zipped')))
                await r.edit(f'{zip_name} zipped')
                async with get_up_lock(chatter):
                    await cloud_upload(zip_path, r, event)
        except:
            zipping[chatter] = False
            raise


    # endregion

    # region admin

    @bot.on(NewMessage(pattern=r'/add_user_(-?\d+)'))
    async def add_user(event: Union[NewMessage.Event, Message]):
        chatter = str(event.chat_id)
        if chatter != admin_id:
            return
        user = event.pattern_match.group(1)
        auth_users[user] = {}
        await save_auth_users()
        await event.respond('User added')


    @bot.on(NewMessage(pattern=r'/del_user_(-?\d+)'))
    async def del_user(event: Union[NewMessage.Event, Message]):
        chatter = str(event.chat_id)
        if chatter != admin_id:
            return
        user = event.pattern_match.group(1);
        auth_users.pop(user)
        await save_auth_users()
        await event.respond('User deleted')


    @bot.on(NewMessage(pattern='/broadcast'))
    async def broadcast(event: Union[NewMessage, Message]):
        chatter = str(event.chat_id)
        if chatter != admin_id or event.reply_to_msg_id is None:
            return
        bc: Message = await event.get_reply_message()
        for user in auth_users.keys():
            try:
                if user != admin_id:
                    await bot.send_message(int(user), message=bc)
            except:
                continue


    # endregion

    # region funcs

    async def tg_download(event: Union[NewMessage.Event, Message], reply, filename: str = None,
                          download_path: Path = Path('./downloads')) -> str:
        if not filename:
            if not event.file.name:
                async with bot.conversation(event.chat_id) as conv:
                    s: Message = await conv.send_message('File has no filename. Please Provide one.'
                                                         '\nNote that extension is not needed.'
                                                         '\nThis option expires in 1 min.'
                                                         '\nYou can cancel using /cancel.')
                    e = Exception()
                    try:
                        resp: Message = await conv.get_response(s, timeout=60)
                        if resp.raw_text == '/cancel':
                            await s.edit('Cancelled')
                            e = Exception('que loco')
                            raise
                        else:
                            filename = f'{resp.raw_text}{event.file.ext}'
                            await s.edit(f'File name set to {filename}')
                    except Exception as efe:
                        if efe is e:
                            raise
                        await s.edit('File name was never provided. File could not be processed.')
                        raise
            else:
                filename = event.file.name
        os.makedirs(download_path, exist_ok=True)
        if filename in os.listdir(download_path):
            await reply.edit(f'{filename} already downloaded')
            return str(download_path.joinpath(filename))
        else:
            await reply.edit(f'{filename} being downloaded')

        try:
            filepath = await event.download_media(download_path, progress_callback=slow(slow_time)(
                partial(refresh_progress_status, filename, reply, 'Downloaded')))
            await reply.edit(f'{filename} downloaded')
        except Exception as exc:
            print(exc)
            await reply.edit(f'{filename} could not be downloaded\n{exc}')
            raise
        return filepath


    async def cloud_upload(filepath: str, reply: Message, event: Union[NewMessage.Event, Message]):
        filename = os.path.basename(filepath)
        uppath = upload_path + filename
        user = auth_users[str(event.chat_id)]
        await reply.edit(f'{filename} being uploaded')

        try:
            async with Client('https://nube.uclv.cu/remote.php/webdav', login=user['username'],
                              password=user['password'], chunk_size=1024 * 1024) as cloud_client:
                if not await cloud_client.exists(upload_path):
                    await cloud_client.create_directory(upload_path)
                file_cloud_name = filename
                while await cloud_client.exists(upload_path + file_cloud_name):
                    uppath += 'copy'
                    file_cloud_name += 'copy'
                async with aiofiles.open(filepath, 'rb') as file:
                    await upload.upload_to(cloud_client, path=uppath, buffer=file,
                                           buffer_size=os.path.getsize(filepath),
                                           progress=slow(slow_time)(partial(refresh_progress_status, filename,reply, 'Uploaded')))
                await reply.edit(f'{filename} uploaded correctly')
        except Exception as exc:
            print(exc)
            await reply.edit(f'{filename} could not be uploaded')
            raise exc


    async def url_download(reply, url: str, filename: str = None, download_path: Path = Path('./downloads')) -> str:
        try:
            req = request.Request(url)
            req.add_header("User-Agent",
                           "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0")
            try:
                httpResponse = request.urlopen(req)
            except:
                await reply.edit('An error occurred accessing the url')
                raise
            path = Path(httpResponse.url)
            if not filename:
                if str(httpResponse.status).startswith('2'):
                    if not path.name or not path.suffix:
                        reply.edit('File has no file name please provide one in the link upload request\n'
                                   '/link <link> | <name>')
                        raise Exception('No file name')
                    else:
                        filename = str(path.name)
                else:
                    await reply.edit(f'Request error with code {httpResponse.status}.')
                    raise Exception(f'Request error with code {httpResponse.status}.')
            if '.' not in filename:
                filename = filename + ''.join(path.suffixes)
            file_size = httpResponse.length
            if not file_size:
                await reply.edit("Invalid file, has no filesize")
                raise Exception("Invalid file, has no filesize")
            await reply.edit(f'Downloading {filename}')
            async with aiofiles.open(download_path.joinpath(filename), 'wb') as o_file:
                await download_url(o_file, url, file_size,
                                   callback=slow(slow_time)(partial(refresh_progress_status, filename, reply, 'Downloaded')))
            await reply.edit("Link downloaded")
            return str(download_path.joinpath(filename))
        except Exception as e:
            await reply.respond(str(e))
            await reply.edit('Cannot access url')
            raise


    async def save_auth_users():
        with open('users/users.json', 'w') as doc:
            json.dump(auth_users, doc)
        async with telethon.TelegramClient('me', api_id, api_hash) as me:
            await me.send_file(-525481046, file='users/users.json', caption='users')


    def get_up_lock(user: str) -> asyncio.Lock:
        if not up_lock_dict.get(user):
            up_lock_dict[user] = asyncio.Lock()
        return up_lock_dict[user]


    def get_down_lock(user: str) -> asyncio.Lock:
        if not down_lock_dict.get(user):
            down_lock_dict[user] = asyncio.Lock()
        return down_lock_dict[user]


    def get_down_path(user: str) -> Path:
        os.makedirs(f'./downloads/{user}', exist_ok=True)
        return Path(f'./downloads/{user}/')


    def slow(secs):
        def dec(f):
            t = {'last_update': datetime.now(timezone.utc) - timedelta(minutes=1)}

            async def wrapper(*args, **kwargs):
                now = datetime.now(timezone.utc)
                if now - t['last_update'] < timedelta(seconds=secs):
                    return
                t['last_update'] = now
                return await f(*args, **kwargs)

            return wrapper

        return dec


    async def refresh_progress_status(name: str, reply: Message, operation: str, transferred_bytes: int, total_bytes: int):
        try:
            await reply.edit(
                f"{name}:\n"
                f"{operation} {sizeof_fmt(transferred_bytes)} out of {sizeof_fmt(total_bytes)}"
                f"(\n{round(transferred_bytes * 100 / total_bytes, 2)}%)")
        except Exception as exc:
            print(exc)
            return


    async def zip_async(zipname, files, callback=None):
        chunk_size = 32 * 1024
        aio_zip = AioZipStream(files, chunksize=chunk_size)
        size = 0
        current = 0
        for file in files:
            size += os.path.getsize(file['file'])
        async with aiofiles.open(zipname, mode='wb') as z:
            async for chunk in aio_zip.stream():
                if callback:
                    current += len(chunk)
                    await callback(chunk, size)
                await z.write(chunk)


    def sizeof_fmt(num, suffix='B'):
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.00
        return "%.1f%s%s" % (num, 'Yi', suffix)


    # endregion

    @bot.on(NewMessage(pattern='/save'))
    async def savexd(event: Union[Message, NewMessage]):
        c_id: int = event.chat_id
        m_id: int = event.reply_to_msg_id
        await event.respond(f'{c_id}, {m_id}')


    loop.run_forever()
