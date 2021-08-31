import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import *
import owncloud
import telethon
from telethon.events import NewMessage
from telethon.tl.custom import Message
from zipfile import PyZipFile
import wget

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
    zipping: bool


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
    zipping = False
    downloads_path = './downloads'

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


    @bot.on(NewMessage())
    async def file_handler(event: Union[NewMessage.Event, Message]):
        chatter = str(event.chat_id)
        if not event.file or event.sticker or event.voice or zipping:
            raise
        if chatter not in auth_users.keys():
            raise
        if not auth_users[chatter]['username']:
            await event.respond('Please type /login')
            raise
        reply: Message = await event.reply('File queued')
        async with get_down_lock(chatter):
            try:
                downloaded_file = await tg_download(event=event, reply=reply)
            except:
                return
        async with get_up_lock(chatter):
            try:
                await cloud_upload(downloaded_file, reply, event)
            except:
                return

    @bot.on(NewMessage(pattern=r'/link\s([^\s]+)(?:\s+\|\s+([^\s].*))?'))
    async def link_handler(event: Union[NewMessage, Message]):
        chatter = str(event.chat_id)
        if chatter not in auth_users.keys() or zipping:
            raise
        if not auth_users[chatter]['username']:
            await event.respond('Please type /login')
            raise
        url = event.pattern_match.group(1)
        try:
            filename = event.pattern_math.group(2)
        except:
            filename = None
        reply: Message = await event.reply('Link queued')
        async with get_down_lock(chatter):
            filepath = await url_download(reply, url, filename, downloads_path)
        async with get_up_lock(chatter):
            await cloud_upload(filepath, reply, event)


    @bot.on(NewMessage(pattern=r'/zip\s(.+)'))
    async def zip_handler(event: Union[NewMessage.Event, Message]):
        global zipping
        chatter = str(event.chat_id)
        if chatter not in auth_users.keys() or zipping:
            raise
        if not auth_users[chatter]['username']:
            await event.respond('Please type /login')
            raise
        folder = event.pattern_match.group(1)
        zipping = True
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
            zipping = False
            if m.raw_text.startswith('/cancel'):
                await conv.send_message('Ok, cancelled', reply_to=m)
                return
            await r.edit('Zip queued')
            filepaths: List[str] = []
            for mes in m_download_list:
                if not mes.file.name:
                    filename = str(m_download_list.index(mes)) + mes.file.ext
                else:
                    filename = mes.file.name
                async with get_down_lock(chatter):
                    filepaths.append(await tg_download(mes, r, filename=filename,
                                                       downpath=downloads_path + '/' + folder))
            zippath = downloads_path + '/' + folder + '.zip'
            with PyZipFile(zippath, 'a') as upzip:
                for path in filepaths:
                    upzip.write(path, folder + '/' + os.path.basename(path))
            async with get_up_lock(chatter):
                await cloud_upload(zippath, r, event)


    # endregion

    # region admin

    @bot.on(NewMessage(pattern=r'/add_user_(-?\d+)'))
    async def add_user(event: Union[NewMessage.Event, Message]):
        chatter = str(event.chat_id)
        if chatter != admin_id:
            return
        user = event.pattern_match.group(1)
        auth_users[user] = {}
        await save_authusers()
        await event.respond('User added')


    @bot.on(NewMessage(pattern=r'/del_user_(-?\d+)'))
    async def del_user(event: Union[NewMessage.Event, Message]):
        chatter = str(event.chat_id)
        if chatter != admin_id:
            return
        user = event.pattern_match.group(1);
        auth_users.pop(user)
        await save_authusers()
        await event.respond('User deleted')


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
                await save_authusers()
                await conv.send_message('User saved correctly, you may start using the bot')
            except:
                await conv.send_message('Login failed')


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


    def get_up_lock(user: str) -> asyncio.Lock:
        if not up_lock_dict.get(user):
            up_lock_dict[user] = asyncio.Lock()
        return up_lock_dict[user]


    def get_down_lock(user: str) -> asyncio.Lock:
        if not down_lock_dict.get(user):
            down_lock_dict[user] = asyncio.Lock()
        return down_lock_dict[user]

    async def tg_download(event: Union[NewMessage.Event, Message], reply, filename: str = None,
                          downpath=downloads_path) -> str:
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
        os.makedirs(downpath, exist_ok=True)
        if filename in os.listdir(downloads_path):
            await reply.edit(f'{filename} already downloaded')
            return str(downpath + '/' + filename)
        else:
            await reply.edit(f'{filename} being downloaded')

        try:
            filepath = await event.download_media(downpath + '/' + f'/{filename}')
            await reply.edit(f'{filename} downloaded')
        except:
            await reply.edit(f'{filename} could not be downloaded')
            raise
        return filepath


    async def cloud_upload(filepath: str, reply: Message, event: Union[NewMessage.Event, Message]):
        filename = os.path.basename(filepath)
        uppath = '/TG Uploads/' + filename
        user = auth_users[str(event.chat_id)]
        await reply.edit(f'{filename} being uploaded')
        try:
            usercloud = owncloud.Client(cloud)
            await loop.run_in_executor(None, usercloud.login, user['username'], user['password'])
            files_list = await loop.run_in_executor(None, usercloud.list, '')
            if 'TG Uploads' not in [file.get_name() for file in files_list if file.is_dir()]:
                await loop.run_in_executor(None, usercloud.mkdir, 'TG Uploads')
            files_list = await loop.run_in_executor(None, usercloud.list, '/TG Uploads')
            file_cloud_name = os.path.basename(filepath)
            while file_cloud_name in [file.get_name() for file in files_list]:
                uppath += 'copy'
                file_cloud_name += 'copy'
            await loop.run_in_executor(None, usercloud.put_file, uppath, filepath)
            await reply.edit(f'{filename} uploaded correctly')
            await loop.run_in_executor(None, usercloud.logout)

        except:
            await reply.reply(f'{filename} could not be uploaded')
            raise

    async def url_download(reply, url: str, filename: str = None, downpath: str = downloads_path) -> str:
        try:
            url_filename = wget.filename_from_url(url)
            url_ext = '.'.join(url_filename.split('.')[1:])
            if not filename:
                filename = url_filename
            else:
                filename += '.' + url_ext
            filepath = downpath + '/' + filename
            reply.edit(f'{filename} being downloaded')
            if os.path.exists(filepath):
                reply.edit(f'{filename} already downloaded')
                return filepath
            wget.download(url, filepath)
            reply.edit(f'{filename} downloaded')
            return filepath
        except:
            reply.edit('Cannot access url')
            raise


    async def save_authusers():
        with open('users/users.json', 'w') as doc:
            json.dump(auth_users, doc)
        async with telethon.TelegramClient('me', api_id, api_hash) as me:
            await me.send_file(-525481046, file='users/users.json', caption='users')


    # endregion


    @bot.on(NewMessage(pattern='/save'))
    async def savexd(event: Union[Message, NewMessage]):
        c_id: int = event.chat_id
        m_id: int = event.reply_to_msg_id
        await event.respond(f'{c_id}, {m_id}')


    loop.run_forever()
