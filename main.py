import asyncio
import json
import logging
import os
from pathlib import Path
from typing import *
import owncloud
import telethon
from telethon.events import NewMessage
from telethon.tl.custom import Message

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


    async def load():
        global admin_id, api_id, api_hash, bot_token, cloud, auth_users
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
    downloads_path = Path(f'./downloads')
    up_lock_dict = {}
    down_lock_dict = {}


    @bot.on(NewMessage(pattern='/start'))
    async def start(event: NewMessage.Event):
        chatter = str(event.chat_id)
        if chatter not in auth_users.keys() and chatter != admin_id:
            return
        if 'username' not in auth_users[chatter].keys():
            await event.respond('Please type /login')
            return
        await event.respond('Send me a message and I will upload it to your owncloud server')


    @bot.on(NewMessage())
    async def file_handler(event: Union[NewMessage.Event, Message]):
        chatter = str(event.chat_id)
        if not event.file or event.sticker or event.voice:
            return
        if chatter not in auth_users.keys() and chatter != admin_id:
            return
        if not auth_users[chatter]['username']:
            await event.respond('Please type /login')
            return
        reply: Message = await event.reply('File queued')
        async with get_up_lock(chatter):
            downloaded_file = await real_upload(event=event, reply=reply)
        async with get_down_lock(chatter):
            await real_upload(downloaded_file, reply, event)


    def get_up_lock(user: str) -> asyncio.Lock:
        if not up_lock_dict.get(user):
            up_lock_dict[user] = asyncio.Lock()
        return up_lock_dict[user]

    def get_down_lock(user: str) -> asyncio.Lock:
        if not down_lock_dict.get(user):
            down_lock_dict[user] = asyncio.Lock()
        return down_lock_dict[user]

    async def real_download(event: Union[NewMessage.Event, Message], reply) -> str:
        if not event.file.name:
            async with bot.conversation(event.chat_id) as conv:
                s: Message = await conv.send_message('File has no filename. Please Provide one.'
                                                     '\nNote that extension is not needed.'
                                                     '\nThis option expires in 1 min.'
                                                     '\nYou can cancel using /cancel.')
                try:
                    resp: Message = await conv.get_response(s, timeout=60)
                    if resp.raw_text == '/cancel':
                        await s.edit('Cancelled')
                        return
                    else:
                        filename = f'{resp.raw_text}{event.file.ext}'
                        await s.edit(f'File name set to {filename}')
                except:
                    await s.edit('File name was never provided. File could not be processed.')
                    raise
        else:
            filename = event.file.name

        if filename in os.listdir(downloads_path):
            await reply.edit(f'{filename} already downloaded')
            return downloads_path.joinpath(filename)
        else:
            await reply.edit(f'{filename} being downloaded')

        try:
            downpath = await event.download_media(downloads_path.joinpath(filename))
            await reply.edit(f'{filename} downloaded')
        except:
            await reply.edit(f'{filename} could not be downloaded')
            raise
        return downpath

    async def real_upload(downpath: str, r: Message, event: Union[NewMessage.Event, Message]):
        filename = os.path.basename(downpath)
        uppath = '/TG Uploads' + filename
        user = auth_users[str(event.chat_id)]
        await r.edit(f'{filename} being uploaded')
        try:
            usercloud = owncloud.Client(cloud)
            await loop.run_in_executor(None, usercloud.login, user['username'], user['password'])
            files_list = await loop.run_in_executor(None, usercloud.list, '')
            if 'TG Uploads' not in [file.get_name() for file in files_list if file.is_dir()]:
                await loop.run_in_executor(None, usercloud.mkdir, 'TG Uploads')
            files_list = await loop.run_in_executor(None, usercloud.list, '/TG Uploads')
            file_cloud_name = os.path.basename(downpath)
            while file_cloud_name in [file.get_name() for file in files_list]:
                uppath += 'copy'
                file_cloud_name += 'copy'
            await loop.run_in_executor(None, usercloud.put_file, uppath, downpath)
            await loop.run_in_executor(None, usercloud.logout)

            await r.edit(f'{filename} uploaded correctly')
        except:
            await r.edit(f'{filename} could not be uploaded')
            raise


    @bot.on(NewMessage(pattern=r'/add_user_-?(\d+)'))
    async def add_user(event: Union[NewMessage.Event, Message]):
        chatter = str(event.chat_id)
        if chatter != admin_id:
            return
        user = event.pattern_match.group(1)
        auth_users[user] = {}
        await save_authusers()
        await event.respond('User added')


    @bot.on(NewMessage(pattern=r'/del_user_-?(\d+)'))
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
            await conv.send_message('Please send your nextcloud username')
            resp: Message = await conv.get_response(timeout=60)
            auth_users[chatter]['username'] = resp.raw_text
            await conv.send_message('Now send your password please')
            resp: Message = await conv.get_response(timeout=60)
            auth_users[chatter]['password'] = resp.raw_text
            await save_authusers()
            await conv.send_message('User saved correctly, you may start using the bot')


    @bot.on(NewMessage(pattern='/broadcast'))
    async def broadcast(event: Union[NewMessage, Message]):
        chatter = str(event.chat_id)
        if chatter != admin_id or event.reply_to_msg_id is None:
            return
        bc: Message = await event.get_reply_message()
        for user in auth_users.keys():
            if user != admin_id:
                await bot.send_message(int(user), message=bc)


    @bot.on(NewMessage(pattern='/save'))
    async def savexd(event: Union[Message, NewMessage]):
        c_id: int = event.chat_id
        m_id: int = event.reply_to_msg_id
        await event.respond(f'{c_id}, {m_id}')
        # await bot.edit_message(637898783, message=76, file='users/users.json')


    async def save_authusers():
        with open('users/users.json', 'w') as doc:
            json.dump(auth_users, doc)
        async with telethon.TelegramClient('me', api_id, api_hash) as me:
            await me.send_file(-525481046, file='users/users.json', caption='users')


    loop.run_forever()
