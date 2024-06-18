import telegram

class Telegram:
    def __init__(self, token, chat_id):
        self.bot = telegram.Bot(token=token)
        self.chat_id = chat_id

    async def send_message(self, message):
        await self.bot.sendMessage(chat_id=self.chat_id, text=message)