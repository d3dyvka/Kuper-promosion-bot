from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from decouple import config

bot = Bot(config('BOT_TOKEN'))
dp = Dispatcher(storage=MemoryStorage())