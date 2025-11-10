import asyncio
import logging
from aiogram.types import BotCommand

from db.db import init_engine, dispose_engine, current_loop_id
from db.create_tables import create_all
from create_bot import bot as bot_instance, dp as dispatcher
from handlers.user_handlers import urouter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

async def main():
    logger.info("main loop id = %s", current_loop_id())
    init_engine()
    dispatcher.include_router(urouter)
    await create_all()
    try:
        await bot_instance.set_my_commands([BotCommand(command="start", description="Start bot")])
    except Exception:
        logger.exception("Can't set commands")

    try:
        logger.info("Start polling")
        await dispatcher.start_polling(bot_instance)
    finally:
        logger.info("Shutting down, disposing engine")
        await dispose_engine()
        await bot_instance.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
