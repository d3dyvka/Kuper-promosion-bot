#!/usr/bin/env python3
"""
Скрипт для удаления всех пользователей из базы данных.
Использование: python delete_all_users.py
"""
import asyncio
import logging
from db.db import init_engine, dispose_engine
from db.crud import delete_all_users, get_all_users

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Удаляет всех пользователей из БД"""
    try:
        init_engine()
        
        # Сначала показываем количество пользователей
        users = await get_all_users()
        count_before = len(users)
        logger.info(f"Найдено пользователей в БД: {count_before}")
        
        if count_before == 0:
            logger.info("В базе данных нет пользователей для удаления")
            return
        
        # Подтверждение
        print(f"\n⚠️  ВНИМАНИЕ: Будет удалено {count_before} пользователей из базы данных!")
        response = input("Продолжить? (yes/no): ")
        
        if response.lower() not in ('yes', 'y', 'да', 'д'):
            logger.info("Операция отменена")
            return
        
        # Удаляем всех пользователей
        deleted_count = await delete_all_users()
        logger.info(f"✅ Успешно удалено пользователей: {deleted_count}")
        
        # Проверяем результат
        users_after = await get_all_users()
        count_after = len(users_after)
        logger.info(f"Пользователей в БД после удаления: {count_after}")
        
    except Exception as e:
        logger.exception(f"Ошибка при удалении пользователей: {e}")
    finally:
        await dispose_engine()


if __name__ == "__main__":
    asyncio.run(main())
