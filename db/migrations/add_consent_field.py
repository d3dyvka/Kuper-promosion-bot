"""
Миграция для добавления поля consent_accepted в таблицу Users.
Выполнить: python -m db.migrations.add_consent_field
"""
import asyncio
from sqlalchemy import text
from db.db import init_engine, dispose_engine


async def add_consent_field():
    """Добавляет колонку consent_accepted в таблицу Users."""
    engine = init_engine()
    try:
        async with engine.begin() as conn:
            # Проверяем, существует ли колонка
            check_query = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='Users' AND column_name='consent_accepted'
            """)
            result = await conn.execute(check_query)
            exists = result.fetchone() is not None
            
            if not exists:
                # Добавляем колонку с значением по умолчанию False
                alter_query = text("""
                    ALTER TABLE "Users" 
                    ADD COLUMN consent_accepted BOOLEAN NOT NULL DEFAULT FALSE
                """)
                await conn.execute(alter_query)
                print("✓ Колонка consent_accepted успешно добавлена в таблицу Users")
            else:
                print("✓ Колонка consent_accepted уже существует в таблице Users")
    finally:
        await dispose_engine()


if __name__ == "__main__":
    asyncio.run(add_consent_field())
