"""
Миграция для добавления таблицы Statistics для отслеживания переходов по ссылкам.
Выполнить: python -m db.migrations.add_statistics_table
"""
import asyncio
from sqlalchemy import text
from db.db import init_engine, dispose_engine


async def add_statistics_table():
    """Создает таблицу Statistics для хранения статистики переходов по ссылкам."""
    engine = init_engine()
    try:
        async with engine.begin() as conn:
            # Проверяем, существует ли таблица
            check_query = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name='Statistics'
            """)
            result = await conn.execute(check_query)
            exists = result.fetchone() is not None
            
            if not exists:
                # Создаем таблицу
                create_table_query = text("""
                    CREATE TABLE "Statistics" (
                        id SERIAL PRIMARY KEY,
                        phone VARCHAR(40) NOT NULL,
                        tg_id BIGINT NOT NULL,
                        link_param VARCHAR(100) NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)
                await conn.execute(create_table_query)
                
                # Создаем индексы
                create_index_phone = text("""
                    CREATE INDEX IF NOT EXISTS ix_statistics_phone ON "Statistics"(phone)
                """)
                await conn.execute(create_index_phone)
                
                create_index_tg_id = text("""
                    CREATE INDEX IF NOT EXISTS ix_statistics_tg_id ON "Statistics"(tg_id)
                """)
                await conn.execute(create_index_tg_id)
                
                print("✓ Таблица Statistics успешно создана")
            else:
                print("✓ Таблица Statistics уже существует")
    finally:
        await dispose_engine()


if __name__ == "__main__":
    asyncio.run(add_statistics_table())
