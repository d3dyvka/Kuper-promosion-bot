from .db import init_engine, Base
from .models import Users

async def create_all():
    engine = init_engine()  # инициализация в текущем loop
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)