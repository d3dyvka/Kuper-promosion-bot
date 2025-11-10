from decouple import config
import asyncio
from typing import Optional, AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = config("DATABASE_URL")  # e.g. postgresql+asyncpg://user:pass@host:port/dbname

Base = declarative_base()

_engine: Optional[AsyncEngine] = None
_SessionMaker: Optional[sessionmaker] = None

def init_engine(url: Optional[str] = None) -> AsyncEngine:

    global _engine, _SessionMaker
    if _engine is None:
        _url = url or DATABASE_URL
        if not _url:
            raise RuntimeError("DATABASE_URL не задан")
        _engine = create_async_engine(_url, echo=False, future=True)
        _SessionMaker = sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)
    return _engine

@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Контекст получения AsyncSession. Гарантирует rollback при исключении.
    Если engine ещё не инициализирован — инициализирует его (лениво) в текущем loop.
    """
    global _engine, _SessionMaker
    if _engine is None:
        init_engine()  # инициализация привяжет engine к текущему loop
    assert _SessionMaker is not None, "SessionMaker не инициализирован"
    async with _SessionMaker() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise

async def dispose_engine():
    global _engine, _SessionMaker
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _SessionMaker = None

def current_loop_id():
    try:
        return id(asyncio.get_running_loop())
    except RuntimeError:
        return None
