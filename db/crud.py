from sqlalchemy import select
from decimal import Decimal
from .db import get_session
from .models import Users, InviteFriends


async def create_user(fio: str, phone: str, city: str = None, tg_id: int = None):
    async with get_session() as session:
        obj = Users(fio=fio, phone=phone, city=city, tg_id=tg_id)
        session.add(obj)
        await session.commit()
        await session.refresh(obj)
        return obj

async def get_user_by_tg_id(tg_id: int):
    async with get_session() as session:
        q = select(Users).where(Users.tg_id == tg_id)
        result = await session.execute(q)
        return result.scalars().first()

async def delete_user_by_phone(phone: str):
    async with get_session() as session:
        q = select(Users).where(Users.phone == phone)
        r = await session.execute(q)
        user = r.scalars().first()
        if not user:
            return False
        await session.delete(user)
        await session.commit()
        return True

async def create_friend(inviter_id: int, inviter_phone: str, fio: str, phone: str, city: str = None, role: str = None):
    async with get_session() as session:
        obj = InviteFriends(name=fio, phone=phone, city=city, role=role)


async def get_all_users():
    async with get_session() as session:
        result = await session.execute(select(Users))
        return list(result.scalars().all())
