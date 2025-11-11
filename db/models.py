from sqlalchemy import Column, Integer, String, text, Numeric, DateTime, ForeignKey, Boolean, UniqueConstraint, \
    BigInteger
from sqlalchemy.orm import relationship

from .db import Base

class Users(Base):
    __tablename__ = "Users"   # маленькими буквами по конвенции; если у тебя именно KuperDB - поставь имя такое же
    id = Column(Integer, primary_key=True, index=True)
    tg_id = Column(BigInteger, index=True, nullable=False)
    fio = Column(String(255), nullable=False)
    phone = Column(String(40), nullable=False, index=True)
    city = Column(String(100), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=text("now()"))

class InviteFriends(Base):
    __tablename__ = "InviteFriends"
    id = Column(Integer, primary_key=True, index=True)
    tg_user_id = Column(Integer, ForeignKey("Users.id", ondelete="SET NULL"), nullable=False, index=True)
    tg_user_phone = Column(String(40), nullable=False, index=True)
    invited_phone = Column(String(40), nullable=True, index=True)
    invited_name = Column(String(255), nullable=True)
    invited_city = Column(String(100), nullable=True)
    invited_role = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=text("now()"))
    note = Column(String(512), nullable=True)
    __table_args__ = (
        UniqueConstraint("tg_user_id", "invited_phone", name="uq_inviter_invitedphone"),
    )
    inviter = relationship("Users", backref="invite_friends", foreign_keys=[tg_user_id])