import json
from decouple import config

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
import os
import logging
import asyncio
from typing import Optional, Dict, Any, List

import gspread
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials

logger = logging.getLogger("services")

SPREADSHEET_ID = config("GOOGLE_SPREADSHEET_ID", default=None)
SHEET_NAME = config("GOOGLE_SHEET_NAME", "Sheet1")
GOOGLE_SA_FILE = config("GOOGLE_SA_FILE", default="botsheets-475807-688c1a47e1da.json")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def load_json():
    with open("config.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg

def build_main_menu():
    cfg = load_json()
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=cfg["Баланс"], callback_data="balance")],
        [InlineKeyboardButton(text=cfg["Выполненые заказы"], callback_data="completed_orders")],
        [InlineKeyboardButton(text=cfg["Пригласить друга"], callback_data="invite_friend")],
        [InlineKeyboardButton(text=cfg["Промоакции"], callback_data="promotions")],
        [InlineKeyboardButton(text=cfg["Вывод средств"], callback_data="withdraw")],
    ])

def build_invite_friend_menu():
    cfg = load_json()
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Вернуться в начало", callback_data="to_start")],
            [InlineKeyboardButton(text="Связаться с менеджером", callback_data="contact_manager")],
        ]
    )

def build_promo_list(promos: list[dict]):
    kb = InlineKeyboardMarkup(row_width=1)
    if not promos:
        return kb
    for p in promos:
        kb.add(InlineKeyboardButton(text=f"{p.get('title')}", callback_data=f"promo_{p.get('id')}"))
    kb.add(InlineKeyboardButton(text="Вернуться в главное меню", callback_data="to_start"))
    return kb

def build_promo_details(promo: dict):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton(text="Участвовать", callback_data=f"promo_claim_{promo.get('id')}"))
    kb.add(InlineKeyboardButton(text="Назад к списку", callback_data="promotions"))
    kb.add(InlineKeyboardButton(text="Вернуться в главное меню", callback_data="to_start"))
    return kb

def contact_kb():
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=load_json().get("contact_button_text", "Отправить мой номер"), request_contact=True)]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return kb

def manager_withdraw_kb(pid: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"withdraw_confirm_{pid}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"withdraw_reject_{pid}")
        ]
    ])
    return kb

def user_after_confirm_kb(pid: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить вывод", callback_data=f"withdraw_user_confirmed_{pid}"),
            InlineKeyboardButton(text="🚩 Вывод не пришёл", callback_data=f"withdraw_user_not_received_{pid}")
        ]
    ])
    return kb

def user_rejected_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩️ Вернуться в главное меню", callback_data="to_start")]
    ])

# Google Sheets helpers (unchanged logic)
def _load_credentials() -> Credentials:
    if GOOGLE_SA_FILE and os.path.exists(GOOGLE_SA_FILE):
        creds = Credentials.from_service_account_file(GOOGLE_SA_FILE, scopes=SCOPES)
        return creds
    raise RuntimeError("Google service account not configured. Set GOOGLE_SA_FILE.")

def _normalize_phone(phone: Optional[str]) -> str:
    if not phone:
        return ""
    import re
    return re.sub(r"\D+", "", phone)

def _normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return s.strip().lower()

def _match_row(row: List[str], name: str, phone: str, city: str) -> bool:
    norm_target_phone = _normalize_phone(phone)
    norm_target_name = _normalize_text(name)
    norm_target_city = _normalize_text(city)

    row_norm = [ _normalize_text(cell) for cell in (row or []) ]
    row_phones = [ _normalize_phone(cell) for cell in (row or []) ]

    for p in row_phones:
        if p and norm_target_phone and p.endswith(norm_target_phone):
            return True

    name_found = any(norm_target_name and norm_target_name in cell for cell in row_norm)
    city_found = any(norm_target_city and norm_target_city in cell for cell in row_norm)

    return name_found and city_found

async def check_user_in_sheet(name: str, phone: str, city: str) -> Dict[str, Any]:
    if not SPREADSHEET_ID:
        return {"found": False, "row": None, "error": "SPREADSHEET_ID not configured"}

    try:
        return await asyncio.to_thread(_sync_check, name, phone, city)
    except Exception as e:
        logger.exception("Error checking sheet")
        return {"found": False, "row": None, "error": str(e)}

def _sync_check(name: str, phone: str, city: str) -> Dict[str, Any]:
    creds = _load_credentials()
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID)
    worksheet = sheet.worksheet(SHEET_NAME)

    all_values = worksheet.get_all_values()
    for row in all_values[1:]:
        if _match_row(row, name, phone, city):
            return {"found": True, "row": row, "error": None}
    return {"found": False, "row": None, "error": None}
