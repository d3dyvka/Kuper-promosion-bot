import json
from decouple import config
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
import os
import logging
import asyncio
from typing import Optional, Dict, Any, List
import gspread
from google.oauth2.service_account import Credentials
import re
from datetime import datetime, timedelta

logger = logging.getLogger("services")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

SPREADSHEET_ID = config("GOOGLE_SPREADSHEET_ID", default=None)
GOOGLE_SA_FILE = config("GOOGLE_SA_FILE", default="../botsheets-475807-688c1a47e1da.json")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def load_json():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

def get_msg(key: str, lang: str = "ru", **kwargs) -> str:
    cfg = load_json()
    val = cfg.get(key)
    if isinstance(val, dict):
        text = val.get(lang) or val.get("ru") or next(iter(val.values()))
    else:
        text = str(val or "")
    if kwargs:
        try:
            return text.format(**kwargs)
        except Exception:
            return text
    return text


def build_main_menu(lang: str = "ru", limited: bool = False, is_admin: bool = False) -> InlineKeyboardMarkup:
    """
    Если limited=True — показываем только Wi‑Fi и промо.
    """
    if limited:
        buttons = [
            [InlineKeyboardButton(text=get_msg("btn_promotions", lang), callback_data="promotions")],
            [InlineKeyboardButton(text=get_msg("btn_wifi_map", lang), callback_data="wifi_map")],
        ]
        if is_admin:
            buttons.append([InlineKeyboardButton(text=get_msg("btn_export_metabase", lang),
                                                 callback_data="export_metabase")])
        return InlineKeyboardMarkup(inline_keyboard=buttons)

    buttons = [
        [InlineKeyboardButton(text=get_msg("btn_completed_orders", lang), callback_data="completed_orders")],
        [InlineKeyboardButton(text=get_msg("btn_invite_friend", lang), callback_data="invite_friend")],
        [InlineKeyboardButton(text=get_msg("btn_promotions", lang), callback_data="promotions")],
        [InlineKeyboardButton(text=get_msg("btn_withdraw", lang), callback_data="withdraw")],
        [InlineKeyboardButton(text=get_msg("btn_wifi_map", lang), callback_data="wifi_map")],
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton(text=get_msg("btn_export_metabase", lang),
                                             callback_data="export_metabase")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def build_invite_friend_menu(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=get_msg("btn_back_to_start", lang), callback_data="to_start")],
            [InlineKeyboardButton(text=get_msg("btn_contact_manager", lang), callback_data="contact_manager")],
        ]
    )

def build_promo_list(promos: List[Dict], lang: str = "ru") -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    if not promos:
        kb.add(InlineKeyboardButton(text=get_msg("btn_back_to_main", lang), callback_data="to_start"))
        return kb

    for p in promos:
        print(p)
        text = p.get("title") or get_msg("promo_default_title", lang)
        reward = p.get("reward")
        if reward:
            text = f"{text} — {reward}"
        kb.add(InlineKeyboardButton(text=text, callback_data=f"promo_{p.get('id')}"))

    kb.add(InlineKeyboardButton(text=get_msg("btn_back_to_main", lang), callback_data="to_start"))
    return kb


def build_promo_details(promo: Dict, lang: str = "ru") -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton(text=get_msg("btn_back_to_list", lang), callback_data="promotions"))
    kb.add(InlineKeyboardButton(text=get_msg("btn_back_to_main", lang), callback_data="to_start"))
    return kb

def contact_kb(lang: str = "ru"):
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=get_msg("contact_button_text", lang), request_contact=True)]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return kb


def location_request_kb(lang: str = "ru"):
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=get_msg("wifi_map_request_location_button", lang), request_location=True)]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return kb

def manager_withdraw_kb(pid: str, lang: str = "ru") -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=get_msg("btn_confirm", lang), callback_data=f"withdraw_confirm_{pid}"),
            InlineKeyboardButton(text=get_msg("btn_reject", lang), callback_data=f"withdraw_reject_{pid}")
        ]
    ])
    return kb

def user_after_confirm_kb(pid: str, lang: str = "ru") -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=get_msg("btn_user_confirm_withdraw", lang), callback_data=f"withdraw_user_confirmed_{pid}"),
            InlineKeyboardButton(text=get_msg("btn_user_withdraw_not_received", lang), callback_data=f"withdraw_user_not_received_{pid}")
        ]
    ])
    return kb

def user_rejected_kb(lang: str = "ru") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=get_msg("btn_back_to_main", lang), callback_data="to_start")]
    ])

def promo_done_kb(promo_id: str, threshold: int = 0, sheet_row: int = 0, lang: str = "ru") -> InlineKeyboardMarkup:
    cb = f"promo_done|{promo_id}|{threshold}|{sheet_row}"
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=get_msg("btn_promo_done", lang), callback_data=cb)]])

def _normalize_phone(phone: Optional[str]) -> str:
    if not phone:
        return ""
    import re
    return re.sub(r"\D+", "", phone)

def _normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return s.strip().lower()

def _load_credentials() -> Credentials:
    if GOOGLE_SA_FILE and os.path.exists(GOOGLE_SA_FILE):
        creds = Credentials.from_service_account_file(GOOGLE_SA_FILE, scopes=SCOPES)
        return creds
    raise RuntimeError("Google service account not configured. Set GOOGLE_SA_FILE.")  # noqa: WPS500

def _get_worksheet(
        title: str,
        spreadsheet_id: Optional[str] = None,
        create_if_missing: bool = False,
        rows: str = "1000",
        cols: str = "20",
):
    creds = _load_credentials()
    client = gspread.authorize(creds)
    sheet = client.open_by_key(spreadsheet_id or SPREADSHEET_ID)
    try:
        return sheet.worksheet(title)
    except Exception:
        logger.exception("Worksheet '%s' not found", title)
        if create_if_missing:
            try:
                return sheet.add_worksheet(title=title, rows=rows, cols=cols)
            except Exception:
                logger.exception("Failed to create worksheet '%s'", title)
        return None

def _get_worksheet_values_by_title(title: str, spreadsheet_id: Optional[str] = None) -> Optional[List[List[str]]]:
    ws = _get_worksheet(title, spreadsheet_id=spreadsheet_id)
    if not ws:
        return None
    return ws.get_all_values()

def _read_first_order_rows_structured() -> List[Dict[str, Any]]:
    title = "Акция Первый заказ"
    vals = _get_worksheet_values_by_title(title)
    out: List[Dict[str, Any]] = []
    if not vals or len(vals) < 2:
        return out
    headers = vals[0]
    norm_headers = [(h or "").strip().lower() for h in headers]

    def find_header(*cands):
        for cand in cands:
            candl = cand.lower()
            for idx, h in enumerate(norm_headers):
                if candl in h:
                    return idx
        return None

    phone_idx = find_header("номер телефона", "телефон", "phone")
    title_idx = find_header("название", "title")
    desc_idx = find_header("описание", "опис")
    reward_idx = find_header("награда", "бонус", "вознаграждение")
    status_idx = find_header("статус", "status")

    for i, row in enumerate(vals[1:], start=2):
        def safe_get(idx):
            try:
                return (row[idx] or "").strip() if idx is not None and idx < len(row) else ""
            except Exception:
                return ""
        out.append({
            "sheet_row": i,
            "phone": safe_get(phone_idx),
            "title": safe_get(title_idx),
            "desc": safe_get(desc_idx),
            "reward": safe_get(reward_idx),
            "status": safe_get(status_idx),
        })
    return out

def find_first_order_row_by_phone(sheet_title: str, phone: str) -> Optional[int]:
    vals = _get_worksheet_values_by_title(sheet_title)
    if not vals or len(vals) < 2:
        return None
    headers = vals[0]
    norm_headers = [(h or "").strip().lower() for h in headers]
    phone_col = None
    for idx, h in enumerate(norm_headers):
        if "телефон" in h or ("номер" in h and "тел" in h) or "тел" in h:
            phone_col = idx
            break
    target = re.sub(r"\D+", "", phone or "")[-10:]
    for row_idx, row in enumerate(vals[1:], start=2):
        try:
            cell = row[phone_col] if phone_col is not None and phone_col < len(row) else ""
            cell_norm = re.sub(r"\D+", "", cell or "")[-10:]
            if cell_norm and target and cell_norm == target:
                return row_idx
        except Exception:
            continue
    return None

def update_first_order_status_by_row(sheet_title: str, row_number: int, status_value: str) -> bool:
    ws = _get_worksheet(sheet_title)
    if not ws:
        return False
    try:
        col = 4
        ws.update_cell(row_number, col, status_value)
        return True
    except Exception:
        logger.exception("Failed to update sheet %s row %s col D", sheet_title, row_number)
        return False

def get_table3_coeffs() -> Dict[int, float]:
    title = "Акция За выполненые заказы"
    vals = _get_worksheet_values_by_title(title)
    if not vals or len(vals) < 2:
        return {}
    headers = vals[0]
    second = vals[1]
    thresholds = [10, 25, 50, 75, 100]
    res: Dict[int, float] = {}
    for idx, th in enumerate(thresholds):
        col_idx = 4 + idx
        if col_idx < len(second):
            raw = second[col_idx]
            if raw in (None, ""):
                continue
            try:
                res[th] = float(str(raw).replace(",", "."))
            except Exception:
                continue
    if not res:
        for idx, h in enumerate(headers):
            if not h:
                continue
            digits = re.findall(r"\d+", h)
            if digits:
                try:
                    th = int(digits[0])
                    raw = second[idx]
                    if raw not in (None, ""):
                        res[th] = float(str(raw).replace(",", "."))
                except Exception:
                    continue
    return res

def build_completed_promo_text(title: str, desc: str, thresholds: List[int], coeffs: Dict[int, Any]) -> str:
    if not thresholds:
        return f"{title}\n\n{desc}"
    thr_sorted = sorted(int(t) for t in thresholds)
    last_idx = len(thr_sorted) - 1
    now = datetime.now().date()
    parts = [f"🏆 {title}\n\n{desc}\n\nСтупени (заказы / дедлайн / сумма):"]
    for idx, th in enumerate(thr_sorted):
        days_offset = 20 - 3 * (last_idx - idx)
        deadline = now + timedelta(days=days_offset)
        date_str = deadline.strftime("%d.%m.%Y")
        c = coeffs.get(th) if isinstance(coeffs, dict) else None
        if c is None or str(c).strip() == "":
            payout_str = "—"
        else:
            try:
                pv = float(c)
                if pv <= 5:
                    payout_str = f"коэффициент {pv}"
                else:
                    if pv.is_integer():
                        payout_str = f"{int(pv)} ₽"
                    else:
                        payout_str = f"{pv} ₽"
            except Exception:
                payout_str = str(c)
        parts.append(f"- {th} / {date_str} / {payout_str}")
    return "\n".join(parts)

def get_refer_a_friend_promo(user_identifier: Optional[str] = None) -> Optional[str]:
    def find_header_index(norm_headers, *candidates):
        for cand in candidates:
            cand_n = _normalize_text(cand)
            for idx, h in enumerate(norm_headers):
                if cand_n in h:
                    return idx
        return None

    vals = _get_worksheet_values_by_title("Акция приведи друга")
    if not vals or len(vals) < 2:
        return None

    headers = vals[0]
    norm_headers = [_normalize_text(h) for h in headers]

    idx_inviter_phone = find_header_index(norm_headers, "номер телефона пригласившего", "телефон пригласившего", "inviter phone", "номер телефона", "телефон")
    idx_inviter_name = find_header_index(norm_headers, "фио пригласившего", "фио", "имя пригласившего", "имя")
    idx_inviter_tg = find_header_index(norm_headers, "telegram id пригласившего", "tg id пригласившего", "telegram id", "tg id", "telegram")

    idx_invited_phone = find_header_index(norm_headers, "номер телефона приглашенного", "телефон приглашенного", "invited phone", "телефон приглашенного")
    idx_invited_name = find_header_index(norm_headers, "фио приглашенного", "фио приглашенного", "имя приглашенного", "имя приглашенного")
    idx_invited_tg = find_header_index(norm_headers, "telegram id приглашенного", "tg id приглашенного", "telegram id invited", "tg id invited")

    idx_status = find_header_index(norm_headers, "статус", "status")
    idx_payout = find_header_index(norm_headers, "выплата", "платёж", "payout")
    idx_friend_order = find_header_index(norm_headers, "заказ друга", "заказ", "first order", "order")

    idx_title = find_header_index(norm_headers, "название", "title", "name")
    idx_desc = find_header_index(norm_headers, "описание", "description", "desc")
    idx_reward = find_header_index(norm_headers, "награда", "бонус", "reward")

    def cell_safe(row, idx):
        try:
            return (row[idx] or "").strip() if idx is not None and idx < len(row) else ""
        except Exception:
            return ""

    if user_identifier is not None and str(user_identifier).strip() != "":
        uid = str(user_identifier).strip()
        uid_norm_digits = re.sub(r"\D+", "", uid)
        uid_low = uid.lower()
        matched_rows = []
        for ridx, row in enumerate(vals[1:], start=2):
            inviter_phone = cell_safe(row, idx_inviter_phone)
            inviter_tg = cell_safe(row, idx_inviter_tg)
            invited_phone = cell_safe(row, idx_invited_phone)

            found = False
            if uid_norm_digits and (re.sub(r"\D+", "", inviter_phone).endswith(uid_norm_digits[-10:]) or re.sub(r"\D+", "", invited_phone).endswith(uid_norm_digits[-10:])):
                found = True
            if not found and inviter_tg and uid_low == inviter_tg.lower():
                found = True
            if not found and uid_norm_digits and (re.sub(r"\D+", "", inviter_phone) == uid_norm_digits or re.sub(r"\D+", "", invited_phone) == uid_norm_digits):
                found = True

            if found:
                matched_rows.append((ridx, row))

        if not matched_rows:
            return None

        parts = []
        for ridx, row in matched_rows:
            inviter_phone = cell_safe(row, idx_inviter_phone)
            inviter_name = cell_safe(row, idx_inviter_name)
            inviter_tg = cell_safe(row, idx_inviter_tg)
            invited_phone = cell_safe(row, idx_invited_phone)
            invited_name = cell_safe(row, idx_invited_name)
            invited_tg = cell_safe(row, idx_invited_tg)
            status = cell_safe(row, idx_status)
            payout = cell_safe(row, idx_payout)
            friend_order = cell_safe(row, idx_friend_order)
            title = cell_safe(row, idx_title)
            desc = cell_safe(row, idx_desc)
            reward = cell_safe(row, idx_reward)

            line = f"Запись (строка {ridx}):\n"
            line += f"- Пригласивший: {inviter_name or '—'} (тел: {inviter_phone or '—'}; tg: {inviter_tg or '—'})\n"
            line += f"- Приглашённый: {invited_name or '—'} (тел: {invited_phone or '—'}; tg: {invited_tg or '—'})\n"
            line += f"- Статус: {status or '—'}\n"
            line += f"- Выплата: {payout or '0'}\n"
            line += f"- Заказ друга: {friend_order or 'Нет'}\n"
            if title or desc or reward:
                line += f"- Акция: {title or '—'}\n  Описание: {desc or '—'}\n  Награда: {reward or '—'}\n"
            parts.append(line)

        return "\n\n".join(parts)

    for row in vals[1:]:
        title = cell_safe(row, idx_title)
        desc = cell_safe(row, idx_desc)
        reward = cell_safe(row, idx_reward)
        if title or desc or reward:
            out = []
            if title:
                out.append(f"У вас доступна акция: {title}")
            if desc:
                out.append(f"Подробности: {desc}")
            if reward:
                out.append(f"Награда: {reward}")
            return "\n".join(out)
    return None

def get_first_order_promos() -> List[str]:
    title = "Акция Первый заказ"
    vals = _get_worksheet_values_by_title(title)
    if not vals or len(vals) < 2:
        return []
    headers = vals[0]
    rows = vals[1:]
    header_map = {_normalize_text(h): idx for idx, h in enumerate(headers)}
    phone_idx = header_map.get(_normalize_text("Номер телефона"))
    name_idx = header_map.get(_normalize_text("Название"))
    desc_idx = header_map.get(_normalize_text("Описание"))
    reward_idx = header_map.get(_normalize_text("Награда"))
    result: List[str] = []

    def safe_get(row, idx):
        try:
            return (row[idx] or "").strip()
        except Exception:
            return ""

    for row in rows:
        phone_val = safe_get(row, phone_idx) if phone_idx is not None else ""
        name_val = safe_get(row, name_idx) if name_idx is not None else ""
        desc_val = safe_get(row, desc_idx) if desc_idx is not None else ""
        reward_val = safe_get(row, reward_idx) if reward_idx is not None else ""
        if not (phone_val or name_val or desc_val or reward_val):
            continue
        text = f"У вас доступна акция: {name_val}\nПодробности:{desc_val}\nБонус: {reward_val}"
        result.append(text)
    return result

def add_invite_friend_row(inviter_tg_id: int,
                          friend_name: str,
                          friend_phone: str,
                          friend_tg_id: Optional[int] = None,
                          inviter_name: Optional[str] = None,
                          inviter_phone: Optional[str] = None,
                          friend_city: Optional[str] = None,
                          friend_role: Optional[str] = None) -> Optional[int]:
    """
    Append invite row. Backwards compatible parameters.
    Writes columns in expected order. # short comment
    """
    ws = _get_worksheet("Акция приведи друга")
    if not ws:
        return None

    # If inviter_name/phone not provided, leave empty (caller should provide)
    inviter_phone_val = inviter_phone or ""
    inviter_name_val = inviter_name or ""
    inviter_tg_val = str(inviter_tg_id) if inviter_tg_id else ""
    friend_phone_val = friend_phone or ""
    friend_name_val = friend_name or ""
    friend_tg_val = str(friend_tg_id) if friend_tg_id else ""

    # default status and payout placeholders
    status = "pending"
    payout = ""
    friend_order = ""
    # keep space for promo meta columns (title/desc/reward) if present
    title = ""
    desc = ""
    reward = ""

    try:
        # row order matches sheet header observed earlier
        row = [
            inviter_phone_val,    # Номер телефона пригласившего
            inviter_name_val,     # ФИО пригласившего
            inviter_tg_val,       # Telegram ID пригласившего
            friend_phone_val,     # Номер телефона приглашенного
            friend_name_val,      # ФИО приглашенного
            status,               # Статус
            payout,               # Выплата
            friend_order,         # Заказ друга
            title,                # Название
            desc,                 # Описание
            reward                # Награда
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")
        vals = ws.get_all_values()
        return len(vals)
    except Exception:
        logger.exception("Failed to add invite friend row")
        return None

def add_person_to_external_sheet(spreadsheet_id: str, sheet_name: str, fio: str, phone: str, city: str, role: str) -> Optional[int]:
    """
    Append person to external sheet by provided id and sheet name.
    Returns new row index or None on failure.
    """
    try:
        creds = _load_credentials()  # reuse credentials
        client = gspread.authorize(creds)
        sheet = client.open_by_key(spreadsheet_id)
        try:
            ws = sheet.worksheet(sheet_name)
        except Exception:
            # try to create worksheet if missing
            try:
                ws = sheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
            except Exception:
                logger.exception("Worksheet '%s' missing and cannot be created", sheet_name)
                return None

        row = [fio or "", phone or "", city or "", role or ""]
        ws.append_row(row, value_input_option="USER_ENTERED")
        vals = ws.get_all_values()
        return len(vals)  # new total rows -> index of appended row
    except Exception:
        logger.exception("Failed to add person to external sheet")
        return None

def find_invite_row_by_phone(phone: str) -> Optional[int]:
    ws = _get_worksheet("Акция приведи друга")
    if not ws:
        return None
    try:
        vals = ws.get_all_values()
        if not vals or len(vals) < 2:
            return None
        for idx, row in enumerate(vals[1:], start=2):
            for cell in row:
                if re.sub(r"\D+", "", cell or "")[-10:] == re.sub(r"\D+", "", phone or "")[-10:]:
                    return idx
    except Exception:
        logger.exception("Error finding invite row by phone")
    return None

def mark_invite_friend_payment(sheet_row: int, payout: float, status: str, first_order_done: bool) -> bool:
    ws = _get_worksheet("Акция приведи друга")
    if not ws:
        return False
    try:
        ws.update_cell(sheet_row, 6, payout)
        ws.update_cell(sheet_row, 7, status)
        ws.update_cell(sheet_row, 8, "Да" if first_order_done else "Нет")
        return True
    except Exception:
        logger.exception("Failed to mark invite friend payment on row %s", sheet_row)
        return False


def find_row_by_phone_in_sheet(title: str, phone: str, spreadsheet_id: Optional[str] = None) -> Optional[Dict[str, str]]:
    """
    Ищет строку по телефону в указанном листе (по первому столбцу, содержащему 'тел' или 'phone').
    Возвращает dict {header: value} или None.
    """
    ws = _get_worksheet(title, spreadsheet_id=spreadsheet_id)
    if not ws:
        return None
    try:
        vals = ws.get_all_values()
    except Exception:
        logger.exception("Error reading sheet '%s'", title)
        return None
    if not vals or len(vals) < 2:
        return None

    headers = vals[0]
    norm_headers = [(h or "").strip().lower() for h in headers]
    phone_col = None
    for idx, h in enumerate(norm_headers):
        if "тел" in h or "phone" in h:
            phone_col = idx
            break

    target = _normalize_phone(phone)[-10:]
    if not target:
        return None

    for row in vals[1:]:
        try:
            cell = row[phone_col] if phone_col is not None and phone_col < len(row) else ""
            cell_norm = _normalize_phone(cell)[-10:]
            if cell_norm and target.endswith(cell_norm):
                return {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        except Exception:
            continue
    return None


UNIFORM_ADDRESSES_SPREADSHEET_ID = "1m6WSlCKC9iR0gmNYSOLQWksZndZEgBnEoPlPlPaG0ck"
UNIFORM_ADDRESSES_SHEET_NAME = "Лист1"


def get_uniform_address_by_city(city: str) -> Optional[str]:
    """
    Ищет адрес получения формы по названию города в таблице Google Sheets.
    
    Args:
        city: Название города, введенное пользователем
        
    Returns:
        Адрес улицы для получения формы или None, если город не найден
    """
    if not city:
        return None
    
    ws = _get_worksheet(UNIFORM_ADDRESSES_SHEET_NAME, spreadsheet_id=UNIFORM_ADDRESSES_SPREADSHEET_ID)
    if not ws:
        logger.warning("Не удалось получить доступ к листу '%s' в таблице адресов формы", UNIFORM_ADDRESSES_SHEET_NAME)
        return None
    
    try:
        vals = ws.get_all_values()
    except Exception:
        logger.exception("Ошибка при чтении листа '%s'", UNIFORM_ADDRESSES_SHEET_NAME)
        return None
    
    if not vals or len(vals) < 1:
        return None
    
    # Нормализуем введенный город для сравнения (без учета регистра и лишних пробелов)
    city_normalized = _normalize_text(city)
    
    # Ищем в данных (столбец A - индекс 0, столбец B - индекс 1)
    for row in vals:
        if len(row) < 2:
            continue
        city_in_sheet = (row[0] or "").strip()
        address = (row[1] or "").strip()
        
        # Нормализуем город из таблицы для сравнения
        city_in_sheet_normalized = _normalize_text(city_in_sheet)
        
        # Сравниваем нормализованные значения
        if city_normalized == city_in_sheet_normalized:
            return address if address else None
    
    return None
