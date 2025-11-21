import datetime
import uuid
import re
import requests
import logging
from typing import List, Dict, Any, Optional
from decouple import config

from handlers.services import (
    get_refer_a_friend_promo,
    _read_first_order_rows_structured,
    get_table3_coeffs,
)

logger = logging.getLogger("metabase_integration")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

BASE = "https://metabase.sbmt.io"
CARD_ID = 87869

def update_metabase_token():
    url = f"https://metabase.sbmt.io/api/session"
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, headers=headers,
                             json={"username": config('METABASE_EMAIL'), "password": config('METABASE_PASSWORD')})
    response.raise_for_status()
    data = response.json()
    return data.get("id")

def normalize_phone(phone: str) -> str:
    if phone is None:
        return ""
    digits = re.sub(r"\D", "", phone)
    if digits.startswith("8") and len(digits) >= 11:
        digits = "7" + digits[1:]
    return digits

def match_by_phone(obj_phone: str, query_phone: str) -> bool:
    a = normalize_phone(obj_phone)
    b = normalize_phone(query_phone)
    if not a or not b:
        return False
    return a[-10:] == b[-10:]

def get_completed_orders_by_phone(phone: str, timeout: int = 15) -> int:
    token = update_metabase_token()
    url = f"{BASE}/api/card/{CARD_ID}/query/json"
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}
    payload = {"parameters": [], "ignore_cache": True}
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    def safe_int(x):
        try:
            return int(x or 0)
        except Exception:
            try:
                return int(float(x))
            except Exception:
                return 0

    if isinstance(data, list):
        for obj in data:
            if match_by_phone(obj.get("Телефон"), phone):
                return safe_int(obj.get("Всего заказов"))
        return 0
    if isinstance(data, dict) and data.get("data"):
        cols = [c.get("name") for c in data["data"].get("cols", [])]
        rows = data["data"].get("rows", []) or []
        if not rows:
            return 0
        try:
            phone_idx = cols.index("Телефон")
            orders_idx = cols.index("Всего заказов")
        except ValueError:
            return 0
        for row in rows:
            if match_by_phone(row[phone_idx], phone):
                return safe_int(row[orders_idx])
        return 0
    return 0

def courier_exists(phone: str, timeout: int = 15):
    try:
        token = update_metabase_token()
    except Exception as e:
        logger.exception("Metabase auth error")
        return {"found": False, "row": None, "error": str(e)}

    url = f"{BASE}/api/card/{CARD_ID}/query/json"
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}
    payload = {"parameters": [], "ignore_cache": True}
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.exception("Error querying Metabase")
        return {"found": False, "row": None, "error": str(e)}

    query_phone = normalize_phone(phone)

    if isinstance(data, list):
        for obj in data:
            obj_phone = normalize_phone(obj.get("Телефон"))
            if obj_phone == query_phone:
                return {"found": True, "row": None, "error": None}
        return {"found": False, "row": None, "error": None}

    elif isinstance(data, dict) and data.get("data"):
        cols = [c.get("name") for c in data["data"].get("cols", [])]
        rows = data["data"].get("rows", []) or []
        if not rows:
            return {"found": False, "row": None, "error": None}
        try:
            phone_idx = cols.index("Телефон")
        except ValueError as e:
            return {"found": False, "row": None, "error": str(e)}
        for idx, row in enumerate(rows, start=1):
            obj_phone = normalize_phone(row[phone_idx])
            if obj_phone == query_phone:
                return {"found": True, "row": idx, "error": None}
        return {"found": False, "row": None, "error": None}
    return {"found": False, "row": None, "error": None}

def _parse_date_lead(value) -> Optional[datetime.datetime]:
    if not value:
        return None
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time())
    s = str(value).strip()
    try:
        return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        pass
    patterns = [
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%d/%m/%Y",
    ]
    for p in patterns:
        try:
            return datetime.datetime.strptime(s.split(".")[0], p)
        except Exception:
            continue
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        try:
            return datetime.datetime.strptime(m.group(1), "%Y-%m-%d")
        except Exception:
            pass
    return None

def get_promotions(phone: str, timeout: int = 15) -> List[Dict[str, Any]]:
    """
    Собирает и возвращает все акции для номера телефона.
    Формат ступеней: "N заказов - DD.MM.YYYY - SUM ₽"
    """
    results: List[Dict[str, Any]] = []

    norm_phone = normalize_phone(phone)
    show_all = False
    if norm_phone:
        if norm_phone == normalize_phone("+79137619949") or norm_phone.endswith("9137619949"):
            show_all = True

    # 1) generic refer promo (always try to fetch general promo info)
    try:
        refer_text_generic = get_refer_a_friend_promo()  # generic info
        refer_text_personal = None
        try:
            refer_text_personal = get_refer_a_friend_promo(user_identifier=phone)
        except Exception:
            refer_text_personal = None
        # prefer personal detailed info if exists, otherwise generic
        refer_text = refer_text_personal or refer_text_generic
        if refer_text:
            results.append({
                "id": f"refer_{uuid.uuid4().hex[:8]}",
                "type": "refer",
                "title": "Приведи друга",
                "desc": refer_text,
                "reward": "",
                "meta": {},
            })
    except Exception:
        logger.exception("Error reading refer promo")

    # 2) first order promos (from sheet)
    try:
        rows = _read_first_order_rows_structured()
        for r in rows:
            if show_all or (r.get("phone") and match_by_phone(r.get("phone"), phone)):
                st = (r.get("status") or "").strip().lower()
                if st != "выполнил":
                    results.append({
                        "id": f"first_{r['sheet_row']}_{uuid.uuid4().hex[:6]}",
                        "type": "first",
                        "title": r.get("title") or "Первый заказ",
                        "desc": r.get("desc") or "",
                        "reward": r.get("reward") or "",
                        "meta": {"sheet_row": r["sheet_row"], "phone": r.get("phone"), "status": r.get("status")},
                    })
    except Exception:
        logger.exception("Error reading first-order sheet")

    # 3) completed promotions from metabase (steps)
    try:
        token = update_metabase_token()
        url = f"{BASE}/api/card/{CARD_ID}/query/json"
        headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}
        payload = {"parameters": [], "ignore_cache": True}
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()

        objs = []
        if isinstance(data, list):
            objs = data
        elif isinstance(data, dict) and data.get("data"):
            cols = [c.get("name") for c in data["data"].get("cols", [])]
            rows = data["data"].get("rows", []) or []
            for row in rows:
                obj = {cols[i]: row[i] for i in range(min(len(cols), len(row)))}
                objs.append(obj)

        table3 = get_table3_coeffs() or {}
        thresholds = [10, 25, 50, 75, 100]
        base_sum = 1000

        for obj in objs:
            if not show_all and not match_by_phone(obj.get("Телефон"), phone):
                continue

            dt_lead = _parse_date_lead(obj.get("Дата лида"))
            try:
                obj_coef = float(str(obj.get("Коэф точеч. мотивации") or "0").replace(",", "."))
            except Exception:
                obj_coef = 0.0

            for th in thresholds:
                # coefficient precedence: table -> per-user metabase -> 0
                table_coef = table3.get(th)
                if table_coef is not None:
                    chosen_coef = float(table_coef)
                else:
                    chosen_coef = obj_coef

                reward_amount = int(base_sum * chosen_coef) if chosen_coef else 0

                # deadline calculation: 100 -> +20, others -3 per step from end
                try:
                    pos = thresholds.index(th)
                except Exception:
                    pos = 0
                num_from_end = (len(thresholds) - 1) - pos
                days_for_th = 20 - (3 * num_from_end)
                if days_for_th < 1:
                    days_for_th = 1

                end_date_str = None
                if dt_lead:
                    end_dt = dt_lead + datetime.timedelta(days=days_for_th)
                    end_date_str = end_dt.strftime("%d.%m.%Y")

                promoid = f"comp_{th}_{uuid.uuid4().hex[:6]}"
                title = f"Бонус за {th} заказов"
                # format as requested with hyphens and word "заказов"
                desc = f"{th} заказов - {end_date_str or '—'} - {reward_amount} ₽"
                results.append({
                    "id": promoid,
                    "type": "completed",
                    "title": title,
                    "desc": desc,
                    "reward": str(reward_amount),
                    "meta": {"threshold": th, "end_date": end_date_str, "coef_used": chosen_coef, "obj": obj},
                })

    except Exception:
        logger.exception("Error loading promotions from Metabase")

    # remove duplicates by (type, title, desc)
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for r in results:
        key = (r.get("type"), r.get("title"), r.get("desc"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    return deduped

def get_date_lead(phone_number: str, timeout=15):
    try:
        token = update_metabase_token()
        url = f"{BASE}/api/card/{CARD_ID}/query/json"
        headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}
        payload = {"parameters": [], "ignore_cache": True}
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        for obj in data:
            if not match_by_phone(obj.get("Телефон"), phone_number):
                continue
            dt_lead = _parse_date_lead(data.get("Дата лида"))
            if dt_lead:
                return dt_lead
            else:
                return None
    except Exception as e:
        logger.exception("Error getting date lead")
