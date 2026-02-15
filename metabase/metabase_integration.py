import datetime
import uuid
import re
from decimal import Decimal

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
CARD_ID = []


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

def debug_query():
    s = requests.Session()
    try:
        token = update_metabase_token()
    except Exception as e:
        print("Auth error:", e)
        return

    # Check /api/user to confirm who we are
    try:
        user_resp = s.get(f"{BASE}/api/user", headers={"X-Metabase-Session": token}, timeout=10)
        print("/api/user", user_resp.status_code, user_resp.text[:1000])
    except Exception as e:
        print("Error calling /api/user:", e)

    # Also try to fetch card metadata (GET) to check read permissions
    try:
        card_meta = s.get(f"{BASE}/api/card/{CARD_ID}", headers={"X-Metabase-Session": token}, timeout=10)
        print(f"GET /api/card/{CARD_ID} ->", card_meta.status_code)
        print("card meta body (truncated):", card_meta.text[:1000])
    except Exception as e:
        print("Error getting card metadata:", e)

    # Now the actual query; capture full response
    url = f"{BASE}/api/card/{CARD_ID}/query/json"
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}
    payload = {"parameters": [], "ignore_cache": True}
    resp = s.post(url, headers=headers, json=payload, timeout=15)
    print("POST query status:", resp.status_code)
    print("POST query headers:", resp.request.headers)
    try:
        print("POST response text (truncated):", resp.text[:2000])
    except Exception:
        pass
    # if non-JSON, this will raise; wrap to inspect
    try:
        print("POST response json preview:", resp.json())
    except Exception as e:
        print("Could not parse json:", e)

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

def courier_data(phone: str, timeout: int = 15):
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
                return obj
        return None


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


def fetch_all_metabase_rows(timeout: int = 30) -> List[Dict[str, Any]]:
    """
    Возвращает все строки карточки Metabase в виде списка dict.
    """
    token = update_metabase_token()
    url = f"{BASE}/api/card/{CARD_ID}/query/json"
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}
    payload = {"parameters": [], "ignore_cache": True}
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    rows: List[Dict[str, Any]] = []
    if isinstance(data, list):
        for obj in data:
            if isinstance(obj, dict):
                rows.append(obj)
        return rows

    if isinstance(data, dict) and data.get("data"):
        cols = [c.get("name") for c in data["data"].get("cols", [])]
        raw_rows = data["data"].get("rows", []) or []
        for row in raw_rows:
            if not isinstance(row, (list, tuple)):
                continue
            obj = {cols[i]: row[i] for i in range(min(len(cols), len(row)))}
            rows.append(obj)
    return rows


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
            dt_lead = _parse_date_lead(obj.get("Дата лида"))
            if dt_lead:
                return dt_lead
            else:
                return None
    except Exception as e:
        logger.exception("Error getting date lead")

def compute_referral_commissions_for_inviter(inviter_identifier: str,
                                             card_id: int = 87866,
                                             date_from: Optional[datetime.date] = None,
                                             date_to: Optional[datetime.date] = None,
                                             timeout: int = 30):
    results: Dict[str, Any] = {
        "inviter": inviter_identifier,
        "date_from": None,
        "date_to": None,
        "total_earned_friends": 0.0,
        "commission_5pct": 0.0,
        "details": [],
        "errors": []
    }

    # period defaults: from first day of current month to today
    today = datetime.date.today()
    if date_to is None:
        date_to = today
    if date_from is None:
        date_from = today.replace(day=1)

    results["date_from"] = date_from.isoformat()
    results["date_to"] = date_to.isoformat()

    # helper to safely parse numeric "Итого"
    def safe_float(x):
        try:
            if x is None:
                return 0.0
            if isinstance(x, (int, float, Decimal)):
                return float(x)
            s = str(x).replace(",", ".")
            # strip non numeric except dot and minus
            s = re.sub(r"[^\d\.-]", "", s)
            return float(s) if s not in ("", ".", "-", "-.") else 0.0
        except Exception:
            return 0.0

    # 1) read invite sheet and collect invited friends for this inviter
    try:
        from handlers.services import _get_worksheet_values_by_title  # matches earlier code style
    except Exception:
        try:
            # fallback import if package structure different
            from handlers.services import _get_worksheet_values_by_title
        except Exception as e:
            results["errors"].append(f"Cannot import worksheet helper: {e}")
            print(results)
            return 0.0

    vals = _get_worksheet_values_by_title("Акция приведи друга")
    if not vals or len(vals) < 2:
        results["errors"].append("Invite sheet empty or not found")
        print(results)
        return 0.0

    headers = vals[0]
    norm_headers = [((h or "").strip().lower()) for h in headers]

    def find_header_index(*candidates):
        for cand in candidates:
            candl = (cand or "").lower()
            for idx, h in enumerate(norm_headers):
                if candl in h:
                    return idx
        return None

    idx_inviter_phone = find_header_index("номер телефона пригласившего", "телефон пригласившего", "inviter phone", "номер телефона", "телефон")
    idx_inviter_tg = find_header_index("telegram id пригласившего", "tg id пригласившего", "telegram id", "tg id", "telegram")
    idx_invited_phone = find_header_index("номер телефона приглашенного", "телефон приглашенного", "invited phone", "телефон приглашенного")
    idx_invited_name = find_header_index("фио приглашенного", "фио приглашенного", "имя приглашенного", "имя приглашенного", "имя")

    # normalize inviter identifier
    inv_id_raw = str(inviter_identifier or "").strip()
    inv_digits = re.sub(r"\D+", "", inv_id_raw)
    inv_low = inv_id_raw.lower()

    invited_list = []  # tuples (friend_name, friend_phone)
    for row in vals[1:]:
        def cell(row, idx):
            try:
                return (row[idx] or "").strip() if idx is not None and idx < len(row) else ""
            except Exception:
                return ""
        try:
            cell_inv_phone = cell(row, idx_inviter_phone)
            cell_inv_tg = cell(row, idx_inviter_tg)
            # match by tg id exact or by phone suffix 10 digits or exact digits
            matched = False
            if inv_digits:
                # compare last 10 digits
                if cell_inv_phone and re.sub(r"\D+", "", cell_inv_phone)[-10:] == inv_digits[-10:]:
                    matched = True
                if not matched and re.sub(r"\D+", "", cell_inv_phone) == inv_digits:
                    matched = True
            if not matched and cell_inv_tg and inv_low and inv_low == cell_inv_tg.lower():
                matched = True

            if matched:
                fphone = cell(row, idx_invited_phone)
                fname = cell(row, idx_invited_name)
                if fphone or fname:
                    invited_list.append({"name": fname or "", "phone": fphone or ""})
        except Exception:
            continue

    if not invited_list:
        results["errors"].append("No invited friends found for inviter")
        print(results)
        return 0.0

    # 2) query Metabase card and aggregate
    try:
        token = update_metabase_token()
    except Exception as e:
        results["errors"].append(f"Metabase auth failed: {e}")
        print(results)
        return 0.0

    url = f"{BASE}/api/card/{int(card_id)}/query/json"
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}

    payload = {"parameters": [], "ignore_cache": True}
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        results["errors"].append(f"Metabase query failed: {e}")
        print(results)
        return 0.0

    # normalize metabase rows into list of dicts
    metarows: List[Dict[str, Any]] = []
    if isinstance(data, list):
        metarows = data
    elif isinstance(data, dict) and data.get("data"):
        cols = [c.get("name") for c in data["data"].get("cols", [])]
        rows = data["data"].get("rows", []) or []
        for r in rows:
            obj = {cols[i]: r[i] for i in range(min(len(cols), len(r)))}
            metarows.append(obj)
    else:
        results["errors"].append("Unexpected metabase response format")
        print(results)
        return 0.0

    col_keys = set()
    if metarows:
        col_keys = set(metarows[0].keys())

    def find_col(*cands):
        for cand in cands:
            for k in col_keys:
                if cand.lower() in (str(k or "").lower()):
                    return k
        return None

    uuid_col = find_col("uuid", "uu id", "u u id")
    phone_col = find_col("телефон", "phone", "phone_number", "contact")
    name_col = find_col("фио", "имя", "name", "full name", "fullname")
    type_col = find_col("тип", "type", "type_event", "event", "type_event")
    total_col = find_col("итого", "итог", "total", "sum", "amount", "amount_total")
    date_col = find_col("дата", "date", "created_at", "lead date", "lead_date", "date_lead")

    # Function to check date within range
    def in_range(dt_val):
        if not dt_val:
            return False
        dt = _parse_date_lead(dt_val)
        if not dt:
            return False
        d = dt.date()
        return (d >= date_from) and (d <= date_to)

    # For each invited friend: find uuid(s) by matching phone+name
    total_sum = 0.0
    details = []
    for f in invited_list:
        fname = f.get("name") or ""
        fphone = f.get("phone") or ""
        fphone_n = normalize_phone(fphone)
        found_uuids = set()

        # First pass: find rows that look like this friend (phone match and/or name match)
        for row in metarows:
            try:
                row_phone = str(row.get(phone_col) or "")
                row_name = str(row.get(name_col) or "")
            except Exception:
                row_phone = ""
                row_name = ""
            if row_phone and fphone_n and normalize_phone(row_phone).endswith(fphone_n[-10:]):
                if uuid_col and row.get(uuid_col):
                    found_uuids.add(str(row.get(uuid_col)))
            elif fname and row_name and fname.strip().lower() == row_name.strip().lower():
                if uuid_col and row.get(uuid_col):
                    found_uuids.add(str(row.get(uuid_col)))

        # If no uuid found by that, also try matching phone-only across any row that has uuid
        if not found_uuids and fphone_n:
            for row in metarows:
                row_phone = str(row.get(phone_col) or "")
                if row_phone and normalize_phone(row_phone).endswith(fphone_n[-10:]) and row.get(uuid_col):
                    found_uuids.add(str(row.get(uuid_col)))

        # Now for each uuid found, sum up 'Итого' for rows where type == "Смена" and date in range
        friend_sum = 0.0
        if found_uuids:
            for row in metarows:
                try:
                    row_uuid = str(row.get(uuid_col) or "")
                except Exception:
                    row_uuid = ""
                if not row_uuid or row_uuid not in found_uuids:
                    continue
                # check type
                row_type = str(row.get(type_col) or "").strip()
                # consider "Смена" match case-insensitive / substring
                if row_type:
                    if "смен" not in row_type.lower() and "shift" not in row_type.lower():
                        continue
                else:
                    # if no type column, skip (conservative)
                    continue
                # date filter
                if not in_range(row.get(date_col)):
                    continue
                # accumulate total
                friend_sum += safe_float(row.get(total_col))
        else:
            # If no uuid found, still attempt matching by phone+type+date to accumulate
            for row in metarows:
                row_phone = str(row.get(phone_col) or "")
                if fphone_n and row_phone and normalize_phone(row_phone).endswith(fphone_n[-10:]):
                    row_type = str(row.get(type_col) or "").strip()
                    if row_type and ("смен" in row_type.lower() or "shift" in row_type.lower()):
                        if in_range(row.get(date_col)):
                            friend_sum += safe_float(row.get(total_col))

        total_sum += friend_sum
        details.append({
            "friend_name": fname,
            "friend_phone": fphone,
            "uuids": list(found_uuids),
            "earned": round(friend_sum, 2),
            "commission": round(friend_sum * 0.05, 2)
        })

    results["details"] = details
    results["total_earned_friends"] = round(total_sum, 2)
    results["commission_5pct"] = round(total_sum * 0.05, 2)

    print(results)

    if "No invited friends found for inviter" in results["errors"]:
        return 0
    else:
        return round(total_sum * 0.05, 2)
