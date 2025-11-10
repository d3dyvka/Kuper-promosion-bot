import requests
import re
from decouple import config

BASE = "https://metabase.sbmt.io"
CARD_ID = 87869

def update_metabase_token():
    url = f"https://metabase.sbmt.io/api/session"
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, headers=headers, json={"username": config('METABASE_EMAIL'), "password": config('METABASE_PASSWORD')})

    response.raise_for_status()
    data = response.json()

    return data.get("id")

def normalize_phone(phone: str) -> str:
    if phone is None:
        return ""
    digits = re.sub(r"\D", "", phone)         # оставить только цифры
    if digits.startswith("8") and len(digits) >= 11:
        digits = "7" + digits[1:]
    # Возвращаем последние 10 цифр (локальная часть), но храним полный вариант тоже:
    return digits

def match_by_phone(obj_phone: str, query_phone: str) -> bool:
    a = normalize_phone(obj_phone)
    b = normalize_phone(query_phone)
    if not a or not b:
        return False
    # Сравниваем по последним 10 цифрам (удобно при разных форматах +7/8/без кода)
    return a[-10:] == b[-10:]

def get_completed_orders_by_phone(phone: str, timeout=15) -> int:
    token = update_metabase_token()
    url = f"{BASE}/api/card/{CARD_ID}/query/json"
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}
    payload = {"parameters": [], "ignore_cache": True}  # если есть параметры — добавь их
    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    # если Metabase вернул массив объектов
    if isinstance(data, list):
        for obj in data:
            if match_by_phone(obj.get("Телефон"), phone):
                return int(obj.get("Всего заказов", 0) or 0)
        return 0

    # если Metabase вернул data.rows формат
    if isinstance(data, dict) and data.get("data"):
        cols = [c.get("name") for c in data["data"].get("cols", [])]
        rows = data["data"].get("rows", [])
        if not rows:
            return 0
        # найдем индексы
        try:
            phone_idx = cols.index("Телефон")
            orders_idx = cols.index("Всего заказов")
        except ValueError:
            # колонки не найдены
            return 0
        for row in rows:
            if match_by_phone(row[phone_idx], phone):
                return int(row[orders_idx] or 0)
        return 0

    # если что-то непредвиденное
    return 0

def courier_exists(phone: str, timeout: int = 15):
    token = update_metabase_token()
    url = f"{BASE}/api/card/{CARD_ID}/query/json"
    headers = {"X-Metabase-Session": token, "Content-Type": "application/json"}

    payload = {"parameters": [], "ignore_cache": True}

    resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    query_phone = normalize_phone(phone)

    if isinstance(data, list):
        for obj in data:
            obj_phone = normalize_phone(obj.get("Телефон"))
            if obj_phone == query_phone:
                return True
        return False

    elif isinstance(data, dict) and data.get("data"):  # формат data.rows
        cols = [c.get("name") for c in data["data"].get("cols", [])]
        rows = data["data"].get("rows", [])
        if not rows:
            return False
        try:
            phone_idx = cols.index("Телефон")
        except ValueError as e:
            return {"found": False, "row": None, "error": str(e)}
        for row in rows:
            obj_phone = normalize_phone(row[phone_idx])
            if obj_phone == query_phone:
                return True
        return False

    return False

