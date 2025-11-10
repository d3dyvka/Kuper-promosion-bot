# amocrm_integration.py
import os
import time
import logging
import requests
from urllib.parse import urljoin
from typing import Optional, Dict, Any
import asyncio
from decouple import config

logger = logging.getLogger("amocrm_sync")

"""
10572202 Ловкова Ксения Викторовна ksuha18pro@gmail.com
12452534 Зорин Ростислав Александрович roster111999@gmail.com
12815686 Илья illague@gmail.com
12916074 Бабич zd333753@gmail.com
13052414 Иван Программист vanroz333@gmail.com
"""

AMO_BASE_URL = config("AMO_BASE_URL")  # e.g. "https://yourcompany.amocrm.ru"
AMO_ACCESS_TOKEN = config("AMO_ACCESS_TOKEN", "")
PHONE_FIELD_ID = int(os.getenv("AMO_PHONE_FIELD_ID", "0"))  # id поля "Телефон" в вашей amoCRM
RESPONSIBLE_USER_ID = int(config("AMO_RESPONSIBLE_USER_ID"))

def _extract_id_from_response(j: Any, prefer_key: str | None = None) -> Optional[int]:
    try:
        if isinstance(j, list) and j:
            first = j[0]
            if isinstance(first, dict) and first.get("id"):
                return int(first.get("id"))

        if isinstance(j, dict):
            if j.get("id"):
                return int(j.get("id"))

            emb = j.get("_embedded") or j.get("embedded") or {}
            if isinstance(emb, dict):
                for key in ("items", "contacts", "leads", "tasks"):
                    arr = emb.get(key)
                    if isinstance(arr, list) and arr and isinstance(arr[0], dict) and arr[0].get("id"):
                        return int(arr[0].get("id"))

        return None
    except Exception:
        logger.exception("Error extracting id from response: %s", repr(j))
        return None

def _safe_json(response: requests.Response) -> Optional[Dict[str, Any]]:
    try:
        return response.json()
    except ValueError:
        text = response.text.strip()
        if text:
            logger.debug("Response is not JSON. Status=%s, Body=%s", response.status_code, text[:1000])
        else:
            logger.debug("Response body is empty. Status=%s", response.status_code)
        return None

def _build_session(access_token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {access_token}" if access_token else "",
        "Content-Type": "application/json",
        "User-Agent": "kuper-bot/1.0"
    })
    return s

def _full_url(path: str) -> str:
    base = (AMO_BASE_URL or "").rstrip("/")
    return urljoin(base + "/", path.lstrip("/"))

# --- Core sync class (requests) --------------------------------
class AmoCRMSession:
    def __init__(self, base_url: str, access_token: str):
        if not base_url:
            raise ValueError("AMO_BASE_URL is not configured")
        self.base_url = (base_url or "").rstrip('/')
        self.session = _build_session(access_token or "")

    def _handle_response(self, r: requests.Response, expect_json: bool = True) -> Dict[str, Any]:
        status = r.status_code
        text = r.text or ""
        parsed = _safe_json(r) if expect_json else None

        if status in (401, 403):
            logger.error("AMO auth error %s: %s", status, text[:1000])
            return {"ok": False, "status": status, "json": parsed, "text": text, "error": "auth"}
        if status >= 400:
            # логируем тело для диагностики
            logger.error("AMO API returned %s: %s", status, text[:1000])
            return {"ok": False, "status": status, "json": parsed, "text": text, "error": f"http_{status}"}
        return {"ok": True, "status": status, "json": parsed, "text": text}

    def get_contact_by_phone(self, phone: str) -> Optional[dict]:
        url = _full_url("api/v4/contacts")
        params = {"query": phone}
        try:
            r = self.session.get(url, params=params, timeout=10)
        except requests.RequestException as e:
            logger.exception("Network error while searching contact by phone")
            return None

        res = self._handle_response(r, expect_json=True)
        if not res["ok"]:
            if res.get("error") == "auth":
                logger.error("Auth error while searching contact by phone. Status=%s", res["status"])
            return None

        data = res["json"]
        if not data:
            logger.debug("Empty/non-JSON response while searching contact by phone: %s", r.text[:1000])
            return None

        # поддерживаем разные ключи
        items = None
        if isinstance(data, dict):
            emb = data.get("_embedded") or {}
            # иногда ключ называется 'items', иногда 'contacts'
            items = emb.get("items") or emb.get("contacts") or emb.get("leads")
        if items is None and isinstance(data, list):
            items = data

        if not items:
            return None

        # возвращаем первый элемент (dict) если есть
        if isinstance(items, list) and items:
            return items[0] if isinstance(items[0], dict) else None

        return None

    def create_contact(self, name: str, phones: list, responsible_user_id: Optional[int] = None) -> Optional[int]:
        url = _full_url("api/v4/contacts")
        contact_obj = {"name": name}
        if responsible_user_id:
            contact_obj["responsible_user_id"] = responsible_user_id
        if PHONE_FIELD_ID:
            contact_obj["custom_fields_values"] = [
                {"field_id": PHONE_FIELD_ID, "values": [{"value": p} for p in phones]}
            ]
        else:
            contact_obj["custom_fields_values"] = []

        payload = [contact_obj]
        try:
            r = self.session.post(url, json=payload, timeout=10)
        except requests.RequestException:
            logger.exception("Network error while creating contact")
            return None

        res = self._handle_response(r, expect_json=True)
        if not res["ok"]:
            logger.error("Create contact failed: status=%s body=%s", res["status"], res["text"][:1000])
            return None

        j = res["json"]
        if not j:
            logger.error("Create contact: empty JSON response")
            return None

        cid = _extract_id_from_response(j)
        if cid:
            return cid

        # В лог детали ответа, чтобы знать новый формат
        logger.error("Unexpected create_contact response structure: %s", j)
        return None

    def create_task(self, text: str, entity_id: int, timestamp: int, entity_type: str = 'contacts') -> Optional[int]:
        url = _full_url("api/v4/tasks")
        payload = [
            {
                "text": text,
                "complete_till": int(timestamp),
                "entity_id": int(entity_id),
                "entity_type": entity_type,
            }
        ]
        if RESPONSIBLE_USER_ID:
            payload[0]["responsible_user_id"] = RESPONSIBLE_USER_ID

        try:
            r = self.session.post(url, json=payload, timeout=10)
        except requests.RequestException:
            logger.exception("Network error while creating task")
            return None

        res = self._handle_response(r, expect_json=True)
        if not res["ok"]:
            logger.error("Create task failed: status=%s body=%s", res["status"], res["text"][:1000])
            return None

        j = res["json"]
        if not j:
            logger.error("Create task: empty JSON response")
            return None

        tid = _extract_id_from_response(j)
        if tid:
            return tid

        logger.error("Unexpected create_task response structure: %s", j)
        return None


# --- async wrappers (to not block event loop) ----------------
async def find_contact_by_phone_async(phone: str) -> Optional[dict]:
    if not AMO_BASE_URL:
        logger.error("AMO_BASE_URL not configured")
        return None

    def sync():
        s = AmoCRMSession(AMO_BASE_URL, AMO_ACCESS_TOKEN)
        return s.get_contact_by_phone(phone)
    try:
        return await asyncio.to_thread(sync)
    except Exception:
        logger.exception("find_contact_by_phone_async failed")
        return None

async def create_contact_async(name: str, phones: list, responsible_user_id: Optional[int] = None) -> Optional[int]:
    if not AMO_BASE_URL:
        logger.error("AMO_BASE_URL not configured")
        return None
    def sync():
        s = AmoCRMSession(AMO_BASE_URL, AMO_ACCESS_TOKEN)
        return s.create_contact(name=name, phones=phones, responsible_user_id=responsible_user_id)
    try:
        return await asyncio.to_thread(sync)
    except Exception:
        logger.exception("create_contact_async failed")
        return None

async def create_task_async(text: str, entity_id: int, timestamp: int, entity_type: str = 'contacts') -> Optional[int]:
    if not AMO_BASE_URL:
        logger.error("AMO_BASE_URL not configured")
        return None
    def sync():
        s = AmoCRMSession(AMO_BASE_URL, AMO_ACCESS_TOKEN)
        return s.create_task(text=text, entity_id=entity_id, timestamp=timestamp, entity_type=entity_type)
    try:
        return await asyncio.to_thread(sync)
    except Exception:
        logger.exception("create_task_async failed")
        return None

async def find_or_create_contact_and_create_task_async(name: str, phone: str, tg_id: int, task_text: Optional[str] = None) -> Dict[str, Any]:
    """
    High-level helper: find contact by phone, if not found create contact, then create task.
    Возвращает detailed dict с полями ok/reason/contact_id/task_id/created_contact
    """
    result = {"ok": False, "reason": None, "contact_id": None, "task_id": None, "created_contact": False}
    try:
        contact = await find_contact_by_phone_async(phone)
        contact_id = None
        created_contact = False
        if contact:
            contact_id = int(contact.get("id"))
        else:
            contact_id = await create_contact_async(name=name, phones=[phone], responsible_user_id=RESPONSIBLE_USER_ID or None)
            created_contact = bool(contact_id)
        if not contact_id:
            result["reason"] = "contact_not_created"
            return result

        due_ts = int(time.time()) + 60 * 60 * 24
        text = task_text or f"Проверить кандидата {name} ({phone}), tg:{tg_id}"
        task_id = await create_task_async(text=text, entity_id=contact_id, timestamp=due_ts, entity_type='contacts')
        if not task_id:
            result["reason"] = "task_creation_failed"
            result["contact_id"] = contact_id
            result["created_contact"] = created_contact
            return result

        result.update({"ok": True, "contact_id": contact_id, "task_id": task_id, "created_contact": created_contact})
        return result

    except Exception as e:
        logger.exception("Error in find_or_create_contact_and_create_task_async")
        result["reason"] = str(e)
        return result


