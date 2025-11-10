import re
import logging
from decimal import Decimal
from typing import Optional, Dict, Any
import requests

from decouple import config

logger = logging.getLogger("jump_api")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

BASE_URL = config("JUMP_BASE_URL", "https://v2.jump.taxi/taxi-public/v1").rstrip("/")
CLIENT_KEY = config("JUMP_CLIENT_KEY", "")   # обязателен
USE_CLIENT_KEY_IN_QUERY = config("JUMP_CLIENT_KEY_IN_QUERY", "") == "1"

def _normalize_phone(phone: str) -> str:
    return re.sub(r"\D+", "", (phone or ""))

def _headers():
    h = {"Accept": "application/json", "Content-Type": "application/json"}
    if CLIENT_KEY and not USE_CLIENT_KEY_IN_QUERY:
        h["Client-Key"] = CLIENT_KEY
    return h

def _params(extra: Optional[Dict[str,Any]] = None):
    p = {}
    if CLIENT_KEY and USE_CLIENT_KEY_IN_QUERY:
        p["client_key"] = CLIENT_KEY
    if extra:
        p.update(extra)
    return p or None

def get_driver_by_phone(phone: str) -> Optional[Dict[str,Any]]:
    phone_norm = _normalize_phone(phone)
    if not phone_norm:
        return None
    url = f"{BASE_URL}/drivers"
    params = _params({"search": phone_norm})
    try:
        r = requests.get(url, headers=_headers(), params=params, timeout=10)
    except Exception:
        logger.exception("Network error GET /drivers")
        return None

    if r.status_code == 401:
        logger.error("Auth error GET /drivers: %s", r.text)
        return None
    if r.status_code != 200:
        logger.warning("GET /drivers returned %s: %s", r.status_code, r.text[:500])
        return None

    j = r.json()
    items = j.get("items") if isinstance(j, dict) else None
    if not items:
        items = j if isinstance(j, list) else []
    for it in items:
        ph = str(it.get("phone") or "")
        if _normalize_phone(ph).endswith(phone_norm[-10:]):
            return it
    return None

def get_balance_by_phone(phone: str) -> Decimal:
    d = get_driver_by_phone(phone)
    if not d:
        return Decimal(0)
    bal = d.get("balance")
    try:
        return Decimal(str(bal)) if bal is not None else Decimal(0)
    except Exception:
        return Decimal(0)

def get_driver_profile(driver_id: int) -> Optional[Dict[str,Any]]:
    url = f"{BASE_URL}/drivers/{int(driver_id)}"
    params = _params()
    try:
        r = requests.get(url, headers=_headers(), params=params, timeout=10)
    except Exception:
        logger.exception("Network error GET /drivers/{id}")
        return None
    if r.status_code != 200:
        logger.warning("GET /drivers/{id} returned %s: %s", r.status_code, r.text[:500])
        return None
    return r.json()

def preview_withdrawal(driver_id: int, amount: float, balance_id: int, include_commission: bool = False) -> Optional[Dict[str,Any]]:
    url = f"{BASE_URL}/drivers/{int(driver_id)}/transactions-withdraw-preview"
    payload = {"amount": amount, "balance_id": int(balance_id), "include_commission": include_commission}
    params = _params()
    try:
        r = requests.post(url, headers=_headers(), params=params, json=payload, timeout=10)
    except Exception:
        logger.exception("Network error POST preview")
        return None
    if r.status_code == 401:
        logger.error("Auth error preview withdraw: %s", r.text)
        return None
    if r.status_code not in (200,):
        logger.warning("Preview withdraw returned %s: %s", r.status_code, r.text[:800])
        try:
            return r.json()
        except Exception:
            return {"status_code": r.status_code, "text": r.text}
    return r.json()
def _create_withdrawal_transaction_api(driver_id: int,
                                       amount: float,
                                       balance_id: int,
                                       transaction_type_id: Optional[int] = None,
                                       message: Optional[str] = None,
                                       create_payment: bool = True,
                                       include_commission: bool = False) -> Dict[str,Any]:
    """
    Низкоуровневый вызов к Jump API, делает PUT /drivers/{id}/transactions
    Возвращает dict с ключом 'ok' и подробностями в 'raw' или 'error'.
    """
    url = f"{BASE_URL}/drivers/{int(driver_id)}/transactions"
    payload: Dict[str,Any] = {
        "operation": "withdraw",
        "amount": amount,
        "balance_id": int(balance_id),
        "create_payment": bool(create_payment),
        "include_commission": bool(include_commission),
    }
    if transaction_type_id is not None:
        payload["transaction_type_id"] = int(transaction_type_id)
    if message:
        payload["message"] = str(message)

    params = _params()
    try:
        r = requests.put(url, headers=_headers(), params=params, json=payload, timeout=15)
    except Exception:
        logger.exception("Network error PUT /transactions")
        return {"ok": False, "error": "network"}

    if r.status_code in (200, 204):
        try:
            return {"ok": True, "status_code": r.status_code, "raw": r.json() if r.text else None}
        except Exception:
            return {"ok": True, "status_code": r.status_code, "raw": None}
    logger.warning("PUT /transactions returned %s: %s", r.status_code, r.text[:1000])
    try:
        return {"ok": False, "status_code": r.status_code, "raw": r.json()}
    except Exception:
        return {"ok": False, "status_code": r.status_code, "text": r.text}

def create_withdrawal_transaction(phone: str,
                                  amount: float,
                                  requisites: str,
                                  *,
                                  use_preview: bool = True,
                                  include_commission: bool = False,
                                  create_payment: bool = True) -> Dict[str, Any]:
    try:
        phone_norm = _normalize_phone(phone)
        if not phone_norm:
            return {"ok": False, "reason": "invalid_phone", "error": "empty phone after normalization"}
    except Exception:
        return {"ok": False, "reason": "invalid_phone", "error": "normalization_failed"}

    # 1) Найти водителя
    driver = get_driver_by_phone(phone)
    if not driver:
        logger.info("Driver not found by phone: %s", phone)
        return {"ok": False, "reason": "driver_not_found", "phone": phone}

    driver_id = driver.get("id") or driver.get("driver_id") or driver.get("driverId")
    if not driver_id:
        logger.warning("Driver found but no id field: %s", driver)
        return {"ok": False, "reason": "driver_missing_id", "driver": driver}

    # 2) Определить balance_id — несколько возможных мест в ответе API
    balance_id = None
    # try common places
    if "balance_id" in driver and driver.get("balance_id"):
        balance_id = int(driver.get("balance_id"))
    elif "balance" in driver and isinstance(driver.get("balance"), dict):
        # иногда balance может быть объект с id
        b = driver.get("balance")
        if isinstance(b, dict) and b.get("id"):
            balance_id = int(b.get("id"))
    # если не нашли — получить профиль драйвера и смотреть поле balances / accounts
    if balance_id is None:
        profile = get_driver_profile(driver_id)
        if profile:
            # варианты: profile.get("balances") -> list of dicts with 'id'
            balances = profile.get("balances") or profile.get("balance_accounts") or profile.get("balances_list")
            if isinstance(balances, list) and balances:
                # попытка выбрать баланс с type 'cash'/'main' или просто первый
                chosen = None
                for b in balances:
                    # tolerant checks
                    if isinstance(b, dict) and b.get("id"):
                        chosen = b
                        # prefer balance that has non-zero id and maybe currency = RUB
                        # if there is an attribute 'is_main' or 'type' we could prefer it
                        if b.get("is_main") or b.get("type") in ("main", "cash"):
                            chosen = b
                            break
                if chosen:
                    balance_id = int(chosen.get("id"))

    # fallback: try driver.get("default_balance_id") or driver.get("balances")
    if balance_id is None:
        if driver.get("default_balance_id"):
            try:
                balance_id = int(driver.get("default_balance_id"))
            except Exception:
                balance_id = None
    # final fallback: try numeric keys in driver
    if balance_id is None:
        for key in ("balances", "driver_balances", "balance_accounts"):
            v = driver.get(key)
            if isinstance(v, list) and v:
                first = v[0]
                if isinstance(first, dict) and first.get("id"):
                    balance_id = int(first.get("id"))
                    break

    if balance_id is None:
        logger.warning("No balance_id found for driver %s (phone=%s)", driver_id, phone)
        return {"ok": False, "reason": "balance_not_found", "driver": driver}

    # 3) Опционально: preview
    preview_res = None
    if use_preview:
        try:
            preview_res = preview_withdrawal(driver_id=int(driver_id), amount=float(amount), balance_id=int(balance_id), include_commission=include_commission)
            # If preview_res indicates an error from API, return error
            if preview_res and isinstance(preview_res, dict) and preview_res.get("status_code") and preview_res.get("status_code") != 200:
                return {"ok": False, "reason": "preview_error", "preview": preview_res}
        except Exception:
            logger.exception("Preview failed")
            # не прерываем, можно попробовать создать транзакцию, но возвращаем preview в рез-те
            preview_res = {"ok": False, "error": "preview_exception"}

    # 4) Выполнить создание транзакции через низкоуровневый call
    try:
        # message: кладём реквизиты/описание
        message_text = f"Ручной вывод: {requisites}"
        tx_res = _create_withdrawal_transaction_api(driver_id=int(driver_id),
                                                    amount=float(amount),
                                                    balance_id=int(balance_id),
                                                    message=message_text,
                                                    create_payment=create_payment,
                                                    include_commission=include_commission)
    except Exception as e:
        logger.exception("Error while creating withdrawal transaction")
        return {"ok": False, "reason": "create_exception", "error": str(e), "driver": driver, "preview": preview_res}

    # 5) Обработать ответ от API
    if not tx_res.get("ok"):
        # вернуть raw ответ если есть, дать понятную причину
        return {
            "ok": False,
            "reason": "api_error",
            "tx_response": tx_res,
            "driver": driver,
            "preview": preview_res
        }

    # успешно
    return {
        "ok": True,
        "reason": "created",
        "tx": tx_res.get("raw") or tx_res,
        "driver": driver,
        "preview": preview_res
    }
def get_payments(params: Optional[Dict[str,Any]] = None) -> Optional[Dict[str,Any]]:
    url = f"{BASE_URL}/payments"
    p = _params(params) or params
    try:
        r = requests.get(url, headers=_headers(), params=p, timeout=12)
    except Exception:
        logger.exception("Network error GET /payments")
        return None
    if r.status_code != 200:
        logger.warning("GET /payments returned %s: %s", r.status_code, r.text[:800])
        return None
    return r.json()
