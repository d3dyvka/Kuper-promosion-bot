#!/usr/bin/env python3
# coding: utf-8
from __future__ import annotations
import re
import logging
import time
import json
import os
from decimal import Decimal
from typing import Optional, Dict, Any, List, Tuple
import requests
from decouple import config

logger = logging.getLogger("jump_api")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

BASE_URL = config("JUMP_BASE_URL", "https://v2.jump.taxi/taxi-public/v1").rstrip("/")
CLIENT_KEY = config("JUMP_CLIENT_KEY", "")
USE_CLIENT_KEY_IN_QUERY = config("JUMP_CLIENT_KEY_IN_QUERY", "") == "1"
DEFAULT_TRANSACTION_TYPE_ID = config("DEFAULT_TRANSACTION_TYPE_ID", "") or None

PREVIEW_PARAM_CANDIDATE_NAMES = ("balance_id", "requisites_id", "write_off_account_id", "bank_account_id", "write_off_account")
TRANSACTION_PARAM_CANDIDATE_NAMES = PREVIEW_PARAM_CANDIDATE_NAMES

OPERATION_TX_TYPE_FALLBACK = {
    "withdraw": 14,
}

def _normalize_phone(phone: str) -> str:
    return re.sub(r"\D+", "", (phone or ""))

def _headers() -> Dict[str, str]:
    h = {"Accept": "application/json", "Content-Type": "application/json"}
    if CLIENT_KEY and not USE_CLIENT_KEY_IN_QUERY:
        h["Client-Key"] = CLIENT_KEY
    return h

def _params(extra: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    p: Dict[str, Any] = {}
    if CLIENT_KEY and USE_CLIENT_KEY_IN_QUERY:
        p["client_key"] = CLIENT_KEY
    if extra:
        p.update(extra)
    return p or None

def _request(method: str, path: str, **kwargs) -> requests.Response:
    url = f"{BASE_URL}{path}"
    params = kwargs.pop("params", None) or _params()
    headers = kwargs.pop("headers", None) or _headers()
    timeout = kwargs.pop("timeout", 15)
    allow_redirects = kwargs.pop("allow_redirects", None)
    if allow_redirects is None:
        allow_redirects = method.upper() in ("GET", "HEAD", "OPTIONS")
    return requests.request(method, url, headers=headers, params=params, timeout=timeout, allow_redirects=allow_redirects, **kwargs)

def get_balance_by_phone(phone: str) -> Decimal:
    d = get_driver_by_phone(phone)
    if not d:
        return Decimal(0)
    bal = d.get("balance")
    try:
        return Decimal(str(bal)) if bal is not None else Decimal(0)
    except Exception:
        return Decimal(0)

def get_driver_by_phone(phone: str) -> Optional[Dict[str, Any]]:
    pn = _normalize_phone(phone)
    if not pn:
        return None
    try:
        r = _request("GET", "/drivers", params={"search": pn})
    except Exception:
        logger.exception("Network error GET /drivers")
        return None
    if r.status_code != 200:
        logger.warning("GET /drivers returned %s: %.300s", r.status_code, r.text)
        return None
    try:
        j = r.json()
    except Exception:
        logger.exception("Failed to parse /drivers JSON")
        return None
    items = j.get("items") if isinstance(j, dict) else (j if isinstance(j, list) else [])
    for it in items:
        ph = str(it.get("phone") or "")
        if _normalize_phone(ph).endswith(pn[-10:]):
            return it
    return None

def get_driver_profile(driver_id: int) -> Optional[Dict[str, Any]]:
    try:
        r = _request("GET", f"/drivers/{int(driver_id)}")
    except Exception:
        logger.exception("Network error GET /drivers/{id}")
        return None
    if r.status_code != 200:
        logger.warning("GET /drivers/%s returned %s: %.500s", driver_id, r.status_code, r.text)
        return None
    try:
        return r.json()
    except Exception:
        logger.exception("Failed to parse profile")
        return None

def get_payments_for_driver(driver_id: int, per_page: int = 5) -> List[Dict[str, Any]]:
    try:
        r = _request("GET", "/payments", params={"driver_ids": str(driver_id), "per_page": per_page})
    except Exception:
        logger.exception("Network error GET /payments")
        return []
    if r.status_code != 200:
        logger.debug("GET /payments returned %s: %.300s", r.status_code, r.text)
        return []
    try:
        j = r.json()
    except Exception:
        logger.exception("Failed to parse payments JSON")
        return []
    if isinstance(j, dict):
        return j.get("items") or j.get("data") or []
    return j if isinstance(j, list) else []

def get_transaction_types() -> List[Dict[str, Any]]:
    try:
        r = _request("GET", "/transaction-types")
    except Exception:
        logger.exception("Network error GET /transaction-types")
        return []
    if r.status_code != 200:
        logger.debug("GET /transaction-types returned %s: %.300s", r.status_code, r.text)
        return []
    try:
        j = r.json()
        if isinstance(j, dict):
            return j.get("items") or j.get("data") or []
        return j if isinstance(j, list) else []
    except Exception:
        logger.exception("Failed to parse transaction-types")
        return []

def choose_transaction_type_id(operation: str = "withdraw", preferred_id: Optional[int] = None) -> Optional[int]:
    if DEFAULT_TRANSACTION_TYPE_ID:
        try:
            return int(DEFAULT_TRANSACTION_TYPE_ID)
        except Exception:
            logger.debug("DEFAULT_TRANSACTION_TYPE_ID invalid")
    if preferred_id:
        try:
            return int(preferred_id)
        except Exception:
            pass
    op_lower = (operation or "").lower()
    if op_lower in OPERATION_TX_TYPE_FALLBACK and OPERATION_TX_TYPE_FALLBACK[op_lower]:
        try:
            return int(OPERATION_TX_TYPE_FALLBACK[op_lower])
        except Exception:
            pass
    types = get_transaction_types()
    if not types:
        return None
    keywords = []
    if op_lower == "withdraw":
        keywords = ("withdraw", "payout", "вывод", "выплата")
    elif op_lower == "deposit":
        keywords = ("deposit", "пополнение", "зачислен")
    elif op_lower == "transfer":
        keywords = ("transfer", "перевод")
    else:
        keywords = (op_lower,)
    for kw in keywords:
        for t in types:
            name = str(t.get("name") or "").lower()
            if kw in name and t.get("id"):
                try:
                    return int(t.get("id"))
                except Exception:
                    continue
    for t in types:
        if t.get("id"):
            try:
                return int(t.get("id"))
            except Exception:
                continue
    return None

def is_antifraud_by_phone(phone: str) -> bool:
    d = get_driver_by_phone(phone)
    if not d:
        return False
    driver_id = d.get("id")
    if not driver_id:
        return False
    profile = get_driver_profile(driver_id) or {}
    mode = profile.get("mode") or profile.get("flags") or profile.get("status") or {}
    try:
        if isinstance(mode, str) and "antifraud" in mode.lower():
            return True
    except Exception:
        pass
    try:
        if isinstance(mode, dict):
            for v in mode.values():
                if isinstance(v, str) and "antifraud" in v.lower():
                    return True
        if isinstance(mode, (list, tuple)):
            for v in mode:
                if isinstance(v, str) and "antifraud" in v.lower():
                    return True
    except Exception:
        pass
    try:
        prof_text = json.dumps(profile).lower()
        if "antifraud" in prof_text:
            return True
    except Exception:
        pass
    return False

def get_driver_group_by_phone(phone: str) -> Optional[str]:
    d = get_driver_by_phone(phone)
    if not d:
        return None
    driver_id = d.get("id")
    if not driver_id:
        return None
    profile = get_driver_profile(driver_id) or {}
    for key in ("group", "segment", "pool", "type", "user_group"):
        v = profile.get(key)
        if v:
            try:
                return str(v)
            except Exception:
                continue
    try:
        s = json.dumps(profile)
        m = re.search(r'\"(group|segment|pool)\"\s*:\s*\"([^\"]+)\"', s)
        if m:
            return m.group(2)
    except Exception:
        pass
    return None

def get_withdraw_conditions_by_phone(phone: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    d = get_driver_by_phone(phone)
    if not d:
        return out
    driver_id = d.get("id")
    if not driver_id:
        return out
    try:
        r = _request("POST", f"/drivers/{driver_id}/transactions-withdraw-preview", json={"amount": 1})
        if r.status_code == 200:
            try:
                out["preview"] = r.json()
            except Exception:
                out["preview_raw_text"] = r.text
    except Exception:
        logger.debug("preview endpoint not available or failed")
    try:
        profile = get_driver_profile(driver_id)
        if profile:
            out["profile_meta"] = {}
            if "balance" in profile:
                out["profile_meta"]["balance"] = profile.get("balance")
            s = json.dumps(profile)
            matches = re.findall(r'(\d+)\s*(?:руб|р|RUB)', s)
            if matches:
                out["profile_meta"]["mentions_money"] = matches[:5]
    except Exception:
        pass
    return out

def _make_value_variants(candidate_value: Any) -> List[Any]:
    vals: List[Any] = []
    vals.append(candidate_value)
    try:
        if isinstance(candidate_value, int):
            vals.append({"id": int(candidate_value)})
            vals.append(str(candidate_value))
    except Exception:
        pass
    if isinstance(candidate_value, str) and "-" in candidate_value:
        vals.append({"uuid": candidate_value})
    seen = set()
    dedup = []
    for v in vals:
        k = repr(v)
        if k not in seen:
            seen.add(k)
            dedup.append(v)
    return dedup

def preview_withdrawal_try_variants(driver_id: int, amount: float, candidate_value: Any, include_commission: bool = False):
    url_path = f"/drivers/{int(driver_id)}/transactions-withdraw-preview"
    value_variants = _make_value_variants(candidate_value)
    last_status = None
    last_raw = None
    for key in PREVIEW_PARAM_CANDIDATE_NAMES:
        for val in value_variants:
            payload = {"amount": float(amount), key: val, "include_commission": bool(include_commission)}
            try:
                r = _request("POST", url_path, json=payload)
            except Exception:
                logger.exception("Network error POST preview attempt key=%s val=%s", key, repr(val))
                last_status = None
                last_raw = {"text": "network_exception"}
                continue
            last_status = r.status_code
            text = (r.text or "").strip()
            try:
                parsed = r.json() if text else None
                last_raw = parsed if parsed is not None else {"text": text, "status_code": r.status_code}
            except Exception:
                last_raw = {"text": text, "status_code": r.status_code}
            logger.debug("Preview try key=%s val=%s -> status=%s raw=%s", key, repr(val), r.status_code, last_raw)
            if r.status_code == 200:
                return True, r.status_code, last_raw, key, val
    return False, last_status, last_raw, None, None

def _create_withdrawal_transaction_api_try_variants(driver_id: int,
                                                    amount: float,
                                                    candidate_value: Any,
                                                    transaction_type_id: Optional[int] = None,
                                                    message: Optional[str] = None,
                                                    create_payment: bool = True,
                                                    include_commission: bool = False) -> Dict[str, Any]:
    url_path = f"/drivers/{int(driver_id)}/transactions"
    value_variants = _make_value_variants(candidate_value)
    last_res = {"ok": False, "status_code": None, "raw": None, "used_key": None, "used_value": None, "tried": None}
    for key in TRANSACTION_PARAM_CANDIDATE_NAMES:
        for val in value_variants:
            payload: Dict[str, Any] = {
                "operation": "withdraw",
                "amount": float(amount),
                key: val,
                "create_payment": bool(create_payment),
                "include_commission": bool(include_commission),
            }
            if transaction_type_id is not None:
                try:
                    payload["transaction_type_id"] = int(transaction_type_id)
                except Exception:
                    payload["transaction_type_id"] = transaction_type_id
            if message:
                payload["message"] = str(message)
            try:
                r = _request("PUT", url_path, json=payload, allow_redirects=False)
            except Exception:
                logger.exception("Network error PUT /transactions attempt key=%s val=%s", key, repr(val))
                last_res.update({"raw": {"text": "network_exception"}, "used_key": key, "used_value": val, "tried": "put"})
                continue
            try:
                raw = r.json() if r.text else None
            except Exception:
                raw = {"text": r.text}
            logger.debug("PUT try key=%s val=%s -> status=%s raw=%s", key, repr(val), r.status_code, raw)
            if r.status_code in (200, 201, 204):
                return {"ok": True, "status_code": r.status_code, "raw": raw, "used_key": key, "used_value": val, "tried": "put"}
            ct = r.headers.get("Content-Type", "") or ""
            if 300 <= r.status_code < 400 or ("text/html" in ct and r.status_code < 500):
                logger.info("PUT produced redirect/HTML; trying POST fallback for key=%s val=%s", key, repr(val))
                try:
                    r2 = _request("POST", url_path, json=payload, allow_redirects=False)
                except Exception:
                    logger.exception("Network error POST fallback")
                    last_res.update({"raw": {"text": "network_exception_post"}, "used_key": key, "used_value": val, "tried": "post"})
                    continue
                try:
                    raw2 = r2.json() if r2.text else None
                except Exception:
                    raw2 = {"text": r2.text}
                logger.debug("POST fallback key=%s val=%s -> status=%s raw=%s", key, repr(val), r2.status_code, raw2)
                if r2.status_code in (200, 201, 204):
                    return {"ok": True, "status_code": r2.status_code, "raw": raw2, "used_key": key, "used_value": val, "tried": "post"}
                last_res.update({"status_code": r2.status_code, "raw": raw2, "used_key": key, "used_value": val, "tried": "post"})
            else:
                last_res.update({"status_code": r.status_code, "raw": raw, "used_key": key, "used_value": val, "tried": "put"})
    return last_res

def _only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def suffix_match_length(a: str, b: str) -> int:
    a_d = _only_digits(a)
    b_d = _only_digits(b)
    if not a_d or not b_d:
        return 0
    i = 0
    ai = len(a_d) - 1
    bi = len(b_d) - 1
    while ai >= 0 and bi >= 0:
        if a_d[ai] != b_d[bi]:
            break
        i += 1
        ai -= 1
        bi -= 1
    return i

def bank_matches_hint(obj: Dict[str, Any], bank_hint: Optional[str]) -> bool:
    if not bank_hint:
        return False
    hint = bank_hint.strip().lower()
    fields = []
    if isinstance(obj.get("name"), str):
        fields.append(obj.get("name"))
    if isinstance(obj.get("title"), str):
        fields.append(obj.get("title"))
    if isinstance(obj.get("card"), dict) and isinstance(obj.get("card").get("name"), str):
        fields.append(obj.get("card").get("name"))
    if isinstance(obj.get("additional"), dict):
        for k in ("bank_name", "name", "title"):
            v = obj.get("additional").get(k)
            if isinstance(v, str):
                fields.append(v)
    exch = obj.get("exchange") or {}
    if isinstance(exch.get("name"), str):
        fields.append(exch.get("name"))
    for v in fields:
        if v and hint in v.lower():
            return True
    return False

def _extract_card_like_objects(profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not profile:
        return []
    p = profile.get("item") if isinstance(profile.get("item"), dict) else profile
    out: List[Dict[str, Any]] = []
    cards = p.get("cards") or []
    reqs = p.get("requisites") or []
    if isinstance(cards, list):
        for c in cards:
            if isinstance(c, dict):
                out.append(c)
    if isinstance(reqs, list):
        for r in reqs:
            if isinstance(r, dict):
                out.append(r)
    for k in ("bank_account", "write_off_account"):
        v = p.get(k)
        if isinstance(v, dict):
            out.append(v)
    return out

def _get_mask_from_obj(obj: Dict[str, Any]) -> Optional[str]:
    for k in ("mask", "card_number", "account_number", "description"):
        v = obj.get(k)
        if isinstance(v, str) and any(ch.isdigit() for ch in v):
            return v
    if isinstance(obj.get("card"), dict):
        v = obj.get("card").get("mask") or obj.get("card").get("uuid")
        if isinstance(v, str):
            return v
    if isinstance(obj.get("additional"), dict):
        v = obj.get("additional").get("card", {}).get("mask") or obj.get("additional").get("account_number")
        if isinstance(v, str):
            return v
    return None

def choose_candidates(profile: Dict[str, Any], card_number_hint: Optional[str], phone_hint: Optional[str], bank_hint: Optional[str]) -> List[Dict[str, Any]]:
    objs = _extract_card_like_objects(profile)
    scored: List[Dict[str, Any]] = []
    for obj in objs:
        mask = _get_mask_from_obj(obj) or ""
        score = 0
        if card_number_hint:
            m = suffix_match_length(mask, card_number_hint)
            score += m * 100
            if _only_digits(mask) and _only_digits(mask) == _only_digits(card_number_hint):
                score += 10000
        if phone_hint:
            acct = (obj.get("account_number") or obj.get("description") or (obj.get("additional") or {}).get("account_number") or "")
            if acct:
                if _only_digits(acct).endswith(_only_digits(phone_hint)[-10:]):
                    score += 500
        if bank_hint and bank_matches_hint(obj, bank_hint):
            score += 300
        cid = None
        if obj.get("id"):
            try:
                cid = int(obj.get("id"))
                score += 50
            except Exception:
                cid = None
        elif isinstance(obj.get("card"), dict) and obj.get("card").get("id"):
            try:
                cid = int(obj.get("card").get("id"))
                score += 50
            except Exception:
                cid = None
        kind = "other"
        if obj in (profile.get("item") or {}).get("cards", []) or obj.get("card"):
            kind = "card"
            score += 20
        elif obj in (profile.get("item") or {}).get("requisites", []):
            kind = "requisite"
        preferred_value = None
        if cid:
            preferred_value = cid
        else:
            uuid = (obj.get("card") or {}).get("uuid") or obj.get("uuid")
            if isinstance(uuid, str) and "-" in uuid:
                preferred_value = uuid
            else:
                mask_v = _get_mask_from_obj(obj)
                if mask_v:
                    preferred_value = mask_v
                else:
                    preferred_value = obj
        scored.append({"kind": kind, "obj": obj, "preferred_value": preferred_value, "score": score})
    scored.sort(key=lambda x: x.get("score", 0), reverse=True)
    return scored

def perform_withdrawal(*,
                       phone: str,
                       amount: float,
                       requisites: Optional[str] = None,
                       card_number: Optional[str] = None,
                       bank_hint: Optional[str] = None,
                       tx_type_id: Optional[int] = None,
                       use_preview: bool = True,
                       include_commission: bool = False,
                       create_payment: bool = True,
                       operation: str = "withdraw",
                       force_try_without_tx_type: bool = False) -> Dict[str, Any]:
    """
    Performs withdrawal; enforces MIN_REMAIN on balance.
    Returns dict with ok/notice/amount_sent fields.
    """
    MIN_REMAIN = Decimal("50")
    if not phone:
        return {"ok": False, "reason": "need_driver_phone"}

    driver = get_driver_by_phone(phone)
    if not driver:
        return {"ok": False, "reason": "driver_not_found", "phone": phone}
    driver_id = driver.get("id")
    if not driver_id:
        return {"ok": False, "reason": "driver_missing_id", "driver": driver}

    try:
        amount = float(amount)
    except Exception:
        return {"ok": False, "reason": "invalid_amount", "amount": amount}

    balance = driver.get("balance")
    try:
        bal_dec = Decimal(str(balance)) if balance is not None else Decimal(0)
    except Exception:
        bal_dec = Decimal(0)

    allowed_withdrawable = float(max(bal_dec - MIN_REMAIN, Decimal(0)))
    if allowed_withdrawable <= 0:
        return {"ok": False, "reason": "insufficient_after_minimum", "allowed": 0, "balance": str(bal_dec)}

    adjusted = False
    if amount > allowed_withdrawable:
        amount_to_send = allowed_withdrawable
        adjusted = True
    else:
        amount_to_send = amount

    profile = get_driver_profile(driver_id) or {}

    candidates_scored = choose_candidates(profile, card_number_hint=card_number, phone_hint=phone, bank_hint=bank_hint)
    if not candidates_scored:
        card_ids = []
        p = profile.get("item") if isinstance(profile.get("item"), dict) else profile
        for c in (p.get("cards") or []):
            if isinstance(c, dict) and c.get("id"):
                try:
                    card_ids.append(int(c.get("id")))
                except Exception:
                    pass
        if not card_ids:
            return {"ok": False, "reason": "no_candidates_found", "driver": driver, "profile": profile}
        for cid in card_ids:
            candidates_scored.append({"kind": "card", "obj": {}, "preferred_value": int(cid), "score": 0})

    if DEFAULT_TRANSACTION_TYPE_ID:
        try:
            tx_type = int(DEFAULT_TRANSACTION_TYPE_ID)
        except Exception:
            tx_type = None
    elif tx_type_id:
        tx_type = tx_type_id
    else:
        tx_type = choose_transaction_type_id(operation=operation, preferred_id=None)
        if tx_type is None:
            tx_type = OPERATION_TX_TYPE_FALLBACK.get(operation.lower())

    preview_errors = []
    create_errors = []

    for idx, cand in enumerate(candidates_scored):
        pref = cand.get("preferred_value")
        obj = cand.get("obj")
        kind = cand.get("kind")
        logger.info("Trying candidate %d/%d kind=%s pref=%s score=%s", idx + 1, len(candidates_scored), kind, repr(pref), cand.get("score"))

        if use_preview and operation.lower() == "withdraw":
            try:
                ok, status, raw, used_key, used_val = preview_withdrawal_try_variants(driver_id=int(driver_id), amount=float(amount_to_send), candidate_value=pref, include_commission=include_commission)
            except Exception:
                logger.exception("Preview exception for candidate %s", repr(pref))
                preview_errors.append({"candidate": pref, "error": "exception"})
                continue
            if not ok:
                logger.warning("Preview failed for candidate %s; status=%s raw=%s", repr(pref), status, raw)
                preview_errors.append({"candidate": pref, "status": status, "raw": raw})
                continue
            logger.info("Preview OK for candidate %s (used_key=%s used_val=%s)", repr(pref), used_key, repr(used_val))

        message_text = requisites or f"Ручной вывод"
        try:
            tx_res = _create_withdrawal_transaction_api_try_variants(driver_id=int(driver_id),
                                                                     amount=float(amount_to_send),
                                                                     candidate_value=pref,
                                                                     transaction_type_id=tx_type,
                                                                     message=message_text,
                                                                     create_payment=create_payment,
                                                                     include_commission=include_commission)
        except Exception:
            logger.exception("Create exception for candidate %s", repr(pref))
            create_errors.append({"candidate": pref, "error": "exception"})
            continue

        if tx_res.get("ok"):
            logger.info("Withdrawal created successfully for driver %s using candidate %s", driver_id, repr(pref))
            res = {"ok": True, "reason": "created", "tx": tx_res.get("raw"), "driver": driver,
                   "candidate": pref, "used_key": tx_res.get("used_key"), "used_value": tx_res.get("used_value"),
                   "tx_type_id": tx_type, "amount_sent": amount_to_send, "adjusted": adjusted, "allowed": allowed_withdrawable}
            if adjusted:
                res["notice"] = f"Сумма уменьшена до {amount_to_send:.2f} ₽ чтобы на счёте осталось 50 ₽."
            return res

        create_errors.append({"candidate": pref, "status": tx_res.get("status_code"), "raw": tx_res.get("raw"), "used_key": tx_res.get("used_key"), "tried": tx_res.get("tried")})
        time.sleep(0.2)
        continue

    return {"ok": False, "reason": "no_candidate_succeeded", "driver": driver, "profile": profile, "candidates": candidates_scored, "preview_errors": preview_errors, "create_errors": create_errors}
