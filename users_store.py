import json
import os
from typing import Dict, List, Optional

FILE_PATH = "users.json"


def _load() -> List[Dict]:
    if not os.path.exists(FILE_PATH):
        return []
    try:
        with open(FILE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception:
        return []
    return []


def _save(data: List[Dict]) -> None:
    try:
        with open(FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        # в худшем случае просто не сохраняем; не мешаем работе бота
        return


def _next_id(items: List[Dict]) -> int:
    if not items:
        return 1
    try:
        return max(int(x.get("id", 0)) for x in items) + 1
    except Exception:
        return len(items) + 1


def add_or_update_user(*, name: Optional[str], phone: Optional[str], tg_id: Optional[int], in_metabase: bool) -> Dict:
    """
    Сохраняет пользователя в users.json. Уникальность по номеру телефона (концовка 10 цифр).
    Возвращает сохранённый объект.
    """
    items = _load()
    norm_phone = _normalize_phone(phone)

    existing = None
    for it in items:
        if _normalize_phone(it.get("phone")) == norm_phone and norm_phone:
            existing = it
            break

    if existing:
        existing["name"] = name or existing.get("name")
        existing["phone"] = phone or existing.get("phone")
        existing["tg_id"] = tg_id if tg_id is not None else existing.get("tg_id")
        existing["in_metabase"] = bool(in_metabase)
        saved = existing
    else:
        new_id = _next_id(items)
        saved = {
            "id": new_id,
            "name": name or "",
            "phone": phone or "",
            "tg_id": tg_id,
            "in_metabase": bool(in_metabase),
        }
        items.append(saved)

    _save(items)
    return saved


def is_in_metabase(phone: Optional[str]) -> Optional[bool]:
    """
    Возвращает True/False если есть запись, None если нет сведений.
    """
    norm_phone = _normalize_phone(phone)
    if not norm_phone:
        return None
    for it in _load():
        if _normalize_phone(it.get("phone")) == norm_phone:
            return bool(it.get("in_metabase"))
    return None


def _normalize_phone(phone: Optional[str]) -> str:
    import re

    if not phone:
        return ""
    return re.sub(r"\D+", "", phone)[-10:]


