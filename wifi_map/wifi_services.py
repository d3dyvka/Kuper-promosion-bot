import json
import logging
import os
from typing import Any, Dict, List

logger = logging.getLogger("wifi_map")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


def _default_wifi_points() -> List[Dict[str, Any]]:
    """
    Базовый набор точек Wi‑Fi, который можно переопределить через файл или переменную окружения.

    Структура одной точки:
    {
        "name": "Название точки",
        "description": "Краткое описание/подсказка",
        "lat": 55.7558,
        "lon": 37.6173
    }
    """
    return [
        {
            "name": "Пример: ТЦ «Центральный»",
            "description": "Бесплатный Wi‑Fi в фудкорте и зоне отдыха.",
            "lat": 55.7558,
            "lon": 37.6173,
        },
    ]


def _load_points_from_file(path: str) -> List[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [p for p in data if isinstance(p, dict)]
        return []
    except Exception:
        logger.exception("Не удалось загрузить точки Wi‑Fi из файла %s", path)
        return []


def _load_points_from_env() -> List[Dict[str, Any]]:
    """
    Если задана переменная WIFI_POINTS_JSON, пытаемся прочитать точки оттуда.
    Ожидается JSON-массив словарей, как в _default_wifi_points.
    """
    raw = os.getenv("WIFI_POINTS_JSON")
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return [p for p in data if isinstance(p, dict)]
        return []
    except Exception:
        logger.exception("Не удалось распарсить WIFI_POINTS_JSON")
        return []


def get_available_wifi_points() -> List[Dict[str, Any]]:
    """
    Возвращает список доступных точек Wi‑Fi.
    Приоритет:
    1. Переменная окружения WIFI_POINTS_JSON.
    2. Файл wifi_points.json в корне проекта (если существует).
    3. Встроенный дефолтный список.
    """
    # 1. из переменной окружения
    pts = _load_points_from_env()
    if pts:
        return pts

    # 2. из файла в корне проекта
    file_path = os.getenv("WIFI_POINTS_FILE", "wifi_points.json")
    pts = _load_points_from_file(file_path)
    if pts:
        return pts

    # 3. дефолтные
    return _default_wifi_points()


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Возвращает расстояние в метрах между двумя точками.
    """
    from math import radians, sin, cos, sqrt, atan2

    R = 6371000  # средний радиус Земли в метрах
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)

    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


def find_wifi_near_location(lat: float, lon: float, radius_m: float = 50.0) -> List[Dict[str, Any]]:
    """
    Возвращает список точек Wi‑Fi, попадающих в радиус radius_m от указанной локации.
    В ответ добавляется поле distance_m.
    """
    points = get_available_wifi_points()
    nearby = []
    for p in points:
        try:
            plat = float(p.get("lat"))
            plon = float(p.get("lon"))
        except (TypeError, ValueError):
            continue
        dist = _haversine_m(lat, lon, plat, plon)
        if dist <= radius_m:
            enriched = dict(p)
            enriched["distance_m"] = round(dist, 1)
            nearby.append(enriched)

    nearby.sort(key=lambda item: item.get("distance_m", radius_m))
    return nearby

