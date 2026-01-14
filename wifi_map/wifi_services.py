import json
from loguru import logger
import os
from typing import Any, Dict, List

import requests
from decouple import config

WIGLE_API_NAME = config("WIGLE_API_NAME", default="")
WIGLE_API_TOKEN = config("WIGLE_API_TOKEN", default="")
WIGLE_API_URL = "https://api.wigle.net/api/v2/network/search"


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


import time


def _query_wigle_near(lat: float, lon: float, radius_m: float = 50.0, max_results: int = 25, max_retries: int = 3) -> list[dict]:
    """
    Запрашивает у WiGLE открытые сети рядом с точкой.
    Добавлена обработка 429 с повторными попытками (экспоненциальный бэкофф).
    """
    if not WIGLE_API_NAME or not WIGLE_API_TOKEN:
        logger.debug("WiGLE API creds not configured; skipping remote search")
        return []

    from math import cos, radians

    delta_lat = radius_m / 111320.0
    cos_lat = cos(radians(lat)) or 1.0
    delta_lon = radius_m / (111320.0 * cos_lat)

    lat1, lat2 = lat - delta_lat, lat + delta_lat
    lon1, lon2 = lon - delta_lon, lon + delta_lon

    params = {
        "latrange1": f"{lat1:.6f}",
        "latrange2": f"{lat2:.6f}",
        "longrange1": f"{lon1:.6f}",
        "longrange2": f"{lon2:.6f}",
        "freenet": "true",
        "paynet": "false",
        "onlymine": "false",
        "resultsPerPage": int(max_results),
    }
    auth = (WIGLE_API_NAME, WIGLE_API_TOKEN)

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(WIGLE_API_URL, params=params, auth=auth, timeout=10)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except Exception:
                    logger.exception("WiGLE response is not JSON")
                    return []
                if not isinstance(data, dict) or not data.get("success", True):
                    logger.warning("WiGLE API success flag is false: {}", data)
                    return []
                results = data.get("results") or []
                return [r for r in results if isinstance(r, dict)]

            elif resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 5))  # сек, если WiGLE вернул
                delay = retry_after * attempt  # экспоненциальный бэкофф
                logger.warning("WiGLE API rate limit exceeded (429), retrying in {} seconds...", delay)
                time.sleep(delay)
                continue

            else:
                logger.warning("WiGLE API returned {}: {:.300}", resp.status_code, resp.text)
                return []

        except requests.RequestException:
            logger.exception("Error querying WiGLE API")
            time.sleep(2 * attempt)
            continue

    logger.error("WiGLE API retries exhausted")
    return []



def find_wifi_near_location(lat: float, lon: float, radius_m: float = 50.0) -> List[Dict[str, Any]]:
    """
    Возвращает список точек Wi‑Fi, попадающих в радиус radius_m от указанной локации.
    В первую очередь используем WiGLE API, при ошибке/отсутствии данных — локальный список.
    В ответ добавляется поле distance_m.
    """
    # 1. Попытка получить сети через WiGLE
    wigle_raw = _query_wigle_near(lat, lon, radius_m=radius_m, max_results=50)
    points: List[Dict[str, Any]] = []

    for r in wigle_raw:
        try:
            plat = float(r.get("trilat"))
            plon = float(r.get("trilong"))
        except (TypeError, ValueError):
            continue
        ssid = (r.get("ssid") or "").strip()
        # Убираем шифрование и BSSID, оставляем только название и пароль (если есть)
        # Пароль обычно не приходит из WiGLE API, так как это открытые сети
        password = (r.get("password") or "").strip()
        description = ""
        if password:
            description = f"Пароль: {password}"
        points.append(
            {
                "name": ssid or "Wi‑Fi сеть",
                "description": description,
                "lat": plat,
                "lon": plon,
            }
        )

    # 2. Если WiGLE ничего не вернул (или отключён) — fallback к локальным точкам
    if not points:
        points = get_available_wifi_points()

    nearby: List[Dict[str, Any]] = []
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


