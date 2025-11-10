import os
import sys
import time
from decouple import config, UndefinedValueError
import requests

# Настройки
FIELD_ID = 964745               # <- ваш ID поля "Парк"
SET_VALUE = "Victory Haven"     # значение для установки
CHUNK_SIZE = 250                # amoCRM позволяет до 250 при PATCH /api/v4/leads
LIMIT = 250                     # максимальное количество сущностей на GET-страницу (максимум 250)

# Получение конфигурации: сначала через окружение, потом через decouple
try:
    TOKEN = os.environ.get('AMO_ACCESS_TOKEN') or config('AMO_ACCESS_TOKEN')
except UndefinedValueError:
    TOKEN = None

# BASE URL: приоритет AMO_BASE_URL, если нет — собрать из AMO_ACCOUNT
BASE = os.environ.get('AMO_BASE_URL') or os.environ.get('AMO_ACCOUNT') or None
if not BASE:
    try:
        BASE = config('AMO_BASE_URL')
    except UndefinedValueError:
        try:
            account = config('AMO_ACCOUNT')
            BASE = f'https://{account}'
        except UndefinedValueError:
            BASE = None

if BASE and not BASE.endswith('/'):
    BASE = BASE + '/'
API_BASE = (BASE or '') + 'api/v4/'

HEADERS = {
    'Authorization': f'Bearer {TOKEN}' if TOKEN else '',
    'Content-Type': 'application/json',
    'Accept': 'application/hal+json'
}

def request_json(method, path, params=None, json_body=None, timeout=30):
    url = API_BASE + path.lstrip('/')
    try:
        resp = requests.request(method, url, headers=HEADERS, params=params, json=json_body, timeout=timeout)
    except requests.RequestException as e:
        raise RuntimeError(f"Request error for {method} {url}: {e}")

    # Логируем статус
    print(f"{method} {url} -> {resp.status_code}")

    # При ошибке статуса — показать тело и поднять исключение
    if resp.status_code >= 400:
        # Показать первые 2000 символов для диагностики
        text_preview = resp.text[:2000] if resp.text else '<empty body>'
        raise RuntimeError(f"HTTP {resp.status_code} returned for {url}.\nResponse headers: {resp.headers}\nBody preview: {text_preview}")

    # Попытка распарсить JSON; если не получилось — вывести тело и ошибиться
    try:
        return resp.json()
    except ValueError:
        text_preview = resp.text[:4000] if resp.text else '<empty body>'
        raise RuntimeError(f"Failed to parse JSON from {method} {url}. Content-Type: {resp.headers.get('Content-Type')}\nBody preview: {text_preview}")


def get_all_leads(with_params='source'):
    """Возвращает список всех сделок (полный список объектов lead)."""
    all_leads = []
    page = 1
    while True:
        params = {'limit': LIMIT, 'page': page}
        if with_params:
            params['with'] = with_params
        try:
            data = request_json('GET', 'leads', params=params)
        except RuntimeError as e:
            raise

        embedded = data.get('_embedded', {})
        leads = embedded.get('leads', [])
        if not leads:
            break

        all_leads.extend(leads)
        print(f"Fetched page {page}: {len(leads)} leads (total so far: {len(all_leads)})")

        # Если есть ссылка "next" — идём дальше. Иначе — выходим.
        links = data.get('_links', {})
        if links.get('next') and links['next'].get('href'):
            page += 1
            # короткая пауза, чтобы не фанить API
            time.sleep(0.1)
            continue
        else:
            break

    print(f"Total leads fetched: {len(all_leads)}")
    return all_leads


def lead_needs_update(lead, field_id, desired_value):
    """Проверяет, надо ли обновлять поле в данной сделке."""
    cfvs = lead.get('custom_fields_values') or []
    for cf in cfvs:
        if cf.get('field_id') == field_id:
            # у поля могут быть несколько values, проверим по полю 'value' в любом элементе
            for v in cf.get('values', []):
                if str(v.get('value')) == str(desired_value):
                    return False
            # поле есть, но не нужное значение -> нужно обновление
            return True
    # поле отсутствует -> нужно добавить
    return True


def batch_update(leads_ids, field_id, value):
    """Обновляет пачками сделки: PATCH /api/v4/leads"""
    total = len(leads_ids)
    if total == 0:
        print("No leads to update.")
        return

    for i in range(0, total, CHUNK_SIZE):
        chunk = leads_ids[i:i+CHUNK_SIZE]
        payload = []
        for lid in chunk:
            payload.append({
                "id": lid,
                "custom_fields_values": [
                    {"field_id": field_id, "values": [{"value": value}]}
                ]
            })
        print(f"Patching chunk {i//CHUNK_SIZE + 1}: {len(chunk)} leads...")
        try:
            resp = request_json('PATCH', 'leads', json_body=payload, timeout=60)
        except RuntimeError as e:
            print("Error while patching:", e)
            raise
        print(f"Chunk patched. Server response keys: {list(resp.keys()) if isinstance(resp, dict) else type(resp)}")
        time.sleep(0.1)


def main(dry_run=True):
    if not TOKEN:
        print("AMO_ACCESS_TOKEN not provided. Set AMO_ACCESS_TOKEN in env or .env and try again.")
        sys.exit(1)
    if not BASE:
        print("AMO_BASE_URL or AMO_ACCOUNT not provided. Set AMO_BASE_URL (https://... ) or AMO_ACCOUNT and try again.")
        sys.exit(1)

    # Тест — получить account чтобы проверить токен/домен
    try:
        account_info = request_json('GET', 'account')
        print("Account OK. Account id keys:", list(account_info.keys()))
    except RuntimeError as e:
        print("Failed to contact API / parse response:", e)
        sys.exit(1)

    # Получаем все сделки
    try:
        leads = get_all_leads(with_params='source')
    except RuntimeError as e:
        print("Failed to fetch leads:", e)
        sys.exit(1)

    # Составляем список id для обновления
    to_update_ids = []
    for lead in leads:
        lid = lead.get('id')
        if not lid:
            continue
        if lead_needs_update(lead, FIELD_ID, SET_VALUE):
            to_update_ids.append(lid)

    print(f"Leads to update: {len(to_update_ids)} (out of {len(leads)} total)")

    if dry_run:
        print("Dry run: not applying updates. To apply updates, call main(dry_run=False) or set dry_run=False in script.")
        print("Sample IDs to update (first 50):", to_update_ids)
        return

    # Применяем обновления пачками
    try:
        batch_update(to_update_ids, FIELD_ID, SET_VALUE)
    except RuntimeError as e:
        print("Error during batch update:", e)
        sys.exit(1)

    print("All done — field updated for applicable leads.")


if __name__ == '__main__':
    # По умолчанию dry-run; замените на False чтобы применить изменения
    main(dry_run=True)