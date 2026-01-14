import asyncio
import datetime
import os
import re
from typing import Any, Dict, List, Optional

import gspread
from gspread.utils import rowcol_to_a1
import aiohttp
from aiogram import Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.types import CallbackQuery, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.utils.formatting import PhoneNumber

from create_bot import bot
from db.crud import create_user, get_user_by_tg_id, get_all_users, update_user_consent, create_statistics_entry, get_statistics_by_phone
from jump.jump_integrations import get_balance_by_phone, perform_withdrawal
from metabase.metabase_integration import get_completed_orders_by_phone, courier_exists, get_promotions, get_date_lead, \
    compute_referral_commissions_for_inviter, courier_data, fetch_all_metabase_rows
from wifi_map.wifi_services import find_wifi_near_location, get_available_wifi_points
from users_store import add_or_update_user, is_in_metabase
from .user_states import RegState, InviteFriendStates, PromoStates, WithdrawStates, WifiStates
from .services import (
    load_json, contact_kb, location_request_kb, wifi_apps_kb, courier_type_kb,
    build_main_menu, build_invite_friend_menu, add_person_to_external_sheet, get_msg, manager_withdraw_kb,
    find_row_by_phone_in_sheet, _load_credentials, SPREADSHEET_ID, get_uniform_address_by_city
)
from amocrm.amocrm_integration import find_or_create_contact_and_create_task_async
from decouple import config
from loguru import logger

urouter = Router()

# pending storage
pending_actions = {}
_local_counter = 0
user_langs = {}
CONTACT_SCREENSHOT_PATH = "contact_request.png"
ADMIN_IDS_RAW = config("ADMIN_IDS", default="")
def _parse_admin_ids(raw: str) -> set[int]:
    ids = set()
    for part in (raw or "").split():
        try:
            ids.add(int(part.strip()))
        except Exception:
            continue
    return ids
ADMIN_IDS = _parse_admin_ids(ADMIN_IDS_RAW)
CANDIDATES_SPREADSHEET_ID = config("CANDIDATES_SPREADSHEET_ID", default=SPREADSHEET_ID)
CANDIDATES_SHEET_NAME = "ВСЕ КАНДИДАТЫ В METABASE"


def _next_local():
    global _local_counter
    _local_counter += 1
    return f"local_{_local_counter}"


def _get_lang_for_user(tg_id: int) -> str:
    try:
        return user_langs.get(int(tg_id), "ru")
    except Exception:
        return "ru"


def _is_admin(tg_id: int) -> bool:
    try:
        return int(tg_id) in ADMIN_IDS
    except Exception:
        return False


async def _find_candidate_row(phone: str) -> Optional[Dict[str, str]]:
    return await asyncio.to_thread(
        find_row_by_phone_in_sheet,
        CANDIDATES_SHEET_NAME,
        phone,
        CANDIDATES_SPREADSHEET_ID,
    )


async def _resolve_access(phone: str, name: Optional[str], tg_id: Optional[int]) -> bool:
    """
    Обновляет локальное хранилище in_metabase и возвращает limited-флаг.
    Порядок:
      1) если есть в Google Sheet "ВСЕ КАНДИДАТЫ В METABASE" — полный доступ
      2) иначе проверяем Metabase
      3) иначе ограниченный доступ
    """
    # 1) Google Sheet
    candidate_row = await _find_candidate_row(phone)
    if candidate_row:
        derived_name = name or candidate_row.get("ФИО партнера") or candidate_row.get("ФИО") or candidate_row.get("fio")
        add_or_update_user(name=derived_name, phone=phone, tg_id=tg_id or 0, in_metabase=True)
        return False

    # 2) Metabase
    res = await asyncio.to_thread(courier_exists, phone=phone)
    if res.get("found"):
        data = await asyncio.to_thread(courier_data, phone=phone)
        derived_name = name
        if not derived_name and isinstance(data, dict):
            derived_name = data.get("ФИО партнера") or data.get("ФИО") or data.get("fio")
        add_or_update_user(name=derived_name, phone=phone, tg_id=tg_id or 0, in_metabase=True)
        return False

    # 3) Ограниченный доступ
    add_or_update_user(name=name, phone=phone, tg_id=tg_id or 0, in_metabase=False)
    return True


async def _is_limited_access(phone: str, name: Optional[str] = None, tg_id: Optional[int] = None) -> bool:
    """
    Ограничиваем функционал, если нет подтверждения в Metabase и в таблице кандидатов.
    """
    metabase_flag = is_in_metabase(phone)
    if metabase_flag is None:
        # нет сведений — определяем и сохраняем
        return await _resolve_access(phone, name, tg_id)
    if metabase_flag:
        return False
    # Был ограничен — перепроверяем таблицу кандидатов
    candidate_row = await _find_candidate_row(phone)
    if candidate_row:
        derived_name = name or candidate_row.get("ФИО партнера") or candidate_row.get("ФИО") or candidate_row.get("fio")
        add_or_update_user(name=derived_name, phone=phone, tg_id=tg_id or 0, in_metabase=True)
        return False
    return True


async def _safe_send_message(message_func, *args, timeout: float = 10.0, error_context: str = "", **kwargs):
    """
    Безопасная отправка сообщения с обработкой таймаутов и сетевых ошибок.
    
    Args:
        message_func: Функция отправки сообщения (например, message.answer)
        *args: Позиционные аргументы для функции
        timeout: Таймаут в секундах (по умолчанию 10)
        error_context: Контекст ошибки для логирования
        **kwargs: Именованные аргументы для функции
    
    Returns:
        Результат выполнения функции или None при ошибке
    """
    try:
        return await asyncio.wait_for(message_func(*args, **kwargs), timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"Таймаут при отправке сообщения {error_context} (превышен лимит {timeout} секунд)")
        return None
    except Exception as e:
        error_type = type(e).__name__
        if "Timeout" in error_type or "Network" in error_type:
            logger.warning(f"Ошибка сети при отправке сообщения {error_context}: {error_type}")
        else:
            logger.exception(f"Не удалось отправить сообщение {error_context}: {error_type}")
        return None


async def _send_contact_screenshot(message: Message):
    """
    Отправляет скриншот с кнопкой контакта, если файл присутствует.
    Не блокирует выполнение при ошибках сети или таймаутах.
    """
    if not CONTACT_SCREENSHOT_PATH or not os.path.exists(CONTACT_SCREENSHOT_PATH):
        return
    try:
        photo = FSInputFile(CONTACT_SCREENSHOT_PATH)
        await _safe_send_message(message.answer_photo, photo, timeout=10.0, error_context="скриншот запроса телефона")
    except Exception as e:
        logger.exception(f"Неожиданная ошибка при подготовке скриншота: {type(e).__name__}")


async def _deny_if_limited(entity, lang: str, user) -> bool:
    """
    Возвращает True, если доступ ограничен и хендлер должен прекратить выполнение.
    entity: Message или CallbackQuery
    """
    try:
        phone = getattr(user, "phone", None) if user else None
    except Exception:
        phone = None
    def _send(text: str):
        try:
            if hasattr(entity, "message") and getattr(entity, "message", None):
                return entity.message.answer(text)
            if hasattr(entity, "answer"):
                return entity.answer(text)
        except Exception:
            logger.exception("Не удалось отправить уведомление об ограничении")
            return None

    if not phone:
        await _send(get_msg("phone_profile_error", lang))
        return True
    if await _is_limited_access(phone, getattr(user, "fio", None), getattr(user, "tg_id", None)):
        await _send(get_msg("limited_access_message", lang))
        return True
    return False


async def _send_registration_post(phone: str, full_name: Optional[str] = None, city: Optional[str] = None, position: Optional[str] = None) -> Dict[str, Any]:
    """
    Отправляет POST запрос на эндпоинт при регистрации пользователя.
    Возвращает dict с ключами:
    - success: bool - успешно ли выполнен запрос
    - already_registered: bool - зарегистрирован ли номер уже
    - response_text: str - текст ответа от сервера
    """
    result = {"success": False, "already_registered": False, "response_text": ""}
    
    # Формируем данные в формате form-data, как ожидает эндпоинт
    data = aiohttp.FormData()
    data.add_field('phone', phone)
    data.add_field('full_name', full_name or f'Сделка #{phone}')
    if city:
        data.add_field('city', city)
    if position:
        data.add_field('position', position)
    data.add_field('respond', 'False')
    data.add_field('is_from_amocrm', 'False')
    
    endpoint = 'http://90.156.205.252:8000/add_candidate_selenium/'
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(endpoint, data=data, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response_text = await response.text()
                result["response_text"] = response_text
                
                if response.status == 200:
                    logger.info(f"POST запрос успешно отправлен на {endpoint} для телефона {phone}. Ответ: {response_text[:200]}")
                    result["success"] = True
                    
                    # Проверяем, зарегистрирован ли номер уже
                    # Ищем в ответе индикаторы того, что номер уже зарегистрирован
                    response_lower = response_text.lower()
                    # Проверяем различные варианты сообщений о том, что номер уже зарегистрирован
                    already_registered_indicators = [
                        "уже зарегистрирован",
                        "already registered",
                        "уже существует",
                        "already exists",
                        "номер уже",
                        "phone already",
                        "уже есть",
                        "duplicate"
                    ]
                    
                    for indicator in already_registered_indicators:
                        if indicator in response_lower:
                            result["already_registered"] = True
                            logger.info(f"Номер {phone} уже зарегистрирован согласно ответу POST запроса")
                            break
                else:
                    logger.warning(f"POST запрос на {endpoint} вернул статус {response.status} для телефона {phone}. Ответ: {response_text[:200]}")
        except Exception as e:
            logger.exception(f"Ошибка при отправке POST запроса на {endpoint} для телефона {phone}: {e}")
            result["response_text"] = str(e)
    
    return result


FIRST_REGISTRATION_MESSAGE = """Что делать дальше:
 
1.📱 Скачай основное приложение для работы (Shopper App)
Android: https://kuper.ru/rabota/app
 
2. 🎓 Зайди в Shopper по своему номеру телефона
→ Пройди «Курс новичка» (всего 10 минут!)"""

FIRST_REGISTRATION_MESSAGE_CONTACTS = """
• Горячая линия +78003332428
• Поддержка нашего парка (WhatsApp и Telegram) +79911122678
• Поддержка нашего парка (для звонков) +74999990125
• Чат для наших партнёров в Телеграм https://t.me/KDlogisTik
• Бонусы и привилегии для партнёров Купер https://partnersbenefits.kuper.ru
• Телеграм-бот для поддержки партнёров https://t.me/sbermarket_manager_bot
"""

FIRST_REGISTRATION_MESSAGE_UNIFORM_EXISTS = """
Для выполнения доставок необходим термокороб или термопакет. 🌟

У вас есть четыре варианта:
✅ Использовать термопакет (который можно купить в любом супермаркете).
✅ Использовать свой короб — без брендирования и в исправном состоянии.
✅ Купить новый — на маркетплейсах или Авито.
✅ Взять в аренду у компании — внести залог 1 500 рублей. Залог вернём полностью, если вернёте короб целым.

Важно: при получении формы нужен паспорт. 🪪

Форму Вы можете получить по адресу: {uniform_address}
"""

FIRST_REGISTRATION_MESSAGE_UNIFORM_NOT_EXISTS = """
Для выполнения доставок необходим термокороб или термопакет. 🌟

У вас есть четыре варианта:
✅ Использовать термопакет (который можно купить в любом супермаркете).
✅ Использовать свой короб — без брендирования и в исправном состоянии.
✅ Купить новый — на маркетплейсах или Авито.
✅ Взять в аренду у компании — внести залог 1 500 рублей. Залог вернём полностью, если вернёте короб целым.

Форма для выполнения заказов НЕ ОБЯЗАТЕЛЬНА! 
"""

MANAGER_CHAT_ID = config('MANAGER_CHAT_ID')
EXTERNAL_SPREADSHEET_ID = config('EXTERNAL_SPREADSHEET_ID')
EXTERNAL_SHEET_NAME = config('EXTERNAL_SHEET_NAME')


@urouter.message(CommandStart())
async def on_startup(message: Message, state: FSMContext):
    # Извлекаем параметр из ссылки ПЕРЕД очисткой state
    # (например, "/start stat" -> "stat")
    link_param = None
    
    # В aiogram 3.x параметры команды передаются в message.text как "/start param"
    if message.text:
        logger.info(f"CommandStart message.text: '{message.text}'")
        # Разбиваем текст команды на части
        parts = message.text.strip().split(maxsplit=1)
        if len(parts) > 1:
            link_param = parts[1].strip()
            logger.info(f"Extracted link_param: '{link_param}'")
        else:
            logger.info("No parameter found in command")
    else:
        logger.warning("CommandStart message.text is None or empty")
    
    # Очищаем state ПОСЛЕ извлечения параметра
    await state.clear()
    
    # Сохраняем параметр в state, если он есть
    if link_param:
        await state.update_data(link_param=link_param)
        # Проверяем, что параметр сохранился
        saved_data = await state.get_data()
        logger.info(f"User {message.from_user.id} started bot with link parameter: '{link_param}'. Saved in state: {saved_data.get('link_param')}")
    else:
        logger.info(f"User {message.from_user.id} started bot without link parameter")
    
    # используем русский вариант, потому что это первый шаг (пока не выбран язык)
    prompt = get_msg("choose_language_prompt", "ru")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=get_msg("lang_ru_label", "ru"), callback_data="lang_ru"),
         InlineKeyboardButton(text=get_msg("lang_uz_label", "ru"), callback_data="lang_uz")],
        [InlineKeyboardButton(text=get_msg("lang_tg_label", "ru"), callback_data="lang_tg"),
         InlineKeyboardButton(text=get_msg("lang_ky_label", "ru"), callback_data="lang_ky")],
        [InlineKeyboardButton(text=get_msg("lang_en_label", "ru"), callback_data="lang_en")]
    ])
    await message.answer(prompt, reply_markup=kb)


@urouter.callback_query(F.data.startswith("lang_"))
async def cb_set_language(call: CallbackQuery, state: FSMContext):
    await call.answer()
    lang = call.data.split("_", 1)[1]  # 'ru', 'uz', 'tg', 'ky'
    user_id = call.from_user.id
    user_langs[user_id] = lang
    
    # Проверяем наличие link_param в state
    state_data = await state.get_data()
    link_param = state_data.get("link_param")
    if link_param:
        logger.info(f"User {user_id} selected language {lang}, link_param preserved in state: '{link_param}'")
    else:
        logger.info(f"User {user_id} selected language {lang}, no link_param in state")

    # Send greeting in selected language
    try:
        await call.message.answer(get_msg("hello_text", lang))
    except Exception:
        # fallback to Russian if key missing
        await call.message.answer(get_msg("hello_text", "ru"))

    # Continue depending on whether user exists
    user = await get_user_by_tg_id(user_id)
    
    # Check if user has given consent
    needs_consent = True
    if user:
        needs_consent = not getattr(user, "consent_accepted", False)
    
    if needs_consent:
        # Show consent message with button
        consent_text = get_msg("consent_message", lang)
        consent_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=get_msg("btn_consent", lang), callback_data="consent_accept")]
        ])
        await call.message.answer(consent_text, reply_markup=consent_kb, parse_mode="Markdown")
        await state.set_state(RegState.awaiting_consent)
        return
    
    # User has consent, continue with normal flow
    if user:
        limited = await _is_limited_access(user.phone, getattr(user, "fio", None), user_id)
        balance = 0 if limited else (get_balance_by_phone(user.phone) if user else 0)
        date = get_date_lead(user.phone) if user and getattr(user, "phone", None) else None
        add_or_update_user(name=getattr(user, "fio", None), phone=user.phone, tg_id=user_id, in_metabase=not limited)
        main_text = get_msg("main_menu_text", lang, bal=balance, date=date or "0", invited=compute_referral_commissions_for_inviter(user.phone))
        if limited:
            await call.message.answer(get_msg("limited_access_message", lang))
        await call.message.answer(main_text, reply_markup=build_main_menu(lang, limited=limited, is_admin=_is_admin(user_id)))
    else:
        # ask for FIO in selected language
        # сначала отправляем скриншот, потом текст с кнопкой
        await _send_contact_screenshot(call.message)
        await call.message.answer(get_msg("get_contact_text", lang), reply_markup=contact_kb())
        await state.set_state(RegState.phone_number)


@urouter.callback_query(F.data == "consent_accept", RegState.awaiting_consent)
async def cb_consent_accept(call: CallbackQuery, state: FSMContext):
    await call.answer()
    lang = _get_lang_for_user(call.from_user.id)
    user_id = call.from_user.id
    
    # Save consent in state for later use when creating user
    await state.update_data(consent_accepted=True)
    
    # Update consent in database if user exists
    user = await get_user_by_tg_id(user_id)
    if user:
        await update_user_consent(user_id, True)
    
    # Continue with normal flow
    user = await get_user_by_tg_id(user_id)
    if user:
        limited = await _is_limited_access(user.phone, getattr(user, "fio", None), user_id)
        balance = 0 if limited else (get_balance_by_phone(user.phone) if user else 0)
        date = get_date_lead(user.phone) if user and getattr(user, "phone", None) else None
        add_or_update_user(name=getattr(user, "fio", None), phone=user.phone, tg_id=user_id, in_metabase=not limited)
        main_text = get_msg("main_menu_text", lang, bal=balance, date=date or "0", invited=compute_referral_commissions_for_inviter(user.phone))
        if limited:
            await call.message.answer(get_msg("limited_access_message", lang))
        await call.message.answer(main_text, reply_markup=build_main_menu(lang, limited=limited, is_admin=_is_admin(user_id)))
        await state.clear()
    else:
        # ask for contact in selected language
        # сначала отправляем скриншот, потом текст с кнопкой
        await _send_contact_screenshot(call.message)
        await call.message.answer(get_msg("get_contact_text", lang), reply_markup=contact_kb())
        await state.set_state(RegState.phone_number)


@urouter.message(RegState.FIO)
async def reg_name(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    await state.update_data(name=message.text.strip())
    await message.answer(
        get_msg("ask_city_text", lang)    )
    await state.set_state(RegState.City)


@urouter.message(RegState.phone_number, PhoneNumber)
async def reg_contact(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    contact = message.contact
    logger.info(f"New contact {contact}")
    phone = contact.phone_number
    if phone:
        logger.info(f"New phone number {phone} for contact {contact}")

    # 1) сначала ищем в таблице кандидатов
    logger.info(f"Checking candidate table for phone: {phone}")
    candidate_row = await _find_candidate_row(phone)
    if candidate_row:
        logger.info(f"User found in candidate table: {phone}")
        derived_name = candidate_row.get("ФИО партнера") or candidate_row.get("ФИО") or candidate_row.get("fio") or contact.first_name
        city = candidate_row.get("Город") or candidate_row.get("город")
        existing = await get_user_by_tg_id(message.from_user.id)
        is_first_registration = not existing
        if not existing:
            state_data = await state.get_data()
            consent = state_data.get("consent_accepted", False)
            await create_user(fio=derived_name or "—", phone=phone, city=city, tg_id=message.from_user.id, consent_accepted=consent)
            
            # Записываем статистику, если пользователь зашел по специальной ссылке
            link_param = state_data.get("link_param")
            logger.info(f"[reg_contact] Checking statistics for phone {phone}, link_param in state: {link_param}, is_first_registration: {is_first_registration}")
            if link_param:
                # Проверяем, что статистика еще не записана для этого номера
                existing_stat = await get_statistics_by_phone(phone)
                if not existing_stat:
                    try:
                        await create_statistics_entry(phone=phone, tg_id=message.from_user.id, link_param=link_param)
                        logger.info(f"Statistics entry created for phone {phone}, tg_id {message.from_user.id}, link_param {link_param}")
                    except Exception as e:
                        logger.exception(f"Failed to create statistics entry for phone {phone}: {e}")
        
        add_or_update_user(name=derived_name, phone=phone, tg_id=message.from_user.id, in_metabase=True)
        
        # НЕ отправляем POST запросы, если пользователь найден в таблице кандидатов
        
        # Показываем специальное сообщение для новых пользователей
        if is_first_registration:
            await message.answer(FIRST_REGISTRATION_MESSAGE)
            await message.answer(FIRST_REGISTRATION_MESSAGE_CONTACTS)
            if city:
                address = await asyncio.to_thread(get_uniform_address_by_city, city)
                if address:
                    await message.answer(FIRST_REGISTRATION_MESSAGE_UNIFORM_EXISTS.format(uniform_address=address))
                else:
                    await message.answer(FIRST_REGISTRATION_MESSAGE_UNIFORM_NOT_EXISTS)
        
        balance = get_balance_by_phone(phone) if phone else 0
        main_text = get_msg("main_menu_text", lang, bal=balance, date=get_date_lead(phone) or "0", invited=compute_referral_commissions_for_inviter(phone))
        await message.answer(main_text, reply_markup=build_main_menu(lang, limited=False, is_admin=_is_admin(message.from_user.id)))
        await state.clear()
        return

    # 2) иначе проверяем Metabase
    logger.info(f"Checking Metabase for phone: {phone}")
    res = await asyncio.to_thread(courier_exists, phone=phone)
    logger.info(f"Metabase check result for {phone}: found={res.get('found')}, error={res.get('error')}")
    if res.get("found"):
        logger.info(f"User found in Metabase: {phone}")
        data = await asyncio.to_thread(courier_data, phone=phone)
        if data is not None:
            existing = await get_user_by_tg_id(message.from_user.id)
            is_first_registration = not existing
            state_data = await state.get_data()
            consent = state_data.get("consent_accepted", False)
            await create_user(fio=data.get("ФИО партнера"), phone=phone, city=data.get("Город"), tg_id=message.from_user.id, consent_accepted=consent)
            
            # Записываем статистику, если пользователь зашел по специальной ссылке
            link_param = state_data.get("link_param")
            logger.info(f"[reg_contact] Checking statistics for phone {phone}, link_param in state: {link_param}, is_first_registration: {is_first_registration}")
            if link_param:
                # Проверяем, что статистика еще не записана для этого номера
                existing_stat = await get_statistics_by_phone(phone)
                if not existing_stat:
                    try:
                        await create_statistics_entry(phone=phone, tg_id=message.from_user.id, link_param=link_param)
                        logger.info(f"Statistics entry created for phone {phone}, tg_id {message.from_user.id}, link_param {link_param}")
                    except Exception as e:
                        logger.exception(f"Failed to create statistics entry for phone {phone}: {e}")
            
            add_or_update_user(name=data.get("ФИО партнера"), phone=phone, tg_id=message.from_user.id, in_metabase=True)
            logger.info(f"New user created {phone} {data.get('ФИО партнера')}")
            
            # НЕ отправляем POST запросы, если пользователь найден в Metabase
            
            # Показываем специальное сообщение для новых пользователей
            if is_first_registration:
                await message.answer(FIRST_REGISTRATION_MESSAGE)
                await message.answer(FIRST_REGISTRATION_MESSAGE_CONTACTS)
                city_from_data = data.get("Город")
                if city_from_data:
                    address = await asyncio.to_thread(get_uniform_address_by_city, city_from_data)
                    if address:
                        await message.answer(FIRST_REGISTRATION_MESSAGE_UNIFORM_EXISTS.format(uniform_address=address))
                    else:
                        await message.answer(FIRST_REGISTRATION_MESSAGE_UNIFORM_NOT_EXISTS)
            
            balance = get_balance_by_phone(phone) if phone else 0
            main_text = get_msg("main_menu_text", lang, bal=balance, date=get_date_lead(phone) or "0", invited=compute_referral_commissions_for_inviter(phone))
            await message.answer(main_text, reply_markup=build_main_menu(lang, limited=False, is_admin=_is_admin(message.from_user.id)))
        else:
            add_or_update_user(name=None, phone=phone, tg_id=message.from_user.id, in_metabase=True)
        await state.clear()
        return

    # 3) Пользователь не найден ни в таблице кандидатов, ни в Metabase
    logger.info(f"User NOT found in candidate table or Metabase: {phone}, proceeding to manual registration")
    await state.update_data(phone=phone)
    await message.answer(get_msg("get_name_text", lang),
                         reply_markup=ReplyKeyboardRemove())
    await state.set_state(RegState.FIO)


@urouter.message(Command("menu"))
async def menu(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    await state.clear()
    user = await get_user_by_tg_id(message.from_user.id)
    limited = await _is_limited_access(user.phone, getattr(user, "fio", None), message.from_user.id) if user else False
    balance = 0 if limited else (get_balance_by_phone(user.phone) if user else 0)
    if user:
        if limited:
            await message.answer(get_msg("limited_access_message", lang))
        main_text = get_msg("main_menu_text", lang, bal=balance, date=get_date_lead(user.phone) or "0", invited=compute_referral_commissions_for_inviter(user.phone))
        await message.answer(main_text, reply_markup=build_main_menu(lang, limited=limited, is_admin=_is_admin(message.from_user.id)))


@urouter.message(RegState.City)
async def reg_city(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    await state.update_data(city=message.text.strip())
    await message.answer(get_msg("courier_type_text", lang), reply_markup=courier_type_kb(lang))
    await state.set_state(RegState.Type_of_curer)


@urouter.callback_query(F.data.in_(["courier_type_walking", "courier_type_bike", "courier_type_car"]), RegState.Type_of_curer)
async def cb_courier_type(call: CallbackQuery, state: FSMContext):
    """Обработчик выбора типа курьера через кнопки."""
    lang = _get_lang_for_user(call.from_user.id)
    await call.answer()
    
    # Валидация: проверяем, что выбран один из трех типов
    courier_type_map = {
        "courier_type_walking": "Пеший",
        "courier_type_bike": "Вело",
        "courier_type_car": "Авто"
    }
    
    selected_type = call.data
    if selected_type not in courier_type_map:
        await call.message.answer(get_msg("invalid_courier_type", lang) if "invalid_courier_type" in load_json() else "Неверный тип курьера. Пожалуйста, выберите один из предложенных вариантов.")
        return
    
    courier_type = courier_type_map[selected_type]
    await state.update_data(courier_type=courier_type)
    
    # Удаляем сообщение с кнопками
    try:
        await call.message.delete()
    except Exception:
        pass
    
    # Продолжаем с существующей логикой регистрации
    await reg_courier_type_continue(call.message, state, lang, call.from_user.id)


async def reg_courier_type_continue(message: Message, state: FSMContext, lang: str, tg_id: int):
    """Продолжение обработки регистрации после выбора типа курьера."""
    data = await state.get_data()
    name = data.get("name")
    phone = data.get("phone")
    city = data.get("city")

    if not (name and phone and city):
        await message.answer(get_msg("incomplete_data_error", lang))
        await state.clear()
        return

    # Проверяем таблицу кандидатов перед проверкой Metabase
    logger.info(f"[reg_courier_type] Checking candidate table for phone: {phone}")
    candidate_row = await _find_candidate_row(phone)
    if candidate_row:
        logger.info(f"[reg_courier_type] User found in candidate table: {phone}")
        # Если найден в таблице кандидатов, обрабатываем как найденного
        derived_name = candidate_row.get("ФИО партнера") or candidate_row.get("ФИО") or candidate_row.get("fio") or name
        city_from_sheet = candidate_row.get("Город") or candidate_row.get("город") or city
        existing = await get_user_by_tg_id(tg_id)
        is_first_registration = not existing
        if not existing:
            state_data = await state.get_data()
            consent = state_data.get("consent_accepted", False)
            await create_user(fio=derived_name or "—", phone=phone, city=city_from_sheet, tg_id=tg_id, consent_accepted=consent)
            
            # Записываем статистику, если пользователь зашел по специальной ссылке
            link_param = state_data.get("link_param")
            logger.info(f"[reg_courier_type_continue] Checking statistics for phone {phone}, link_param in state: {link_param}, is_first_registration: {is_first_registration}")
            if link_param:
                # Проверяем, что статистика еще не записана для этого номера
                existing_stat = await get_statistics_by_phone(phone)
                if not existing_stat:
                    try:
                        await create_statistics_entry(phone=phone, tg_id=tg_id, link_param=link_param)
                        logger.info(f"Statistics entry created for phone {phone}, tg_id {tg_id}, link_param {link_param}")
                    except Exception as e:
                        logger.exception(f"Failed to create statistics entry for phone {phone}: {e}")
        
        add_or_update_user(name=derived_name, phone=phone, tg_id=tg_id, in_metabase=True)
        
        # НЕ отправляем POST запросы, если пользователь найден в таблице кандидатов
        
        # Показываем специальное сообщение для новых пользователей
        if is_first_registration:
            await message.answer(FIRST_REGISTRATION_MESSAGE)
            await message.answer(FIRST_REGISTRATION_MESSAGE_CONTACTS)
            if city_from_sheet:
                address = await asyncio.to_thread(get_uniform_address_by_city, city_from_sheet)
                if address:
                    await message.answer(FIRST_REGISTRATION_MESSAGE_UNIFORM_EXISTS.format(uniform_address=address))
                else:
                    await message.answer(FIRST_REGISTRATION_MESSAGE_UNIFORM_NOT_EXISTS)
        
        balance = get_balance_by_phone(phone) if phone else 0
        main_text = get_msg("main_menu_text", lang, bal=balance, date=get_date_lead(phone) or "0", invited=compute_referral_commissions_for_inviter(phone))
        await message.answer(main_text, reply_markup=build_main_menu(lang, limited=False, is_admin=_is_admin(message.from_user.id)))
        await state.clear()
        return

    await message.answer(get_msg("checking_in_park", lang))
    logger.info(f"[reg_courier_type] Checking Metabase for phone: {phone}")
    try:
        res = await asyncio.to_thread(courier_exists, phone=phone)
    except Exception as e:
        logger.exception("Ошибка при проверке Metabase")
        res = {"found": False, "row": None, "error": str(e)}
    
    logger.info(f"[reg_courier_type] Metabase check result for {phone}: found={res.get('found')}, error={res.get('error')}")

    # special bypass for admin phone
    #if phone and re.sub(r"\D+", "", phone).endswith("9137619949"):
    #    logger.info(f"[reg_courier_type] Admin phone bypass for {phone}")
    #    res = {"found": True, "row": None, "error": None}

    if not res:
        await message.answer(get_msg("error_check", lang))
        pid = _next_local()
        pending_actions[pid] = {"telegram_id": tg_id, "name": name, "phone": phone, "city": city, "status": "error",
                                "meta": res.get("error"), "type": "not_in_park"}
        await state.clear()
        return

    if res.get("found"):
        logger.info(f"[reg_courier_type] User found in Metabase: {phone}")
        existing = await get_user_by_tg_id(tg_id)
        is_first_registration = not existing
        state_data = await state.get_data()
        consent = state_data.get("consent_accepted", False)
        # Получаем данные из Metabase
        metabase_data = await asyncio.to_thread(courier_data, phone=phone)
        user_name = name if name else (metabase_data.get("ФИО партнера") if metabase_data else "—")
        user_city = city if city else (metabase_data.get("Город") if metabase_data else None)
        await create_user(fio=user_name, phone=phone, city=user_city, tg_id=tg_id, consent_accepted=consent)
        
        # Записываем статистику, если пользователь зашел по специальной ссылке
        link_param = state_data.get("link_param")
        logger.info(f"[reg_courier_type_continue] Checking statistics for phone {phone}, link_param in state: {link_param}, is_first_registration: {is_first_registration}")
        if link_param:
            # Проверяем, что статистика еще не записана для этого номера
            existing_stat = await get_statistics_by_phone(phone)
            if not existing_stat:
                try:
                    await create_statistics_entry(phone=phone, tg_id=tg_id, link_param=link_param)
                    logger.info(f"Statistics entry created for phone {phone}, tg_id {tg_id}, link_param {link_param}")
                except Exception as e:
                    logger.exception(f"Failed to create statistics entry for phone {phone}: {e}")
        
        add_or_update_user(name=user_name, phone=phone, tg_id=tg_id, in_metabase=True)
        
        # НЕ отправляем POST запросы, если пользователь найден в Metabase
        
        # Показываем специальное сообщение для новых пользователей
        if is_first_registration:
            await message.answer(FIRST_REGISTRATION_MESSAGE)
            await message.answer(FIRST_REGISTRATION_MESSAGE_CONTACTS)
            if user_city:
                address = await asyncio.to_thread(get_uniform_address_by_city, user_city)
                if address:
                    await message.answer(FIRST_REGISTRATION_MESSAGE_UNIFORM_EXISTS.format(uniform_address=address))
                else:
                    await message.answer(FIRST_REGISTRATION_MESSAGE_UNIFORM_NOT_EXISTS)
        
        balance = get_balance_by_phone(phone)
        main_text = get_msg("main_menu_text", lang, bal=balance, date=get_date_lead(phone) or "0", invited=compute_referral_commissions_for_inviter(phone))
        await message.answer(main_text,
                             reply_markup=build_main_menu(lang, limited=False, is_admin=_is_admin(tg_id)))
        await state.clear()
        return
    else:
        # Пользователь не найден ни в таблице кандидатов, ни в Metabase
        logger.info(f"[reg_courier_type] User NOT found in candidate table or Metabase: {phone}, creating task in amoCRM and adding to Google Sheets")
        await message.answer(get_msg("not_exist", lang))
        try:
            task_text = f"Проверить кандидата {name} ({phone}), город: {city} — не найден в парке."
            logger.info(f"[reg_courier_type] Attempting to create task in amoCRM for user not found: {name} ({phone}), tg_id: {tg_id}")
            res_amo = await find_or_create_contact_and_create_task_async(name=name, phone=phone, tg_id=tg_id,
                                                                         task_text=task_text)
            logger.info(f"amoCRM result: ok={res_amo.get('ok')}, task_id={res_amo.get('task_id')}, contact_id={res_amo.get('contact_id')}, reason={res_amo.get('reason')}")
        except Exception as e:
            logger.exception("AMO error")
            res_amo = {"ok": False, "reason": str(e)}

        pid = _next_local()
        pending_actions[pid] = {"telegram_id": tg_id, "name": name, "phone": phone, "city": city, "status": "pending",
                                "type": "not_in_park", "amo_result": res_amo}
        if res_amo.get("ok"):
            logger.info(f"Задача создана в amoCRM. ID задачи: {res_amo.get('task_id')}, contact_id: {res_amo.get('contact_id')}")
            await message.answer(get_msg("manager_answer", lang))
        else:
            logger.warning(f"Не удалось создать задачу в amoCRM. Причина: {res_amo.get('reason')}")
        # создаём локального пользователя с ограниченным доступом
        existing = await get_user_by_tg_id(tg_id)
        is_first_registration = not existing
        if not existing:
            state_data = await state.get_data()
            consent = state_data.get("consent_accepted", False)
            await create_user(name, phone, city, tg_id, consent_accepted=consent)
            
            # Записываем статистику, если пользователь зашел по специальной ссылке
            link_param = state_data.get("link_param")
            logger.info(f"[reg_courier_type_continue] Checking statistics for phone {phone}, link_param in state: {link_param}, is_first_registration: {is_first_registration}")
            if link_param:
                # Проверяем, что статистика еще не записана для этого номера
                existing_stat = await get_statistics_by_phone(phone)
                if not existing_stat:
                    try:
                        await create_statistics_entry(phone=phone, tg_id=tg_id, link_param=link_param)
                        logger.info(f"Statistics entry created for phone {phone}, tg_id {tg_id}, link_param {link_param}")
                    except Exception as e:
                        logger.exception(f"Failed to create statistics entry for phone {phone}: {e}")
        
        add_or_update_user(name=name, phone=phone, tg_id=tg_id, in_metabase=False)
        
        # Добавляем пользователя в таблицу
        courier_type = data.get("courier_type") or ""
        logger.info(f"[reg_courier_type] Attempting to add user to Google Sheets: {name} ({phone}), spreadsheet_id={EXTERNAL_SPREADSHEET_ID or SPREADSHEET_ID}")
        try:
            # Используем EXTERNAL_SPREADSHEET_ID или SPREADSHEET_ID по умолчанию
            spreadsheet_id = EXTERNAL_SPREADSHEET_ID or SPREADSHEET_ID
            logger.info(f"[reg_courier_type] Using spreadsheet_id: {spreadsheet_id}")
            if spreadsheet_id:
                ext_row = await asyncio.to_thread(
                    add_person_to_external_sheet,
                    spreadsheet_id=spreadsheet_id,
                    sheet_name="Лист1",
                    fio=name,
                    phone=phone,
                    city=city,
                    role=courier_type
                )
                if ext_row:
                    logger.info(f"Пользователь {name} ({phone}) добавлен в таблицу, строка {ext_row}")
                else:
                    logger.warning(f"Не удалось добавить пользователя {name} ({phone}) в таблицу")
            else:
                logger.warning("EXTERNAL_SPREADSHEET_ID и SPREADSHEET_ID не заданы, пропускаем добавление в таблицу")
        except Exception as e:
            logger.exception(f"Ошибка при добавлении пользователя в таблицу: {e}")
        
        # Отправляем POST запросы при регистрации (только если пользователь не найден ни в таблице кандидатов, ни в Metabase)
        post_result = await _send_registration_post(phone=phone, full_name=name, city=city, position=courier_type)
        
        # Показываем специальное сообщение для новых пользователей только если номер НЕ зарегистрирован
        if is_first_registration:
            # Отправляем 3 сообщения только если номер НЕ зарегистрирован
            if not post_result.get("already_registered", False):
                await message.answer(FIRST_REGISTRATION_MESSAGE)
                await message.answer(FIRST_REGISTRATION_MESSAGE_CONTACTS)
                if city:
                    address = await asyncio.to_thread(get_uniform_address_by_city, city)
                    if address:
                        await message.answer(FIRST_REGISTRATION_MESSAGE_UNIFORM_EXISTS.format(uniform_address=address))
                    else:
                        await message.answer(FIRST_REGISTRATION_MESSAGE_UNIFORM_NOT_EXISTS)
            else:
                logger.info(f"Номер {phone} уже зарегистрирован, пропускаем отправку приветственных сообщений")
        
        await message.answer(get_msg("limited_access_message", lang))
        main_text = get_msg("main_menu_text", lang, bal=0, date="—", invited=0)
        await message.answer(main_text, reply_markup=build_main_menu(lang, limited=True, is_admin=_is_admin(tg_id)))
        await state.clear()
        return


@urouter.callback_query(F.data == "completed_orders")
async def cb_completed(call: CallbackQuery):
    lang = _get_lang_for_user(call.from_user.id)
    user = await get_user_by_tg_id(call.from_user.id)
    if await _deny_if_limited(call, lang, user):
        return
    total_user_orders = get_completed_orders_by_phone(user.phone)
    await call.answer()
    await call.message.answer(get_msg("completed_orders_text", lang, total_user_orders=total_user_orders))


@urouter.callback_query(F.data == "invited_friends")
async def cb_invited_friends(call: CallbackQuery):
    lang = _get_lang_for_user(call.from_user.id)
    user = await get_user_by_tg_id(call.from_user.id)
    if await _deny_if_limited(call, lang, user):
        return
    await call.answer()
    inviter = call.from_user.id
    invited = [(k, v) for k, v in pending_actions.items() if v.get("type") == "invite" and v.get("inviter") == inviter]
    if not invited:
        await call.message.answer(get_msg("not_invited_friends", lang))
    else:
        txt = get_msg("list_invited", lang)
        for k, v in invited:
            txt += f"- {v.get('friend_name', '?')} (тел: {v.get('friend_phone', '?')}), статус: {v.get('status')}\n"
        await call.message.answer(txt)


@urouter.callback_query(F.data == "invite_friend")
async def cb_invite_friend_start(call: CallbackQuery, state: FSMContext):
    lang = _get_lang_for_user(call.from_user.id)
    user = await get_user_by_tg_id(call.from_user.id)
    if await _deny_if_limited(call, lang, user):
        return
    await call.answer()
    # Show consent message for friend registration
    consent_text = get_msg("friend_consent_message", lang)
    consent_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=get_msg("btn_consent", lang), callback_data="friend_consent_accept")]
    ])
    await call.message.answer(consent_text, reply_markup=consent_kb)
    await state.set_state(InviteFriendStates.awaiting_friend_consent)


@urouter.callback_query(F.data == "friend_consent_accept", InviteFriendStates.awaiting_friend_consent)
async def cb_friend_consent_accept(call: CallbackQuery, state: FSMContext):
    lang = _get_lang_for_user(call.from_user.id)
    await call.answer()
    await state.set_state(InviteFriendStates.friend_name)
    await call.message.answer(get_msg("invite_intro", lang),
                              reply_markup=build_invite_friend_menu())
    await call.message.answer(get_msg("invite_step_name", lang))


@urouter.message(InviteFriendStates.friend_name)
async def invite_friend_name(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    await state.update_data(friend_name=message.text.strip())
    await message.answer(get_msg("invite_step_contact", lang))
    await state.set_state(InviteFriendStates.friend_contact)


@urouter.message(InviteFriendStates.friend_contact)
async def invite_friend_contact(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    phone = message.text.strip()
    await state.update_data(friend_phone=phone)
    await message.answer(get_msg("invite_step_city", lang),
                         reply_markup=ReplyKeyboardRemove())
    await state.set_state(InviteFriendStates.friend_city)


@urouter.message(InviteFriendStates.friend_city)
async def invite_friend_city(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    await state.update_data(friend_city=message.text.strip())
    await message.answer(get_msg("invite_step_role", lang), reply_markup=courier_type_kb(lang))
    await state.set_state(InviteFriendStates.friend_role)


@urouter.callback_query(F.data.in_(["courier_type_walking", "courier_type_bike", "courier_type_car"]), InviteFriendStates.friend_role)
async def cb_friend_role(call: CallbackQuery, state: FSMContext):
    """Обработчик выбора типа курьера для друга через кнопки."""
    lang = _get_lang_for_user(call.from_user.id)
    await call.answer()
    
    # Валидация: проверяем, что выбран один из трех типов
    courier_type_map = {
        "courier_type_walking": "Пеший",
        "courier_type_bike": "Вело",
        "courier_type_car": "Авто"
    }
    
    selected_type = call.data
    if selected_type not in courier_type_map:
        await call.message.answer(get_msg("invalid_courier_type", lang) if "invalid_courier_type" in load_json() else "Неверный тип курьера. Пожалуйста, выберите один из предложенных вариантов.")
        return
    
    friend_role = courier_type_map[selected_type]
    await state.update_data(friend_role=friend_role)
    
    # Удаляем сообщение с кнопками
    try:
        await call.message.delete()
    except Exception:
        pass
    
    # Продолжаем с запросом дня рождения
    await call.message.answer(get_msg("invite_step_birthday", lang))
    await state.set_state(InviteFriendStates.friend_birthday)


@urouter.message(InviteFriendStates.friend_birthday)
async def invite_friend_birthday(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    await state.update_data(friend_birthday=message.text.strip())
    data = await state.get_data()
    inviter = message.from_user.id
    user = await get_user_by_tg_id(inviter)
    name = data.get("friend_name")
    phone = data.get("friend_phone")
    city = data.get("friend_city")
    role = data.get("friend_role")

    await message.answer(get_msg("manager_invite_friend_text", lang))
    try:
        from .services import add_invite_friend_row
        sheet_row = add_invite_friend_row(inviter_tg_id=inviter,
                                          friend_name=name,
                                          friend_phone=phone,
                                          friend_tg_id=None,
                                          inviter_name=user.fio,
                                          inviter_phone=user.phone,
                                          friend_city=city,
                                          friend_role=role)
    except Exception:
        logger.exception("Ошибка записи в Google Sheets")
        sheet_row = None

    try:
        ext_row = add_person_to_external_sheet(
            spreadsheet_id=EXTERNAL_SPREADSHEET_ID,
            sheet_name=EXTERNAL_SHEET_NAME,
            fio=name,
            phone=phone,
            city=city,
            role=role
        )
        res = await find_or_create_contact_and_create_task_async(
            name=name, phone=phone, tg_id=inviter,
            task_text=f"Приглашённый: {name} {phone}. Роль: {role}, город: {city}"
        )


    except Exception as e:
        logger.exception("AMO error")
        res = {"ok": False, "reason": str(e)}

    local_id = _next_local()
    pending_actions[local_id] = {
        "type": "invite",
        "inviter": inviter,
        "friend_name": name,
        "friend_phone": phone,
        "friend_city": city,
        "friend_role": role,
        "friend_birthday": data.get("friend_birthday"),
        "status": "pending",
        "amo": res
    }

    if res.get("ok"):
        await message.answer(get_msg("invite_done_text", lang))

    await state.set_state(InviteFriendStates.friend_check)
    await message.answer(get_msg("wait_friend_reg", lang))
    return


@urouter.message(InviteFriendStates.friend_check)
async def invite_friend_check_commands(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    text = message.text.strip()
    if text.startswith("confirm_friend_registered"):
        parts = text.split()
        if len(parts) >= 2:
            pid = parts[1]
            entry = pending_actions.get(pid)
            if entry and entry.get("type") == "invite":
                entry["status"] = "registered"
                await message.answer(get_msg("invite_fried_success", lang))
                inviter = entry.get("inviter")
                try:
                    # notify inviter in their language if possible
                    inv_lang = _get_lang_for_user(inviter)
                    await bot.send_message(inviter,
                                           get_msg("friend_invite_success", inv_lang, name=entry.get("friend_name")))
                except Exception:
                    logger.exception("Can't notify inviter")
                await state.clear()
                return
    if text.startswith("friend_registration_error"):
        parts = text.split()
        if len(parts) >= 2:
            pid = parts[1]
            entry = pending_actions.get(pid)
            if entry and entry.get("type") == "invite":
                entry["status"] = "error"
                entry["error"] = "registration_failed"
                await message.answer(get_msg("phone_fail_invite", lang))
                await message.answer(
                    get_msg("phone_retry_prompt", lang),
                    reply_markup=contact_kb())
                await state.update_data(retry_pid=pid)
                await state.set_state(InviteFriendStates.friend_contact)
                return
    await message.answer(get_msg("wait_confirm_text", lang))


def _split_text_chunks(text: str, limit: int = 3900) -> list:
    """
    Разбивает длинный текст на части <= limit символов.
    """
    if not text:
        return []
    if len(text) <= limit:
        return [text]
    parts = []
    paragraphs = text.split("\n\n")
    current = ""
    for p in paragraphs:
        chunk = p + ("\n\n" if not p.endswith("\n\n") else "")
        if len(current) + len(chunk) <= limit:
            current += chunk
            continue
        if current:
            parts.append(current.rstrip())
        if len(chunk) > limit:
            lines = chunk.split("\n")
            cur2 = ""
            for ln in lines:
                ln_chunk = ln + "\n"
                if len(cur2) + len(ln_chunk) <= limit:
                    cur2 += ln_chunk
                else:
                    if cur2:
                        parts.append(cur2.rstrip())
                    cur2 = ln_chunk
            if cur2:
                parts.append(cur2.rstrip())
            current = ""
        else:
            current = chunk
    if current:
        parts.append(current.rstrip())
    return parts


def _normalize_sheet_value(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, (datetime.datetime, datetime.date)):
        return val.isoformat()
    return str(val)


def _prepare_candidates_dataset(metabase_rows: List[Dict[str, Any]], local_users: List[Any]) -> (List[str], List[List[str]]):
    headers: List[str] = []

    def add_header(h):
        if h is None:
            return
        hstr = str(h).strip()
        if hstr and hstr not in headers:
            headers.append(hstr)

    for row in metabase_rows:
        if isinstance(row, dict):
            for k in row.keys():
                add_header(k)

    extra_fields = ["source", "local_id", "tg_id", "ФИО партнера", "ФИО", "Телефон", "phone", "Город", "city", "created_at"]
    for f in extra_fields:
        add_header(f)

    records: List[Dict[str, Any]] = []

    for row in metabase_rows:
        if not isinstance(row, dict):
            continue
        rec = {k: _normalize_sheet_value(v) for k, v in row.items()}
        rec.setdefault("source", "metabase")
        records.append(rec)

    for u in local_users or []:
        rec = {
            "source": "local_db",
            "local_id": getattr(u, "id", None),
            "tg_id": getattr(u, "tg_id", None),
            "ФИО партнера": getattr(u, "fio", None),
            "ФИО": getattr(u, "fio", None),
            "Телефон": getattr(u, "phone", None),
            "phone": getattr(u, "phone", None),
            "Город": getattr(u, "city", None),
            "city": getattr(u, "city", None),
            "created_at": getattr(u, "created_at", None),
        }
        records.append({k: _normalize_sheet_value(v) for k, v in rec.items()})

    table: List[List[str]] = []
    for rec in records:
        row_values = []
        for h in headers:
            row_values.append(_normalize_sheet_value(rec.get(h)))
        table.append(row_values)
    return headers, table


def _write_candidates_sheet(headers: List[str], table: List[List[str]]):
    creds = _load_credentials()
    client = gspread.authorize(creds)
    sheet = client.open_by_key(CANDIDATES_SPREADSHEET_ID or SPREADSHEET_ID)
    try:
        ws = sheet.worksheet(CANDIDATES_SHEET_NAME)
    except Exception:
        ws = sheet.add_worksheet(title=CANDIDATES_SHEET_NAME, rows=str(max(len(table) + 10, 1000)),
                                 cols=str(max(len(headers) + 5, 20)))
    ws.clear()
    payload = [headers] + table
    if not payload:
        return
    # auto-range from A1 to bottom-right
    end_cell = rowcol_to_a1(len(payload), len(headers)) if headers else "A1"
    ws.update(f"A1:{end_cell}", payload, value_input_option="USER_ENTERED")


async def _export_metabase_dataset() -> int:
    metabase_rows = await asyncio.to_thread(fetch_all_metabase_rows)
    local_users = await get_all_users()
    headers, table = _prepare_candidates_dataset(metabase_rows, local_users)
    await asyncio.to_thread(_write_candidates_sheet, headers, table)
    return len(table)


@urouter.callback_query(F.data == "export_metabase")
async def cb_export_metabase(call: CallbackQuery):
    lang = _get_lang_for_user(call.from_user.id)
    if not _is_admin(call.from_user.id):
        await call.answer(get_msg("metabase_export_denied", lang), show_alert=True)
        return
    await call.answer()
    await call.message.answer(get_msg("metabase_export_started", lang))
    try:
        total = await _export_metabase_dataset()
        await call.message.answer(get_msg("metabase_export_done", lang, count=total))
    except Exception as e:
        logger.exception("Failed to export metabase dataset")
        await call.message.answer(get_msg("metabase_export_error", lang, reason=str(e)))


@urouter.callback_query(F.data == "promotions")
async def cb_promotions(call: CallbackQuery, state: FSMContext):
    lang = _get_lang_for_user(call.from_user.id)
    await call.answer()
    user = await get_user_by_tg_id(call.from_user.id)
    phone = user.phone if user else None
    if not phone:
        await call.message.answer(get_msg("phone_profile_error", lang))
        return

    try:
        promos = await asyncio.to_thread(get_promotions, phone)
    except Exception:
        logger.exception("Error getting promotions")
        await call.message.answer(get_msg("promotions_error", lang))
        return

    if not promos:
        await call.message.answer(get_msg("not_promotions", lang))
        return

    today = datetime.date.today()
    # total completed orders for emoji determination
    try:
        total_orders = get_completed_orders_by_phone(phone)
    except Exception:
        total_orders = 0

    lines = []
    seen = set()

    # iterate promos in returned order and build formatted lines
    for promo in promos:
        ptype = promo.get("type")
        title = promo.get("title", "") or ""
        desc = promo.get("desc", "") or ""
        reward = promo.get("reward", "") or ""
        meta = promo.get("meta") or {}

        if ptype == "refer":
            # build refer line
            base = title or get_msg("refer_title_default", lang)
            parts = [base]
            if desc:
                parts.append(desc)
            if reward:
                parts.append(get_msg("reward_label", lang) + f" {reward}")
            line = " - ".join(parts)
            if line not in seen:
                seen.add(line)
                lines.append(line)
            continue

        # if ptype == "first":
        #     base = title or get_msg("first_order_title_default", lang)
        #     parts = [base]
        #     if desc:
        #         parts.append(desc)
        #     if reward:
        #         parts.append(get_msg("bonus_label", lang) + f" {reward}")
        #     line = " - ".join(parts)
        #     if line not in seen:
        #         seen.add(line)
        #         lines.append(line)
        #     continue

        #         # if ptype == "completed":
        #     # expected meta: threshold, end_date, coef_used, obj
        #     th = meta.get("threshold") or meta.get("thresholds") or None
        #     try:
        #         th_int = int(th)
        #     except Exception:
        #         # maybe title contains number or promo id
        #         try:
        #             th_int = int(str(title).split()[0])
        #         except Exception:
        #             th_int = None
        #     if th_int is None:
        #         # fallback: include title text
        #         line = f"{title} - {desc or '—'} - {reward} ₽"
        #         if line not in seen:
        #             seen.add(line)
        #             lines.append(line)
        #         continue
        #
        #     end_date_raw = meta.get("end_date") or meta.get("end_date_str") or None
        #     end_date = None
        #     if end_date_raw:
        #         # many formats possible; try dd.mm.yyyy then iso
        #         try:
        #             end_date = datetime.datetime.strptime(end_date_raw, "%d.%m.%Y").date()
        #             end_date_str = end_date.strftime("%d.%m.%Y")
        #         except Exception:
        #             try:
        #                 dt = datetime.datetime.fromisoformat(end_date_raw)
        #                 end_date = dt.date()
        #                 end_date_str = end_date.strftime("%d.%m.%Y")
        #             except Exception:
        #                 end_date = None
        #                 end_date_str = str(end_date_raw)
        #     else:
        #         end_date_str = "—"
        #
        #     # emoji logic
        #     emoji = "⏳"  # default if no end_date
        #     try:
        #         if isinstance(total_orders, (int, float)) and total_orders >= th_int:
        #             emoji = "✅"
        #         else:
        #             if end_date is None:
        #                 emoji = "⏳"
        #             else:
        #                 if end_date >= today:
        #                     emoji = "⏳"
        #                 else:
        #                     emoji = "❌"
        #     except Exception:
        #         emoji = "⏳"
        #
        #     # reward numeric normalize
        #     reward_str = str(reward).strip()
        #     if reward_str == "":
        #         reward_str = "0"
        #     # ensure date formatted dd.mm.YYYY or —
        #     line = f"{th_int} заказов - {end_date_str} - {reward_str} ₽ {emoji}"
        #     if line not in seen:
        #         seen.add(line)
        #         lines.append(line)
        #     continue

        # fallback
        # base = title or get_msg("promo_default_title", lang)
        # parts = [base]
        # if desc:
        #     parts.append(desc)
        # if reward:
        #     parts.append(get_msg("reward_label", lang) + f" {reward}")
        # line = " - ".join(parts)
        # if line not in seen:
        #     seen.add(line)
        #     lines.append(line)

    # prepare header + lines joined with blank line between
    header = get_msg("active_promotions", lang)
    # ensure each line on its own paragraph
    body = "\n\n".join(lines)
    full_text = header + body

    # split into chunks safe for Telegram
    chunks = _split_text_chunks(full_text, limit=3900)
    kb_last = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=get_msg("to_start_kb", lang), callback_data="to_start")]])

    try:
        for i, ch in enumerate(chunks):
            if i == len(chunks) - 1:
                await call.message.answer(ch, reply_markup=kb_last)
            else:
                await call.message.answer(ch)
        await state.set_state(PromoStates.viewing)
    except Exception:
        logger.exception("Failed to send promo chunks")
        # fallback short message
        try:
            short = header + ("\n\n".join(lines[:20]))
            await call.message.answer(short, reply_markup=kb_last)
            await state.set_state(PromoStates.viewing)
        except Exception:
            await call.message.answer(get_msg("not_promotions", lang), reply_markup=kb_last)
            await state.set_state(PromoStates.viewing)


@urouter.callback_query(F.data == "withdraw")
async def cb_withdraw_start(call: CallbackQuery, state: FSMContext):
    lang = _get_lang_for_user(call.from_user.id)
    user = await get_user_by_tg_id(call.from_user.id)
    if await _deny_if_limited(call, lang, user):
        return
    await call.answer()
    await state.set_state(WithdrawStates.ask_amount)
    await call.message.answer(get_msg("withdrawal_amount", lang))


@urouter.message(WithdrawStates.ask_amount)
async def withdraw_enter_amount(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    user = await get_user_by_tg_id(message.from_user.id)
    if await _deny_if_limited(message, lang, user):
        return
    if not user or not user.phone:
        await message.answer(get_msg("phone_profile_error", lang))
        await state.clear()
        return
    text = message.text.strip().replace(",", ".")
    try:
        amount = float(re.sub(r"[^\d\.]", "", text))
    except Exception:
        await message.answer(get_msg("withdrawal_amount_error", lang))
        return

    # get balance to check minimum remain 50
    balance = get_balance_by_phone(user.phone)
    try:
        bal = float(balance)
    except Exception:
        bal = 0.0
    allowed = bal - 50.0
    if amount > allowed:
        await message.answer(get_msg("withdrawal_request_error", lang))
        await state.clear()
        return

    # Сохраняем сумму и предлагаем выбрать способ (СБП / Карта)
    await state.update_data(withdraw_amount=amount)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=get_msg("withdraw_method_card", lang), callback_data="withdraw_method_card"),
         InlineKeyboardButton(text=get_msg("withdraw_method_sbp", lang), callback_data="withdraw_method_sbp")]
    ])
    await state.set_state(WithdrawStates.choose_method)
    await message.answer(get_msg("withdraw_choose_method", lang), reply_markup=kb)


@urouter.callback_query(F.data == "withdraw_method_card")
async def cb_withdraw_method_card(call: CallbackQuery, state: FSMContext):
    lang = _get_lang_for_user(call.from_user.id)
    await call.answer()
    await state.set_state(WithdrawStates.card_number)
    await call.message.answer(get_msg("ask_card_number", lang))


@urouter.callback_query(F.data == "withdraw_method_sbp")
async def cb_withdraw_method_sbp(call: CallbackQuery, state: FSMContext):
    lang = _get_lang_for_user(call.from_user.id)
    await call.answer()
    await state.set_state(WithdrawStates.sbp_phone)
    await call.message.answer(get_msg("ask_sbp_phone", lang))


@urouter.message(WithdrawStates.card_number)
async def withdraw_card_number_enter(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    card = re.sub(r"\s+", "", message.text.strip())
    # minimal validation: digits and length 13-19
    digits = re.sub(r"\D+", "", card)
    if not (13 <= len(digits) <= 19):
        await message.answer(get_msg("ask_card_number_invalid", lang))
        return
    await state.update_data(withdraw_method="card", card_number=digits)
    data = await state.get_data()
    amount = data.get("withdraw_amount")
    # создаём локальную заявку менеджеру
    pid = _next_local()
    user = await get_user_by_tg_id(message.from_user.id)
    pending_actions[pid] = {
        "type": "withdraw",
        "user_id": message.from_user.id,
        "user_phone": user.phone if user else None,
        "amount": amount,
        "method": "card",
        "card_number": digits,
        "status": "pending",
        "created_at": datetime.datetime.utcnow().isoformat()
    }
    manager_text = (
        f"Заявка вывода #{pid}\n"
        f"Пользователь: {(user.fio if user and getattr(user, "fio", None) else str(message.from_user.id))} (TG: {message.from_user.id})\n"
        f"Сумма: {amount} ₽\n"
        f"Способ: Карта \n"
        f"{digits}\n"
        "\nПодтвердите / отклоните заявку."
    )
    try:
        await bot.send_message(int(MANAGER_CHAT_ID), manager_text, reply_markup=manager_withdraw_kb(pid))
    except Exception:
        logger.exception("Failed to notify manager about withdrawal request")
    await message.answer(get_msg("withdraw_request_sent_to_manager", lang))
    await state.set_state(WithdrawStates.awaiting_manager)


@urouter.message(WithdrawStates.sbp_phone)
async def withdraw_sbp_phone_enter(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    phone = re.sub(r"\D+", "", message.text.strip())
    if len(phone) < 7:
        await message.answer(get_msg("ask_sbp_phone_invalid", lang))
        return
    await state.update_data(withdraw_method="sbp", sbp_phone=phone)
    await state.set_state(WithdrawStates.sbp_bank)
    await message.answer(get_msg("ask_sbp_bank", lang))


@urouter.message(WithdrawStates.sbp_bank)
async def withdraw_sbp_bank_enter(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    bank = message.text.strip()
    if not bank:
        await message.answer(get_msg("ask_sbp_bank_invalid", lang))
        return
    await state.update_data(sbp_bank=bank)
    data = await state.get_data()
    amount = data.get("withdraw_amount")
    sbp_phone = data.get("sbp_phone")
    # создаём локальную заявку менеджеру
    pid = _next_local()
    user = await get_user_by_tg_id(message.from_user.id)
    pending_actions[pid] = {
        "type": "withdraw",
        "user_id": message.from_user.id,
        "user_phone": user.phone if user else None,
        "amount": amount,
        "method": "sbp",
        "sbp_phone": sbp_phone,
        "sbp_bank": bank,
        "status": "pending",
        "created_at": datetime.datetime.utcnow().isoformat()
    }
    # уведомляем менеджера (русский язык для менеджера)

    manager_text = (
        f"Заявка вывода #{pid}\n"
        f"Пользователь: {(user.fio if user and getattr(user, "fio", None) else str(message.from_user.id))} (TG: {message.from_user.id})\n"
        f"Сумма: {amount} ₽\n"
        f"Способ: СБП \n"
        f"{sbp_phone}{bank}\n"
        "\nПодтвердите / отклоните заявку."
    )
    try:
        await bot.send_message(int(MANAGER_CHAT_ID), manager_text, reply_markup=manager_withdraw_kb(pid))
    except Exception:
        logger.exception("Failed to notify manager about withdrawal request")
    await message.answer(get_msg("withdraw_request_sent_to_manager", lang))
    await state.set_state(WithdrawStates.awaiting_manager)


# Менеджер подтверждает / отклоняет (кнопки приходят на MANAGER_CHAT_ID)
@urouter.callback_query(F.data.startswith("withdraw_confirm_"))
async def cb_manager_confirm_withdraw(call: CallbackQuery):
    await call.answer()
    # разрешаем подтверждать только менеджеру (жёсткая проверка)
    try:
        if str(call.from_user.id) != str(MANAGER_CHAT_ID):
            await call.message.answer("Нет прав для подтверждения операции.")
            return
    except Exception:
        pass

    pid = call.data.split("withdraw_confirm_", 1)[1]
    entry = pending_actions.get(pid)
    if not entry or entry.get("type") != "withdraw":
        await call.message.answer("Заявка не найдена или уже обработана.")
        return

    # Помечаем как approved, запускаем API-вывод
    entry["status"] = "approved"
    entry["manager_id"] = call.from_user.id
    # Подготавливаем параметры вызова perform_withdrawal
    user_phone_for_api = entry.get("user_phone")
    amount = entry.get("amount")
    method = entry.get("method")
    # уведомляем менеджера, что выполняем
    await call.message.answer(get_msg("manager_started_withdraw", "ru", pid=pid))
    # выполняем вывод в отдельном потоке
    try:
        if method == "card":
            card = entry.get("card_number")
            res = await asyncio.to_thread(perform_withdrawal, phone=user_phone_for_api, amount=amount, card_number=card)
        else:
            # sbp
            sbp_phone = entry.get("sbp_phone")
            sbp_bank = entry.get("sbp_bank")
            res = await asyncio.to_thread(perform_withdrawal, phone=user_phone_for_api, amount=amount, phone_hint=sbp_phone, bank_hint=sbp_bank)
    except Exception as e:
        logger.exception("Error performing withdrawal for pid %s", pid)
        res = {"ok": False, "reason": "exception", "error": str(e)}

    # уведомляем менеджера и пользователя о результате
    entry["api_result"] = res
    if res.get("ok"):
        entry["status"] = "done"
        # уведомляем пользователя
        try:
            user_id = entry.get("user_id")
            user_lang = _get_lang_for_user(user_id)
            await bot.send_message(user_id, get_msg("withdraw_success_user", user_lang, amount_sent=float(res.get("amount_sent", amount))))
        except Exception:
            logger.exception("Can't notify user about successful withdrawal")
        await call.message.answer(get_msg("manager_withdraw_done", "ru", pid=pid))
    else:
        entry["status"] = "failed"
        reason = res.get("reason") or res.get("error") or "unknown"
        try:
            user_id = entry.get("user_id")
            user_lang = _get_lang_for_user(user_id)
            await bot.send_message(user_id, get_msg("withdraw_failed_user", user_lang, reason=reason))
        except Exception:
            logger.exception("Can't notify user about failed withdrawal")
        await call.message.answer(get_msg("manager_withdraw_failed", "ru", pid=pid, reason=reason))

    # опционально: показываем менеджеру raw result
    try:
        await call.message.answer(f"API result: {str(res)[:1500]}")
    except Exception:
        pass


@urouter.callback_query(F.data.startswith("withdraw_reject_"))
async def cb_manager_reject_withdraw(call: CallbackQuery):
    await call.answer()
    try:
        if str(call.from_user.id) != str(MANAGER_CHAT_ID):
            await call.message.answer("Нет прав для отклонения операции.")
            return
    except Exception:
        pass

    pid = call.data.split("withdraw_reject_", 1)[1]
    entry = pending_actions.get(pid)
    if not entry or entry.get("type") != "withdraw":
        await call.message.answer("Заявка не найдена или уже обработана.")
        return

    entry["status"] = "rejected"
    entry["manager_id"] = call.from_user.id
    # уведомляем пользователя
    try:
        user_id = entry.get("user_id")
        user_lang = _get_lang_for_user(user_id)
        await bot.send_message(user_id, get_msg("withdraw_rejected_user", user_lang))
    except Exception:
        logger.exception("Can't notify user about rejected withdrawal")
    await call.message.answer(get_msg("manager_withdraw_rejected", "ru", pid=pid))

@urouter.callback_query(F.data == "to_start")
async def cb_to_start(call: CallbackQuery):
    lang = _get_lang_for_user(call.from_user.id)
    await call.answer()
    user = await get_user_by_tg_id(call.from_user.id)
    limited = await _is_limited_access(user.phone, getattr(user, "fio", None), call.from_user.id) if user else False
    balance = 0 if limited else (get_balance_by_phone(user.phone) if user else 0)
    if limited:
        await _safe_send_message(
            call.message.answer,
            get_msg("limited_access_message", lang),
            timeout=10.0,
            error_context="сообщение об ограниченном доступе"
        )
    main_text = get_msg("main_menu_text", lang, bal=balance, date=get_date_lead(user.phone) if user else "0", invited=compute_referral_commissions_for_inviter(user.phone))
    await _safe_send_message(
        call.message.answer,
        main_text,
        timeout=10.0,
        error_context="главное меню",
        reply_markup=build_main_menu(lang, limited=limited, is_admin=_is_admin(call.from_user.id))
    )


@urouter.callback_query(F.data == "contact_manager")
async def cb_contact_manager(call: CallbackQuery):
    lang = _get_lang_for_user(call.from_user.id)
    user = await get_user_by_tg_id(call.from_user.id)
    if await _deny_if_limited(call, lang, user):
        return
    await call.answer()
    await call.message.answer(get_msg("manager_contact", lang))


# --- Wi‑Fi карта ------------------------------------------------------------


@urouter.callback_query(F.data == "wifi_map")
async def cb_wifi_map(call: CallbackQuery, state: FSMContext):
    """
    Показываем экран с приложениями перед запросом геопозиции.
    """
    lang = _get_lang_for_user(call.from_user.id)
    await call.answer()

    points = get_available_wifi_points()
    if not points:
        await call.message.answer(get_msg("wifi_map_no_points", lang))
        return

    await state.set_state(WifiStates.showing_apps)
    await call.message.answer(
        get_msg("wifi_map_apps_intro", lang),
        reply_markup=wifi_apps_kb(lang),
        parse_mode="Markdown"
    )


@urouter.callback_query(F.data == "wifi_continue")
async def cb_wifi_continue(call: CallbackQuery, state: FSMContext):
    """
    Обработчик кнопки 'Продолжить' - переводит к запросу геопозиции.
    """
    lang = _get_lang_for_user(call.from_user.id)
    await call.answer()

    await state.set_state(WifiStates.waiting_location)
    await call.message.answer(
        get_msg("wifi_map_request_location", lang),
        reply_markup=location_request_kb(lang)
    )


@urouter.message(WifiStates.waiting_location)
async def wifi_receive_location(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    if not message.location:
        await message.answer(get_msg("wifi_map_request_location_retry", lang), reply_markup=location_request_kb(lang))
        return

    lat = message.location.latitude
    lon = message.location.longitude
    await state.clear()

    await message.answer(get_msg("wifi_map_processing", lang), reply_markup=ReplyKeyboardRemove())

    nearby = find_wifi_near_location(lat, lon, radius_m=50.0)
    if not nearby:
        await message.answer(get_msg("wifi_map_no_nearby", lang))
        return

    header = get_msg("wifi_map_results_header", lang, count=len(nearby))
    await message.answer(header)

    for point in nearby:
        plat = point.get("lat")
        plon = point.get("lon")
        name = point.get("name") or get_msg("wifi_map_point_default_name", lang)
        desc = point.get("description") or ""
        dist = point.get("distance_m") or 0
        # Формируем текст: только название и пароль (если есть), без шифрования
        if desc:
            # Если есть описание (пароль), показываем его
            text = get_msg("wifi_map_point_line_with_distance", lang, name=name, desc=desc, distance=dist)
        else:
            # Если описания нет, показываем только название и расстояние
            # Используем шаблон и убираем пустую строку описания
            text_template = get_msg("wifi_map_point_line_with_distance", lang, name=name, desc="", distance=dist)
            # Убираем пустую строку между названием и расстоянием (заменяем \n\n на \n)
            text = text_template.replace(f"📍 {name}\n\n", f"📍 {name}\n")
        try:
            await message.answer_location(latitude=plat, longitude=plon)
        except Exception:
            logger.exception("Не удалось отправить геолокацию для точки Wi-Fi %s", name)
        if text.strip():
            await message.answer(text)


