import asyncio
import logging
import datetime
from aiogram import Router, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.utils.formatting import PhoneNumber

from create_bot import bot
from db.crud import create_user, get_user_by_tg_id
from jump.jump_integrations import get_balance_by_phone, perform_withdrawal
from metabase.metabase_integration import get_completed_orders_by_phone, courier_exists, get_promotions, get_date_lead, \
    compute_referral_commissions_for_inviter
from wifi_map.wifi_services import find_wifi_near_location, get_available_wifi_points
from .user_states import RegState, InviteFriendStates, PromoStates, WithdrawStates, WifiStates
from .services import (
    load_json, contact_kb, location_request_kb,
    build_main_menu, build_invite_friend_menu, add_person_to_external_sheet, get_msg, manager_withdraw_kb
)
from amocrm.amocrm_integration import find_or_create_contact_and_create_task_async
from decouple import config
from loguru import logger

import re

urouter = Router()

# pending storage
pending_actions = {}
_local_counter = 0
user_langs = {}


def _next_local():
    global _local_counter
    _local_counter += 1
    return f"local_{_local_counter}"


def _get_lang_for_user(tg_id: int) -> str:
    try:
        return user_langs.get(int(tg_id), "ru")
    except Exception:
        return "ru"


MANAGER_CHAT_ID = config('MANAGER_CHAT_ID')
EXTERNAL_SPREADSHEET_ID = config('EXTERNAL_SPREADSHEET_ID')
EXTERNAL_SHEET_NAME = config('EXTERNAL_SHEET_NAME')


@urouter.message(CommandStart())
async def on_startup(message: Message, state: FSMContext):
    await state.clear()
    # используем русский вариант, потому что это первый шаг (пока не выбран язык)
    prompt = get_msg("choose_language_prompt", "ru")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=get_msg("lang_ru_label", "ru"), callback_data="lang_ru"),
         InlineKeyboardButton(text=get_msg("lang_uz_label", "ru"), callback_data="lang_uz")],
        [InlineKeyboardButton(text=get_msg("lang_tg_label", "ru"), callback_data="lang_tg"),
         InlineKeyboardButton(text=get_msg("lang_ky_label", "ru"), callback_data="lang_ky")]
    ])
    await message.answer(prompt, reply_markup=kb)


@urouter.callback_query(F.data.startswith("lang_"))
async def cb_set_language(call: CallbackQuery, state: FSMContext):
    await call.answer()
    lang = call.data.split("_", 1)[1]  # 'ru', 'uz', 'tg', 'ky'
    user_id = call.from_user.id
    user_langs[user_id] = lang

    # Send greeting in selected language
    try:
        await call.message.answer(get_msg("hello_text", lang))
    except Exception:
        # fallback to Russian if key missing
        await call.message.answer(get_msg("hello_text", "ru"))

    # Continue depending on whether user exists
    user = await get_user_by_tg_id(user_id)
    if user:
        balance = get_balance_by_phone(user.phone) if user else 0
        date = get_date_lead(user.phone) if user and getattr(user, "phone", None) else None
        # build_main_menu may accept lang in your services implementation
        main_text = get_msg("main_menu_text", lang, bal=balance, date=date or "0", invited=compute_referral_commissions_for_inviter(user.phone))
        await call.message.answer(main_text, reply_markup=build_main_menu(lang))
    else:
        # ask for FIO in selected language
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
    phone = contact.phone_number
    if phone:
        logger.info(f"New phone number {phone} for contact {contact}")
    await state.update_data(phone=phone)
    await message.answer(get_msg("get_name_text", lang),
                         reply_markup=ReplyKeyboardRemove())
    await state.set_state(RegState.FIO)


@urouter.message(Command("menu"))
async def menu(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    await state.clear()
    user = await get_user_by_tg_id(message.from_user.id)
    balance = get_balance_by_phone(user.phone) if user else 0
    if user:
        main_text = get_msg("main_menu_text", lang, bal=balance, date=get_date_lead(user.phone) or "0", invited=compute_referral_commissions_for_inviter(user.phone))
        await message.answer(main_text, reply_markup=build_main_menu(lang))


@urouter.message(RegState.City)
async def reg_city(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    await state.update_data(city=message.text.strip())
    await message.answer(get_msg("courier_type_text", lang))
    await state.set_state(RegState.Type_of_curer)


@urouter.message(RegState.Type_of_curer)
async def reg_courier_type(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    await state.update_data(courier_type=message.text.strip())
    data = await state.get_data()
    name = data.get("name")
    phone = data.get("phone")
    city = data.get("city")
    tg_id = message.from_user.id

    if not (name and phone and city):
        await message.answer(get_msg("incomplete_data_error", lang))
        await state.clear()
        return

    await message.answer(get_msg("checking_in_park", lang))
    try:
        res = courier_exists(phone=phone)
    except Exception as e:
        logger.exception("Ошибка при проверке Metabase")
        res = {"found": False, "row": None, "error": str(e)}

    # special bypass for admin phone
    if phone and re.sub(r"\D+", "", phone).endswith("9137619949"):
        res = {"found": True, "row": None, "error": None}

    if not res:
        await message.answer(get_msg("error_check", lang))
        pid = _next_local()
        pending_actions[pid] = {"telegram_id": tg_id, "name": name, "phone": phone, "city": city, "status": "error",
                                "meta": res.get("error"), "type": "not_in_park"}
        await state.clear()
        return

    if res.get("found"):
        await create_user(data.get("name"), data.get("phone"), data.get("city"), message.from_user.id)
        balance = get_balance_by_phone(data.get("phone"))
        main_text = get_msg("main_menu_text", lang, bal=balance, date=get_date_lead(phone) or "0", invited=compute_referral_commissions_for_inviter(phone))
        await message.answer(main_text,
                             reply_markup=build_main_menu(lang))
        await state.clear()
        return
    else:
        await message.answer(get_msg("not_exist", lang))
        try:
            task_text = f"Проверить кандидата {name} ({phone}), город: {city} — не найден в парке."
            res_amo = await find_or_create_contact_and_create_task_async(name=name, phone=phone, tg_id=tg_id,
                                                                         task_text=task_text)
        except Exception as e:
            logger.exception("AMO error")
            res_amo = {"ok": False, "reason": str(e)}

        pid = _next_local()
        pending_actions[pid] = {"telegram_id": tg_id, "name": name, "phone": phone, "city": city, "status": "pending",
                                "type": "not_in_park", "amo_result": res_amo}
        if res_amo.get("ok"):
            logger.info(f"Задача создана в amoCRM. ID задачи: {res_amo.get('task_id')}.")
            await message.answer(get_msg("manager_answer", lang))
        else:
            await state.clear()
        return


@urouter.callback_query(F.data == "completed_orders")
async def cb_completed(call: CallbackQuery):
    lang = _get_lang_for_user(call.from_user.id)
    user = await get_user_by_tg_id(call.from_user.id)
    total_user_orders = get_completed_orders_by_phone(user.phone)
    await call.answer()
    await call.message.answer(get_msg("completed_orders_text", lang, total_user_orders=total_user_orders))


@urouter.callback_query(F.data == "invited_friends")
async def cb_invited_friends(call: CallbackQuery):
    lang = _get_lang_for_user(call.from_user.id)
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
    await call.answer()
    await state.set_state(InviteFriendStates.friend_name)
    await call.message.answer(get_msg("invite_intro", lang),
                              reply_markup=build_invite_friend_menu())
    await call.message.answer(get_msg("invite_step_name", lang))
    await state.set_state(InviteFriendStates.friend_name)


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
    await message.answer(get_msg("invite_step_role", lang))
    await state.set_state(InviteFriendStates.friend_role)


@urouter.message(InviteFriendStates.friend_role)
async def invite_friend_role(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    await state.update_data(friend_role=message.text.strip())
    await message.answer(
        get_msg("invite_step_birthday", lang))
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

        print(ptype)

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

        if ptype == "first":
            base = title or get_msg("first_order_title_default", lang)
            parts = [base]
            if desc:
                parts.append(desc)
            if reward:
                parts.append(get_msg("bonus_label", lang) + f" {reward}")
            line = " - ".join(parts)
            if line not in seen:
                seen.add(line)
                lines.append(line)
            continue

        if ptype == "completed":
            # expected meta: threshold, end_date, coef_used, obj
            th = meta.get("threshold") or meta.get("thresholds") or None
            try:
                th_int = int(th)
            except Exception:
                # maybe title contains number or promo id
                try:
                    th_int = int(str(title).split()[0])
                except Exception:
                    th_int = None
            if th_int is None:
                # fallback: include title text
                line = f"{title} - {desc or '—'} - {reward} ₽"
                if line not in seen:
                    seen.add(line)
                    lines.append(line)
                continue

            end_date_raw = meta.get("end_date") or meta.get("end_date_str") or None
            end_date = None
            if end_date_raw:
                # many formats possible; try dd.mm.yyyy then iso
                try:
                    end_date = datetime.datetime.strptime(end_date_raw, "%d.%m.%Y").date()
                    end_date_str = end_date.strftime("%d.%m.%Y")
                except Exception:
                    try:
                        dt = datetime.datetime.fromisoformat(end_date_raw)
                        end_date = dt.date()
                        end_date_str = end_date.strftime("%d.%m.%Y")
                    except Exception:
                        end_date = None
                        end_date_str = str(end_date_raw)
            else:
                end_date_str = "—"

            # emoji logic
            emoji = "⏳"  # default if no end_date
            try:
                if isinstance(total_orders, (int, float)) and total_orders >= th_int:
                    emoji = "✅"
                else:
                    if end_date is None:
                        emoji = "⏳"
                    else:
                        if end_date >= today:
                            emoji = "⏳"
                        else:
                            emoji = "❌"
            except Exception:
                emoji = "⏳"

            # reward numeric normalize
            reward_str = str(reward).strip()
            if reward_str == "":
                reward_str = "0"
            # ensure date formatted dd.mm.YYYY or —
            line = f"{th_int} заказов - {end_date_str} - {reward_str} ₽ {emoji}"
            if line not in seen:
                seen.add(line)
                lines.append(line)
            continue

        # fallback
        base = title or get_msg("promo_default_title", lang)
        parts = [base]
        if desc:
            parts.append(desc)
        if reward:
            parts.append(get_msg("reward_label", lang) + f" {reward}")
        line = " - ".join(parts)
        if line not in seen:
            seen.add(line)
            lines.append(line)

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
    await call.answer()
    await state.set_state(WithdrawStates.ask_amount)
    await call.message.answer(get_msg("withdrawal_amount", lang))


@urouter.message(WithdrawStates.ask_amount)
async def withdraw_enter_amount(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    user = await get_user_by_tg_id(message.from_user.id)
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
    balance = get_balance_by_phone(user.phone) if user else 0
    main_text = get_msg("main_menu_text", lang, bal=balance, date=get_date_lead(user.phone) if user else "0", invited=compute_referral_commissions_for_inviter(user.phone))
    await call.message.answer(main_text, reply_markup=build_main_menu(lang))


@urouter.callback_query(F.data == "contact_manager")
async def cb_contact_manager(call: CallbackQuery):
    lang = _get_lang_for_user(call.from_user.id)
    await call.answer()
    await call.message.answer(get_msg("manager_contact", lang))


# --- Wi‑Fi карта ------------------------------------------------------------


@urouter.callback_query(F.data == "wifi_map")
async def cb_wifi_map(call: CallbackQuery, state: FSMContext):
    """
    Запрашиваем у пользователя геопозицию и ищем точки Wi‑Fi в радиусе 50 метров.
    """
    lang = _get_lang_for_user(call.from_user.id)
    await call.answer()

    points = get_available_wifi_points()
    if not points:
        await call.message.answer(get_msg("wifi_map_no_points", lang))
        return

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
        text = get_msg("wifi_map_point_line_with_distance", lang, name=name, desc=desc, distance=dist)
        try:
            await message.answer_location(latitude=plat, longitude=plon)
        except Exception:
            logger.exception("Не удалось отправить геолокацию для точки Wi-Fi %s", name)
        if text.strip():
            await message.answer(text)


# debug util
async def debug_print_pending():
    logger.info("Pending actions: %s", pending_actions)
