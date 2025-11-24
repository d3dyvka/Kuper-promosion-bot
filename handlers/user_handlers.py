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
from metabase.metabase_integration import get_completed_orders_by_phone, courier_exists, get_promotions, get_date_lead
from .user_states import RegState, InviteFriendStates, PromoStates, WithdrawStates
from .services import (
    load_json, contact_kb,
    build_main_menu, build_invite_friend_menu, add_person_to_external_sheet, get_msg
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
        main_text = get_msg("main_menu_text", lang, bal=balance, date=date or "0", invited=None)
        await call.message.answer(main_text, reply_markup=build_main_menu(lang))
    else:
        # ask for FIO in selected language
        await call.message.answer(get_msg("get_name_text", lang))
        await state.set_state(RegState.FIO)


@urouter.message(RegState.FIO)
async def reg_name(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    await state.update_data(name=message.text.strip())
    await message.answer(
        get_msg("get_contact_text", lang),
        reply_markup=contact_kb()
    )
    await state.set_state(RegState.phone_number)


@urouter.message(RegState.phone_number, PhoneNumber)
async def reg_contact(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    contact = message.contact
    phone = contact.phone_number
    await state.update_data(phone=phone)
    await message.answer(get_msg("ask_city_text", lang),
                         reply_markup=ReplyKeyboardRemove())
    await state.set_state(RegState.City)


@urouter.message(Command("menu"))
async def menu(message: Message, state: FSMContext):
    lang = _get_lang_for_user(message.from_user.id)
    await state.clear()
    user = await get_user_by_tg_id(message.from_user.id)
    balance = get_balance_by_phone(user.phone) if user else 0
    if user:
        main_text = get_msg("main_menu_text", lang, bal=balance, date=get_date_lead(user.phone) or "—", invited=None)
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
        main_text = get_msg("main_menu_text", lang, bal=balance, date=get_date_lead(phone) or "—", invited=None)
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

    try:
        res = await asyncio.to_thread(perform_withdrawal, phone=user.phone, amount=amount)
    except Exception:
        logger.exception("Withdrawal error")
        await message.answer(get_msg("withdrawal_attempt_error", lang))
        await state.clear()
        return

    if not res.get("ok"):
        reason = res.get("reason")
        if reason == "insufficient_after_minimum":
            await message.answer(get_msg("withdrawal_insufficient", lang))
        else:
            await message.answer(get_msg("withdrawal_create_failed", lang, reason=reason))
        await state.clear()
        return

    amount_sent = res.get("amount_sent") or amount
    await message.answer(
        get_msg("withgrawal_request", lang, amount_sent=float(amount_sent)),
        reply_markup=build_main_menu(lang))
    await state.clear()


@urouter.callback_query(F.data == "to_start")
async def cb_to_start(call: CallbackQuery):
    lang = _get_lang_for_user(call.from_user.id)
    await call.answer()
    user = await get_user_by_tg_id(call.from_user.id)
    balance = get_balance_by_phone(user.phone) if user else 0
    main_text = get_msg("main_menu_text", lang, bal=balance, date=get_date_lead(user.phone) if user else "—", invited=None)
    await call.message.answer(main_text, reply_markup=build_main_menu(lang))


@urouter.callback_query(F.data == "contact_manager")
async def cb_contact_manager(call: CallbackQuery):
    lang = _get_lang_for_user(call.from_user.id)
    await call.answer()
    await call.message.answer(get_msg("manager_contact", lang))


# debug util
async def debug_print_pending():
    logger.info("Pending actions: %s", pending_actions)
