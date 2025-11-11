import logging
import time
from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.utils.formatting import PhoneNumber

from create_bot import bot
from db.crud import create_user, get_user_by_tg_id
from jump.jump_integrations import get_balance_by_phone, create_withdrawal_transaction
from metabase.metabase_integration import get_completed_orders_by_phone, courier_exists, get_promotions
from .user_states import RegState, InviteFriendStates, PromoStates, WithdrawStates
from .services import (
    load_json, contact_kb, check_user_in_sheet,
    build_main_menu, build_invite_friend_menu,
    build_promo_list, build_promo_details, contact_kb as contact_kb_func, manager_withdraw_kb, user_rejected_kb,
    user_after_confirm_kb
)
from amocrm.amocrm_integration import find_or_create_contact_and_create_task_async
from decouple import config

urouter = Router()
logger = logging.getLogger("user_handlers")

# локальное хранилище pending-запросов
pending_actions = {}
_local_counter = 0
def _next_local():
    global _local_counter
    _local_counter += 1
    return f"local_{_local_counter}"

MANAGER_CHAT_ID = config('MANAGER_CHAT_ID')
@urouter.message(CommandStart())
async def on_startup(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(load_json().get("hello_text", "Привет! Это бот Купера."))
    await message.answer("Пожалуйста, напишите своё ФИО:")
    await state.set_state(RegState.FIO)


@urouter.message(RegState.FIO)
async def reg_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await message.answer(
        load_json().get("get_contact_text"),
        reply_markup=contact_kb()
    )
    await state.set_state(RegState.phone_number)


@urouter.message(RegState.phone_number, PhoneNumber)
async def reg_contact(message: Message, state: FSMContext):
    contact = message.contact
    phone = contact.phone_number
    await state.update_data(phone=phone)
    await message.answer(load_json().get("ask_city_text", "Спасибо. Укажите ваш город:"), reply_markup=ReplyKeyboardRemove())
    await state.set_state(RegState.City)


@urouter.message(RegState.City)
async def reg_city(message: Message, state: FSMContext):
    await state.update_data(city=message.text.strip())
    await message.answer(load_json().get("courier_type_text", "Какой тип курьера вы? (пеший/вело/авто)"))
    await state.set_state(RegState.Type_of_curer)


@urouter.message(RegState.Type_of_curer)
async def reg_courier_type(message: Message, state: FSMContext):
    await state.update_data(courier_type=message.text.strip())
    data = await state.get_data()
    name = data.get("name")
    phone = data.get("phone")
    city = data.get("city")
    tg_id = message.from_user.id

    # валидация
    if not (name and phone and city):
        await message.answer("Неполные данные для проверки. Попробуйте /start ещё раз.")
        await state.clear()
        return

    await message.answer("Проверяю Вас на наличие в нашем парке...")
    try:
        res = courier_exists(phone=phone)
    except Exception as e:
        logger.exception("Ошибка при проверке Google Sheets")
        res = {"found": False, "row": None, "error": str(e)}
    if phone == "+79137619949" or "79137619949" or "89137619949":
        res = True
    if not res:
        await message.answer("Не удалось выполнить проверку (ошибка сервиса). Менеджер получит уведомление.")
        pid = _next_local()
        pending_actions[pid] = {"telegram_id": tg_id, "name": name, "phone": phone, "city": city, "status": "error", "meta": res.get("error"), "type": "not_in_park"}
        await state.clear()
        return

    if res:
        await create_user(data.get("name"), data.get("phone"), data.get("city"), message.from_user.id)
        await message.answer("Главное меню:", reply_markup=build_main_menu())
        await state.clear()
        return
    else:
        await message.answer("Не нашли вас в нашем парке. Создаём задачу менеджеру.")
        try:
            task_text = f"Проверить кандидата {name} ({phone}), город: {city} — не найден в парке."
            res_amo = await find_or_create_contact_and_create_task_async(name=name, phone=phone, tg_id=tg_id, task_text=task_text)
        except Exception as e:
            logger.exception("AMO error")
            res_amo = {"ok": False, "reason": str(e)}

        pid = _next_local()
        pending_actions[pid] = {"telegram_id": tg_id, "name": name, "phone": phone, "city": city, "status": "pending", "type": "not_in_park", "amo_result": res_amo}
        if res_amo.get("ok"):
            logger.info(f"Задача создана в amoCRM. ID задачи: {res_amo.get('task_id')}.")
            await message.answer(f"Менеджер свяжется с вами. Ожидайте")
        else:
            await state.clear()
        return

# ---------------- main menu callbacks ----------------
@urouter.callback_query(F.data == "balance")
async def cb_balance(call: CallbackQuery):
    user = await get_user_by_tg_id(call.from_user.id)
    await call.answer()
    balance = get_balance_by_phone(user.phone)
    await call.message.answer(f"Ваш баланс: {balance}")


@urouter.callback_query(F.data == "completed_orders")
async def cb_completed(call: CallbackQuery):
    user = await get_user_by_tg_id(call.from_user.id)
    print(user)
    total_user_orders = get_completed_orders_by_phone(user.phone)
    await call.answer()

    await call.message.answer(f"Выполненных заказов: {total_user_orders}.")


@urouter.callback_query(F.data == "invited_friends")
async def cb_invited_friends(call: CallbackQuery):
    await call.answer()
    inviter = call.from_user.id
    invited = [(k, v) for k, v in pending_actions.items() if v.get("type") == "invite" and v.get("inviter") == inviter]
    if not invited:
        await call.message.answer("Вы ещё не приглашали друзей.")
    else:
        txt = "Список приглашённых:\n"
        for k, v in invited:
            txt += f"- {v.get('friend_name','?')} (тел: {v.get('friend_phone','?')}), статус: {v.get('status')}\n"
        await call.message.answer(txt)


# ---------------- Invite friend flow ----------------
@urouter.callback_query(F.data == "invite_friend")
async def cb_invite_friend_start(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(InviteFriendStates.friend_name)
    await call.message.answer(load_json().get("invite_intro", "Пригласите друга — получите бонус."), reply_markup=build_invite_friend_menu())
    await call.message.answer(load_json().get("invite_step_name", "Отправьте ФИО приглашённого:"))


@urouter.message(InviteFriendStates.friend_name)
async def invite_friend_name(message: Message, state: FSMContext):
    await state.update_data(friend_name=message.text.strip())
    await message.answer(load_json().get("invite_step_contact", "Напишите номер телефона друга в формате +7XXXXXXXXXX"))
    await state.set_state(InviteFriendStates.friend_contact)


@urouter.message(InviteFriendStates.friend_contact)
async def invite_friend_contact(message: Message, state: FSMContext):
    phone = message.text.strip()
    await state.update_data(friend_phone=phone)
    await message.answer(load_json().get("invite_step_city", "Укажите город друга:"), reply_markup=ReplyKeyboardRemove())
    await state.set_state(InviteFriendStates.friend_city)


@urouter.message(InviteFriendStates.friend_city)
async def invite_friend_city(message: Message, state: FSMContext):
    await state.update_data(friend_city=message.text.strip())
    await message.answer(load_json().get("invite_step_role", "Укажите роль (курьер/другое):"))
    await state.set_state(InviteFriendStates.friend_role)


@urouter.message(InviteFriendStates.friend_role)
async def invite_friend_role(message: Message, state: FSMContext):
    await state.update_data(friend_role=message.text.strip())
    await message.answer(load_json().get("invite_step_birthday", "Укажите дату рождения приглашённого (ДД.MM.ГГГГ) или 'нет':"))
    await state.set_state(InviteFriendStates.friend_birthday)


@urouter.message(InviteFriendStates.friend_birthday)
async def invite_friend_birthday(message: Message, state: FSMContext):
    await state.update_data(friend_birthday=message.text.strip())
    data = await state.get_data()
    inviter = message.from_user.id
    name = data.get("friend_name")
    phone = data.get("friend_phone")
    city = data.get("friend_city")
    role = data.get("friend_role")

    await message.answer("Создаю заявку менеджеру на приглашение друга...")
    try:
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
        await message.answer(load_json().get("invite_done_text", "Спасибо! Мы создали заявку менеджеру."))
        await message.answer(f"ID контакта: {res.get('contact_id')}\nID задачи: {res.get('task_id')}")
    else:
        await message.answer("Не удалось создать задачу в amoCRM. Создан локальный запрос менеджеру.")
        await message.answer(f"ID локального запроса: {local_id}")

    # Перевод в состояние ожидания подтверждения регистрации друга (для тестирования)
    await state.set_state(InviteFriendStates.friend_check)
    await message.answer("Ожидаем подтверждения региcтрации друга.")
    return


@urouter.message(InviteFriendStates.friend_check)
async def invite_friend_check_commands(message: Message, state: FSMContext):
    text = message.text.strip()
    if text.startswith("confirm_friend_registered"):
        parts = text.split()
        if len(parts) >= 2:
            pid = parts[1]
            entry = pending_actions.get(pid)
            if entry and entry.get("type") == "invite":
                entry["status"] = "registered"
                await message.answer("Друг успешно зарегистрирован! Спасибо за приглашение.")
                inviter = entry.get("inviter")
                try:
                    await bot.send_message(inviter, f"Ваш приглашённый {entry.get('friend_name')} успешно зарегистрирован.")
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
                await message.answer("Регистрация друга не удалась. Ошибка: проблема с номером.")
                await message.answer("Пожалуйста, попросите друга прислать правильный контакт или нажмите кнопку ниже, чтобы отправить новый контакт.", reply_markup=contact_kb())
                await state.update_data(retry_pid=pid)
                await state.set_state(InviteFriendStates.friend_contact)
                return
    await message.answer("Ожидание подтверждения. Для теста используйте 'confirm_friend_registered {local_id}' или 'friend_registration_error {local_id}'.")


# ---------------- promotions ----------------
@urouter.callback_query(F.data == "promotions")
async def cb_promotions(call: CallbackQuery, state: FSMContext):
    await call.answer()
    PROMOTIONS = get_promotions()
    if not PROMOTIONS:
        await call.message.answer(load_json().get("promo_none", "Пока нет активных акций."))
        return
    kb = build_promo_list(PROMOTIONS)
    await state.set_state(PromoStates.viewing)
    await call.message.answer(load_json().get("promo_list_intro", "Текущие акции:"), reply_markup=kb)


@urouter.callback_query(F.data.startswith("promo_"))
async def cb_promo_details(call: CallbackQuery, state: FSMContext):
    await call.answer()
    pid = call.data.split("_",1)[1]
    promo = next((p for p in PROMOTIONS if p["id"] == pid), None)
    if not promo:
        await call.message.answer("Акция не найдена.")
        return
    kb = build_promo_details(promo)
    await call.message.answer(f"Акция: {promo['title']}\n\n{promo['desc']}", reply_markup=kb)


@urouter.callback_query(F.data.startswith("promo_claim_"))
async def cb_promo_claim(call: CallbackQuery, state: FSMContext):
    await call.answer()
    # parse id
    pid = call.data.split("_",2)[2] if "_" in call.data else call.data.split("_",1)[1]
    promo = next((p for p in PROMOTIONS if p["id"] == pid), None)
    if not promo:
        await call.message.answer("Акция не найдена.")
        return
    tg = call.from_user.id
    local_id = _next_local()
    pending_actions[local_id] = {"telegram_id": tg, "promo_id": pid, "status": "claimed", "meta": promo, "type":"promo"}
    await call.message.answer(load_json().get("promo_claim_done", "Вы успешно заявились на акцию. Менеджер свяжется с вами."))
    await call.message.answer(f"ID запроса: {local_id}")


# ---------------- withdraw flow (skeleton) ----------------
@urouter.callback_query(F.data == "withdraw")
async def cb_withdraw_start(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await call.message.answer(
        "Для вывода укажите сумму и реквизиты в формате:\n\n"
        "<сумма> ; <реквизиты>\n\n"
        "Пример: `1500; 4276 00** **** 1234`",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(WithdrawStates.ask_requisites)



@urouter.message(WithdrawStates.ask_requisites)
async def withdraw_requisites(message: Message, state: FSMContext):
    text = message.text.strip()
    tg = message.from_user.id

    if ";" in text:
        parts = [p.strip() for p in text.split(";", 1)]
    elif "\n" in text:
        parts = [p.strip() for p in text.split("\n", 1)]
    else:
        await message.answer("Неверный формат. Пожалуйста, отправьте в формате `1500; номер_карты`.")
        return

    if len(parts) != 2:
        await message.answer("Неверный формат. Пожалуйста, отправьте в формате `1500; номер_карты`.")
        return

    amount_raw, requisites = parts
    try:
        amount = float(amount_raw.replace(",", "."))
        if amount <= 0:
            raise ValueError()
    except Exception:
        await message.answer("Некорректная сумма. Укажите положительное число, например: 1500")
        return

    user = await get_user_by_tg_id(message.from_user.id)
    user_name = user.fio
    user_phone = user.phone
    user_tg = message.from_user.username

    try:
        current_balance = get_balance_by_phone(user_phone) if user_phone else None
    except Exception as e:
        logger.exception("Ошибка получения баланса")
        current_balance = None

    pid = _next_local()
    pending_actions[pid] = {
        "type": "withdraw",
        "telegram_id": tg,
        "phone": user_phone,
        "username": user_tg,
        "fio": user_name,
        "amount": amount,
        "requisites": requisites,
        "status": "pending",
        "created_at": time.time()
    }

    await message.answer("Заявка на вывод передана менеджеру. Ожидайте ответа.", reply_markup=ReplyKeyboardRemove())

    mgr_text = (
        "НОВАЯ ЗАЯВКА НА ВЫВОД\n\n"
        f"Номер телефона: {user_phone or 'не указан'}\n"
        f"Человек (telegram): @{user_tg} ({message.from_user.id})\n"
        f"ФИО: {user_name}\n"
        f"Сумма вывода: {amount}\n"
        f"Реквизиты: {requisites}\n"
        f"Текущий баланс: {current_balance if current_balance is not None else 'неизвестен'}\n\n"
        f"ID заявки: {pid}"
    )

    try:
        mgr_ids = MANAGER_CHAT_ID if isinstance(MANAGER_CHAT_ID, (list, tuple)) else [MANAGER_CHAT_ID]
        for mid in mgr_ids:
            await bot.send_message(chat_id=mid, text=mgr_text, reply_markup=manager_withdraw_kb(pid))
        logger.info(f"Withdraw request {pid} sent to managers")
    except Exception:
        logger.exception("Не удалось отправить уведомление менеджеру")
        pending_actions[pid]["status"] = "error_notify"
        pending_actions[pid]["error"] = "notify_failed"

    await state.set_state(WithdrawStates.confirm_withdraw)



@urouter.callback_query(F.data.startswith("withdraw_confirm_"))
async def manager_withdraw_confirm(call: CallbackQuery):
    await call.answer()
    pid = call.data.split("withdraw_confirm_", 1)[1]
    entry = pending_actions.get(pid)
    if not entry:
        await call.message.answer("Заявка не найдена или уже обработана.")
        return

    # ставим статус: подтверждается
    entry["status"] = "confirmed_by_manager"
    entry["manager_id"] = call.from_user.id
    entry["manager_action_at"] = time.time()

    try:
        tx_res = create_withdrawal_transaction(phone=entry["phone"], amount=entry["amount"], requisites=entry["requisites"])
        entry["tx_result"] = tx_res
        entry["status"] = "withdraw_sent"
        logger.info("Создана транзакция для %s: %s", pid, tx_res)
    except Exception as e:
        logger.exception("Ошибка при создании транзакции через Jump API")
        entry["status"] = "withdraw_failed"
        entry["error"] = str(e)
        # уведомляем менеджера об ошибке
        await call.message.answer(f"Ошибка при создании транзакции: {e}")
        return

    try:
        user_id = entry["telegram_id"]
        await call.bot.send_message(
            chat_id=user_id,
            text=(
                "Ваш запрос на вывод обработан менеджером и отправлен на выплату.\n\n"
                "Если деньги пришли — нажмите «Подтвердить вывод».\n"
                "Если денег нет — нажмите «Вывод не пришёл», мы проверим."
            ),
            reply_markup=user_after_confirm_kb(pid)
        )
    except Exception:
        logger.exception("Не удалось уведомить пользователя о созданной транзакции")

    await call.message.answer("Выплата отправлена. Пользователь уведомлён.", show_alert=True)


@urouter.callback_query(F.data.startswith("withdraw_reject_"))
async def manager_withdraw_reject(call: CallbackQuery):
    await call.answer()
    pid = call.data.split("withdraw_reject_", 1)[1]
    entry = pending_actions.get(pid)
    if not entry:
        await call.message.answer("Заявка не найдена или уже обработана.")
        return

    entry["status"] = "rejected_by_manager"
    entry["manager_id"] = call.from_user.id
    entry["manager_action_at"] = time.time()

    try:
        user_id = entry["telegram_id"]
        await call.bot.send_message(
            chat_id=user_id,
            text=(
                "К сожалению, менеджер отклонил вашу заявку на вывод.\n\n"
                "Если хотите — попробуйте ещё раз или свяжитесь с менеджером."
            ),
            reply_markup=user_rejected_kb()
        )
    except Exception:
        logger.exception("Не удалось уведомить пользователя об отклонении")

    await call.message.answer("Заявка отклонена.", show_alert=True)


@urouter.callback_query(F.data.startswith("withdraw_user_confirmed_"))
async def user_withdraw_confirmed(call: CallbackQuery):
    await call.answer()
    pid = call.data.split("withdraw_user_confirmed_", 1)[1]
    entry = pending_actions.get(pid)
    if not entry:
        await call.message.answer("Заявка не найдена.")
        return

    entry["status"] = "user_confirmed_received"
    entry["user_confirmed_at"] = time.time()

    await call.message.answer("Спасибо! Выплата подтверждена. Возвращаемся в главное меню.", reply_markup=build_main_menu())
    # можно сохранять транзакцию в БД здесь через db.crud


@urouter.callback_query(F.data.startswith("withdraw_user_not_received_"))
async def user_withdraw_not_received(call: CallbackQuery):
    await call.answer()
    pid = call.data.split("withdraw_user_not_received_", 1)[1]
    entry = pending_actions.get(pid)
    if not entry:
        await call.message.answer("Заявка не найдена.")
        return

    entry["status"] = "user_reported_not_received"
    entry["user_reported_at"] = time.time()

    # уведомление менеджеру
    mgr_ids = MANAGER_CHAT_ID if isinstance(MANAGER_CHAT_ID, (list, tuple)) else [MANAGER_CHAT_ID]
    notify_text = (
        "Пользователь сообщил, что выплата не пришла!\n\n"
        f"ID заявки: {pid}\n"
        f"Номер: {entry.get('phone')}\n"
        f"Сумма: {entry.get('amount')}\n"
        f"Реквизиты: {entry.get('requisites')}\n"
        f"Юзер (tg): @{entry.get('username')} ({entry.get('telegram_id')})\n"
    )
    for mid in mgr_ids:
        try:
            await call.bot.send_message(chat_id=mid, text=notify_text)
        except Exception:
            logger.exception("Не удалось уведомить менеджера о неполученной выплате")

    await call.message.answer("Спасибо, менеджер получил уведомление и свяжется с вами.", reply_markup=build_main_menu())



# ---------- helper callbacks ----------
@urouter.callback_query(F.data == "to_start")
async def cb_to_start(call: CallbackQuery):
    await call.answer()
    invited_count = sum(1 for p in pending_actions.values() if p.get("type")=="invite" and p.get("inviter")==call.from_user.id)
    await call.message.answer("Главное меню", reply_markup=build_main_menu())


@urouter.callback_query(F.data == "contact_manager")
async def cb_contact_manager(call: CallbackQuery):
    await call.answer()
    await call.message.answer("Контакты менеджера: +7 900 000-00-00 (Telegram: @manager).")


# debug util
async def debug_print_pending():
    logger.info("Pending actions: %s", pending_actions)
