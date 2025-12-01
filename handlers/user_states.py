# user_states.py
from aiogram.fsm.state import StatesGroup, State

class RegState(StatesGroup):
    FIO = State()
    phone_number = State()
    City = State()
    Type_of_curer = State()

class InviteFriendStates(StatesGroup):
    friend_name = State()
    friend_contact = State()
    friend_city = State()
    friend_role = State()
    friend_birthday = State()
    friend_check = State()

class PromoStates(StatesGroup):
    viewing = State()

class WithdrawStates(StatesGroup):
    ask_amount = State()
    choose_method = State()
    card_number = State()
    sbp_phone = State()
    sbp_bank = State()
    awaiting_manager = State()


class WifiStates(StatesGroup):
    waiting_location = State()
