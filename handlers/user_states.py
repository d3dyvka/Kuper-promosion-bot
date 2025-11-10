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
    friend_check = State()  # ожидание подтверждения регистрации друга

class PromoStates(StatesGroup):
    viewing = State()
    claiming = State()

class WithdrawStates(StatesGroup):
    ask_requisites = State()
    confirm_withdraw = State()
