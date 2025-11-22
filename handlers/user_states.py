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

# Заменить/дополнить существующие состояния
class WithdrawStates(StatesGroup):
    choose_method = State()    # выбрать метод выплаты
    ask_amount = State()       # ввести сумму вывода
    sbp_phone = State()        # ввести телефон SBP
    sbp_bank = State()        # ввести банк SBP
    card_number = State()      # ввести номер карты
    confirm_withdraw = State() # ожидание подтверждения менеджера
