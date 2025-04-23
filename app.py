# --- Файл: app.py (Финальная версия v3, все включено) ---

import os
import logging
import datetime
import time
import telebot
import gspread
from flask import Flask, request

# Используем современную библиотеку google-auth
from google.oauth2.service_account import Credentials

# Используем библиотеку pybit для Bybit API v5
from pybit.unified_trading import HTTP

# Используем типы телебота для кнопок
from telebot import types

# Используем dotenv для загрузки .env файла при локальном запуске (опционально)
# from dotenv import load_dotenv
# load_dotenv() # Раскомментируй, если будешь запускать бота локально с .env

# === Настройка логирования ===
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# === Константы и Загрузка Настроек ===
# --- Основные ---
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    logger.error("FATAL: TELEGRAM_BOT_TOKEN not set!")

# --- Google Sheets ---
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Таблица сделок")
GLOSSARY_SHEET_NAME = os.getenv(
    "GLOSSARY_SHEET_NAME", "Глоссарий"
)  # Имя листа глоссария
CREDENTIALS_PATH = "/etc/secrets/credentials.json"  # Путь к секретному файлу Render

# --- Структура Таблицы Сделок (A-AD, 30 столбцов) ---
COL_IDX = {
    "entry_date": 0,
    "entry_time": 1,
    "exit_date": 2,
    "exit_time": 3,
    "pair": 4,
    "type": 5,
    "entry_price": 6,
    "sl_price": 7,
    "tp_price": 8,
    "volume_coins": 9,
    "volume_usd": 10,
    "plan_long_usd": 11,
    "plan_long_pct": 12,
    "plan_short_usd": 13,
    "plan_short_pct": 14,
    "rrr": 15,
    "commission_entry": 16,
    "commission_exit": 17,
    "exit_method": 18,
    "exit_price_actual": 19,
    "pnl_actual_usd": 20,
    "pnl_net_usd": 21,
    "duration": 22,
    "weekday": 23,
    "worked": 24,
    "not_worked": 25,
    "entry_reason": 26,
    "conclusions": 27,
    "screenshot": 28,
    "bybit_exec_id": 29,  # AC: Bybit Exec ID
    "entry_order_id": 30,  # AD: Bybit Order ID
}
# Ожидаемое количество колонок в основном листе
EXPECTED_COLUMNS = 30

# --- Bybit ---
BYBIT_ENV = os.getenv("BYBIT_ENV", "LIVE").upper()
BYBIT_CATEGORY = os.getenv("BYBIT_CATEGORY", "linear")

# === Глобальные переменные ===
bot = None
sheet = None  # Лист "Таблица сделок"
glossary_sheet = None  # Лист "Глоссарий"
bybit_session = None
google_creds = None
google_client = None
app = None
user_states = {}  # Для хранения состояния разговора (например, для глоссария)

# === Инициализация Flask ===
# Инициализируем Flask ДО попытки использования 'app'
app = Flask(__name__)

# === Инициализация Telegram бота ===
if TOKEN:
    try:
        bot = telebot.TeleBot(TOKEN, threaded=False)
        logger.info("Telegram bot initialized.")
    except Exception as e:
        logger.error(f"Telegram init error: {e}", exc_info=True)
        bot = None  # Убедимся, что bot=None при ошибке
else:
    logger.error("TELEGRAM_BOT_TOKEN not set!")
    bot = None


# === Функции Инициализации Сервисов ===
def init_google_sheets():
    """Инициализирует подключение к Google Sheets и обоим листам."""
    global sheet, glossary_sheet, google_creds, google_client
    logger.info("Attempting to connect to Google Sheets...")
    if not SPREADSHEET_ID:
        logger.error("FATAL: SPREADSHEET_ID not set!")
        return False
    # Используем CREDENTIALS_PATH определенный выше
    if not os.path.exists(CREDENTIALS_PATH):
        logger.error(f"FATAL: Credentials file not found at {CREDENTIALS_PATH}!")
        return False
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive",
        ]
        google_creds = Credentials.from_service_account_file(
            CREDENTIALS_PATH, scopes=scope
        )
        google_client = gspread.authorize(google_creds)
        spreadsheet = google_client.open_by_key(SPREADSHEET_ID)
        # Инициализируем листы в отдельных try-except
        try:
            sheet = spreadsheet.worksheet(SHEET_NAME)
            logger.info(f"Connected to Main Sheet: '{sheet.title}'")
            if sheet.col_count < EXPECTED_COLUMNS:
                logger.warning(
                    f"Main Sheet has {sheet.col_count} cols, expected {EXPECTED_COLUMNS}."
                )
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"FATAL: Main Worksheet '{SHEET_NAME}' not found!")
            sheet = None
        try:
            glossary_sheet = spreadsheet.worksheet(GLOSSARY_SHEET_NAME)
            logger.info(f"Connected to Glossary Sheet: '{glossary_sheet.title}'")
        except gspread.exceptions.WorksheetNotFound:
            logger.error(
                f"FATAL: Glossary Worksheet '{GLOSSARY_SHEET_NAME}' not found!"
            )
            glossary_sheet = None
        return (
            sheet is not None or glossary_sheet is not None
        )  # Успех, если хоть один лист найден
    except gspread.exceptions.APIError as e:  # Обработка ошибок API Google
        if hasattr(e, "response") and e.response.status_code == 401:
            logger.warning("Google API Error 401. Refresh needed?")
            # Просто логируем, рефреш часто не помогает с сервисными аккаунтами
        else:
            logger.error(f"FATAL: Google API Error: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"FATAL: Error connecting Google Sheets: {e}", exc_info=True)
        return False


def init_bybit():
    """Инициализирует подключение к Bybit, читая ключи из Secret Files."""
    global bybit_session
    env = os.getenv("BYBIT_ENV", "LIVE").upper()
    logger.info(f"Attempting to connect to Bybit {env} environment...")
    api_key = None
    api_secret = None
    testnet_flag = False
    if env == "TESTNET":
        key_path = "/etc/secrets/BYBIT_API_KEY_TESTNET"
        secret_path = "/etc/secrets/BYBIT_API_SECRET_TESTNET"
        testnet_flag = True
    else:
        env = "LIVE"
        key_path = "/etc/secrets/BYBIT_API_KEY_LIVE"
        secret_path = "/etc/secrets/BYBIT_API_SECRET_LIVE"
        testnet_flag = False
    if not os.path.exists(key_path):
        logger.error(f"FATAL: Key file not found for {env} at {key_path}!")
        return False
    if not os.path.exists(secret_path):
        logger.error(f"FATAL: Secret file not found for {env} at {secret_path}!")
        return False
    try:
        with open(key_path, "r") as f:
            api_key = f.read().strip()
        with open(secret_path, "r") as f:
            api_secret = f.read().strip()
        if not api_key or not api_secret:
            logger.error(f"FATAL: Key or Secret file for {env} is empty!")
            return False
        bybit_session = HTTP(
            testnet=testnet_flag, api_key=api_key, api_secret=api_secret
        )
        # Проверка соединения (опционально) - делаем простой запрос
        logger.info("Checking Bybit API connection with get_instruments_info...")
        check_conn = bybit_session.get_instruments_info(
            category=BYBIT_CATEGORY, limit=1
        )
        if check_conn.get("retCode") != 0:
            logger.error(f"Bybit API connection check failed for {env}: {check_conn}")
            bybit_session = None  # Сбрасываем сессию при ошибке
            return False
        logger.info(f"Successfully initialized Bybit API connection for {env}.")
        return True
    except Exception as e:
        logger.error(f"FATAL: Error connecting to Bybit {env} API: {e}", exc_info=True)
        return False


# === Инициализация сервисов при старте ===
if not init_google_sheets():
    logger.error("CRITICAL: Failed to initialize Google Sheets.")
if not init_bybit():
    logger.error("CRITICAL: Failed to initialize Bybit.")


# === Вспомогательные функции ===
def find_next_empty_row(sheet_instance, column_index=1):
    """Находит номер следующей пустой строки по заданному столбцу (A по умолчанию)."""
    if not sheet_instance:
        logger.error("Sheet instance is None in find_next_empty_row")
        return None
    try:
        logger.debug(f"Fetching column {column_index} values...")
        col_values = sheet_instance.col_values(
            column_index, value_render_option="UNFORMATTED_VALUE"
        )
        logger.debug(f"Found {len(col_values)} values.")
        # Ищем последнюю непустую ячейку
        last_data_row_index = len(col_values) - 1
        while (
            last_data_row_index >= 0
            and str(col_values[last_data_row_index]).strip() == ""
        ):  # >= 0 чтобы учесть пустой лист
            last_data_row_index -= 1
        target_row_number = (
            last_data_row_index + 2
        )  # Следующая строка после последней непустой
        logger.info(f"Target starting row for update is {target_row_number}.")
        return target_row_number
    except Exception as e:
        logger.error(f"Error finding next empty row: {e}", exc_info=True)
        return None


# === Обработчики команд и Кнопок ===
if bot:  # Только если бот инициализирован

    @bot.message_handler(commands=["start", "menu"])
    def handle_menu(message):
        """Показывает меню с кнопками."""
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
        btn_add_manual = types.KeyboardButton("Добавить вручную (/add)")
        btn_add_by_id = types.KeyboardButton("Добавить по ID Транз. (/fetch)")
        btn_close = types.KeyboardButton(
            "Закрыть сделку"
        )  # Убрал /close из текста кнопки
        btn_glossary = types.KeyboardButton("Глоссарий")
        btn_hide = types.KeyboardButton("Скрыть меню")
        markup.add(btn_add_manual, btn_add_by_id, btn_close, btn_glossary, btn_hide)
        bot.send_message(message.chat.id, "Выберите действие:", reply_markup=markup)
        # Справка по командам
        bot.send_message(
            message.chat.id,
            "Доступные команды:\n"
            "`/add <Пара> <Тип> <Вход> <TP> <SL> <Объем> <OrderID>`\n"
            "`/fetch <ExecID>` - добавить по ID Транзакции\n"
            "`/close <Пара> <ЦенаВыхода>`\n"
            "`/menu` - показать это меню",
            parse_mode="Markdown",
        )

    @bot.message_handler(func=lambda m: m.text == "Скрыть меню")
    def hide_menu(message):
        """Скрывает клавиатуру."""
        bot.send_message(
            message.chat.id, "Меню скрыто.", reply_markup=types.ReplyKeyboardRemove()
        )

    # --- ОБРАБОТЧИК КНОПКИ "Добавить вручную (/add)" ---
    @bot.message_handler(func=lambda message: message.text == "Добавить вручную (/add)")
    def kb_add_manual_prompt(message):
        """Напоминает формат команды /add для ручного ввода."""
        bot.reply_to(
            message,
            "Для ручного добавления сделки введите команду в формате:\n"
            "`/add <Пара> <Тип> <Вход> <TP> <SL> <Объем_монет> <OrderID>`\n"
            "*(Замените параметры на ваши значения)*",
            parse_mode="Markdown",
        )

    # /add - Обработчик самой команды
    @bot.message_handler(commands=["add"])
    def handle_add(message):
        # ... (Код функции handle_add без изменений) ...
        chat_id = message.chat.id
        logger.info(f"Received /add command from {chat_id}: {message.text}")
        if not sheet:
            logger.error("Sheet not initialized in /add")
            bot.reply_to(message, "Ошибка: Нет подключения к Google Sheets.")
            return
        try:
            parts = message.text.split()
            if len(parts) != 8:
                logger.warning(f"Invalid format for /add...")
                bot.reply_to(
                    message,
                    "Неверный формат! Нужно 8 частей.\nПример:\n`/add SOL/USDT Лонг 139.19 141.8 136.9 1.5 <Bybit_Order_ID>`",
                    parse_mode="Markdown",
                )
                return
            (
                _,
                asset,
                direction,
                entry_price_str,
                tp_str,
                sl_str,
                amount_str,
                bybit_order_id,
            ) = parts
            now = datetime.datetime.now()
            entry_date = now.strftime("%d.%m.%Y")
            entry_time = now.strftime("%H:%M:%S")
            target_row_number = find_next_empty_row(sheet)
            if not target_row_number:
                bot.reply_to(message, "Ошибка: не удалось найти пустую строку.")
                return
            updates = []
            try:
                updates.append(
                    {"range": f"A{target_row_number}", "values": [[entry_date]]}
                )
                updates.append(
                    {"range": f"B{target_row_number}", "values": [[entry_time]]}
                )
                updates.append({"range": f"E{target_row_number}", "values": [[asset]]})
                updates.append(
                    {"range": f"F{target_row_number}", "values": [[direction]]}
                )
                updates.append(
                    {
                        "range": f"G{target_row_number}",
                        "values": [[float(entry_price_str)]],
                    }
                )
                updates.append(
                    {"range": f"H{target_row_number}", "values": [[float(sl_str)]]}
                )
                updates.append(
                    {"range": f"I{target_row_number}", "values": [[float(tp_str)]]}
                )
                updates.append(
                    {"range": f"J{target_row_number}", "values": [[float(amount_str)]]}
                )
                # Записываем Order ID в AD (индекс 29)
                if "entry_order_id" in COL_IDX and COL_IDX["entry_order_id"] == 29:
                    updates.append(
                        {
                            "range": f"AD{target_row_number}",
                            "values": [[bybit_order_id]],
                        }
                    )
                else:
                    logger.warning(
                        "Column AD for Entry Order ID not found or incorrect index."
                    )
            except ValueError as e:
                logger.error(f"ValueError converting numbers in /add: {e}")
                bot.reply_to(message, f"Ошибка в формате чисел: {e}.")
                return
            logger.debug(f"Prepared batch update data for /add: {updates}")
            sheet.batch_update(updates, value_input_option="USER_ENTERED")
            logger.info(
                f"Updated cells via /add for {asset} (Order ID: {bybit_order_id}) in row {target_row_number}."
            )
            bot.reply_to(
                message,
                f"Сделка по {asset} (ID: {bybit_order_id}) ДОБАВЛЕНА ВРУЧНУЮ в строку {target_row_number}!",
            )
        except Exception as e:
            logger.error(f"Error processing /add command: {e}", exc_info=True)
            bot.reply_to(message, "Ошибка при обработке /add.")

    # /fetch - Обработчик самой команды (использует execId)
    @bot.message_handler(commands=["fetch"])
    def handle_fetch(message):
        # ... (Код функции handle_fetch без изменений, использует get_executions с execId) ...
        chat_id = message.chat.id
        logger.info(f"Received /fetch command from {chat_id}: {message.text}")
        if not sheet or not bybit_session:
            error_msg = "Ошибка: " + (
                "Нет Google Sheets." if not sheet else "Нет Bybit API."
            )
            logger.error(error_msg)
            return bot.reply_to(message, error_msg)
        try:
            parts = message.text.split()
            if len(parts) != 2:
                return bot.reply_to(
                    message,
                    "Неверный формат!\nПример:\n`/fetch <Bybit_Exec_ID>`",
                    parse_mode="Markdown",
                )
            exec_id_to_fetch = parts[1]
            logger.info(f"Fetching execution details for Exec ID: {exec_id_to_fetch}")
            response = bybit_session.get_executions(
                execId=exec_id_to_fetch, category=BYBIT_CATEGORY, limit=1
            )
            logger.debug(
                f"Raw Bybit Executions response for {exec_id_to_fetch}: {response}"
            )
            if not (response and response.get("retCode") == 0):
                logger.error(f"Error fetching execution: {response}")
                return bot.reply_to(
                    message,
                    f"Ошибка запроса транз. {exec_id_to_fetch}: {response.get('retMsg', 'Error')}",
                )
            exec_list = response.get("result", {}).get("list", [])
            if not exec_list:
                logger.warning(f"No execution found for Exec ID: {exec_id_to_fetch}")
                return bot.reply_to(
                    message, f"Не найдено исполнение (транз.) с ID {exec_id_to_fetch}."
                )
            exec_item = exec_list[0]
            try:
                asset = exec_item.get("symbol", "")
                side = "Лонг" if exec_item.get("side") == "Buy" else "Шорт"
                entry_price = float(exec_item.get("execPrice") or 0)
                total_qty = float(exec_item.get("execQty") or 0)
                exec_time_ms = int(exec_item.get("execTime", 0))
                entry_dt = (
                    datetime.datetime.fromtimestamp(exec_time_ms / 1000)
                    if exec_time_ms > 0
                    else None
                )
                entry_date_str = entry_dt.strftime("%d.%m.%Y") if entry_dt else ""
                entry_time_str = entry_dt.strftime("%H:%M:%S") if entry_dt else ""
                related_order_id = exec_item.get("orderId", "")
                fee = float(exec_item.get("execFee", 0))
                if not asset or total_qty <= 0:
                    logger.error(f"Incomplete data for {exec_id_to_fetch}")
                    return bot.reply_to(
                        message, f"Неполные данные для транз. {exec_id_to_fetch}."
                    )
            except (ValueError, TypeError, KeyError, IndexError) as e:
                logger.error(
                    f"Error parsing execution data: {e}. Data: {exec_item}",
                    exc_info=True,
                )
                return bot.reply_to(
                    message, f"Ошибка обработки данных транз. {exec_id_to_fetch}."
                )
            target_row_number = find_next_empty_row(sheet)
            if not target_row_number:
                return bot.reply_to(message, "Ошибка: не удалось найти пустую строку.")
            updates = [
                {"range": f"A{target_row_number}", "values": [[entry_date_str]]},
                {"range": f"B{target_row_number}", "values": [[entry_time_str]]},
                {"range": f"E{target_row_number}", "values": [[asset]]},
                {"range": f"F{target_row_number}", "values": [[side]]},
                {"range": f"G{target_row_number}", "values": [[entry_price]]},
                {"range": f"J{target_row_number}", "values": [[total_qty]]},
                {"range": f"Q{target_row_number}", "values": [[fee]]},  # Комиссия в Q
                {
                    "range": f"AD{target_row_number}",
                    "values": [[related_order_id]],
                },  # OrderID в AD
                {
                    "range": f"AC{target_row_number}",
                    "values": [[exec_id_to_fetch]],
                },  # ExecID в AC
            ]
            logger.debug(f"Prepared batch update data for /fetch: {updates}")
            sheet.batch_update(updates, value_input_option="USER_ENTERED")
            logger.info(
                f"Updated cells via /fetch for {asset} (Exec ID: {exec_id_to_fetch}) in row {target_row_number}."
            )
            bot.reply_to(
                message,
                f"Сделка по {asset} (Exec ID: {exec_id_to_fetch}) добавлена из Bybit в строку {target_row_number}!",
            )
        except Exception as e:
            logger.error(f"Error processing /fetch command: {e}", exc_info=True)
            bot.reply_to(message, "Ошибка при обработке /fetch.")

    # --- Обработчик для кнопки "Добавить по ID Транз. (/fetch)" ---
    @bot.message_handler(
        func=lambda message: message.text == "Добавить по ID Транз. (/fetch)"
    )
    def kb_addid(message):
        """Запрашивает ID Транзакции для вызова /fetch."""
        msg = bot.send_message(
            message.chat.id, "Введите ID ТРАНЗАКЦИИ (Exec ID) с Bybit:"
        )
        bot.register_next_step_handler(msg, fetch_wrapper_for_next_step)

    def fetch_wrapper_for_next_step(message):
        """Обертка для вызова handle_fetch из next_step_handler."""
        logger.info(f"Received Exec ID '{message.text}' via next_step_handler")
        # Создаем объект, похожий на сообщение с командой
        fake_command_message = type(
            "FakeCommandMessage",
            (object,),
            {
                "text": f"/fetch {message.text.strip()}",  # Формируем команду /fetch с ID
                "chat": message.chat,
                "from_user": message.from_user,
                "message_id": message.message_id,
            },
        )()
        # Вызываем основной обработчик команды /fetch
        handle_fetch(fake_command_message)

    # --- Обработчик для кнопки "Закрыть сделку" ---
    @bot.message_handler(
        func=lambda message: message.text == "Закрыть сделку"
    )  # Убрал /close из текста
    def kb_close_trade_prompt(message):
        """Спрашивает пару и цену для закрытия."""
        msg = bot.send_message(
            message.chat.id,
            "Введите пару и фактическую цену выхода через пробел (например: `SOL/USDT 145.88`):",
            parse_mode="Markdown",
        )
        bot.register_next_step_handler(msg, process_close_trade_input)

    def process_close_trade_input(message):
        """Обрабатывает ответ пользователя с парой и ценой, закрывает сделку."""
        # ... (Код функции process_close_trade_input без изменений) ...
        chat_id = message.chat.id
        logger.info(f"Received close trade input from {chat_id}: {message.text}")
        if not sheet:
            logger.error("Sheet not initialized...")
            return bot.send_message(chat_id, "Ошибка: Нет Google Sheets.")
        try:
            parts = message.text.split()
            if len(parts) != 2:
                logger.warning(f"Invalid format for close input: {parts}")
                msg = bot.send_message(
                    chat_id, "Неверный формат. Нужно ПАРУ и ЦЕНУ. Попробуйте еще раз:"
                )
                bot.register_next_step_handler(msg, process_close_trade_input)
                return
            asset_to_close = parts[0].upper()
            exit_price_str = parts[1].replace(",", ".")
            exit_price = float(exit_price_str)
            now = datetime.datetime.now()
            exit_date = now.strftime("%d.%m.%Y")
            exit_time = now.strftime("%H:%M:%S")
            exit_method = "вручную (кнопка)"
            list_of_lists = sheet.get_all_values()
            logger.info(f"Fetched {len(list_of_lists)} rows for button close.")
            header_row = list_of_lists[0] if list_of_lists else []
            asset_col_name = "Торгуемая пара (актив)"
            actual_exit_price_col_name = "Фактическая цена выхода ($)"
            try:
                asset_col_index = header_row.index(asset_col_name)
                actual_exit_price_col_index = header_row.index(
                    actual_exit_price_col_name
                )
            except (ValueError, IndexError) as e:
                logger.error(f"Header error in button close: {e}")
                return bot.send_message(chat_id, "Крит. ошибка: Не найдены столбцы.")
            found = False
            for i in range(len(list_of_lists) - 1, 0, -1):
                row = list_of_lists[i]
                current_row_number = i + 1
                if len(row) > max(asset_col_index, actual_exit_price_col_index):
                    asset_in_row = row[asset_col_index].upper()
                    exit_price_in_row = row[actual_exit_price_col_index]
                    if asset_in_row == asset_to_close and (
                        exit_price_in_row == "" or exit_price_in_row is None
                    ):
                        logger.info(
                            f"Found open trade for {asset_to_close} at row {current_row_number}. Closing via button..."
                        )
                        updates = [
                            {
                                "range": f"C{current_row_number}",
                                "values": [[exit_date]],
                            },
                            {
                                "range": f"D{current_row_number}",
                                "values": [[exit_time]],
                            },
                            {
                                "range": f"S{current_row_number}",
                                "values": [[exit_method]],
                            },
                            {
                                "range": f"T{current_row_number}",
                                "values": [[exit_price]],
                            },
                        ]
                        sheet.batch_update(updates, value_input_option="USER_ENTERED")
                        logger.info(
                            f"Updated row {current_row_number} for button closed trade {asset_to_close}."
                        )
                        bot.send_message(
                            chat_id,
                            f"Сделка по {asset_to_close} закрыта вручную по {exit_price}.",
                        )
                        found = True
                        break
            if not found:
                logger.info(
                    f"No open trade found for {asset_to_close} via button close."
                )
                bot.send_message(
                    chat_id, f"Не найдена ОТКРЫТАЯ сделка по {asset_to_close}."
                )
        except ValueError:
            logger.error(
                f"ValueError converting close price: {exit_price_str}", exc_info=True
            )
            msg = bot.send_message(chat_id, "Ошибка формата цены. Попробуйте еще раз:")
            bot.register_next_step_handler(msg, process_close_trade_input)
        except Exception as e:
            logger.error(f"Error processing close trade input: {e}", exc_info=True)
            bot.send_message(chat_id, "Ошибка при закрытии сделки.")

    # /close - Обработчик самой команды (оставляем для прямого ввода)
    @bot.message_handler(commands=["close"])
    def handle_close(message):
        # ... (Код функции handle_close без изменений) ...
        chat_id = message.chat.id
        logger.info(f"Received /close command from {chat_id}: {message.text}")
        if not sheet:
            logger.error("Sheet not initialized in /close")
            bot.reply_to(message, "Ошибка: Нет подключения к Google Sheets.")
            return
        try:
            parts = message.text.split()
            if len(parts) != 3:
                logger.warning(f"Invalid format for /close...")
                bot.reply_to(
                    message,
                    "Неверный формат. Пример:\n`/close SOL/USDT 140.55`",
                    parse_mode="Markdown",
                )
                return
            _, asset_to_close, exit_price_str = parts
            exit_price = float(exit_price_str)
            now = datetime.datetime.now()
            exit_date = now.strftime("%d.%m.%Y")
            exit_time = now.strftime("%H:%M:%S")
            exit_method = "вручную"
            list_of_lists = sheet.get_all_values()
            logger.info(f"Fetched {len(list_of_lists)} rows for /close.")
            header_row = list_of_lists[0] if list_of_lists else []
            logger.debug(f"Header row: {header_row}")
            asset_col_name = "Торгуемая пара (актив)"
            actual_exit_price_col_name = "Фактическая цена выхода ($)"  # T
            exit_date_col_letter = "C"
            exit_time_col_letter = "D"
            exit_method_col_letter = "S"
            actual_exit_price_col_letter = "T"
            try:
                asset_col_index = header_row.index(asset_col_name)
                actual_exit_price_col_index = header_row.index(
                    actual_exit_price_col_name
                )
            except ValueError as e:
                logger.error(f"Column name mismatch in /close: '{e}'")
                bot.reply_to(message, f"Критическая ошибка: Не найден столбец '{e}'.")
                return
            except IndexError:
                logger.error(f"Header row not found or empty in /close.")
                bot.reply_to(message, f"Критическая ошибка: Не найден заголовок.")
                return
            found = False
            for i in range(len(list_of_lists) - 1, 0, -1):
                row = list_of_lists[i]
                current_row_number = i + 1
                if len(row) > max(asset_col_index, actual_exit_price_col_index):
                    asset_in_row = row[asset_col_index].upper()
                    exit_price_in_row = row[
                        actual_exit_price_col_index
                    ]  # Сравниваем без upper()
                    if asset_in_row == asset_to_close.upper() and (
                        exit_price_in_row == "" or exit_price_in_row is None
                    ):  # Сравниваем пару case-insensitive
                        logger.info(
                            f"Found open trade for {asset_to_close} at row {current_row_number}. Closing manually..."
                        )
                        updates = [
                            {
                                "range": f"{exit_date_col_letter}{current_row_number}",
                                "values": [[exit_date]],
                            },
                            {
                                "range": f"{exit_time_col_letter}{current_row_number}",
                                "values": [[exit_time]],
                            },
                            {
                                "range": f"{exit_method_col_letter}{current_row_number}",
                                "values": [[exit_method]],
                            },
                            {
                                "range": f"{actual_exit_price_col_letter}{current_row_number}",
                                "values": [[exit_price]],
                            },
                        ]
                        sheet.batch_update(updates, value_input_option="USER_ENTERED")
                        logger.info(
                            f"Updated row {current_row_number} for manually closed trade {asset_to_close}."
                        )
                        bot.reply_to(
                            message,
                            f"Сделка по {asset_to_close} закрыта вручную по {exit_price}.",
                        )
                        found = True
                        break
            if not found:
                logger.info(
                    f"No open trade found for {asset_to_close} to close manually."
                )
                bot.reply_to(
                    message, f"Не найдена ОТКРЫТАЯ сделка по {asset_to_close}."
                )
        except ValueError as e:
            logger.error(f"ValueError processing /close: {e}")
            bot.reply_to(message, f"Ошибка в формате цены выхода: {e}.")
        except Exception as e:
            logger.error(f"Error processing /close command: {e}", exc_info=True)
            bot.reply_to(message, "Ошибка при закрытии сделки.")

    # --- НОВЫЕ ОБРАБОТЧИКИ ДЛЯ ГЛОССАРИЯ ---
    @bot.message_handler(func=lambda message: message.text == "Глоссарий")
    def kb_glossary_start(message):
        """Запрашивает термин для поиска в глоссарии."""
        chat_id = message.chat.id
        if not glossary_sheet:
            return bot.send_message(chat_id, "Ошибка: Лист 'Глоссарий' не подключен.")
        msg = bot.send_message(chat_id, "Какой термин вы ищете?")
        bot.register_next_step_handler(msg, process_glossary_search)

    def process_glossary_search(message):
        """Ищет термин в глоссарии и отвечает."""
        chat_id = message.chat.id
        term_to_search = message.text.strip()
        logger.info(f"User {chat_id} searching for term: {term_to_search}")
        if not glossary_sheet:
            return bot.send_message(chat_id, "Ошибка: Лист 'Глоссарий' не подключен.")
        if not term_to_search:
            return bot.send_message(chat_id, "Вы не ввели термин для поиска.")
        try:
            cell = glossary_sheet.find(
                term_to_search, in_column=1, case_sensitive=False
            )
            if cell:
                definition = glossary_sheet.cell(cell.row, 2).value
                logger.info(f"Found term '{term_to_search}' at row {cell.row}.")
                response = f"*{term_to_search.capitalize()}*\n\n{definition or 'Определение не найдено.'}"  # Используем Markdown
                bot.send_message(chat_id, response, parse_mode="Markdown")
            else:
                logger.info(f"Term '{term_to_search}' not found.")
                response = f"Термин '{term_to_search}' не найден. Хотите добавить определение?\n\nЕсли да, просто напишите определение. Если нет, отправьте 'нет' или /cancel."
                msg = bot.send_message(chat_id, response)
                user_states[chat_id] = {
                    "action": "add_definition",
                    "term": term_to_search,
                }
                bot.register_next_step_handler(msg, process_glossary_add_definition)
        except (
            gspread.exceptions.CellNotFound
        ):  # На случай если find вернет None или ошибку
            logger.info(f"Term '{term_to_search}' not found (CellNotFound exception).")
            response = f"Термин '{term_to_search}' не найден. Хотите добавить определение?\n\nЕсли да, просто напишите определение. Если нет, отправьте 'нет' или /cancel."
            msg = bot.send_message(chat_id, response)
            user_states[chat_id] = {"action": "add_definition", "term": term_to_search}
            bot.register_next_step_handler(msg, process_glossary_add_definition)
        except Exception as e:
            logger.error(
                f"Error searching glossary for '{term_to_search}': {e}", exc_info=True
            )
            bot.send_message(chat_id, "Ошибка при поиске в глоссарии.")

    def process_glossary_add_definition(message):
        """Обрабатывает ответ пользователя с определением и добавляет его."""
        chat_id = message.chat.id
        user_input = message.text.strip()
        state = user_states.get(chat_id)
        if chat_id in user_states:
            del user_states[chat_id]  # Убираем состояние
        if not state or state.get("action") != "add_definition":
            return
        term_to_add = state.get("term")
        if user_input.lower() in ["нет", "no", "/cancel", "отмена"]:
            logger.info(f"User cancelled add definition for '{term_to_add}'.")
            return bot.send_message(chat_id, "Определение не добавлено.")
        if not term_to_add:
            logger.error("Term to add was lost from state.")
            return bot.send_message(chat_id, "Ошибка, термин потерян.")
        new_definition = user_input
        logger.info(
            f"User {chat_id} adding definition for '{term_to_add}': '{new_definition}'"
        )
        if not glossary_sheet:
            return bot.send_message(chat_id, "Ошибка: Лист 'Глоссарий' не подключен.")
        try:
            glossary_sheet.append_row(
                [term_to_add, new_definition], value_input_option="USER_ENTERED"
            )
            logger.info(f"Successfully added '{term_to_add}' to glossary.")
            bot.send_message(
                chat_id, f"Термин '{term_to_add}' и его определение успешно добавлены!"
            )
        except Exception as e:
            logger.error(f"Error appending to glossary sheet: {e}", exc_info=True)
            bot.send_message(chat_id, "Ошибка при добавлении.")

    # --- КОНЕЦ ОБРАБОТЧИКОВ ГЛОССАРИЯ ---

else:  # Если bot is None
    logger.error(
        "CRITICAL: Bot object is None, Telegram command handlers cannot be registered!"
    )

# === Webhook-роут ===
if app and TOKEN:

    @app.route(f"/{TOKEN}", methods=["POST"])
    def webhook():
        # ... (Код функции webhook без изменений) ...
        logger.info("Webhook received!")
        if not bot:
            logger.error("Webhook received but bot is not initialized!")
            return "error", 500
        try:
            json_str = request.get_data().decode("UTF-8")
            update = telebot.types.Update.de_json(json_str)
            bot.process_new_updates([update])
        except Exception as e:
            logger.error(f"Error in webhook processing: {e}", exc_info=True)
        return "ok", 200

else:
    logger.error(
        "CRITICAL: Flask app or TOKEN not defined, webhook route cannot be registered!"
    )

# === Запуск Flask-сервера ===
if __name__ == "__main__":
    # ... (Код блока __main__ без изменений) ...
    logger.info(
        "Attempting to run Flask development server (should only happen locally)"
    )
    if bot and app:
        port = int(os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        logger.error(
            "Could not start Flask dev server: Bot or Flask app not initialized."
        )
