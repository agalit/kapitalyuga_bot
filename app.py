# --- Файл: app.py (Финальная версия) ---

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
    logger.error("FATAL: TELEGRAM_BOT_TOKEN environment variable is not set!")
    # В реальном приложении здесь лучше остановить выполнение
    # exit()

# --- Google Sheets ---
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Таблица сделок")
CREDENTIALS_PATH = "/etc/secrets/credentials.json"  # Путь к секретному файлу Render

# --- Структура Таблицы (A-AD, 30 столбцов) ---
# Индексы столбцов (начиная с 0)
COL_IDX = {
    "entry_date": 0,  # A: Дата ВХОДА
    "entry_time": 1,  # B: Время ВХОДА
    "exit_date": 2,  # C: Дата ВЫХОДА
    "exit_time": 3,  # D: Время ВЫХОДА
    "pair": 4,  # E: Торгуемая пара
    "type": 5,  # F: Тип сделки
    "entry_price": 6,  # G: Цена входа
    "sl_price": 7,  # H: Уровень Stop Loss
    "tp_price": 8,  # I: Уровень Take Profit
    "volume_coins": 9,  # J: Объём сделки (в монетах)
    "volume_usd": 10,  # K: Объём сделки ($) - Формула в таблице
    "plan_long_usd": 11,  # L: План Лонг $ - Формула
    "plan_long_pct": 12,  # M: План Лонг % - Формула
    "plan_short_usd": 13,  # N: План Шорт $ - Формула
    "plan_short_pct": 14,  # O: План Шорт % - Формула
    "rrr": 15,  # P: RRR - Формула
    "commission_entry": 16,  # Q: Комиссия входа
    "commission_exit": 17,  # R: Комиссия выхода
    "exit_method": 18,  # S: Способ выхода
    "exit_price_actual": 19,  # T: Факт. цена выхода
    "pnl_actual_usd": 20,  # U: Факт. PnL $ - Формула
    "pnl_net_usd": 21,  # V: Чистый PnL $ - Формула
    "duration": 22,  # W: Время жизни сделки - Формула
    "weekday": 23,  # X: День недели входа - Формула
    "worked": 24,  # Y: Что сработало
    "not_worked": 25,  # Z: Что пошло не так
    "entry_reason": 26,  # AA: Причина входа
    "conclusions": 27,  # AB: Выводы по сделке
    "screenshot": 28,  # AC: Скрин сделки
    "entry_order_id": 29,  # AD: Entry Order ID (Bybit ID)
}
EXPECTED_COLUMNS = 30  # A-AD

# --- Bybit ---
BYBIT_ENV = os.getenv("BYBIT_ENV", "LIVE").upper()  # LIVE или TESTNET
BYBIT_CATEGORY = os.getenv("BYBIT_CATEGORY", "linear")  # 'linear' или 'inverse'

# === Глобальные переменные для клиентов ===
bot = None
sheet = None
bybit_session = None
google_creds = None
google_client = None
app = None  # Определяем заранее

# === Инициализация ===
# --- ВАЖНО: Инициализация Flask ПЕРЕД использованием app и bot ---
app = Flask(__name__)
# --------------------------------------------------------------

# Теперь инициализируем бота (если TOKEN есть)
if TOKEN:
    try:
        bot = telebot.TeleBot(TOKEN, threaded=False)
        logger.info("Telegram bot initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize Telegram bot: {e}", exc_info=True)
        bot = None  # Оставляем None, если ошибка
else:
    bot = None


# === Функции Инициализации Сервисов ===
def init_google_sheets():
    """Инициализирует подключение к Google Sheets."""
    global sheet, google_creds, google_client
    # ... (Код функции init_google_sheets без изменений, как в твоем последнем полном коде) ...
    logger.info("Attempting to connect to Google Sheets...")
    if not SPREADSHEET_ID:
        logger.error("FATAL: SPREADSHEET_ID not set!")
        return False
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
        sheet = spreadsheet.worksheet(SHEET_NAME)
        logger.info(
            f"Successfully connected to Google Sheet: '{spreadsheet.title}', Worksheet: '{sheet.title}'"
        )
        actual_col_count = sheet.col_count
        if actual_col_count < EXPECTED_COLUMNS:
            logger.warning(
                f"WARNING: Sheet '{SHEET_NAME}' has {actual_col_count} columns, expected {EXPECTED_COLUMNS} (A-AD)."
            )
        return True
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 401:
            logger.warning("Google API Error 401. Attempting to refresh credentials.")
            try:
                google_client.login()
                spreadsheet = google_client.open_by_key(SPREADSHEET_ID)
                sheet = spreadsheet.worksheet(SHEET_NAME)
                logger.info("Successfully reconnected to Google Sheets after refresh.")
                return True
            except Exception as refresh_e:
                logger.error(
                    f"FATAL: Failed to refresh Google credentials: {refresh_e}",
                    exc_info=True,
                )
                return False
        else:
            logger.error(f"FATAL: Google API Error: {e}", exc_info=True)
            return False
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"FATAL: Worksheet '{SHEET_NAME}' not found!")
        return False
    except Exception as e:
        logger.error(f"FATAL: Error connecting to Google Sheets: {e}", exc_info=True)
        return False


def init_bybit():
    """Инициализирует подключение к Bybit, читая ключи из Secret Files."""
    global bybit_session
    # ... (Код функции init_bybit без изменений, как в моем последнем полном коде) ...
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
        logger.info(f"Successfully initialized Bybit API connection for {env}.")
        return True
    except Exception as e:
        logger.error(f"FATAL: Error connecting to Bybit {env} API: {e}", exc_info=True)
        return False


# === Инициализация сервисов при старте ===
if not init_google_sheets():
    logger.error("CRITICAL: Failed to initialize Google Sheets connection.")
if not init_bybit():
    logger.error("CRITICAL: Failed to initialize Bybit connection.")


# === Вспомогательные функции ===
def find_next_empty_row(sheet_instance, column_index=1):
    """Находит номер следующей пустой строки по заданному столбцу (A по умолчанию)."""
    # ... (Код функции find_next_empty_row без изменений) ...
    if not sheet_instance:
        return None  # Добавим проверку
    try:
        logger.debug(f"Fetching column {column_index} values to find next empty row...")
        col_values = sheet_instance.col_values(
            column_index, value_render_option="UNFORMATTED_VALUE"
        )
        logger.debug(f"Found {len(col_values)} values in column {column_index}.")
        last_data_row_index = len(col_values) - 1
        while (
            last_data_row_index > 0
            and str(col_values[last_data_row_index]).strip() == ""
        ):
            last_data_row_index -= 1
        target_row_number = last_data_row_index + 2
        logger.info(f"Target starting row for update is {target_row_number}.")
        return target_row_number
    except Exception as e:
        logger.error(f"Error finding next empty row: {e}", exc_info=True)
        return None


# === Обработчики команд Telegram ===
if bot:  # Регистрируем хендлеры только если бот создался

    @bot.message_handler(commands=["start"])
    def handle_start(message):
        # ... (Код функции handle_start без изменений) ...
        chat_id = message.chat.id
        logger.info(f"Received /start command from chat ID: {chat_id}")
        try:
            bot.send_message(
                chat_id,
                "Привет! Я — бот помощник трейдера.\nИспользуй:\n`/add <Пара> <Тип> <Вход> <TP> <SL> <Объем_монет> <OrderID>`\n`/fetch <OrderID>`\n`/close <Пара> <Цена_выхода>`",
                parse_mode="Markdown",
            )
            logger.info(f"Sent start message reply to chat ID: {chat_id}")
        except Exception as e:
            logger.error(
                f"Error sending start message to {chat_id}: {e}", exc_info=True
            )

    # ОБНОВЛЕННЫЙ /add (использует batch_update)
    @bot.message_handler(commands=["add"])
    def handle_add(message):
        # ... (Код функции handle_add без изменений, как в моем последнем полном коде) ...
        # ... (Проверяет sheet, парсит 8 аргументов, находит target_row_number, готовит updates для A, B, E, F, G, H, I, J, AD, вызывает sheet.batch_update) ...
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
                    "Неверный формат!\nПример:\n`/add SOL/USDT Лонг 139.19 141.8 136.9 1.5 <Bybit_Order_ID>`",
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
                updates.append(
                    {"range": f"AD{target_row_number}", "values": [[bybit_order_id]]}
                )  # AD = 30й столбец, индекс 29
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
                f"Сделка по {asset} (ID: {bybit_order_id}) добавлена в строку {target_row_number}!",
            )
        except Exception as e:
            logger.error(f"Error processing /add command: {e}", exc_info=True)
            bot.reply_to(message, "Ошибка при обработке /add.")

    # НОВЫЙ обработчик /fetch
    @bot.message_handler(commands=["fetch"])
    def handle_fetch(message):
        # ... (Код функции handle_fetch без изменений, как в моем последнем полном коде) ...
        # ... (Проверяет sheet, bybit_session, парсит OrderID, вызывает bybit_session.get_executions, парсит ответ, готовит updates для A,B,E,F,G,H?,I?,J,Q,AD, вызывает sheet.batch_update) ...
        chat_id = message.chat.id
        logger.info(f"Received /fetch command from {chat_id}: {message.text}")
        if not sheet:
            logger.error("Sheet not initialized in /fetch")
            bot.reply_to(message, "Ошибка: Нет подключения к Google Sheets.")
            return
        if not bybit_session:
            logger.error("Bybit session not initialized in /fetch")
            bot.reply_to(message, "Ошибка: Нет подключения к Bybit API.")
            return
        try:
            parts = message.text.split()
            if len(parts) != 2:
                bot.reply_to(
                    message,
                    "Неверный формат!\nПример:\n`/fetch <Bybit_Order_ID>`",
                    parse_mode="Markdown",
                )
                return
            order_id_to_fetch = parts[1]
            logger.info(f"Fetching executions for Order ID: {order_id_to_fetch}")
            response = bybit_session.get_executions(
                orderId=order_id_to_fetch, category=BYBIT_CATEGORY, limit=10
            )
            logger.debug(
                f"Raw Bybit Executions response for {order_id_to_fetch}: {response}"
            )
            if not (response and response.get("retCode") == 0):
                logger.error(f"Error fetching executions from Bybit: {response}")
                bot.reply_to(
                    message,
                    f"Ошибка запроса ордера {order_id_to_fetch}: {response.get('retMsg', 'Error')}",
                )
                return
            exec_list = response.get("result", {}).get("list", [])
            if not exec_list:
                logger.warning(f"No executions found for Order ID: {order_id_to_fetch}")
                bot.reply_to(
                    message, f"Не найдено исполнений для ордера {order_id_to_fetch}."
                )
                return

            first_exec = exec_list[0]
            total_qty = 0
            total_fee = 0
            avg_exec_price = 0
            total_value = 0
            entry_dt = None
            for exec_item in exec_list:
                exec_qty = float(exec_item.get("execQty", 0))
                exec_price = float(exec_item.get("execPrice", 0))
                exec_fee = float(exec_item.get("execFee", 0))
                total_qty += exec_qty
                total_fee += exec_fee
                total_value += exec_qty * exec_price
                if not entry_dt:
                    exec_time_ms = int(exec_item.get("execTime", 0))
                if exec_time_ms > 0:
                    entry_dt = datetime.datetime.fromtimestamp(exec_time_ms / 1000)
            if total_qty > 0:
                avg_exec_price = total_value / total_qty
            else:
                avg_exec_price = float(first_exec.get("orderPrice", 0))
                total_qty = float(first_exec.get("orderQty", 0))
            asset = first_exec.get("symbol", "")
            side = first_exec.get("side", "").capitalize()
            direction = (
                "Лонг" if side == "Buy" else ("Шорт" if side == "Sell" else side)
            )
            entry_date_str = entry_dt.strftime("%d.%m.%Y") if entry_dt else ""
            entry_time_str = entry_dt.strftime("%H:%M:%S") if entry_dt else ""
            tp_price_str = first_exec.get("takeProfit", "")
            sl_price_str = first_exec.get("stopLoss", "")

            target_row_number = find_next_empty_row(sheet)
            if not target_row_number:
                bot.reply_to(message, "Ошибка: не удалось найти пустую строку.")
                return
            updates = []
            try:
                updates.append(
                    {"range": f"A{target_row_number}", "values": [[entry_date_str]]}
                )
                updates.append(
                    {"range": f"B{target_row_number}", "values": [[entry_time_str]]}
                )
                updates.append({"range": f"E{target_row_number}", "values": [[asset]]})
                updates.append(
                    {"range": f"F{target_row_number}", "values": [[direction]]}
                )
                updates.append(
                    {"range": f"G{target_row_number}", "values": [[avg_exec_price]]}
                )
                updates.append(
                    {
                        "range": f"H{target_row_number}",
                        "values": [[float(sl_price_str) if sl_price_str else ""]],
                    }
                )
                updates.append(
                    {
                        "range": f"I{target_row_number}",
                        "values": [[float(tp_price_str) if tp_price_str else ""]],
                    }
                )
                updates.append(
                    {"range": f"J{target_row_number}", "values": [[total_qty]]}
                )
                updates.append(
                    {"range": f"Q{target_row_number}", "values": [[total_fee]]}
                )
                updates.append(
                    {"range": f"AD{target_row_number}", "values": [[order_id_to_fetch]]}
                )
            except ValueError as e:
                logger.error(f"ValueError converting fetched data: {e}")
                bot.reply_to(message, f"Ошибка обработки данных Bybit: {e}")
                return
            logger.debug(f"Prepared batch update data for /fetch: {updates}")
            sheet.batch_update(updates, value_input_option="USER_ENTERED")
            logger.info(
                f"Updated cells via /fetch for {asset} (Order ID: {order_id_to_fetch}) in row {target_row_number}."
            )
            bot.reply_to(
                message,
                f"Сделка по {asset} (ID: {order_id_to_fetch}) успешно добавлена из Bybit в строку {target_row_number}!",
            )
        except Exception as e:
            logger.error(f"Error processing /fetch command: {e}", exc_info=True)
            bot.reply_to(message, "Ошибка при обработке /fetch.")

    # ОБНОВЛЕННЫЙ /close
    @bot.message_handler(commands=["close"])
    def handle_close(message):
        # ... (Код функции handle_close без изменений, как в моем последнем полном коде) ...
        # ... (Проверяет sheet, парсит 3 аргумента, находит строку по E и пустому T, готовит updates для C, D, S, T, вызывает sheet.batch_update) ...
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
            actual_exit_price_col_name = "Фактическая цена выхода ($)"
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
                    asset_in_row = row[asset_col_index]
                    exit_price_in_row = row[actual_exit_price_col_index]
                    if asset_in_row == asset_to_close and (
                        exit_price_in_row == "" or exit_price_in_row is None
                    ):
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

else:  # Если bot is None
    logger.error(
        "CRITICAL: Bot object is None, Telegram command handlers cannot be registered!"
    )

# === Webhook-роут ===
if app and TOKEN:  # Регистрируем роут только если Flask app и TOKEN существуют

    @app.route(f"/{TOKEN}", methods=["POST"])
    def webhook():
        # ... (Код функции webhook без изменений, как в моем последнем полном коде) ...
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

# === Запуск Flask-сервера (Используется Gunicorn'ом на Render) ===
# Этот блок if __name__ ... на Render не выполняется
if __name__ == "__main__":
    logger.info(
        "Attempting to run Flask development server (should only happen locally)"
    )
    # Запускаем только если и бот, и Flask app успешно инициализированы
    if bot and app:
        port = int(os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        logger.error(
            "Could not start Flask dev server: Bot or Flask app not initialized."
        )
