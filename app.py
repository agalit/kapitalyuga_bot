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
# ЗАГРУЖАЕМ ТОКЕН БОТА ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ RENDER!
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    logger.error("FATAL: TELEGRAM_BOT_TOKEN environment variable is not set!")
    # В реальном приложении здесь лучше остановить выполнение
    # exit()

# --- Google Sheets ---
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Таблица сделок")
# Путь к секретному файлу Render для Google Credentials
CREDENTIALS_PATH = "/etc/secrets/credentials.json"
# Индексы столбцов (начиная с 0) для структуры A-AC (29 столбцов)
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
    "bybit_id": 29,  # Используем столбец AD (индекс 29) для ID ордера Bybit
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


# === Функции Инициализации ===
def init_telegram_bot():
    global bot
    if not TOKEN:
        return False
    try:
        bot = telebot.TeleBot(TOKEN, threaded=False)
        logger.info("Telegram bot initialized.")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize Telegram bot: {e}", exc_info=True)
        return False


def init_google_sheets():
    """Инициализирует подключение к Google Sheets."""
    global sheet, google_creds, google_client
    logger.info("Attempting to connect to Google Sheets...")
    if not SPREADSHEET_ID:
        logger.error("FATAL: SPREADSHEET_ID environment variable is not set!")
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
        # Проверка столбцов
        actual_col_count = sheet.col_count
        if actual_col_count < EXPECTED_COLUMNS:
            logger.warning(
                f"WARNING: Sheet '{SHEET_NAME}' has {actual_col_count} columns, expected {EXPECTED_COLUMNS} (A-AD)."
            )
        return True
    except gspread.exceptions.APIError as e:
        # Проверка на случай истечения срока действия токена OAuth
        if e.response.status_code == 401:
            logger.warning(
                "Google API Error 401 (Unauthorized). Attempting to refresh credentials."
            )
            try:
                google_client.login()  # Попытка обновить токен
                spreadsheet = google_client.open_by_key(
                    SPREADSHEET_ID
                )  # Повторная попытка
                sheet = spreadsheet.worksheet(SHEET_NAME)
                logger.info(
                    "Successfully reconnected to Google Sheets after refreshing credentials."
                )
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
        logger.error(f"FATAL: Worksheet '{SHEET_NAME}' not found in the Google Sheet!")
        return False
    except Exception as e:
        logger.error(f"FATAL: Error connecting to Google Sheets: {e}", exc_info=True)
        return False


def init_bybit():
    """Инициализирует подключение к Bybit."""
    global bybit_session
    logger.info(f"Attempting to connect to Bybit {BYBIT_ENV} environment...")
    api_key = None
    api_secret = None
    testnet_flag = False

    if BYBIT_ENV == "TESTNET":
        key_path = "/etc/secrets/BYBIT_API_KEY_TESTNET"
        secret_path = "/etc/secrets/BYBIT_API_SECRET_TESTNET"
        testnet_flag = True
    else:  # LIVE
        key_path = "/etc/secrets/BYBIT_API_KEY_LIVE"
        secret_path = "/etc/secrets/BYBIT_API_SECRET_LIVE"
        testnet_flag = False

    if not os.path.exists(key_path):
        logger.error(
            f"FATAL: Bybit API Key file not found for {BYBIT_ENV} at {key_path}!"
        )
        return False
    if not os.path.exists(secret_path):
        logger.error(
            f"FATAL: Bybit API Secret file not found for {BYBIT_ENV} at {secret_path}!"
        )
        return False

    try:
        with open(key_path, "r") as f:
            api_key = f.read().strip()
        with open(secret_path, "r") as f:
            api_secret = f.read().strip()
        if not api_key or not api_secret:
            logger.error(
                f"FATAL: Bybit API Key or Secret file for {BYBIT_ENV} is empty!"
            )
            return False

        bybit_session = HTTP(
            testnet=testnet_flag, api_key=api_key, api_secret=api_secret
        )
        # Проверка соединения (опционально, может требовать прав на баланс)
        # balance_check = bybit_session.get_wallet_balance(accountType="UNIFIED")
        # if balance_check.get('retCode') != 0:
        #     logger.error(f"Bybit API connection check failed: {balance_check}")
        #     return False
        logger.info(f"Successfully initialized Bybit API connection for {BYBIT_ENV}.")
        return True
    except Exception as e:
        logger.error(
            f"FATAL: Error connecting to Bybit {BYBIT_ENV} API: {e}", exc_info=True
        )
        return False


# === Инициализация при старте ===
# Убрали @app.before_first_request, инициализируем напрямую
if not init_telegram_bot():
    logger.error("CRITICAL: Failed to initialize Telegram Bot. Exiting.")
    # В реальном приложении здесь может быть более сложная логика или выход
    # exit()
if not init_google_sheets():
    logger.error("CRITICAL: Failed to initialize Google Sheets connection.")
if not init_bybit():
    logger.error("CRITICAL: Failed to initialize Bybit connection.")


# === Вспомогательные функции ===
def find_next_empty_row(sheet_instance, column_index=1):
    """Находит номер следующей пустой строки по заданному столбцу."""
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
        return None  # Возвращаем None в случае ошибки


# === Обработчики команд ===


@bot.message_handler(commands=["start"])
def handle_start(message):
    chat_id = message.chat.id
    logger.info(f"Received /start command from chat ID: {chat_id}")
    if not bot:
        logger.error("Bot not initialized in /start")
        return
    try:
        bot.send_message(
            chat_id,
            "Привет! Я — бот помощник трейдера.\nИспользуй:\n`/add <Пара> <Тип> <Вход> <TP> <SL> <Объем_монет> <OrderID>` для ручного добавления ОТКРЫТОЙ сделки.\n`/fetch <OrderID>` для авто-добавления ОТКРЫТОЙ сделки по ID из Bybit.\n`/close <Пара> <Цена_выхода>` для ручного ЗАКРЫТИЯ сделки.",
            parse_mode="Markdown",
        )
        logger.info(f"Sent start message reply to chat ID: {chat_id}")
    except Exception as e:
        logger.error(f"Error sending start message to {chat_id}: {e}", exc_info=True)


# ОБНОВЛЕННЫЙ /add
@bot.message_handler(commands=["add"])
def handle_add(message):
    chat_id = message.chat.id
    logger.info(f"Received /add command from {chat_id}: {message.text}")

    if not bot:
        return
    if not sheet:
        logger.error("Sheet not initialized in /add")
        bot.reply_to(message, "Ошибка: Нет подключения к Google Sheets.")
        return

    try:
        parts = message.text.split()
        # Ожидаем 8 частей: /add <Pair> <Type> <Entry> <TP> <SL> <Volume> <OrderID>
        if len(parts) != 8:
            logger.warning(
                f"Invalid format for /add from {chat_id}. Parts: {len(parts)}"
            )
            bot.reply_to(
                message,
                "Неверный формат!\nПример:\n`/add SOL/USDT Лонг 139.19 141.8 136.9 1.5 <Bybit_Order_ID>`",
                parse_mode="Markdown",
            )
            return

        # Извлекаем данные
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

        # Находим следующую строку
        target_row_number = find_next_empty_row(sheet)
        if not target_row_number:
            bot.reply_to(message, "Ошибка: не удалось найти пустую строку в таблице.")
            return

        # Готовим данные для обновления ячеек A, B, E, F, G, H, I, J, AD
        updates = []
        try:
            updates.append(
                {"range": f"A{target_row_number}", "values": [[entry_date]]}
            )  # Дата ВХОДА
            updates.append(
                {"range": f"B{target_row_number}", "values": [[entry_time]]}
            )  # Время ВХОДА
            updates.append(
                {"range": f"E{target_row_number}", "values": [[asset]]}
            )  # Торгуемая пара
            updates.append(
                {"range": f"F{target_row_number}", "values": [[direction]]}
            )  # Тип сделки
            updates.append(
                {"range": f"G{target_row_number}", "values": [[float(entry_price_str)]]}
            )  # Цена входа
            updates.append(
                {"range": f"H{target_row_number}", "values": [[float(sl_str)]]}
            )  # Уровень Stop Loss
            updates.append(
                {"range": f"I{target_row_number}", "values": [[float(tp_str)]]}
            )  # Уровень Take Profit
            updates.append(
                {"range": f"J{target_row_number}", "values": [[float(amount_str)]]}
            )  # Объём сделки (в монетах)
            updates.append(
                {"range": f"AD{target_row_number}", "values": [[bybit_order_id]]}
            )  # Entry Order ID (Новый столбец AD)
        except ValueError as e:
            logger.error(f"ValueError converting numbers in /add: {e}", exc_info=True)
            bot.reply_to(
                message,
                f"Ошибка в формате чисел: {e}. Проверьте цены, стоп, тейк и объем.",
            )
            return

        logger.debug(f"Prepared batch update data for /add: {updates}")
        sheet.batch_update(updates, value_input_option="USER_ENTERED")
        logger.info(
            f"Updated cells in row {target_row_number} for {asset} (Order ID: {bybit_order_id}) via /add."
        )
        bot.reply_to(
            message,
            f"Сделка по {asset} (ID: {bybit_order_id}) добавлена в строку {target_row_number}!",
        )

    except Exception as e:
        logger.error(
            f"Error processing /add command from {chat_id}: {e}", exc_info=True
        )
        bot.reply_to(
            message, "Произошла непредвиденная ошибка при обработке команды /add."
        )


# НОВЫЙ обработчик /fetch
@bot.message_handler(commands=["fetch"])
def handle_fetch(message):
    chat_id = message.chat.id
    logger.info(f"Received /fetch command from {chat_id}: {message.text}")

    if not bot:
        return
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

        # Запрашиваем исполнения для данного ордера
        # limit=10 - обычно исполнений немного для одного ордера
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
                f"Ошибка при запросе данных ордера {order_id_to_fetch} из Bybit: {response.get('retMsg', 'Неизвестная ошибка')}",
            )
            return

        exec_list = response.get("result", {}).get("list", [])
        if not exec_list:
            logger.warning(f"No executions found for Order ID: {order_id_to_fetch}")
            bot.reply_to(
                message,
                f"Не найдено исполнений для ордера {order_id_to_fetch} в категории {BYBIT_CATEGORY}. Возможно, он еще не исполнился или ID неверен.",
            )
            return

        # --- Логика извлечения данных (упрощенная) ---
        # Берем данные из первого исполнения, но суммируем объем и комиссию
        first_exec = exec_list[0]
        total_qty = 0
        total_fee = 0
        avg_exec_price = 0
        total_value = 0
        entry_dt = None  # Время первого исполнения

        for exec_item in exec_list:
            exec_qty = float(exec_item.get("execQty", 0))
            exec_price = float(exec_item.get("execPrice", 0))
            exec_fee = float(exec_item.get("execFee", 0))
            total_qty += exec_qty
            total_fee += exec_fee
            total_value += exec_qty * exec_price
            if not entry_dt:  # Берем время первого исполнения
                exec_time_ms = int(exec_item.get("execTime", 0))
                if exec_time_ms > 0:
                    entry_dt = datetime.datetime.fromtimestamp(exec_time_ms / 1000)

        if total_qty > 0:
            avg_exec_price = total_value / total_qty
        else:  # Если объем 0, используем цену ордера как запасной вариант
            avg_exec_price = float(first_exec.get("orderPrice", 0))
            total_qty = float(first_exec.get("orderQty", 0))  # Используем объем ордера

        asset = first_exec.get("symbol", "")
        side = first_exec.get("side", "").capitalize()
        direction = "Лонг" if side == "Buy" else ("Шорт" if side == "Sell" else side)
        entry_date_str = entry_dt.strftime("%d.%m.%Y") if entry_dt else ""
        entry_time_str = entry_dt.strftime("%H:%M:%S") if entry_dt else ""
        # Пытаемся получить TP/SL из данных ордера (может не быть)
        tp_price_str = first_exec.get("takeProfit", "")
        sl_price_str = first_exec.get("stopLoss", "")

        # Находим следующую строку
        target_row_number = find_next_empty_row(sheet)
        if not target_row_number:
            bot.reply_to(message, "Ошибка: не удалось найти пустую строку в таблице.")
            return

        # Готовим данные для обновления
        updates = []
        try:
            updates.append(
                {"range": f"A{target_row_number}", "values": [[entry_date_str]]}
            )  # A: Дата ВХОДА
            updates.append(
                {"range": f"B{target_row_number}", "values": [[entry_time_str]]}
            )  # B: Время ВХОДА
            updates.append(
                {"range": f"E{target_row_number}", "values": [[asset]]}
            )  # E: Торгуемая пара
            updates.append(
                {"range": f"F{target_row_number}", "values": [[direction]]}
            )  # F: Тип сделки
            updates.append(
                {"range": f"G{target_row_number}", "values": [[avg_exec_price]]}
            )  # G: Цена входа (средняя по исполнениям)
            updates.append(
                {
                    "range": f"H{target_row_number}",
                    "values": [[float(sl_price_str) if sl_price_str else ""]],
                }
            )  # H: Уровень Stop Loss (если есть)
            updates.append(
                {
                    "range": f"I{target_row_number}",
                    "values": [[float(tp_price_str) if tp_price_str else ""]],
                }
            )  # I: Уровень Take Profit (если есть)
            updates.append(
                {"range": f"J{target_row_number}", "values": [[total_qty]]}
            )  # J: Объём сделки (суммарный)
            updates.append(
                {"range": f"Q{target_row_number}", "values": [[total_fee]]}
            )  # Q: Комиссия входа (суммарная)
            updates.append(
                {"range": f"AD{target_row_number}", "values": [[order_id_to_fetch]]}
            )  # AD: Entry Order ID
        except ValueError as e:
            logger.error(f"ValueError converting fetched data: {e}", exc_info=True)
            bot.reply_to(message, f"Ошибка при обработке данных из Bybit: {e}")
            return

        logger.debug(f"Prepared batch update data for /fetch: {updates}")
        sheet.batch_update(updates, value_input_option="USER_ENTERED")
        logger.info(
            f"Updated cells in row {target_row_number} for {asset} (Order ID: {order_id_to_fetch}) via /fetch."
        )
        bot.reply_to(
            message,
            f"Сделка по {asset} (ID: {order_id_to_fetch}) успешно добавлена/обновлена в строке {target_row_number} из Bybit!",
        )

    except Exception as e:
        logger.error(
            f"Error processing /fetch command from {chat_id}: {e}", exc_info=True
        )
        bot.reply_to(
            message, "Произошла непредвиденная ошибка при обработке команды /fetch."
        )


# ОБНОВЛЕННЫЙ /close
@bot.message_handler(commands=["close"])
def handle_close(message):
    chat_id = message.chat.id
    logger.info(f"Received /close command from {chat_id}: {message.text}")

    if not bot:
        return
    if not sheet:
        logger.error("Sheet not initialized in /close")
        bot.reply_to(message, "Ошибка: Нет подключения к Google Sheets.")
        return

    try:
        parts = message.text.split()
        # /close <ПАРА> <ФАКТ_ЦЕНА_ВЫХОДА> (3 части)
        if len(parts) != 3:
            logger.warning(
                f"Invalid format for /close from {chat_id}. Parts: {len(parts)}"
            )
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
        exit_method = "вручную"  # /close всегда означает ручной выход

        # Получаем все значения листа
        list_of_lists = sheet.get_all_values()
        logger.info(
            f"Fetched {len(list_of_lists)} rows (including header) from Google Sheet."
        )

        header_row = list_of_lists[0] if list_of_lists else []
        logger.debug(f"Header row: {header_row}")

        # Названия столбцов (по структуре A-AC)
        asset_col_name = "Торгуемая пара (актив)"  # Колонка E
        actual_exit_price_col_name = "Фактическая цена выхода ($)"  # Колонка T

        # Буквы столбцов для обновления
        exit_date_col_letter = "C"
        exit_time_col_letter = "D"
        exit_method_col_letter = "S"
        actual_exit_price_col_letter = "T"

        # Находим индексы нужных столбцов по заголовкам
        try:
            asset_col_index = header_row.index(asset_col_name)  # Индекс E = 4
            actual_exit_price_col_index = header_row.index(
                actual_exit_price_col_name
            )  # Индекс T = 19
        except ValueError as e:
            logger.error(
                f"Column name mismatch in /close: '{e}' not found in header row: {header_row}"
            )
            bot.reply_to(
                message,
                f"Критическая ошибка: Не найден столбец '{e}' в заголовке таблицы.",
            )
            return
        except IndexError:
            logger.error(f"Header row not found or empty in /close.")
            bot.reply_to(message, f"Критическая ошибка: Не найден заголовок таблицы.")
            return

        found = False
        # Ищем сделку снизу вверх
        for i in range(len(list_of_lists) - 1, 0, -1):
            row = list_of_lists[i]
            current_row_number = i + 1

            # Проверяем, что строка не пустая и содержит нужные столбцы
            if len(row) > max(asset_col_index, actual_exit_price_col_index):
                asset_in_row = row[asset_col_index]  # Значение в столбце E
                exit_price_in_row = row[
                    actual_exit_price_col_index
                ]  # Значение в столбце T

                # Ищем строку с нужным активом и ПУСТОЙ факт. ценой выхода (в столбце T)
                if asset_in_row == asset_to_close and (
                    exit_price_in_row == "" or exit_price_in_row is None
                ):
                    logger.info(
                        f"Found open trade for {asset_to_close} at row {current_row_number}. Closing manually..."
                    )

                    updates = [
                        {  # Обновляем Дату ВЫХОДА (C)
                            "range": f"{exit_date_col_letter}{current_row_number}",
                            "values": [[exit_date]],
                        },
                        {  # Обновляем Время ВЫХОДА (D)
                            "range": f"{exit_time_col_letter}{current_row_number}",
                            "values": [[exit_time]],
                        },
                        {  # Обновляем Способ выхода (S)
                            "range": f"{exit_method_col_letter}{current_row_number}",
                            "values": [[exit_method]],
                        },
                        {  # Обновляем Факт. цену выхода (T)
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
            logger.info(f"No open trade found for {asset_to_close} to close manually.")
            bot.reply_to(
                message,
                f"Не найдена ОТКРЫТАЯ сделка по {asset_to_close} для ручного закрытия.",
            )

    except ValueError as e:
        logger.error(
            f"ValueError processing /close from {chat_id}: {e}. Check price format.",
            exc_info=True,
        )
        bot.reply_to(message, f"Ошибка в формате цены выхода: {e}.")
    except Exception as e:
        logger.error(
            f"Error processing /close command from {chat_id}: {e}", exc_info=True
        )
        bot.reply_to(message, "Ошибка при закрытии сделки.")


# === Webhook-роут ===
@app.route(f"/{TOKEN}", methods=["POST"])
def webhook():
    logger.info("Webhook received!")
    if not bot:
        logger.error("Webhook received but bot is not initialized!")
        return "error", 500  # Возвращаем ошибку, если бот не инициализирован
    try:
        json_str = request.get_data().decode("UTF-8")
        # logger.debug(f"Update JSON: {json_str}") # Можно раскомментировать для отладки
        update = telebot.types.Update.de_json(json_str)
        # logger.info("Processing update...") # Эта и след. строка не так важны, т.к. process_new_updates сама вызовет логи в хендлерах
        bot.process_new_updates([update])
        # logger.info("Update processed.")
    except Exception as e:
        logger.error(f"Error in webhook processing: {e}", exc_info=True)
    return "ok", 200


# === Запуск Flask-сервера (Используется Gunicorn'ом на Render) ===
# Убрали app.before_first_request - вебхук нужно установить вручную один раз!
# Этот блок if __name__ ... на Render не выполняется
if __name__ == "__main__":
    logger.info(
        "Attempting to run Flask development server (should only happen locally)"
    )
    # Запускаем только если бот успешно инициализирован
    if bot:
        port = int(os.environ.get("PORT", 10000))
        # Внимание: app.run() не для продакшена! На Render используется Gunicorn.
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        logger.error("Could not start Flask dev server: Bot not initialized.")
