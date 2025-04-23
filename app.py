# --- Файл: app.py (Версия с get_order_history для /fetch) ---

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
CREDENTIALS_PATH = "/etc/secrets/credentials.json"
# Индексы столбцов (A-AD, 30 столбцов)
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
    "entry_order_id": 29,
}
EXPECTED_COLUMNS = 30  # A-AD

# --- Bybit ---
BYBIT_ENV = os.getenv("BYBIT_ENV", "LIVE").upper()
BYBIT_CATEGORY = os.getenv("BYBIT_CATEGORY", "linear")

# === Глобальные переменные ===
bot = None
sheet = None
bybit_session = None
google_creds = None
google_client = None
app = None

# === Инициализация ===
app = Flask(__name__)

if TOKEN:
    try:
        bot = telebot.TeleBot(TOKEN, threaded=False)
        logger.info("Telegram bot initialized.")
    except Exception as e:
        logger.error(f"Telegram init error: {e}", exc_info=True)
        bot = None
else:
    logger.error("TELEGRAM_BOT_TOKEN not set!")
    bot = None


# === Функции Инициализации Сервисов ===
def init_google_sheets():
    global sheet, google_creds, google_client
    # ... (Код функции без изменений) ...
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
        if sheet.col_count < EXPECTED_COLUMNS:
            logger.warning(
                f"Sheet has {sheet.col_count} cols, expected {EXPECTED_COLUMNS}."
            )
        return True
    except gspread.exceptions.APIError as e:
        if e.response.status_code == 401:
            logger.warning("Google API Error 401. Attempting refresh.")
        try:
            google_client.login()
            spreadsheet = google_client.open_by_key(SPREADSHEET_ID)
            sheet = spreadsheet.worksheet(SHEET_NAME)
            logger.info("Reconnected to Google Sheets after refresh.")
            return True
        except Exception as refresh_e:
            logger.error(
                f"FATAL: Failed Google credentials refresh: {refresh_e}", exc_info=True
            )
            return False
        else:
            logger.error(f"FATAL: Google API Error: {e}", exc_info=True)
            return False
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"FATAL: Worksheet '{SHEET_NAME}' not found!")
        return False
    except Exception as e:
        logger.error(f"FATAL: Error connecting Google Sheets: {e}", exc_info=True)
        return False


def init_bybit():
    global bybit_session
    # ... (Код функции без изменений, читает ключи из секретных файлов Render) ...
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


# === Инициализация при старте ===
if not init_google_sheets():
    logger.error("CRITICAL: Failed to initialize Google Sheets.")
if not init_bybit():
    logger.error("CRITICAL: Failed to initialize Bybit.")


# === Вспомогательные функции ===
def find_next_empty_row(sheet_instance, column_index=1):
    # ... (Код функции без изменений) ...
    if not sheet_instance:
        return None
    try:
        logger.debug(f"Fetching column {column_index} values...")
        col_values = sheet_instance.col_values(
            column_index, value_render_option="UNFORMATTED_VALUE"
        )
        logger.debug(f"Found {len(col_values)} values.")
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


# === Обработчики команд и Кнопок ===
if bot:  # Только если бот инициализирован

    @bot.message_handler(commands=["start", "menu"])
    def handle_menu(message):
        # ... (Код функции handle_menu с кнопками без изменений) ...
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        markup.row(
            "Добавить сделку", "Подтянуть исполнение"
        )  # Эти кнопки пока не обрабатываются
        markup.row("Закрыть сделку", "Добавить по ID")
        markup.row("Скрыть меню")
        bot.send_message(message.chat.id, "Выберите действие:", reply_markup=markup)

    @bot.message_handler(func=lambda m: m.text == "Скрыть меню")
    def hide_menu(message):
        # ... (Код функции hide_menu без изменений) ...
        bot.send_message(
            message.chat.id, "Меню скрыто.", reply_markup=types.ReplyKeyboardRemove()
        )

    # ОБНОВЛЕННЫЙ /add
    @bot.message_handler(commands=["add"])
    def handle_add(message):
        # ... (Код функции handle_add без изменений, использует batch_update) ...
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
                f"Сделка по {asset} (ID: {bybit_order_id}) добавлена в строку {target_row_number}!",
            )
        except Exception as e:
            logger.error(f"Error processing /add command: {e}", exc_info=True)
            bot.reply_to(message, "Ошибка при обработке /add.")

    # --- ИЗМЕНЕННЫЙ /fetch ---
    @bot.message_handler(commands=["fetch"])
    def handle_fetch(message):
        chat_id = message.chat.id
        logger.info(f"Received /fetch command from {chat_id}: {message.text}")

        if not sheet or not bybit_session:
            error_msg = "Ошибка: " + (
                "Нет подключения к Google Sheets."
                if not sheet
                else "Нет подключения к Bybit API."
            )
            logger.error(error_msg + " Cannot process /fetch.")
            return bot.reply_to(message, error_msg)

        try:
            parts = message.text.split()
            if len(parts) != 2:
                return bot.reply_to(
                    message,
                    "Неверный формат!\nПример:\n`/fetch <Bybit_Order_ID>`",
                    parse_mode="Markdown",
                )

            order_id_to_fetch = parts[1]
            logger.info(f"Fetching order history for Order ID: {order_id_to_fetch}")

            # --- ИСПОЛЬЗУЕМ get_order_history ---
            response = bybit_session.get_order_history(
                orderId=order_id_to_fetch,
                category=BYBIT_CATEGORY,
                limit=1,  # Лимит 1, т.к. нужен только этот ордер
            )
            logger.debug(
                f"Raw Bybit Order History response for {order_id_to_fetch}: {response}"
            )
            # ------------------------------------

            if not (response and response.get("retCode") == 0):
                logger.error(f"Error fetching order history from Bybit: {response}")
                return bot.reply_to(
                    message,
                    f"Ошибка при запросе ордера {order_id_to_fetch}: {response.get('retMsg', 'Error')}",
                )

            order_list = response.get("result", {}).get("list", [])
            if not order_list:
                logger.warning(f"No order found for Order ID: {order_id_to_fetch}")
                return bot.reply_to(
                    message,
                    f"Не найден ордер {order_id_to_fetch} в категории {BYBIT_CATEGORY}. Проверьте ID и категорию.",
                )

            # Берем данные из найденного ордера
            order_data = order_list[0]
            try:
                asset = order_data.get("symbol", "")
                side = order_data.get("side", "").capitalize()
                direction = (
                    "Лонг" if side == "Buy" else ("Шорт" if side == "Sell" else side)
                )
                # Используем цену ордера как цену входа
                entry_price = float(
                    order_data.get("price") or order_data.get("avgPrice") or 0
                )  # avgPrice может быть ценой исполнения
                # Используем объем ордера
                total_qty = float(order_data.get("qty", 0))
                # Время создания ордера
                created_time_ms = int(order_data.get("createdTime", 0))
                entry_dt = (
                    datetime.datetime.fromtimestamp(created_time_ms / 1000)
                    if created_time_ms > 0
                    else None
                )
                entry_date_str = entry_dt.strftime("%d.%m.%Y") if entry_dt else ""
                entry_time_str = entry_dt.strftime("%H:%M:%S") if entry_dt else ""
                # SL/TP из ордера
                tp_price_str = order_data.get("takeProfit", "")
                sl_price_str = order_data.get("stopLoss", "")
                # Комиссию взять неоткуда из истории ордера, оставляем пустой

                if not asset or total_qty <= 0:
                    logger.error(
                        f"Incomplete essential data from order history for {order_id_to_fetch}: {order_data}"
                    )
                    return bot.reply_to(
                        message,
                        f"Не удалось извлечь основные данные (символ/объем) для ордера {order_id_to_fetch}.",
                    )

            except (ValueError, TypeError, KeyError, IndexError) as e:
                logger.error(
                    f"Error parsing order history data for {order_id_to_fetch}: {e}. Data: {order_data}",
                    exc_info=True,
                )
                return bot.reply_to(
                    message, f"Ошибка обработки данных ордера {order_id_to_fetch}."
                )

            # Находим следующую строку
            target_row_number = find_next_empty_row(sheet)
            if not target_row_number:
                return bot.reply_to(
                    message, "Ошибка: не удалось найти пустую строку в таблице."
                )

            # Готовим данные для обновления
            updates = [
                {
                    "range": f"A{target_row_number}",
                    "values": [[entry_date_str]],
                },  # Дата ВХОДА
                {
                    "range": f"B{target_row_number}",
                    "values": [[entry_time_str]],
                },  # Время ВХОДА
                {"range": f"E{target_row_number}", "values": [[asset]]},  # Пара
                {"range": f"F{target_row_number}", "values": [[direction]]},  # Тип
                {
                    "range": f"G{target_row_number}",
                    "values": [[entry_price]],
                },  # Цена входа (по ордеру)
                {
                    "range": f"H{target_row_number}",
                    "values": [[float(sl_price_str) if sl_price_str else ""]],
                },  # SL
                {
                    "range": f"I{target_row_number}",
                    "values": [[float(tp_price_str) if tp_price_str else ""]],
                },  # TP
                {"range": f"J{target_row_number}", "values": [[total_qty]]},  # Объем
                # Q (Комиссия входа) - пусто
                {
                    "range": f"AD{target_row_number}",
                    "values": [[order_id_to_fetch]],
                },  # Entry Order ID
            ]

            logger.debug(
                f"Prepared batch update data for /fetch (using order history): {updates}"
            )
            sheet.batch_update(updates, value_input_option="USER_ENTERED")
            logger.info(
                f"Updated cells in row {target_row_number} for {asset} (Order ID: {order_id_to_fetch}) via /fetch (order history)."
            )
            bot.reply_to(
                message,
                f"Данные ордера {order_id_to_fetch} ({asset}) добавлены в строку {target_row_number}!",
            )

        except Exception as e:
            logger.error(
                f"Error processing /fetch command from {chat_id}: {e}", exc_info=True
            )
            bot.reply_to(
                message, "Произошла непредвиденная ошибка при обработке команды /fetch."
            )

    # --- Обработчик для кнопки "Добавить по ID" ---
    @bot.message_handler(func=lambda m: m.text == "Добавить по ID")
    def kb_addid(message):
        # Просто просим ID и передаем его в обработчик handle_fetch
        msg = bot.send_message(message.chat.id, "Введите OrderID с Bybit:")
        # Важно: Регистрируем следующий шаг на функцию handle_fetch, а не process_addid
        bot.register_next_step_handler(msg, fetch_wrapper_for_next_step)

    def fetch_wrapper_for_next_step(message):
        # Эта обертка нужна, чтобы текст сообщения стал командой /fetch ID
        logger.info(f"Received Order ID '{message.text}' via next_step_handler")
        # Формируем "фальшивое" сообщение, как будто пользователь ввел /fetch ID
        fake_command_message = type(
            "FakeCommandMessage",
            (object,),
            {
                "text": f"/fetch {message.text.strip()}",
                "chat": message.chat,
                "from_user": message.from_user,  # Копируем пользователя для логирования/идентификации
                "message_id": message.message_id,  # Копируем ID исходного сообщения
            },
        )()
        # Передаем фальшивое сообщение в основной обработчик /fetch
        handle_fetch(fake_command_message)

    # ОБНОВЛЕННЫЙ /close
    @bot.message_handler(commands=["close"])
    def handle_close(message):
        # ... (Код функции handle_close без изменений) ...
        # ... (Ищет по E и пустому T, обновляет C, D, S, T) ...
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
            actual_exit_price_col_name = "Фактическая цена выхода ($)"  # Столбец T
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
