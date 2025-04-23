import os
import logging
import datetime
import time
import telebot
from telebot import types
import gspread
from flask import Flask, request

# Используем современную библиотеку google-auth
from google.oauth2.service_account import Credentials

# Используем библиотеку pybit для Bybit API v5
from pybit.unified_trading import HTTP

# Используем dotenv для загрузки .env файла при локальном запуске (опционально)
# from dotenv import load_dotenv
# load_dotenv()

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

# --- Google Sheets ---
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Таблица сделок")
CREDENTIALS_PATH = "/etc/secrets/credentials.json"  # Путь к секретному файлу Render

# --- Структура Таблицы (A-AD, 30 столбцов) ---
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
EXPECTED_COLUMNS = 30

# --- Bybit ---
BYBIT_ENV = os.getenv("BYBIT_ENV", "LIVE").upper()
BYBIT_CATEGORY = os.getenv("BYBIT_CATEGORY", "linear")

# Endpoint constants for active-order queries
BYBIT_TESTNET_ENDPOINT = "https://api-testnet.bybit.com"
BYBIT_LIVE_ENDPOINT = "https://api.bybit.com"

# === Глобальные переменные для клиентов ===
bot = None
sheet = None
bybit_session = None
bybit = None
google_creds = None
google_client = None
app = None

# === Инициализация Flask и Telegram ===
app = Flask(__name__)
if TOKEN:
    try:
        bot = telebot.TeleBot(TOKEN, threaded=False)
        logger.info("Telegram bot initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize Telegram bot: {e}", exc_info=True)
        bot = None
else:
    bot = None


# === Функции Инициализации ===
def init_google_sheets():
    global sheet, google_creds, google_client
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
        logger.info("Successfully connected to Google Sheet.")
        if sheet.col_count < EXPECTED_COLUMNS:
            logger.warning(
                f"WARNING: Sheet has {sheet.col_count} cols, expected {EXPECTED_COLUMNS}."
            )
        return True
    except Exception as e:
        logger.error(f"FATAL: Error connecting to Google Sheets: {e}", exc_info=True)
        return False


def init_bybit():
    global bybit_session, bybit
    env = BYBIT_ENV
    logger.info(f"Attempting to connect to Bybit {env} environment...")
    api_key = api_secret = None
    testnet_flag = False
    if env == "TESTNET":
        key_path = "/etc/secrets/BYBIT_API_KEY_TESTNET"
        secret_path = "/etc/secrets/BYBIT_API_SECRET_TESTNET"
        testnet_flag = True
    else:
        key_path = "/etc/secrets/BYBIT_API_KEY_LIVE"
        secret_path = "/etc/secrets/BYBIT_API_SECRET_LIVE"
    if not os.path.exists(key_path) or not os.path.exists(secret_path):
        logger.error(
            f"FATAL: Bybit key/secret not found at {key_path} or {secret_path}!"
        )
        return False
    try:
        with open(key_path) as f:
            api_key = f.read().strip()
        with open(secret_path) as f:
            api_secret = f.read().strip()
        bybit_session = HTTP(
            testnet=testnet_flag, api_key=api_key, api_secret=api_secret
        )
        # Клиент для активных ордеров
        endpoint = BYBIT_TESTNET_ENDPOINT if testnet_flag else BYBIT_LIVE_ENDPOINT
        bybit = HTTP(
            endpoint=endpoint,
            api_key=api_key,
            api_secret=api_secret,
            category=BYBIT_CATEGORY,
        )
        logger.info(f"Successfully initialized Bybit API clients for {env}.")
        return True
    except Exception as e:
        logger.error(f"FATAL: Error connecting to Bybit API: {e}", exc_info=True)
        return False


# Инициализация при старте
if not init_google_sheets():
    logger.error("CRITICAL: Failed to init Google Sheets.")
if not init_bybit():
    logger.error("CRITICAL: Failed to init Bybit API.")


# === Вспомогательные функции ===
def find_next_empty_row(sheet_instance, column_index=1):
    if not sheet_instance:
        return None
    try:
        col_vals = sheet_instance.col_values(
            column_index, value_render_option="UNFORMATTED_VALUE"
        )
        idx = len(col_vals) - 1
        while idx > 0 and str(col_vals[idx]).strip() == "":
            idx -= 1
        return idx + 2
    except Exception as e:
        logger.error(f"Error finding next empty row: {e}", exc_info=True)
        return None


# === Обработчики команд ===
if bot:

    @bot.message_handler(commands=["start", "menu"])
    def handle_menu(message):
        markup = types.ReplyKeyboardMarkup(
            resize_keyboard=True, one_time_keyboard=False
        )
        # Первая строка кнопок
        btn_add = types.KeyboardButton("Добавить сделку")
        btn_fetch = types.KeyboardButton("Подтянуть исполнение")
        btn_close = types.KeyboardButton("Закрыть сделку")
        btn_addid = types.KeyboardButton("Добавить по ID")
        btn_hide = types.KeyboardButton("Скрыть меню")
        # Компоновка
        markup.row(btn_add, btn_fetch)
        markup.row(btn_close, btn_addid)
        markup.row(btn_hide)
        bot.send_message(message.chat.id, "Выберите действие:", reply_markup=markup)

    @bot.message_handler(func=lambda m: m.text == "Добавить сделку")
    def kb_add(message):
        bot.send_message(
            message.chat.id,
            "Чтобы добавить сделку, используйте:\n"
            "/add <Пара> <Лонг|Шорт> <Вход> <TP> <SL> <Объем> <OrderID>",
        )

    @bot.message_handler(func=lambda m: m.text == "Подтянуть исполнение")
    def kb_fetch(message):
        bot.send_message(
            message.chat.id,
            "Чтобы подтянуть исполнение, используйте:\n" "/fetch <OrderID>",
        )

    @bot.message_handler(func=lambda m: m.text == "Закрыть сделку")
    def kb_close(message):
        bot.send_message(
            message.chat.id,
            "Чтобы закрыть вручную, используйте:\n" "/close <Пара> <Цена_выхода>",
        )

    @bot.message_handler(func=lambda m: m.text == "Добавить по ID")
    def kb_addid(message):
        bot.send_message(
            message.chat.id,
            "Чтобы автоматически добавить по ID, используйте:\n" "/addid <OrderID>",
        )

    @bot.message_handler(func=lambda m: m.text == "Скрыть меню")
    def kb_hide(message):
        hide = types.ReplyKeyboardRemove()
        bot.send_message(message.chat.id, "Меню скрыто.", reply_markup=hide)

    @bot.message_handler(commands=["add"])
    def handle_add(message):
        if not sheet:
            return bot.reply_to(message, "Ошибка: Нет подключения к Google Sheets.")
        parts = message.text.split()
        if len(parts) != 8:
            return bot.reply_to(
                message,
                "Неверный формат!\nПример:\n`/add SOL/USDT Лонг 139.19 141.8 136.9 1.5 <OrderID>`",
                parse_mode="Markdown",
            )
        _, asset, direction, entry_s, tp_s, sl_s, qty_s, order_id = parts
        now = datetime.datetime.now()
        entry_date = now.strftime("%d.%m.%Y")
        entry_time = now.strftime("%H:%M:%S")
        row = find_next_empty_row(sheet)
        if not row:
            return bot.reply_to(message, "Ошибка: не удалось найти пустую строку.")
        try:
            vals = [
                {"range": f"A{row}", "values": [[entry_date]]},
                {"range": f"B{row}", "values": [[entry_time]]},
                {"range": f"E{row}", "values": [[asset]]},
                {"range": f"F{row}", "values": [[direction]]},
                {"range": f"G{row}", "values": [[float(entry_s)]]},
                {"range": f"H{row}", "values": [[float(sl_s)]]},
                {"range": f"I{row}", "values": [[float(tp_s)]]},
                {"range": f"J{row}", "values": [[float(qty_s)]]},
                {"range": f"AD{row}", "values": [[order_id]]},
            ]
        except ValueError as e:
            return bot.reply_to(message, f"Ошибка в формате чисел: {e}")
        sheet.batch_update(vals, value_input_option="USER_ENTERED")
        bot.reply_to(
            message, f"Сделка по {asset} (ID: {order_id}) добавлена в строку {row}!"
        )

    @bot.message_handler(commands=["fetch"])
    def handle_fetch(message):
        if not sheet or not bybit_session:
            return bot.reply_to(
                message, "Ошибка: Нет подключения к Google Sheets или Bybit API."
            )
        parts = message.text.split()
        if len(parts) != 2:
            return bot.reply_to(
                message,
                "Неверный формат!\nПример:\n`/fetch <OrderID>`",
                parse_mode="Markdown",
            )
        order_id = parts[1]
        resp = bybit_session.get_executions(
            orderId=order_id, category=BYBIT_CATEGORY, limit=10
        )
        if not resp or resp.get("retCode") != 0:
            return bot.reply_to(
                message, f"Ошибка запроса ордера {order_id}: {resp.get('retMsg', '')}"
            )
        lst = resp.get("result", {}).get("list", [])
        if not lst:
            return bot.reply_to(
                message, f"Не найдено исполнений для ордера {order_id}."
            )
        first = lst[0]
        total_qty = sum(float(i.get("execQty", 0)) for i in lst)
        total_fee = sum(float(i.get("execFee", 0)) for i in lst)
        avg_price = (
            sum(float(i.get("execQty", 0)) * float(i.get("execPrice", 0)) for i in lst)
            / total_qty
            if total_qty
            else float(first.get("orderPrice", 0))
        )
        ts_ms = int(lst[0].get("execTime", 0))
        dt = datetime.datetime.fromtimestamp(ts_ms / 1000)
        entry_date, entry_time = dt.strftime("%d.%m.%Y"), dt.strftime("%H:%M:%S")
        asset = first.get("symbol", "")
        side = first.get("side", "").capitalize()
        direction = "Лонг" if side == "Buy" else "Шорт"
        tp_p = first.get("takeProfit", "")
        sl_p = first.get("stopLoss", "")
        row = find_next_empty_row(sheet)
        updates = []
        try:
            updates.extend(
                [
                    {"range": f"A{row}", "values": [[entry_date]]},
                    {"range": f"B{row}", "values": [[entry_time]]},
                    {"range": f"E{row}", "values": [[asset]]},
                    {"range": f"F{row}", "values": [[direction]]},
                    {"range": f"G{row}", "values": [[avg_price]]},
                    {"range": f"H{row}", "values": [[float(sl_p) if sl_p else ""]]},
                    {"range": f"I{row}", "values": [[float(tp_p) if tp_p else ""]]},
                    {"range": f"J{row}", "values": [[total_qty]]},
                    {"range": f"Q{row}", "values": [[total_fee]]},
                    {"range": f"AD{row}", "values": [[order_id]]},
                ]
            )
        except ValueError as e:
            return bot.reply_to(message, f"Ошибка обработки данных Bybit: {e}")
        sheet.batch_update(updates, value_input_option="USER_ENTERED")
        bot.reply_to(
            message,
            f"Сделка по {asset} (ID: {order_id}) успешно добавлена из Bybit в строку {row}!",
        )

    @bot.message_handler(commands=["close"])
    def handle_close(message):
        if not sheet:
            return bot.reply_to(message, "Ошибка: Нет подключения к Google Sheets.")
        parts = message.text.split()
        if len(parts) != 3:
            return bot.reply_to(
                message,
                "Неверный формат. Пример:\n`/close SOL/USDT 140.55`",
                parse_mode="Markdown",
            )
        _, asset_to_close, exit_price_str = parts
        try:
            exit_price = float(exit_price_str)
        except ValueError:
            return bot.reply_to(message, f"Ошибка в формате цены: {exit_price_str}")
        now = datetime.datetime.now()
        exit_date, exit_time = now.strftime("%d.%m.%Y"), now.strftime("%H:%M:%S")
        data = sheet.get_all_values()
        header = data[0] if data else []
        try:
            idx_asset = header.index("Торгуемая пара")
            idx_exit = header.index("Фактическая цена выхода ($)")
        except ValueError as e:
            return bot.reply_to(message, f"Не найден столбец: {e}")
        found = False
        for i in range(len(data) - 1, 0, -1):
            row = data[i]
            if (
                len(row) > max(idx_asset, idx_exit)
                and row[idx_asset] == asset_to_close
                and not row[idx_exit]
            ):
                num = i + 1
                updates = [
                    {"range": f"C{num}", "values": [[exit_date]]},
                    {"range": f"D{num}", "values": [[exit_time]]},
                    {"range": f"S{num}", "values": [["вручную"]]},
                    {"range": f"T{num}", "values": [[exit_price]]},
                ]
                sheet.batch_update(updates, value_input_option="USER_ENTERED")
                bot.reply_to(
                    message,
                    f"Сделка по {asset_to_close} закрыта вручную по {exit_price}.",
                )
                found = True
                break
        if not found:
            bot.reply_to(message, f"Не найдена ОТКРЫТАЯ сделка по {asset_to_close}.")

    # Помощник для программного вызова /add
    def add_trade_from_cmd(cmd_text, chat_id):
        Fake = type("FakeMsg", (), {})
        msg = Fake()
        msg.text = cmd_text
        msg.chat = type("C", (), {"id": chat_id})
        handle_add(msg)

    @bot.message_handler(commands=["addid"])
    def handle_addid(message):
        parts = message.text.split()
        if len(parts) != 2:
            return bot.reply_to(
                message, "Неверный формат. Используйте /addid <OrderID>"
            )
        order_id = parts[1]
        try:
            resp = bybit.query_active_order(symbol="BTCUSDT", order_id=order_id)
        except Exception as e:
            logger.error(f"Error querying active order: {e}", exc_info=True)
            return bot.reply_to(message, f"Ошибка запроса ордера {order_id}.")
        code = resp.get("retCode", resp.get("ret_code"))
        if code != 0 or not resp.get("result"):
            return bot.reply_to(message, f"Ордер {order_id} не найден.")
        data = resp["result"]
        side = "Лонг" if data.get("side") == "Buy" else "Шорт"
        entry = data.get("price")
        tp = data.get("take_profit") or 0
        sl = data.get("stop_loss") or 0
        qty = data.get("qty")
        cmd = f"/add BTC/USDT {side} {entry} {tp} {sl} {qty} {order_id}"
        add_trade_from_cmd(cmd, message.chat.id)
        bot.reply_to(message, f"Сделка добавлена автоматически:\n{cmd}")

else:
    logger.error("CRITICAL: Bot object is None, handlers won't be registered!")

# === Webhook ===
if app and TOKEN:

    @app.route(f"/{TOKEN}", methods=["POST"])
    def webhook():
        if not bot:
            return "error", 500
        try:
            json_str = request.get_data().decode("UTF-8")
            update = telebot.types.Update.de_json(json_str)
            bot.process_new_updates([update])
        except Exception as e:
            logger.error(f"Error in webhook processing: {e}", exc_info=True)
        return "ok", 200


# === Main ===
if __name__ == "__main__":
    if bot and app:
        port = int(os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        logger.error("Could not start server: Bot or app not initialized.")
