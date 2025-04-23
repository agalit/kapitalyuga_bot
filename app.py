import os
import logging
import datetime

import telebot
import gspread
from flask import Flask, request
from google.oauth2.service_account import Credentials
from pybit.unified_trading import HTTP
from telebot import types

# === Настройка логирования ===
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# === Константы и переменные окружения ===
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Таблица сделок")
CREDENTIALS_PATH = "/etc/secrets/credentials.json"
BYBIT_ENV = os.getenv("BYBIT_ENV", "TESTNET").upper()  # TESTNET или LIVE
BYBIT_CATEGORY = os.getenv("BYBIT_CATEGORY", "linear")

# Индексы столбцов A-AD (0-29)
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

# === Глобальные объекты ===
app = Flask(__name__)
bot = None
sheet = None
bybit_session = None

# === Инициализация Telegram ===
if TOKEN:
    try:
        bot = telebot.TeleBot(TOKEN, threaded=False)
        logger.info("Telegram bot initialized.")
    except Exception as e:
        logger.error(f"Telegram init error: {e}", exc_info=True)


# === Инициализация Google Sheets ===
def init_google_sheets():
    global sheet
    if not SPREADSHEET_ID or not os.path.exists(CREDENTIALS_PATH):
        logger.error("Missing SPREADSHEET_ID or credentials file.")
        return False
    try:
        creds = Credentials.from_service_account_file(
            CREDENTIALS_PATH,
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
        logger.info("Connected to Google Sheet.")
        if sheet.col_count < EXPECTED_COLUMNS:
            logger.warning(
                f"Sheet has {sheet.col_count} cols, expected {EXPECTED_COLUMNS}."
            )
        return True
    except Exception as e:
        logger.error(f"Error connecting to Google Sheets: {e}", exc_info=True)
        return False


# === Инициализация Bybit API ===
def init_bybit():
    global bybit_session
    testnet = BYBIT_ENV == "TESTNET"
    key_name = "BYBIT_API_KEY_TESTNET" if testnet else "BYBIT_API_KEY_LIVE"
    secret_name = "BYBIT_API_SECRET_TESTNET" if testnet else "BYBIT_API_SECRET_LIVE"
    api_key = os.getenv(key_name)
    api_secret = os.getenv(secret_name)
    if not api_key or not api_secret:
        logger.error("Bybit API key/secret missing.")
        return False
    try:
        bybit_session = HTTP(testnet=testnet, api_key=api_key, api_secret=api_secret)
        logger.info("Bybit API initialized.")
        return True
    except Exception as e:
        logger.error(f"Error initializing Bybit API: {e}", exc_info=True)
        return False


# === Вспомогательная функция ===
def find_next_empty_row(sheet_instance, column_index=1):
    try:
        vals = sheet_instance.col_values(
            column_index, value_render_option="UNFORMATTED_VALUE"
        )
        idx = len(vals) - 1
        while idx > 0 and not str(vals[idx]).strip():
            idx -= 1
        return idx + 2
    except Exception as e:
        logger.error(f"Error finding next empty row: {e}", exc_info=True)
        return None


# === Запуск инициализация при старте ===
if not init_google_sheets():
    logger.error("Google Sheets init failed.")
if not init_bybit():
    logger.error("Bybit init failed.")


# === Меню и хендлеры ===
@bot.message_handler(commands=["start", "menu"])
def handle_menu(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("Добавить сделку", "Подтянуть исполнение")
    markup.row("Закрыть сделку", "Добавить по ID")
    markup.row("Скрыть меню")
    bot.send_message(message.chat.id, "Выберите действие:", reply_markup=markup)


@bot.message_handler(func=lambda m: m.text == "Скрыть меню")
def hide_menu(message):
    bot.send_message(
        message.chat.id, "Меню скрыто.", reply_markup=types.ReplyKeyboardRemove()
    )


@bot.message_handler(func=lambda m: m.text == "Добавить сделку")
def kb_add(message):
    bot.send_message(
        message.chat.id,
        "Введите данные сделки в формате:\n"
        "/add <Пара> <Лонг|Шорт> <Вход> <TP> <SL> <Объем> <OrderID>",
    )


@bot.message_handler(func=lambda m: m.text == "Подтянуть исполнение")
def kb_fetch(message):
    bot.send_message(message.chat.id, "Введите ID ордера без команды:\n" "<OrderID>")
    msg = bot.send_message(message.chat.id, "OrderID:")
    bot.register_next_step_handler(msg, process_fetch)


def process_fetch(message):
    order_id = message.text.strip()
    resp = bybit_session.get_executions(
        orderId=order_id, category=BYBIT_CATEGORY, limit=10
    )
    if not resp or resp.get("retCode") != 0:
        return bot.send_message(message.chat.id, f"Ордер {order_id} не найден.")
    # вызов стандартного handle_fetch
    fake = type("F", (), {})
    fake_msg = fake()
    fake_msg.text = f"/fetch {order_id}"
    fake_msg.chat = message.chat
    handle_fetch(fake_msg)


@bot.message_handler(func=lambda m: m.text == "Добавить по ID")
def kb_addid(message):
    msg = bot.send_message(
        message.chat.id, "Введите OrderID для автоматического добавления:"
    )
    bot.register_next_step_handler(msg, process_addid)


def process_addid(message):
    order_id = message.text.strip()
    resp = bybit_session.query_active_order(symbol="BTCUSDT", order_id=order_id)
    if not resp or resp.get("retCode", resp.get("ret_code", 1)) != 0:
        return bot.send_message(message.chat.id, f"Ордер {order_id} не найден.")
    data = resp["result"]
    side = "Лонг" if data.get("side") == "Buy" else "Шорт"
    entry = data.get("price")
    tp = data.get("take_profit") or 0
    sl = data.get("stop_loss") or 0
    qty = data.get("qty")
    cmd = f"/add BTC/USDT {side} {entry} {tp} {sl} {qty} {order_id}"
    fake = type("F", (), {})
    fake_msg = fake()
    fake_msg.text = cmd
    fake_msg.chat = message.chat
    handle_add(fake_msg)


@bot.message_handler(func=lambda m: m.text == "Закрыть сделку")
def kb_close(message):
    bot.send_message(message.chat.id, "Введите цену выхода без команды:\n" "<Цена>")
    msg = bot.send_message(message.chat.id, "Цена:")
    bot.register_next_step_handler(msg, process_close)


def process_close(message):
    fake = type("F", (), {})
    fake_msg = fake()
    fake_msg.text = f"/close {message.text.strip()}"
    fake_msg.chat = message.chat
    handle_close(fake_msg)


# === Существующие команды /add, /fetch, /close ===
# Скопируйте сюда ваши реализационные хендлеры handle_add, handle_fetch, handle_close


@bot.message_handler(commands=["add"])
def handle_add(message):
    if not sheet:
        return bot.reply_to(message, "Ошибка: нет Google Sheets.")
    parts = message.text.split()
    if len(parts) != 8:
        return bot.reply_to(
            message,
            "Неверный формат! Пример: /add SOL/USDT Лонг 139.19 141.8 136.9 1.5 12345",
            parse_mode="Markdown",
        )
    _, asset, direction, entry_s, tp_s, sl_s, qty_s, order_id = parts
    now = datetime.datetime.now()
    entry_date = now.strftime("%d.%m.%Y")
    entry_time = now.strftime("%H:%M:%S")
    row = find_next_empty_row(sheet)
    if not row:
        return bot.reply_to(message, "Ошибка: не нашёл пустую строку.")
    try:
        updates = [
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
        return bot.reply_to(message, f"Ошибка формата чисел: {e}")
    sheet.batch_update(updates, value_input_option="USER_ENTERED")
    bot.reply_to(message, f"Сделка {asset} добавлена в строку {row}.")


@bot.message_handler(commands=["fetch"])
def handle_fetch(message):
    if not sheet or not bybit_session:
        return bot.reply_to(message, "Ошибка: нет Google Sheets или Bybit.")
    parts = message.text.split()
    if len(parts) != 2:
        return bot.reply_to(message, "Используйте: /fetch <OrderID>")
    order_id = parts[1]
    resp = bybit_session.get_executions(
        orderId=order_id, category=BYBIT_CATEGORY, limit=10
    )
    if not resp or resp.get("retCode") != 0:
        return bot.reply_to(message, f'Ошибка: {resp.get("retMsg")}')
    lst = resp["result"]["list"]
    if not lst:
        return bot.reply_to(message, "Нет исполнений.")
    first = lst[0]
    total_qty = sum(float(i.get("execQty", 0)) for i in lst)
    total_fee = sum(float(i.get("execFee", 0)) for i in lst)
    avg_price = (
        sum(float(i.get("execQty", 0)) * float(i.get("execPrice", 0)) for i in lst)
        / total_qty
    )
    dt = datetime.datetime.fromtimestamp(int(lst[0]["execTime"]) / 1000)
    entry_date, entry_time = dt.strftime("%d.%m.%Y"), dt.strftime("%H:%M:%S")
    asset = first.get("symbol")
    direction = "Лонг" if first.get("side") == "Buy" else "Шорт"
    tp = first.get("takeProfit", "")
    sl = first.get("stopLoss", "")
    row = find_next_empty_row(sheet)
    updates = [
        {"range": f"A{row}", "values": [[entry_date]]},
        {"range": f"B{row}", "values": [[entry_time]]},
        {"range": f"E{row}", "values": [[asset]]},
        {"range": f"F{row}", "values": [[direction]]},
        {"range": f"G{row}", "values": [[avg_price]]},
        {"range": f"H{row}", "values": [[float(sl) if sl else ""]]},
        {"range": f"I{row}", "values": [[float(tp) if tp else ""]]},
        {"range": f"J{row}", "values": [[total_qty]]},
        {"range": f"Q{row}", "values": [[total_fee]]},
        {"range": f"AD{row}", "values": [[order_id]]},
    ]
    sheet.batch_update(updates, value_input_option="USER_ENTERED")
    bot.reply_to(message, f"Fetch done, row {row}.")


@bot.message_handler(commands=["close"])
def handle_close(message):
    if not sheet:
        return bot.reply_to(message, "Ошибка: нет Google Sheets.")
    parts = message.text.split()
    if len(parts) != 3:
        return bot.reply_to(message, "Используйте: /close <Пара> <Цена>")
    _, asset_to_close, exit_price_s = parts
    try:
        exit_price = float(exit_price_s)
    except ValueError:
        return bot.reply_to(message, "Ошибка цены")
    dt = datetime.datetime.now()
    exit_date, exit_time = dt.strftime("%d.%m.%Y"), dt.strftime("%H:%M:%S")
    data = sheet.get_all_values()
    header = data[0]
    ai = header.index("Торгуемая пара")
    ei = header.index("Фактическая цена выхода ($)")
    for i in range(len(data) - 1, 0, -1):
        row = data[i]
        if len(row) > ei and row[ai] == asset_to_close and not row[ei]:
            num = i + 1
            upd = [
                {"range": f"C{num}", "values": [[exit_date]]},
                {"range": f"D{num}", "values": [[exit_time]]},
                {"range": f"S{num}", "values": [["вручную"]]},
                {"range": f"T{num}", "values": [[exit_price]]},
            ]
            sheet.batch_update(upd, value_input_option="USER_ENTERED")
            return bot.reply_to(message, f"Закрыл row {num}.")
    bot.reply_to(message, "Не найдено.")


# === Webhook ===
@app.route(f"/{TOKEN}", methods=["POST"])
def webhook():
    update = telebot.types.Update.de_json(request.get_data().decode())
    bot.process_new_updates([update])
    return "ok", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
