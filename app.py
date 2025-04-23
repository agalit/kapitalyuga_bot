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

# === Переменные окружения ===
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Таблица сделок")
CREDENTIALS_PATH = "/etc/secrets/credentials.json"
BYBIT_ENV = os.getenv("BYBIT_ENV", "TESTNET").upper()
BYBIT_CATEGORY = os.getenv("BYBIT_CATEGORY", "linear")

# Индексы столбцов A-AD
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

# === Инициализация ===
app = Flask(__name__)
bot = None
sheet = None
bybit_session = None

# === Telegram Bot ===
if TOKEN:
    try:
        bot = telebot.TeleBot(TOKEN, threaded=False)
        logger.info("Telegram bot initialized.")
    except Exception as e:
        logger.error(f"Telegram init error: {e}", exc_info=True)
else:
    logger.error("TELEGRAM_BOT_TOKEN not set!")


# === Google Sheets ===
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


# === Bybit API ===
def init_bybit():
    global bybit_session
    testnet = BYBIT_ENV == "TESTNET"
    # Читаем секретные файлы
    key_file = f"/etc/secrets/BYBIT_API_KEY_{BYBIT_ENV}"
    secret_file = f"/etc/secrets/BYBIT_API_SECRET_{BYBIT_ENV}"
    try:
        with open(key_file) as f:
            api_key = f.read().strip()
        with open(secret_file) as f:
            api_secret = f.read().strip()
    except Exception as e:
        logger.error(f"Error reading Bybit secrets: {e}", exc_info=True)
        return False
    if not api_key or not api_secret:
        logger.error("Bybit API key/secret is empty.")
        return False
    try:
        bybit_session = HTTP(testnet=testnet, api_key=api_key, api_secret=api_secret)
        logger.info(f"Bybit API initialized for {'TESTNET' if testnet else 'LIVE'}.")
        return True
    except Exception as e:
        logger.error(f"Error initializing Bybit API: {e}", exc_info=True)
        return False


# === Утилиты ===
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


# Запуск инициализация
init_google_sheets()
init_bybit()


# === Меню и шаговые хендлеры ===
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
        "Введите данные в формате:\n/add <Пара> <Лонг|Шорт> <Вход> <TP> <SL> <Объем> <OrderID>",
    )


@bot.message_handler(func=lambda m: m.text == "Подтянуть исполнение")
def kb_fetch(message):
    msg = bot.send_message(message.chat.id, "Введите OrderID для /fetch:")
    bot.register_next_step_handler(msg, process_fetch)


def process_fetch(message):
    order_id = message.text.strip()
    fake = type("F", (), {})()
    fake.text = f"/fetch {order_id}"
    fake.chat = message.chat
    handle_fetch(fake)


@bot.message_handler(func=lambda m: m.text == "Закрыть сделку")
def kb_close(message):
    msg = bot.send_message(message.chat.id, "Введите цену выхода:")
    bot.register_next_step_handler(msg, process_close)


def process_close(message):
    price = message.text.strip()
    fake = type("F", (), {})()
    fake.text = f"/close {price}"
    fake.chat = message.chat
    handle_close(fake)


@bot.message_handler(func=lambda m: m.text == "Добавить по ID")
def kb_addid(message):
    msg = bot.send_message(message.chat.id, "Введите OrderID для автодобавления:")
    bot.register_next_step_handler(msg, process_addid)


def process_addid(message):
    order_id = message.text.strip()
    try:
        resp = bybit_session.get_active_orders(symbol="BTCUSDT", orderId=order_id)
    except Exception as e:
        return bot.send_message(message.chat.id, f"Ошибка запроса: {e}")
    if not resp or resp.get("retCode") != 0:
        return bot.send_message(message.chat.id, f"Ордер {order_id} не найден.")
    orders = resp.get("result", {}).get("list", [])
    if not orders:
        return bot.send_message(message.chat.id, f"Ордер {order_id} не найден.")
    data = orders[0]
    side = "Лонг" if data.get("side") == "Buy" else "Шорт"
    entry = data.get("price")
    tp = data.get("takeProfit") or 0
    sl = data.get("stopLoss") or 0
    qty = data.get("qty")
    cmd = f"/add BTC/USDT {side} {entry} {tp} {sl} {qty} {order_id}"
    fake = type("F", (), {})()
    fake.text = cmd
    fake.chat = message.chat
    handle_add(fake)


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
