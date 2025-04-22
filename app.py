import os
import logging
import datetime

import telebot
import gspread
from flask import Flask, request
from oauth2client.service_account import ServiceAccountCredentials

# === Настройка логирования ===
logging.basicConfig(level=logging.INFO)

# === Константы ===
TOKEN = "8121463359:AAE7Q4RGlRJ35TMWbHQtuU4YzHmdym4OXLI"
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Таблица сделок")
CREDENTIALS_PATH = "/etc/secrets/credentials.json"

# === Инициализация ===
bot = telebot.TeleBot(TOKEN)
app = Flask(__name__)

# === Подключение к Google Sheets ===
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
client = gspread.authorize(credentials)
sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

# === Обработчики команд ===


@bot.message_handler(commands=["start"])
def start(message):
    bot.send_message(
        message.chat.id,
        "Привет! Я — бот помощник трейдера. Используй /add и /close для работы со сделками.",
    )


@bot.message_handler(commands=["add"])
def add_trade(message):
    try:
        parts = message.text.split()
        if len(parts) != 7:
            bot.reply_to(
                message, "Формат: /add SOL/USDT Лонг 139.19 141.80 136.90 214.6"
            )
            return
        _, asset, direction, entry_price, tp, sl, amount = parts
        today = datetime.datetime.now().strftime("%d.%m.%Y")
        new_row = [
            today,
            asset,
            direction,
            float(entry_price),
            "",
            float(amount),
            "=E:E*F:F - D:D*F:F",
            "=(E:E - D:D)/D:D*100",
            float(sl),
            float(tp),
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ]
        sheet.append_row(new_row, value_input_option="USER_ENTERED")
        bot.reply_to(message, f"Сделка по {asset} добавлена!")
    except Exception as e:
        logging.error(e)
        bot.reply_to(message, "Произошла ошибка при добавлении сделки.")


@bot.message_handler(commands=["close"])
def close_trade(message):
    try:
        parts = message.text.split()
        if len(parts) != 3:
            bot.reply_to(message, "Формат: /close SOL/USDT 140.55")
            return
        _, asset, exit_price = parts
        exit_price = float(exit_price)
        records = sheet.get_all_records()
        for i, row in enumerate(records):
            if (
                row["Торгуемая пара (актив)"] == asset
                and row["Фактическая цена выхода ($)"] == ""
            ):
                row_number = i + 2  # с учётом заголовков
                sheet.update(f"Q{row_number}", exit_price)
                sheet.update(
                    f"R{row_number}",
                    f"=Q{row_number}*F{row_number} - D{row_number}*F{row_number}",
                )
                today = datetime.datetime.now().strftime("%d.%m.%Y")
                sheet.update(f"P{row_number}", today)
                sheet.update(f"O{row_number}", today)
                bot.reply_to(message, f"Сделка по {asset} закрыта по {exit_price}.")
                return
        bot.reply_to(message, f"Не найдена открытая сделка по {asset}.")
    except Exception as e:
        logging.error(e)
        bot.reply_to(message, "Ошибка при закрытии сделки.")


# === Webhook-роут ===
@app.route(f"/{TOKEN}", methods=["POST"])
def webhook():
    json_str = request.get_data().decode("UTF-8")
    update = telebot.types.Update.de_json(json_str)
    bot.process_new_updates([update])
    return "ok", 200


# === Установка webhook при старте ===
@app.before_first_request
def setup_webhook():
    webhook_url = f"https://srv-d03mhhili9vc73fqppt0.onrender.com/{TOKEN}"
    bot.remove_webhook()
    bot.set_webhook(url=webhook_url)


# === Запуск Flask-сервера ===
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
