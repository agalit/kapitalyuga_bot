import telebot
from flask import Flask, request

TOKEN = "8121463359:AAE7Q4RGlRJ35TMWbHQtuU4YzHmdym4OXLI"
bot = telebot.TeleBot(TOKEN)

app = Flask(__name__)


# Обработчик команды /start
@bot.message_handler(commands=["start"])
def start_message(message):
    bot.send_message(message.chat.id, "Привет! Я бот, работающий через webhook 🌐")


# Пример обработки любого текста
@bot.message_handler(func=lambda message: True)
def echo_message(message):
    bot.send_message(message.chat.id, f"Вы написали: {message.text}")


# Роут для приёма обновлений от Telegram
@app.route(f"/{TOKEN}", methods=["POST"])
def webhook():
    json_str = request.get_data().decode("UTF-8")
    update = telebot.types.Update.de_json(json_str)
    bot.process_new_updates([update])
    return "ok", 200


# Установка webhook при старте сервера
@app.before_first_request
def setup_webhook():
    webhook_url = f"https://srv-d03mhhili9vc73fqppt0.onrender.com/{TOKEN}"
    bot.remove_webhook()
    bot.set_webhook(url=webhook_url)


# Запуск Flask-сервера
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
