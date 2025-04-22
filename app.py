import os
import logging
import datetime
import telebot
import gspread
from flask import Flask, request
from oauth2client.service_account import (
    ServiceAccountCredentials,
)  # Устаревшая библиотека, но пока работает

# === Настройка логирования ===
# Настраиваем формат и уровень логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
# Создаем именованный логгер для нашего приложения
logger = logging.getLogger(__name__)

# === Константы ===
# РЕКОМЕНДАЦИЯ: Вынеси TOKEN в переменные окружения Render для безопасности
TOKEN = "8121463359:AAE7Q4RGlRJ35TMWbHQtuU4YzHmdym4OXLI"
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
# РЕКОМЕНДАЦИЯ: Вынеси SHEET_NAME в переменные окружения Render, если может меняться
SHEET_NAME = os.getenv("SHEET_NAME", "Таблица сделок")  # Имя листа по умолчанию
CREDENTIALS_PATH = "/etc/secrets/credentials.json"  # Путь к секретному файлу на Render

# === Инициализация ===
logger.info("Initializing bot...")
bot = telebot.TeleBot(TOKEN, threaded=False)
logger.info("Initializing Flask app...")
app = Flask(__name__)

# === Подключение к Google Sheets ===
logger.info("Attempting to connect to Google Sheets...")
sheet = None  # Инициализируем как None на случай ошибки
if not SPREADSHEET_ID:
    logger.error("FATAL: SPREADSHEET_ID environment variable is not set!")
else:
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        # Проверяем существование файла учетных данных
        if not os.path.exists(CREDENTIALS_PATH):
            logger.error(f"FATAL: Credentials file not found at {CREDENTIALS_PATH}!")
        else:
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                CREDENTIALS_PATH, scope
            )
            client = gspread.authorize(credentials)
            # Пытаемся открыть таблицу и лист
            spreadsheet = client.open_by_key(SPREADSHEET_ID)
            sheet = spreadsheet.worksheet(SHEET_NAME)
            logger.info(
                f"Successfully connected to Google Sheet: '{spreadsheet.title}', Worksheet: '{sheet.title}'"
            )

    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"FATAL: Worksheet '{SHEET_NAME}' not found in the Google Sheet!")
    except Exception as e:
        # Логируем любые другие ошибки при подключении
        logger.error(f"FATAL: Error connecting to Google Sheets: {e}", exc_info=True)
        # sheet остается None

# === Обработчики команд ===


@bot.message_handler(commands=["start"])
def start(message):
    chat_id = message.chat.id
    logger.info(f"Received /start command from chat ID: {chat_id}")
    try:
        # Отправляем приветственное сообщение
        bot.send_message(
            chat_id,
            "Привет! Я — бот помощник трейдера. Используй /add и /close для работы со сделками.",
        )
        logger.info(f"Sent start message reply to chat ID: {chat_id}")
    except Exception as e:
        # Логируем ошибку, если не удалось отправить сообщение
        logger.error(f"Error sending start message to {chat_id}: {e}", exc_info=True)


@bot.message_handler(commands=["add"])
def add_trade(message):
    chat_id = message.chat.id
    logger.info(f"Received /add command from {chat_id}: {message.text}")

    # Проверяем, удалось ли подключиться к таблице при старте
    if not sheet:
        logger.error("Google Sheet object is not initialized. Cannot add trade.")
        bot.reply_to(
            message,
            "Ошибка: Не удалось подключиться к Google Sheets при запуске. Проверьте логи сервера.",
        )
        return

    try:
        parts = message.text.split()
        # Ожидаем 7 частей: /add, пара, направление, цена_входа, тейк, стоп, сумма
        if len(parts) != 7:
            logger.warning(
                f"Invalid format for /add from {chat_id}. Parts: {len(parts)}"
            )
            bot.reply_to(
                message,
                "Неверный формат. Пример:\n`/add SOL/USDT Лонг 139.19 141.80 136.90 214.6`",
            )
            return

        # Извлекаем данные из сообщения
        _, asset, direction, entry_price_str, tp_str, sl_str, amount_str = parts
        today = datetime.datetime.now().strftime("%d.%m.%Y")

        # Формируем строку для добавления в таблицу
        # ВАЖНО: Убедитесь, что порядок и формулы соответствуют ВАШЕЙ таблице!
        # Особенно формулы в столбцах G и H. И количество пустых "" для остальных столбцов.
        new_row = [
            today,  # Дата входа (A)
            asset,  # Торгуемая пара (B)
            direction,  # Направление (C)
            float(entry_price_str),  # Цена входа (D)
            "",  # Факт. цена выхода (E) - оставляем пустой
            float(amount_str),  # Сумма ($) (F)
            # Формула для Результат ($) (G): ЕСЛИ(E5 пусто; ТО 0; ИНАЧЕ E5*F5 - D5*F5)
            '=IF(ISBLANK(INDIRECT("R"&ROW()&"C5",FALSE)), 0, INDIRECT("R"&ROW()&"C5",FALSE)*INDIRECT("R"&ROW()&"C6",FALSE) - INDIRECT("R"&ROW()&"C4",FALSE)*INDIRECT("R"&ROW()&"C6",FALSE))',
            # Формула для Результат (%) (H): ЕСЛИ(E5 пусто; ТО ""; ИНАЧЕ (E5-D5)/D5*100)
            '=IF(ISBLANK(INDIRECT("R"&ROW()&"C5",FALSE)), "", (INDIRECT("R"&ROW()&"C5",FALSE) - INDIRECT("R"&ROW()&"C4",FALSE))/INDIRECT("R"&ROW()&"C4",FALSE)*100)',
            float(sl_str),  # Стоп лосс ($) (I)
            float(tp_str),  # Тейк профит ($) (J)
            # Добавьте столько "", сколько у вас еще столбцов до конца строки
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",  # K L M N O P Q R - пример, уточни!
        ]
        logger.debug(f"Prepared row data: {new_row}")

        # Добавляем строку в таблицу
        sheet.append_row(
            new_row, value_input_option="USER_ENTERED"
        )  # USER_ENTERED чтобы формулы сработали
        logger.info(f"Appended row for {asset} to Google Sheet initiated by {chat_id}.")
        bot.reply_to(message, f"Сделка по {asset} добавлена!")

    except ValueError as e:
        logger.error(
            f"ValueError processing /add from {chat_id}: {e}. Check number format.",
            exc_info=True,
        )
        bot.reply_to(
            message, f"Ошибка в формате чисел: {e}. Проверьте цены, стоп, тейк и сумму."
        )
    except Exception as e:
        # Логируем любые другие ошибки при обработке /add
        logger.error(
            f"Error processing /add command from {chat_id}: {e}", exc_info=True
        )
        bot.reply_to(message, "Произошла непредвиденная ошибка при добавлении сделки.")


@bot.message_handler(commands=["close"])
def close_trade(message):
    chat_id = message.chat.id
    logger.info(f"Received /close command from {chat_id}: {message.text}")

    # Проверяем, удалось ли подключиться к таблице при старте
    if not sheet:
        logger.error("Google Sheet object is not initialized. Cannot close trade.")
        bot.reply_to(
            message,
            "Ошибка: Не удалось подключиться к Google Sheets при запуске. Проверьте логи сервера.",
        )
        return

    try:
        parts = message.text.split()
        # Ожидаем 3 части: /close, пара, цена_выхода
        if len(parts) != 3:
            logger.warning(
                f"Invalid format for /close from {chat_id}. Parts: {len(parts)}"
            )
            bot.reply_to(message, "Неверный формат. Пример:\n`/close SOL/USDT 140.55`")
            return

        _, asset_to_close, exit_price_str = parts
        exit_price = float(exit_price_str)
        today = datetime.datetime.now().strftime("%d.%m.%Y")

        # Получаем все значения листа (более эффективно для поиска, чем get_all_records)
        list_of_lists = sheet.get_all_values()
        logger.info(
            f"Fetched {len(list_of_lists)} rows (including header) from Google Sheet."
        )

        header_row = list_of_lists[0] if list_of_lists else []
        logger.debug(f"Header row: {header_row}")

        # ВАЖНО: Укажите ТОЧНЫЕ названия столбцов из вашей таблицы!
        asset_col_name = "Торгуемая пара (актив)"  # Пример, замените на ваше название
        exit_price_col_name = (
            "Факт. цена выхода ($)"  # Пример, замените на ваше название
        )
        # Укажите БУКВЫ столбцов для обновления
        exit_price_col_letter = "E"  # Пример, замените на вашу букву
        result_formula_col_letter = "G"  # Пример, замените на вашу букву
        exit_date_col_letter = (
            "P"  # Пример, замените на вашу букву (Дата закрытия факт)
        )
        # exit_fact_date_col_letter = "O" # Пример, если есть еще одна дата выхода (Дата выхода факт)

        # Находим индексы нужных столбцов по заголовкам
        try:
            asset_col_index = header_row.index(asset_col_name)
            exit_price_col_index = header_row.index(exit_price_col_name)
        except ValueError as e:
            logger.error(
                f"Column name mismatch: '{e}' not found in header row: {header_row}"
            )
            bot.reply_to(
                message,
                f"Ошибка: Не найден столбец '{e}' в заголовке таблицы. Проверьте названия.",
            )
            return

        found = False
        # Ищем сделку снизу вверх (предполагаем, что последние добавленные - самые актуальные)
        for i in range(
            len(list_of_lists) - 1, 0, -1
        ):  # Начинаем с последней строки, идем до 1 (исключая заголовок)
            row = list_of_lists[i]
            current_row_number = i + 1  # Номер строки в таблице (1-based)

            # Проверяем, что строка не пустая и содержит нужные столбцы
            if len(row) > max(asset_col_index, exit_price_col_index):
                asset_in_row = row[asset_col_index]
                exit_price_in_row = row[exit_price_col_index]

                # Ищем строку с нужным активом и ПУСТОЙ ценой выхода
                if asset_in_row == asset_to_close and (
                    exit_price_in_row == "" or exit_price_in_row is None
                ):
                    logger.info(
                        f"Found open trade for {asset_to_close} at row {current_row_number}. Closing..."
                    )

                    # Формируем данные для обновления ячеек
                    updates = []
                    updates.append(
                        {
                            "range": f"{exit_price_col_letter}{current_row_number}",
                            "values": [[exit_price]],
                        }
                    )
                    updates.append(
                        {
                            "range": f"{exit_date_col_letter}{current_row_number}",
                            "values": [[today]],
                        }
                    )
                    # Если есть вторая дата выхода - добавьте ее тоже
                    # updates.append({
                    #      'range': f"{exit_fact_date_col_letter}{current_row_number}",
                    #      'values': [[today]],
                    # })

                    # Обновляем ячейки одним запросом batch_update
                    sheet.batch_update(updates, value_input_option="USER_ENTERED")

                    # Обновление формулы результата делаем отдельно, т.к. batch_update может пересчитать ее до обновления цены выхода
                    # ВАЖНО: Убедитесь, что формула и буквы столбцов (E, F, D) верны для вашей таблицы!
                    # Формула будет такой же, как при добавлении, но с другими буквами.
                    # result_formula = f"={exit_price_col_letter}{current_row_number}*F{current_row_number} - D{current_row_number}*F{current_row_number}" # Пример, если стобцы E, F, D
                    # sheet.update(f"{result_formula_col_letter}{current_row_number}", result_formula, value_input_option='USER_ENTERED')
                    # ПРИМЕЧАНИЕ: Обновление формулы после batch_update может быть сложным.
                    # Возможно, проще будет записать статическое значение результата, рассчитанное в Python,
                    # или оставить формулу как есть (она должна пересчитаться сама после обновления цены выхода).
                    # Пока оставим без обновления формулы здесь, предполагая, что она в таблице уже есть и обновится сама.

                    logger.info(
                        f"Updated row {current_row_number} for closed trade {asset_to_close}."
                    )
                    bot.reply_to(
                        message, f"Сделка по {asset_to_close} закрыта по {exit_price}."
                    )
                    found = True
                    break  # Выходим из цикла, так как нашли и закрыли сделку

        if not found:
            logger.info(f"No open trade found for {asset_to_close}.")
            bot.reply_to(message, f"Не найдена ОТКРЫТАЯ сделка по {asset_to_close}.")

    except ValueError as e:
        logger.error(
            f"ValueError processing /close from {chat_id}: {e}. Check price format.",
            exc_info=True,
        )
        bot.reply_to(message, f"Ошибка в формате цены выхода: {e}.")
    except Exception as e:
        # Логируем любые другие ошибки при обработке /close
        logger.error(
            f"Error processing /close command from {chat_id}: {e}", exc_info=True
        )
        bot.reply_to(message, "Ошибка при закрытии сделки.")


@bot.message_handler(func=lambda message: True)  # Этот обработчик ловит ЛЮБОЕ сообщение
def echo_all(message):
    logger.info(
        f"Catch-all handler received message: '{message.text}' from {message.chat.id}"
    )
    # Можно раскомментировать следующую строку, чтобы бот отвечал тем же сообщением
    # bot.reply_to(message, f"Catch-all received: {message.text}")


# === Webhook-роут ===
# Этот роут принимает обновления от Telegram
@app.route(f"/{TOKEN}", methods=["POST"])
def webhook():
    # Логируем сам факт получения запроса вебхука
    logger.info("Webhook received!")
    try:
        # Получаем тело запроса
        json_str = request.get_data().decode("UTF-8")
        # Раскомментируйте следующую строку, если хотите видеть полный JSON обновления в логах (может быть очень много текста)
        # logger.debug(f"Update JSON: {json_str}")

        # Преобразуем JSON в объект Update библиотеки pyTelegramBotAPI
        update = telebot.types.Update.de_json(json_str)
        logger.info("Processing update...")
        # Передаем обновление в библиотеку для обработки и вызова нужного message_handler (@bot.message_handler)
        bot.process_new_updates([update])
        logger.info("Update processed.")
    except Exception as e:
        # Логируем ошибку, если что-то пошло не так при обработке вебхука
        logger.error(f"Error in webhook processing: {e}", exc_info=True)

    # Отвечаем Telegram "ok", чтобы он знал, что мы получили обновление
    return "ok", 200


# === Запуск Flask-сервера ===
# Эта часть НЕ используется при запуске через Gunicorn на Render.
# Gunicorn сам импортирует объект `app` и запускает его.
# Но можно оставить на случай локального запуска для отладки.
if __name__ == "__main__":
    # Этот блок выполнится только если запустить файл напрямую: > python app.py
    logger.info("Starting Flask development server (NOT FOR PRODUCTION/RENDER)")
    # На Render будет использоваться Gunicorn, а не этот сервер.
    # Не используйте app.run в production!
    port = int(os.environ.get("PORT", 10000))  # Render установит PORT
    # Запуск встроенного сервера Flask (только для локальной отладки)
    app.run(host="0.0.0.0", port=port, debug=False)  # debug=False для безопасности
