import os
import logging
import datetime
import time
import telebot
import gspread
from flask import Flask, request

# Используем google-auth
from google.oauth2.service_account import Credentials

# === Настройка логирования ===
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# === Константы ===
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  # ЗАГРУЖАЕМ ТОКЕН ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ!
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Таблица сделок")  # Имя листа по умолчанию
CREDENTIALS_PATH = "/etc/secrets/credentials.json"  # Путь к секретному файлу на Render
EXPECTED_COLUMNS = 29  # Ожидаемое количество столбцов (A-AC)

# === Инициализация ===
if not TOKEN:
    logger.error("FATAL: TELEGRAM_BOT_TOKEN environment variable is not set!")
    bot = None
else:
    logger.info("Initializing bot...")
    bot = telebot.TeleBot(TOKEN, threaded=False)  # threaded=False

logger.info("Initializing Flask app...")
app = Flask(__name__)

# === Подключение к Google Sheets ===
logger.info("Attempting to connect to Google Sheets...")
sheet = None
if not SPREADSHEET_ID:
    logger.error("FATAL: SPREADSHEET_ID environment variable is not set!")
elif not os.path.exists(CREDENTIALS_PATH):
    logger.error(f"FATAL: Credentials file not found at {CREDENTIALS_PATH}!")
else:
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive",
        ]
        credentials = Credentials.from_service_account_file(
            CREDENTIALS_PATH, scopes=scope
        )
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        sheet = spreadsheet.worksheet(SHEET_NAME)
        # Проверка количества столбцов
        actual_col_count = sheet.col_count
        if actual_col_count < EXPECTED_COLUMNS:
            logger.warning(
                f"WARNING: Sheet '{SHEET_NAME}' has only {actual_col_count} columns, expected {EXPECTED_COLUMNS} (A-AC). Some data might not fit."
            )
        logger.info(
            f"Successfully connected to Google Sheet: '{spreadsheet.title}', Worksheet: '{sheet.title}'"
        )
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"FATAL: Worksheet '{SHEET_NAME}' not found in the Google Sheet!")
        sheet = None
    except Exception as e:
        logger.error(f"FATAL: Error connecting to Google Sheets: {e}", exc_info=True)
        sheet = None

# === Обработчики команд ===


@bot.message_handler(commands=["start"])
def handle_start(message):
    # (Код для /start остается таким же, как в предыдущей версии)
    chat_id = message.chat.id
    logger.info(f"Received /start command from chat ID: {chat_id}")
    if not bot:
        logger.error("Bot is not initialized (token missing?). Cannot send message.")
        return
    try:
        bot.send_message(
            chat_id,
            "Привет! Я — бот помощник трейдера. Используй /add и /close для работы со сделками.",
        )
        logger.info(f"Sent start message reply to chat ID: {chat_id}")
    except Exception as e:
        logger.error(f"Error sending start message to {chat_id}: {e}", exc_info=True)


@bot.message_handler(commands=["add"])
def handle_add(message):
    chat_id = message.chat.id
    logger.info(f"Received /add command from {chat_id}: {message.text}")

    if not bot:
        return
    if not sheet:
        logger.error("Google Sheet object is not initialized. Cannot process /add.")
        bot.reply_to(
            message, "Ошибка: Нет подключения к Google Sheets. Проверьте логи сервера."
        )
        return

    try:
        parts = message.text.split()
        # /add <ПАРА> <ТИП> <ЦЕНА_ВХОДА> <ТЕЙК_ПРОФИТ> <СТОП_ЛОСС> <ОБЪЕМ_В_МОНЕТАХ> (7 частей)
        if len(parts) != 7:
            logger.warning(
                f"Invalid format for /add from {chat_id}. Parts: {len(parts)}"
            )
            bot.reply_to(
                message,
                "Неверный формат. Пример:\n`/add SOL/USDT Лонг 139.19 141.8 136.9 1.5`\n(Объем в конце - в МОНЕТАХ!)",
            )
            return

        # Извлекаем данные
        _, asset, direction, entry_price_str, tp_str, sl_str, amount_str = parts
        now = datetime.datetime.now()
        entry_date = now.strftime("%d.%m.%Y")
        entry_time = now.strftime("%H:%M:%S")

        # Формируем строку для A-AC (29 столбцов)
        new_row_data = [""] * EXPECTED_COLUMNS  # Создаем список из 29 пустых строк

        try:
            new_row_data[0] = entry_date  # A: Дата ВХОДА
            new_row_data[1] = entry_time  # B: Время ВХОДА
            # C, D (Выход) - остаются ""
            new_row_data[4] = asset  # E: Торгуемая пара
            new_row_data[5] = direction  # F: Тип сделки
            new_row_data[6] = float(entry_price_str)  # G: Цена входа
            new_row_data[7] = float(sl_str)  # H: Уровень Stop Loss
            new_row_data[8] = float(tp_str)  # I: Уровень Take Profit
            new_row_data[9] = float(amount_str)  # J: Объём сделки (в монетах)
            # K-AC остаются "" (или будут рассчитаны формулами в таблице)

        except ValueError as e:
            logger.error(
                f"ValueError converting numbers in /add from {chat_id}: {e}",
                exc_info=True,
            )
            bot.reply_to(
                message,
                f"Ошибка в формате чисел: {e}. Проверьте цены, стоп, тейк и объем.",
            )
            return
        except IndexError:
            logger.error(
                f"IndexError preparing row. Expected {EXPECTED_COLUMNS} columns."
            )
            bot.reply_to(
                message, "Внутренняя ошибка при подготовке данных для таблицы."
            )
            return

        logger.debug(f"Prepared row data (len={len(new_row_data)}): {new_row_data}")

        # --- ИЗМЕНЕНИЕ ЛОГИКИ: Вместо append_row ищем следующую пустую строку ---
        try:
            # Получаем все значения из первого столбца (A: Дата ВХОДА)
            col_a_values = sheet.col_values(1)  # col_values нумерует с 1
            # Ищем индекс последней непустой ячейки в столбце A
            last_data_row_index = (
                len(col_a_values) - 1
            )  # Индекс последней строки с ЛЮБЫМ значением
            while last_data_row_index > 0 and col_a_values[last_data_row_index] == "":
                last_data_row_index -= 1

            # Номер строки для вставки = индекс последней строки с данными + 1 (для 1-based нумерации) + 1 (для след. строки)
            insert_row_number = last_data_row_index + 2
            logger.info(
                f"Found last data in column A at index {last_data_row_index}. Inserting new row at sheet row {insert_row_number}."
            )

            # Вставляем данные в найденную строку
            # ВАЖНО: insert_row может сдвинуть формулы ниже, если они есть.
            # Если формулы протянуты на много строк вперед, лучше использовать update диапазона.
            # Попробуем сначала insert_row, он проще.
            sheet.insert_row(
                new_row_data, index=insert_row_number, value_input_option="USER_ENTERED"
            )

            # Альтернатива (если insert_row сдвигает формулы):
            # target_range = f"A{insert_row_number}:{gspread.utils.rowcol_to_a1(insert_row_number, EXPECTED_COLUMNS)}"
            # sheet.update(target_range, [new_row_data], value_input_option='USER_ENTERED')
            # logger.info(f"Updated range {target_range} for new trade.")

            logger.info(
                f"Inserted row for {asset} to Google Sheet at row {insert_row_number} initiated by {chat_id}."
            )
            bot.reply_to(
                message, f"Сделка по {asset} добавлена в строку {insert_row_number}!"
            )

        except Exception as e:
            logger.error(
                f"Error finding insert row or inserting data: {e}", exc_info=True
            )
            # Если вставка не удалась, пробуем старый append_row как запасной вариант? Или просто сообщаем об ошибке.
            # bot.reply_to(message, "Ошибка при вставке строки, попробуйте добавить вручную или проверьте таблицу.")
            # Пока оставим так, чтобы видеть ошибку.
            # Можно раскомментировать append_row ниже как fallback:
            # logger.warning("Inserting row failed, falling back to append_row")
            # sheet.append_row(new_row_data, value_input_option="USER_ENTERED")
            # bot.reply_to(message, f"Сделка по {asset} добавлена в конец таблицы (ошибка вставки).")
            raise  # Передаем ошибку дальше, чтобы увидеть traceback в логах

    except Exception as e:
        logger.error(
            f"General error processing /add command from {chat_id}: {e}", exc_info=True
        )
        bot.reply_to(message, "Произошла непредвиденная ошибка при добавлении сделки.")


@bot.message_handler(commands=["close"])
def handle_close(message):
    chat_id = message.chat.id
    logger.info(f"Received /close command from {chat_id}: {message.text}")

    if not bot:
        return
    if not sheet:
        logger.error("Google Sheet object is not initialized. Cannot process /close.")
        bot.reply_to(
            message, "Ошибка: Нет подключения к Google Sheets. Проверьте логи сервера."
        )
        return

    try:
        parts = message.text.split()
        # /close <ПАРА> <ФАКТ_ЦЕНА_ВЫХОДА> (3 части)
        if len(parts) != 3:
            logger.warning(
                f"Invalid format for /close from {chat_id}. Parts: {len(parts)}"
            )
            bot.reply_to(message, "Неверный формат. Пример:\n`/close SOL/USDT 140.55`")
            return

        _, asset_to_close, exit_price_str = parts
        exit_price = float(exit_price_str)  # Преобразуем цену выхода в число
        now = datetime.datetime.now()
        exit_date = now.strftime("%d.%m.%Y")
        exit_time = now.strftime("%H:%M:%S")
        exit_method = "вручную"

        # Получаем все значения листа
        list_of_lists = sheet.get_all_values()
        logger.info(
            f"Fetched {len(list_of_lists)} rows (including header) from Google Sheet."
        )

        header_row = list_of_lists[0] if list_of_lists else []
        logger.debug(f"Header row: {header_row}")

        # ТОЧНЫЕ названия столбцов из ТВОЕЙ таблицы!
        asset_col_name = "Торгуемая пара (актив)"  # Колонка E
        actual_exit_price_col_name = "Фактическая цена выхода ($)"  # Колонка T

        # Буквы столбцов для обновления
        exit_date_col_letter = "C"
        exit_time_col_letter = "D"
        exit_method_col_letter = "S"
        actual_exit_price_col_letter = "T"

        # Находим индексы нужных столбцов по заголовкам
        try:
            # Индекс столбца E (Торгуемая пара)
            asset_col_index = header_row.index(asset_col_name)
            # Индекс столбца T (Факт. цена выхода) - для проверки на пустоту
            actual_exit_price_col_index = header_row.index(actual_exit_price_col_name)
        except ValueError as e:
            logger.error(
                f"Column name mismatch: '{e}' not found in header row: {header_row}"
            )
            bot.reply_to(
                message,
                f"Критическая ошибка: Не найден столбец '{e}' в заголовке таблицы. Проверьте названия.",
            )
            return
        except IndexError:
            logger.error(f"Header row not found or empty.")
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
                        f"Found open trade for {asset_to_close} at row {current_row_number}. Closing..."
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
                        f"Updated row {current_row_number} for closed trade {asset_to_close}."
                    )
                    bot.reply_to(
                        message, f"Сделка по {asset_to_close} закрыта по {exit_price}."
                    )
                    found = True
                    break

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
        return "error", 500
    try:
        json_str = request.get_data().decode("UTF-8")
        # logger.debug(f"Update JSON: {json_str}")
        update = telebot.types.Update.de_json(json_str)
        logger.info("Processing update...")
        bot.process_new_updates([update])
        logger.info("Update processed.")
    except Exception as e:
        logger.error(f"Error in webhook processing: {e}", exc_info=True)
    return "ok", 200


# === Запуск Flask-сервера (Не используется Gunicorn'ом) ===
if __name__ == "__main__":
    logger.info("Starting Flask development server (NOT FOR PRODUCTION/RENDER)")
    if bot:
        port = int(os.environ.get("PORT", 10000))
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        logger.error(
            "Could not start development server: Bot not initialized (Missing Token?)"
        )
