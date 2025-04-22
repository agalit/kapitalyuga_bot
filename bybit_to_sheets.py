import os
import logging
import datetime
import time

# Все стандартные библиотеки вместе

# Все сторонние библиотеки вместе
import telebot
import gspread
from google.oauth2.service_account import Credentials
from pybit.unified_trading import HTTP
from dotenv import load_dotenv

# --- УПРОЩЕННЫЕ ОТЛАДОЧНЫЕ ВЫВОДЫ ---
print(f"Текущая рабочая папка: {os.getcwd()}")
# Просто вызываем load_dotenv() без аргументов, он сам найдет .env
load_success = load_dotenv()
print(
    f"load_dotenv() выполнился успешно: {load_success}"
)  # Должно быть True, если .env найден и прочитан
# Проверяем значение переменной СРАЗУ после загрузки
spreadsheet_id_value = os.getenv("SPREADSHEET_ID")
print(
    f"Значение SPREADSHEET_ID после load_dotenv(): '{spreadsheet_id_value}'"
)  # Покажет, что считалось
# --- КОНЕЦ ОТЛАДОЧНЫХ ВЫВОДОВ ---

# === Настройка логирования ===
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# === Константы и Загрузка Настроек ===

# --- Google Sheets ---
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Таблица сделок")  # Имя листа по умолчанию
# Путь к файлу credentials.json, который должен лежать рядом со скриптом ИЛИ в секретах Render
# Если используешь секретный файл Render, путь будет /etc/secrets/credentials.json
CREDENTIALS_PATH = "credentials.json"
# Индексы столбцов (начиная с 0) для структуры A-AC (29 столбцов)
# ВНИМАНИЕ: Перепроверь соответствие твоей финальной структуре таблицы!
COL_IDX = {
    "entry_date": 0,  # A: Дата ВХОДА
    "entry_time": 1,  # B: Время ВХОДА
    "exit_date": 2,  # C: Дата ВЫХОДА
    "exit_time": 3,  # D: Время ВЫХОДА
    "pair": 4,  # E: Торгуемая пара (актив)
    "type": 5,  # F: Тип сделки (Лонг / Шорт)
    "entry_price": 6,  # G: Цена входа ($)
    "sl_price": 7,  # H: Уровень Stop Loss ($) - Не приходит из PNL API
    "tp_price": 8,  # I: Уровень Take Profit ($) - Не приходит из PNL API
    "volume_coins": 9,  # J: Объём сделки (в монетах)
    # K-P: Расчетные (Объем $, План PNL $, План PNL %, RRR) - Не трогаем
    "commission_entry": 16,  # Q: Комиссия входа - Не приходит из PNL API
    "commission_exit": 17,  # R: Комиссия выхода - Не приходит из PNL API
    "exit_method": 18,  # S: Способ выхода (TP / SL / вручную) - Получаем тип ордера Bybit
    "exit_price_actual": 19,  # T: Фактическая цена выхода ($)
    # U: Факт. PnL ($) - Формула в таблице
    # V: Чистый PnL ($) - Формула в таблице
    # W: Время жизни сделки - Формула в таблице
    # X: День недели входа - Формула в таблице
    # Y: Что сработало - Пусто
    # Z: Что пошло не так - Пусто
    # AA: Причина входа - Пусто
    # AB: Выводы по сделке - Пусто
    "bybit_exec_id": 28,  # AC: Bybit ID (для проверки дубликатов)
}
EXPECTED_COLUMNS = 29  # A-AC

# --- Bybit ---
BYBIT_ENV = os.getenv("BYBIT_ENV", "LIVE").upper()  # LIVE или TESTNET
# Категория для запроса PNL (linear = USDT/USDC контракты)
BYBIT_CATEGORY = os.getenv("BYBIT_CATEGORY", "linear")

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # Твой ID чата для уведомлений

# === Инициализация Telegram бота (если настроен) ===
bot = None
if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    try:
        bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN, threaded=False)
        logger.info(
            f"Telegram bot initialized for notifications to chat ID: {TELEGRAM_CHAT_ID}"
        )
    except Exception as e:
        logger.error(f"Failed to initialize Telegram bot: {e}")
        bot = None

# === Функции ===


def send_telegram_notification(message):
    """Отправляет простое текстовое уведомление в Telegram."""
    if bot and TELEGRAM_CHAT_ID:
        try:
            logger.info(
                f"Sending plain text notification to Telegram chat ID {TELEGRAM_CHAT_ID}..."
            )
            # Отправляем без parse_mode
            bot.send_message(TELEGRAM_CHAT_ID, message)
            logger.info("Notification sent successfully.")
            time.sleep(1)  # Небольшая пауза
        except Exception as e:
            logger.error(f"Failed to send Telegram notification: {e}", exc_info=True)
    else:
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            logger.warning(
                "Telegram bot object not available or chat ID missing. Cannot send notification."
            )


def connect_google_sheets():
    """Подключается к Google Sheets."""
    logger.info("Attempting to connect to Google Sheets...")
    sheet_instance = None
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
            sheet_instance = spreadsheet.worksheet(SHEET_NAME)
            logger.info(
                f"Successfully connected to Google Sheet: '{spreadsheet.title}', Worksheet: '{sheet_instance.title}'"
            )
            actual_col_count = sheet_instance.col_count
            if actual_col_count < EXPECTED_COLUMNS:
                logger.warning(
                    f"WARNING: Sheet '{SHEET_NAME}' has {actual_col_count} columns, expected {EXPECTED_COLUMNS} (A-AC)."
                )
        except gspread.exceptions.WorksheetNotFound:
            logger.error(
                f"FATAL: Worksheet '{SHEET_NAME}' not found in the Google Sheet!"
            )
            sheet_instance = None
        except Exception as e:
            logger.error(
                f"FATAL: Error connecting to Google Sheets: {e}", exc_info=True
            )
            sheet_instance = None
    return sheet_instance


# ИСПРАВЛЕННАЯ ФУНКЦИЯ (заменить существующую connect_bybit)
def connect_bybit():
    """Подключается к Bybit API (Live или Testnet), читая ключи из ПЕРЕМЕННЫХ ОКРУЖЕНИЯ."""
    # Определяем окружение ('LIVE' по умолчанию)
    env = os.getenv("BYBIT_ENV", "LIVE").upper()
    logger.info(f"Attempting to connect to Bybit {env} environment...")

    api_key = None
    api_secret = None
    testnet_flag = False

    # Выбираем правильные переменные для ключей
    if env == "TESTNET":
        api_key = os.getenv("BYBIT_API_KEY_TESTNET")
        api_secret = os.getenv("BYBIT_API_SECRET_TESTNET")
        testnet_flag = True
        if not api_key or not api_secret:
            logger.error(
                "FATAL: BYBIT_API_KEY_TESTNET or BYBIT_API_SECRET_TESTNET environment variable not set!"
            )
            return None
    else:  # По умолчанию или если BYBIT_ENV="LIVE"
        env = "LIVE"
        api_key = os.getenv("BYBIT_API_KEY_LIVE")
        api_secret = os.getenv("BYBIT_API_SECRET_LIVE")
        testnet_flag = False
        if not api_key or not api_secret:
            logger.error(
                "FATAL: BYBIT_API_KEY_LIVE or BYBIT_API_SECRET_LIVE environment variable not set!"
            )
            return None

    session = None
    try:
        # Подключаемся с правильным флагом testnet и ключами из переменных окружения
        session = HTTP(testnet=testnet_flag, api_key=api_key, api_secret=api_secret)
        logger.info(
            f"Successfully initialized Bybit API connection for {env} (credentials loaded from environment)."
        )
        # Опционально: можно добавить тестовый запрос для проверки ключей
        # logger.info(f"Trying to get wallet balance to check keys...")
        # balance_check = session.get_wallet_balance(accountType="UNIFIED")
        # if balance_check.get('retCode') != 0:
        #      logger.error(f"Bybit API connection check failed: {balance_check}")
        #      return None
    except Exception as e:
        logger.error(f"FATAL: Error connecting to Bybit {env} API: {e}", exc_info=True)
        session = None
    return session


def get_existing_exec_ids(sheet_instance):
    """Получает список ID сделок Bybit из столбца AC."""
    logger.info("Fetching existing execution IDs from Google Sheet...")
    try:
        # Столбец AC это 29-й столбец
        col_index_for_id = COL_IDX["bybit_exec_id"] + 1
        if col_index_for_id > sheet_instance.col_count:
            logger.warning(
                f"Column AC (index {col_index_for_id}) for Bybit ID seems to be missing in the sheet. Cannot check for duplicates."
            )
            return set()

        existing_ids = sheet_instance.col_values(
            col_index_for_id, value_render_option="FORMATTED_VALUE"
        )
        # Удаляем заголовок, если он есть и совпадает (регистронезависимо)
        if (
            existing_ids
            and isinstance(existing_ids[0], str)
            and existing_ids[0].lower() == "bybit id"
        ):
            existing_ids = existing_ids[1:]
        # Оставляем только непустые ID
        existing_ids_set = set(filter(None, existing_ids))
        logger.info(f"Found {len(existing_ids_set)} existing Bybit IDs in column AC.")
        return existing_ids_set
    except Exception as e:
        logger.error(f"Error fetching existing Bybit IDs: {e}", exc_info=True)
        return set()  # Возвращаем пустой сет в случае ошибки


def fetch_bybit_closed_pnl(session, start_time_ms=None, end_time_ms=None, limit=50):
    """Запрашивает историю закрытых PNL с Bybit с пагинацией."""
    logger.info(
        f"Fetching closed PNL from Bybit (Category: {BYBIT_CATEGORY}, Limit: {limit})..."
    )
    # --- ДОБАВЛЕНО ДЛЯ ОТЛАДКИ ---
    logger.info(
        f"DEBUG: Requesting Bybit with Start MS: {start_time_ms}, End MS: {end_time_ms}"
    )  # Используем INFO для видимости
    # ---------------------------
    all_results = []
    cursor = None
    page_count = 0
    max_pages = 20  # Ограничение на всякий случай, чтобы не уйти в бесконечный цикл
    try:
        while page_count < max_pages:
            page_count += 1
            params = {
                "category": BYBIT_CATEGORY,
                "limit": limit,
            }
            if start_time_ms:
                params["startTime"] = start_time_ms
            if end_time_ms:
                params["endTime"] = end_time_ms
            if cursor:
                params["cursor"] = cursor

            logger.debug(
                f"Requesting Bybit page {page_count} with params: {params}"
            )  # Оставим DEBUG для деталей запроса
            response = session.get_closed_pnl(**params)
            # --- ДОБАВЛЕНО ДЛЯ ОТЛАДКИ ---
            # Логируем сырой ответ с уровнем INFO, чтобы точно увидеть
            logger.info(f"DEBUG: Raw Bybit Response (Page {page_count}): {response}")
            # ---------------------------

            if response and response.get("retCode") == 0:
                result = response.get("result", {})
                data_list = result.get("list", [])
                if data_list:  # Проверяем, что список не пустой
                    all_results.extend(data_list)
                    logger.info(
                        f"Fetched {len(data_list)} records from Bybit (page {page_count}). Total: {len(all_results)}"
                    )
                else:
                    logger.info(
                        f"Received empty list on page {page_count}. Assuming end of data."
                    )
                    break  # Выходим, если пришел пустой список

                cursor = result.get("nextPageCursor")
                if not cursor:
                    logger.info("No more pages (nextPageCursor is empty).")
                    break
                else:
                    # Логируем только часть курсора для краткости
                    log_cursor = (
                        cursor[:10] + "..." if cursor and len(cursor) > 10 else cursor
                    )
                    logger.info(
                        f"Found nextPageCursor: {log_cursor}. Fetching next page..."
                    )
                    time.sleep(0.5)  # Пауза между запросами API
            else:
                # Логируем ошибку, если retCode не 0
                logger.error(
                    f"Error response fetching closed PNL from Bybit (retCode={response.get('retCode')}, retMsg='{response.get('retMsg')}'). Response: {response}"
                )
                break  # Выходим при ошибке API

        # Проверка, если достигли лимита страниц
        if page_count >= max_pages:
            logger.warning(
                f"Reached maximum page limit ({max_pages}). Stopped fetching potentially incomplete data."
            )

    except Exception as e:
        logger.error(f"Exception during Bybit API call: {e}", exc_info=True)
        # Возвращаем пустой список в случае исключения
        return []

    logger.info(f"Total closed PNL records fetched: {len(all_results)}")
    return all_results


def parse_and_prepare_sheet_data(pnl_records, existing_ids):
    """Парсит данные от Bybit и готовит строки для записи в таблицу, пропуская дубликаты."""
    rows_to_add = []
    new_ids_added = set()
    logger.info(f"Parsing {len(pnl_records)} fetched records from Bybit...")

    # Сортируем записи по времени обновления (от старых к новым), чтобы добавлять в правильном порядке
    pnl_records_sorted = sorted(pnl_records, key=lambda x: int(x.get("updatedTime", 0)))

    for record in pnl_records_sorted:
        # Определяем уникальный ID для этой записи PNL
        unique_id = None
        updated_time_ms_str = record.get("updatedTime")
        symbol = record.get("symbol", "UNKNOWN")
        if updated_time_ms_str:
            # Используем комбинацию symbol + updatedTime как ID
            unique_id = f"{symbol}_{updated_time_ms_str}"
        else:
            # В крайнем случае, если нет времени, используем orderId или пропускаем
            order_id = record.get("orderId")
            if order_id:
                unique_id = order_id
                logger.warning(
                    f"Record for symbol {symbol} missing 'updatedTime', using 'orderId' ({order_id}) as unique ID. This might cause issues."
                )
            else:
                logger.warning(
                    f"Record for symbol {symbol} skipped: missing unique identifier (updatedTime or orderId). Record: {record}"
                )
                continue

        # Проверяем на дубликат по нашему уникальному ID
        if unique_id in existing_ids or unique_id in new_ids_added:
            # logger.debug(f"Skipping duplicate record with ID: {unique_id}")
            continue

        logger.info(f"Processing new record with ID: {unique_id}")
        row_data = [""] * EXPECTED_COLUMNS

        try:
            # Время выхода (из updatedTime)
            if updated_time_ms_str:
                exit_ts_ms = int(updated_time_ms_str)
                exit_dt = datetime.datetime.fromtimestamp(exit_ts_ms / 1000)
                row_data[COL_IDX["exit_date"]] = exit_dt.strftime("%d.%m.%Y")  # C
                row_data[COL_IDX["exit_time"]] = exit_dt.strftime(
                    "%H.%M.%S"
                )  # D - Используем точки для времени тоже? Уточни формат. Или "%H:%M:%S"

            # Время входа (из createdTime) - В PNL записи это время СОЗДАНИЯ ПОЗИЦИИ
            entry_ts_ms_str = record.get("createdTime")
            if entry_ts_ms_str:
                entry_ts_ms = int(entry_ts_ms_str)
                entry_dt = datetime.datetime.fromtimestamp(entry_ts_ms / 1000)
                row_data[COL_IDX["entry_date"]] = entry_dt.strftime("%d.%m.%Y")  # A
                row_data[COL_IDX["entry_time"]] = entry_dt.strftime("%H.%M.%S")  # B

            row_data[COL_IDX["pair"]] = symbol  # E
            side = record.get("side", "").capitalize()
            row_data[COL_IDX["type"]] = (
                "Лонг" if side == "Buy" else ("Шорт" if side == "Sell" else side)
            )  # F
            row_data[COL_IDX["entry_price"]] = float(
                record.get("avgEntryPrice", 0)
            )  # G
            # SL/TP (H, I) - нет в PNL API
            row_data[COL_IDX["volume_coins"]] = float(record.get("qty", 0))  # J
            # K-R - расчетные или пустые
            # S (Способ выхода) - API PNL не дает его явно. Можно попробовать orderType
            row_data[COL_IDX["exit_method"]] = record.get("orderType", "")  # S
            row_data[COL_IDX["exit_price_actual"]] = float(
                record.get("avgExitPrice", 0)
            )  # T
            # U-AB - расчетные или пустые
            row_data[COL_IDX["bybit_exec_id"]] = (
                unique_id  # AC - Уникальный ID, который мы создали
            )

            rows_to_add.append(row_data)
            new_ids_added.add(unique_id)

        except ValueError as e:
            logger.error(
                f"ValueError parsing record ID {unique_id}: {e}. Record: {record}",
                exc_info=True,
            )
        except Exception as e:
            logger.error(
                f"Unexpected error parsing record ID {unique_id}: {e}. Record: {record}",
                exc_info=True,
            )

    logger.info(f"Prepared {len(rows_to_add)} new rows for Google Sheet.")
    return rows_to_add


def add_data_to_sheet(sheet_instance, data_rows):
    """Добавляет подготовленные строки данных в таблицу через batch_update."""
    if not data_rows:
        logger.info("No new data rows to add to the sheet.")
        return 0

    logger.info(f"Adding {len(data_rows)} rows to Google Sheet...")
    try:
        logger.debug("Fetching column A values to find next empty row...")
        col_a_values = sheet_instance.col_values(
            1, value_render_option="UNFORMATTED_VALUE"
        )
        logger.debug(f"Found {len(col_a_values)} values in column A.")
        last_data_row_index = len(col_a_values) - 1
        while (
            last_data_row_index > 0
            and str(col_a_values[last_data_row_index]).strip() == ""
        ):
            last_data_row_index -= 1
        target_row_number = last_data_row_index + 2
        logger.info(f"Target starting row for update is {target_row_number}.")

        updates = []
        current_row = target_row_number
        for row_data in data_rows:
            # Обновляем только те ячейки, для которых у нас есть данные из API
            # A, B, C, D, E, F, G, J, S, T, AC
            cell_updates = [
                {
                    "range": f"A{current_row}",
                    "values": [[row_data[COL_IDX["entry_date"]]]],
                },
                {
                    "range": f"B{current_row}",
                    "values": [[row_data[COL_IDX["entry_time"]]]],
                },
                {
                    "range": f"C{current_row}",
                    "values": [[row_data[COL_IDX["exit_date"]]]],
                },
                {
                    "range": f"D{current_row}",
                    "values": [[row_data[COL_IDX["exit_time"]]]],
                },
                {"range": f"E{current_row}", "values": [[row_data[COL_IDX["pair"]]]]},
                {"range": f"F{current_row}", "values": [[row_data[COL_IDX["type"]]]]},
                {
                    "range": f"G{current_row}",
                    "values": [[row_data[COL_IDX["entry_price"]]]],
                },
                # H, I (SL/TP) - Пропускаем
                {
                    "range": f"J{current_row}",
                    "values": [[row_data[COL_IDX["volume_coins"]]]],
                },
                # K-R - Пропускаем (расчетные/комиссии)
                {
                    "range": f"S{current_row}",
                    "values": [[row_data[COL_IDX["exit_method"]]]],
                },  # Способ выхода (тип ордера Bybit)
                {
                    "range": f"T{current_row}",
                    "values": [[row_data[COL_IDX["exit_price_actual"]]]],
                },  # Факт. цена выхода
                # U-AB - Пропускаем (расчетные/текст)
                {
                    "range": f"AC{current_row}",
                    "values": [[row_data[COL_IDX["bybit_exec_id"]]]],
                },  # Записываем ID Bybit
            ]
            # Фильтруем обновления, где значение None (на всякий случай)
            updates.extend(
                [upd for upd in cell_updates if upd["values"][0][0] is not None]
            )
            current_row += 1

        if updates:
            logger.debug(f"Executing batch update for {len(data_rows)} rows.")
            sheet_instance.batch_update(updates, value_input_option="USER_ENTERED")
            logger.info(
                f"Successfully added/updated {len(data_rows)} rows in the sheet."
            )
            return len(data_rows)
        else:
            logger.info("No updates to execute.")
            return 0

    except Exception as e:
        logger.error(f"Error adding data to sheet: {e}", exc_info=True)
        return -1  # Возвращаем -1 в случае ошибки


# === Основной блок выполнения ===
if __name__ == "__main__":
    start_run_time = time.time()
    logger.info(f"----- Script started at {datetime.datetime.now()} -----")

    # 1. Подключаемся к Google Sheets
    sheet = connect_google_sheets()
    if not sheet:
        send_telegram_notification(
            "❌ *Ошибка:* Не удалось подключиться к Google Sheets\\."
        )
        exit()

    # 2. Подключаемся к Bybit
    bybit_session = connect_bybit()
    if not bybit_session:
        send_telegram_notification(
            "❌ *Ошибка:* Не удалось подключиться к Bybit API\\."
        )
        exit()

    # 3. Получаем ID уже существующих сделок из таблицы
    existing_ids = get_existing_exec_ids(sheet)

    # 4. Запрашиваем закрытые PNL с Bybit
    # Устанавливаем период выборки (например, последние 7 дней)
    # TODO: Сделать период настраиваемым (например, через аргументы командной строки или env переменные)
    try:
        days_to_fetch = int(os.getenv("BYBIT_FETCH_DAYS", "7"))
    except ValueError:
        days_to_fetch = 7
    logger.info(f"Fetching data for the last {days_to_fetch} days.")
    end_dt = datetime.datetime.now()
    start_dt = end_dt - datetime.timedelta(days=days_to_fetch)
    start_timestamp_ms = int(start_dt.timestamp() * 1000)
    end_timestamp_ms = int(end_dt.timestamp() * 1000)

    closed_pnl_records = fetch_bybit_closed_pnl(
        bybit_session, start_timestamp_ms, end_timestamp_ms, limit=100
    )

    # 5. Парсим данные и готовим строки для таблицы
    new_data_rows = parse_and_prepare_sheet_data(closed_pnl_records, existing_ids)

    # 6. Добавляем новые данные в таблицу
    added_count = add_data_to_sheet(sheet, new_data_rows)

    # 7. Отправляем уведомление
    if added_count > 0:
        summary_message = f"✅ Успешно добавлено *{added_count}* новых закрытых сделок Bybit ({BYBIT_CATEGORY}) в таблицу\\."
        logger.info(summary_message)
        send_telegram_notification(summary_message)
    elif added_count == 0:
        logger.info("Новых закрытых сделок для добавления не найдено.")
        # send_telegram_notification(f"ℹ️ Новых закрытых сделок Bybit ({BYBIT_CATEGORY}) не найдено за последние {days_to_fetch} дней\\.")
    else:  # added_count == -1
        error_message = f"❌ Произошла ошибка при добавлении данных Bybit ({BYBIT_CATEGORY}) в Google Sheet\\."
        logger.error(error_message)
        send_telegram_notification(error_message)

    end_run_time = time.time()
    logger.info(
        f"----- Script finished in {end_run_time - start_run_time:.2f} seconds -----"
    )
