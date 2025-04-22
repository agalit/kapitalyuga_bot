# -*- coding: utf-8 -*-
import os
import logging
import datetime
import time
import sys
from decimal import Decimal, ROUND_HALF_UP

# Сторонние библиотеки
import telebot  # Для уведомлений
import gspread

# Используем современную библиотеку google-auth
from google.oauth2.service_account import Credentials

# Используем библиотеку pybit для Bybit API v5
from pybit.unified_trading import HTTP

# Используем dotenv для загрузки .env файла при локальном запуске
from dotenv import load_dotenv

# --- ЗАГРУЗКА ПЕРЕМЕННЫХ ИЗ .ENV ---
# Вызываем load_dotenv() ПОСЛЕ всех импортов, но ДО чтения переменных
# Загружает переменные из .env файла (если он есть)
load_dotenv()
# ----------------------------------

# === Настройка логирования ===
# Улучшенный формат логирования
log_format = (
    "%(asctime)s - %(levelname)-8s - %(name)s - %(funcName)s:%(lineno)d - %(message)s"
)
logging.basicConfig(level=logging.INFO, format=log_format, stream=sys.stdout)
# Уменьшаем уровень логирования для библиотек
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("google.auth.transport.requests").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# === Константы и Загрузка Настроек ===

# --- Google Sheets ---
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Таблица сделок")  # Имя листа по умолчанию
# Путь к файлу credentials.json (ищем рядом со скриптом)
CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")

# --- Bybit ---
# Устанавливаем окружение по умолчанию TESTNET для этого скрипта
BYBIT_ENV = os.getenv("BYBIT_ENV", "TESTNET").upper()
# Категория для запроса исполнений (linear = USDT/USDC контракты)
BYBIT_CATEGORY = os.getenv("BYBIT_CATEGORY", "linear")
# Количество дней для запроса истории
try:
    BYBIT_FETCH_DAYS = int(os.getenv("BYBIT_FETCH_DAYS", "7"))
except ValueError:
    BYBIT_FETCH_DAYS = 7
# Лимит записей на страницу API
BYBIT_API_LIMIT = 100
# Максимальное кол-во страниц для запроса (защита от бесконечного цикла)
MAX_API_PAGES = 50
# Допуск для сравнения количества (из-за точности float/Decimal)
QTY_TOLERANCE = Decimal("1e-9")

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- Структура таблицы Google Sheets ---
# Индексы столбцов (начиная с 1 для gspread) и их назначение
# A-AC (29 столбцов) - перепроверь свою структуру!
# Мы будем заполнять: A, B, C, D, E, F, G, J, Q, R, S, T, AC
# Остальные предполагаются пустыми, расчетными в таблице или не получаемыми из API исполнений
COL_IDX = {
    "entry_date": 1,  # A: Дата ВХОДА (первого исполнения)
    "entry_time": 2,  # B: Время ВХОДА (первого исполнения)
    "exit_date": 3,  # C: Дата ВЫХОДА (последнего закр. исполнения)
    "exit_time": 4,  # D: Время ВЫХОДА (последнего закр. исполнения)
    "pair": 5,  # E: Торгуемая пара (актив)
    "type": 6,  # F: Тип сделки (Лонг / Шорт)
    "entry_price_avg": 7,  # G: Средняя Цена входа ($)
    # "sl_price": 8,        # H: Уровень Stop Loss ($) - Не приходит из API executions
    # "tp_price": 9,        # I: Уровень Take Profit ($) - Не приходит из API executions
    "volume_coins": 10,  # J: Объём сделки (в монетах) - Суммарный закрытый
    # K-P: Расчетные - Не трогаем
    "commission_entry": 17,  # Q: Суммарная комиссия входа ($)
    "commission_exit": 18,  # R: Суммарная комиссия выхода ($)
    "exit_method": 19,  # S: Способ выхода (Order Type посл. закр. исполнения)
    "exit_price_avg": 20,  # T: Средняя Цена выхода ($)
    # U: Факт. PnL ($) - Формула в таблице
    # V: Чистый PnL ($) - Формула в таблице
    # W: Время жизни сделки - Формула в таблице
    # X: День недели входа - Формула в таблице
    # Y-AB: Текстовые/Пустые - Не трогаем
    "involved_exec_ids": 29,  # AC: Bybit ID всех исполнений сделки (через запятую)
}
# Ожидаемое минимальное количество столбцов в таблице для работы скрипта
EXPECTED_MIN_COLUMNS = max(COL_IDX.values())


# === Инициализация Telegram бота (если настроен) ===
bot = None
if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    try:
        # threaded=False может быть важно в некоторых окружениях (например, AWS Lambda)
        bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN, threaded=False)
        logger.info(f"Telegram bot initialized for chat ID: {TELEGRAM_CHAT_ID}")
    except Exception as e:
        logger.error(f"Failed to initialize Telegram bot: {e}", exc_info=True)
        bot = None
else:
    logger.warning(
        "Telegram BOT_TOKEN or CHAT_ID not found in environment variables. Notifications disabled."
    )

# === Функции ===


def send_telegram_notification(message, parse_mode="Markdown"):
    """Отправляет уведомление в Telegram с указанным режимом форматирования."""
    if bot and TELEGRAM_CHAT_ID:
        try:
            logger.info(
                f"Sending notification to Telegram chat ID {TELEGRAM_CHAT_ID}..."
            )
            bot.send_message(
                TELEGRAM_CHAT_ID,
                message,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
            logger.info("Notification sent successfully.")
            # time.sleep(1) # Убрано, т.к. обычно не требуется для одиночных уведомлений
        except Exception as e:
            logger.error(f"Failed to send Telegram notification: {e}", exc_info=True)
    elif not bot:
        # Логируем само сообщение, если бот не инициализирован, чтобы не терять информацию
        logger.warning(f"Telegram bot not available. Notification content: {message}")


def connect_google_sheets():
    """Подключается к Google Sheets, проверяет наличие листа и количество столбцов."""
    logger.info("Attempting to connect to Google Sheets...")
    sheet_instance = None
    if not SPREADSHEET_ID:
        logger.error("FATAL: SPREADSHEET_ID environment variable is not set!")
        return None
    if not os.path.exists(CREDENTIALS_PATH):
        logger.error(f"FATAL: Credentials file not found at {CREDENTIALS_PATH}!")
        logger.error(
            "Please ensure the GOOGLE_CREDENTIALS_PATH variable points to the correct file or place 'credentials.json' next to the script."
        )
        return None

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
        try:
            sheet_instance = spreadsheet.worksheet(SHEET_NAME)
            logger.info(
                f"Successfully connected to Google Sheet: '{spreadsheet.title}', Worksheet: '{sheet_instance.title}'"
            )

            # Проверка количества столбцов
            actual_col_count = sheet_instance.col_count
            if actual_col_count < EXPECTED_MIN_COLUMNS:
                logger.error(
                    f"FATAL: Worksheet '{SHEET_NAME}' has only {actual_col_count} columns, but expected at least {EXPECTED_MIN_COLUMNS} (up to column AC). Cannot proceed."
                )
                send_telegram_notification(
                    f"❌ *Ошибка:* В таблице '{SHEET_NAME}' всего {actual_col_count} столбцов. Требуется минимум {EXPECTED_MIN_COLUMNS} (до AC)."
                )
                return None  # Не возвращаем лист, если столбцов недостаточно

        except gspread.exceptions.WorksheetNotFound:
            logger.error(
                f"FATAL: Worksheet '{SHEET_NAME}' not found in the Google Sheet!"
            )
            send_telegram_notification(
                f"❌ *Ошибка:* Лист '{SHEET_NAME}' не найден в Google Sheets!"
            )
            sheet_instance = None

    except Exception as e:
        logger.error(f"FATAL: Error connecting to Google Sheets: {e}", exc_info=True)
        send_telegram_notification(
            f"❌ *Ошибка:* Не удалось подключиться к Google Sheets. Детали в логах."
        )
        sheet_instance = None

    return sheet_instance


def connect_bybit():
    """Подключается к Bybit API (Live или Testnet), читая ключи из ПЕРЕМЕННЫХ ОКРУЖЕНИЯ."""
    logger.info(f"Attempting to connect to Bybit {BYBIT_ENV} environment...")

    api_key = None
    api_secret = None
    testnet_flag = False

    if BYBIT_ENV == "TESTNET":
        api_key = os.getenv("BYBIT_API_KEY_TESTNET")
        api_secret = os.getenv("BYBIT_API_SECRET_TESTNET")
        testnet_flag = True
        if not api_key or not api_secret:
            logger.error(
                "FATAL: BYBIT_API_KEY_TESTNET or BYBIT_API_SECRET_TESTNET environment variable not set for TESTNET!"
            )
            return None
    elif BYBIT_ENV == "LIVE":
        api_key = os.getenv("BYBIT_API_KEY_LIVE")
        api_secret = os.getenv("BYBIT_API_SECRET_LIVE")
        testnet_flag = False
        if not api_key or not api_secret:
            logger.error(
                "FATAL: BYBIT_API_KEY_LIVE or BYBIT_API_SECRET_LIVE environment variable not set for LIVE!"
            )
            return None
    else:
        logger.error(
            f"FATAL: Invalid BYBIT_ENV value '{BYBIT_ENV}'. Must be 'TESTNET' or 'LIVE'."
        )
        return None

    session = None
    try:
        session = HTTP(testnet=testnet_flag, api_key=api_key, api_secret=api_secret)
        # Простой запрос для проверки соединения и ключей
        response = session.get_api_key_information()
        if response and response.get("retCode") == 0:
            logger.info(
                f"Successfully connected and verified Bybit API connection for {BYBIT_ENV}."
            )
        else:
            logger.error(
                f"Failed to verify Bybit API connection for {BYBIT_ENV}. Response: {response}"
            )
            send_telegram_notification(
                f"❌ *Ошибка:* Не удалось верифицировать ключи Bybit API для {BYBIT_ENV}. Проверьте ключи и разрешения."
            )
            session = None  # Считаем соединение неудачным

    except Exception as e:
        logger.error(
            f"FATAL: Error connecting to Bybit {BYBIT_ENV} API: {e}", exc_info=True
        )
        send_telegram_notification(
            f"❌ *Ошибка:* Ошибка подключения к Bybit API {BYBIT_ENV}. Детали в логах."
        )
        session = None

    return session


def get_existing_exec_ids(sheet_instance):
    """Получает множество ID из столбца AC ('involved_exec_ids') таблицы."""
    logger.info("Fetching existing execution IDs from Google Sheet...")
    existing_ids_set = set()
    col_index_for_ids = COL_IDX["involved_exec_ids"]  # Используем индекс из словаря

    try:
        # Получаем все значения из столбца AC
        all_id_cells = sheet_instance.col_values(
            col_index_for_ids, value_render_option="FORMATTED_VALUE"
        )
        logger.info(f"Found {len(all_id_cells)} cells in column AC.")

        # Пропускаем заголовок, если он есть (проверяем первую непустую ячейку)
        header_skipped = False
        for cell_value in all_id_cells:
            if cell_value:  # Нашли первую непустую
                if (
                    not header_skipped
                    and isinstance(cell_value, str)
                    and "id" in cell_value.lower()
                ):
                    logger.debug(f"Skipping potential header: {cell_value}")
                    header_skipped = True
                    continue
                # Разделяем строку по запятой и добавляем каждый ID в множество
                ids_in_cell = str(cell_value).split(",")
                for exec_id in ids_in_cell:
                    cleaned_id = exec_id.strip()
                    if cleaned_id:
                        existing_ids_set.add(cleaned_id)

        logger.info(
            f"Found {len(existing_ids_set)} unique existing execution IDs in column AC."
        )

    except gspread.exceptions.APIError as e:
        logger.error(f"Google API error fetching column AC: {e}", exc_info=True)
        send_telegram_notification(
            "⚠️ *Предупреждение:* Ошибка API при чтении ID из Google Sheet. Возможны дубликаты сделок."
        )
    except Exception as e:
        logger.error(
            f"Error fetching or parsing existing Bybit IDs from column AC: {e}",
            exc_info=True,
        )
        send_telegram_notification(
            "⚠️ *Предупреждение:* Ошибка при чтении ID из Google Sheet. Возможны дубликаты сделок."
        )
        # Возвращаем пустое множество, чтобы скрипт мог продолжить, но с риском дубликатов

    return existing_ids_set


def fetch_bybit_executions(session, start_time_ms=None, end_time_ms=None):
    """Запрашивает историю исполнений ордеров с Bybit с пагинацией."""
    logger.info(
        f"Fetching executions from Bybit (Category: {BYBIT_CATEGORY}, Limit: {BYBIT_API_LIMIT})..."
    )
    if start_time_ms:
        logger.info(
            f"Time range: From {datetime.datetime.fromtimestamp(start_time_ms/1000)} to {datetime.datetime.fromtimestamp(end_time_ms/1000)}"
        )

    all_results = []
    cursor = None
    page_count = 0

    try:
        while page_count < MAX_API_PAGES:
            page_count += 1
            params = {
                "category": BYBIT_CATEGORY,
                "limit": BYBIT_API_LIMIT,
                # "execType": "Trade" # Можно раскомментировать, если нужны только торговые исполнения
            }
            # Bybit API ожидает startTime/endTime только если они нужны
            if start_time_ms:
                params["startTime"] = start_time_ms
            if end_time_ms:
                params["endTime"] = end_time_ms
            if cursor:
                params["cursor"] = cursor

            logger.debug(
                f"Requesting Bybit executions page {page_count} with params: {params}"
            )
            response = session.get_executions(**params)
            # logger.debug(f"Raw Bybit Executions Response (Page {page_count}): {response}") # Слишком многословно для INFO

            if response and response.get("retCode") == 0:
                result = response.get("result", {})
                data_list = result.get("list", [])

                if data_list:
                    all_results.extend(data_list)
                    logger.info(
                        f"Fetched {len(data_list)} execution records from Bybit (page {page_count}). Total: {len(all_results)}"
                    )
                else:
                    logger.info(
                        f"Received empty execution list on page {page_count}. Assuming end of data."
                    )
                    break  # Нет данных на этой странице

                cursor = result.get("nextPageCursor")
                if not cursor:
                    logger.info("No more execution pages (nextPageCursor is empty).")
                    break  # Нет следующей страницы
                else:
                    # Логируем только часть курсора для краткости
                    log_cursor = (
                        cursor[:10] + "..." if cursor and len(cursor) > 10 else cursor
                    )
                    logger.debug(
                        f"Found nextPageCursor: {log_cursor}. Fetching next page..."
                    )
                    time.sleep(0.5)  # Пауза между запросами API

            else:
                ret_code = response.get("retCode", "N/A")
                ret_msg = response.get("retMsg", "N/A")
                logger.error(
                    f"Error response fetching executions from Bybit (retCode={ret_code}, retMsg='{ret_msg}'). Stopping fetch."
                )
                logger.debug(f"Full error response: {response}")
                # Отправляем уведомление только при первой ошибке
                if page_count == 1:
                    send_telegram_notification(
                        f"❌ *Ошибка:* Не удалось получить исполнения от Bybit ({ret_code}: {ret_msg})."
                    )
                break  # Прерываем цикл при ошибке API

        if page_count >= MAX_API_PAGES:
            logger.warning(
                f"Reached maximum page limit ({MAX_API_PAGES}) for executions. Stopped fetching."
            )
            send_telegram_notification(
                f"⚠️ *Предупреждение:* Достигнут лимит страниц ({MAX_API_PAGES}) при запросе исполнений Bybit."
            )

    except Exception as e:
        logger.error(
            f"Exception during Bybit get_executions API call: {e}", exc_info=True
        )
        send_telegram_notification(
            "❌ *Ошибка:* Исключение при запросе исполнений Bybit. Детали в логах."
        )
        return []  # Возвращаем пустой список при исключении

    logger.info(f"Total execution records fetched: {len(all_results)}")
    # Сортируем все результаты по времени исполнения перед возвратом
    all_results_sorted = sorted(all_results, key=lambda x: int(x.get("execTime", 0)))
    logger.info(f"Sorted {len(all_results_sorted)} execution records by time.")
    return all_results_sorted


def parse_executions_to_trades(executions, existing_ids):
    """
    Парсит ИСПОЛНЕНИЯ от Bybit и реконструирует ЗАКРЫТЫЕ СДЕЛКИ.
    Возвращает список словарей, где каждый словарь представляет одну закрытую сделку.
    """
    logger.info(
        f"Attempting to reconstruct trades from {len(executions)} execution records..."
    )
    closed_trades = []
    open_positions = {}  # symbol -> {details}
    processed_exec_ids_in_run = set()  # Отслеживаем ID, обработанные в этом запуске

    # Используем Decimal для точности расчетов с деньгами и количеством
    ZERO = Decimal("0")

    for exec_record in executions:
        exec_id = exec_record.get("execId")

        # Пропускаем, если нет ID или уже записан в таблицу ИЛИ обработан в этом запуске
        if (
            not exec_id
            or exec_id in existing_ids
            or exec_id in processed_exec_ids_in_run
        ):
            # logger.debug(f"Skipping execId: {exec_id} (Missing, already in sheet, or processed in this run)")
            continue

        # Извлекаем и преобразуем данные из исполнения
        try:
            symbol = exec_record.get("symbol")
            side = exec_record.get("side")  # 'Buy' или 'Sell'
            exec_qty_dec = Decimal(exec_record.get("execQty", "0"))
            exec_price_dec = Decimal(exec_record.get("execPrice", "0"))
            exec_fee_dec = Decimal(
                exec_record.get("execFee", "0")
            )  # Комиссия этого исполнения
            exec_time_ms = int(exec_record.get("execTime", "0"))
            order_type = exec_record.get("orderType", "")
            # fee_rate = exec_record.get('feeRate', '') # Может быть полезно

            if (
                not symbol
                or not side
                or exec_qty_dec <= ZERO
                or exec_price_dec <= ZERO
                or exec_time_ms == 0
            ):
                logger.warning(
                    f"Skipping execId {exec_id} due to missing/invalid core data: {exec_record}"
                )
                continue

            exec_value_dec = exec_qty_dec * exec_price_dec
            processed_exec_ids_in_run.add(
                exec_id
            )  # Отмечаем как обработанный в этом запуске

        except (TypeError, ValueError, KeyError) as e:
            logger.error(
                f"Error parsing execution record for execId {exec_id}: {e}. Record: {exec_record}",
                exc_info=True,
            )
            continue

        # --- Логика реконструкции сделок ---
        pos = open_positions.get(symbol)

        if not pos:
            # --- Открытие новой позиции ---
            open_positions[symbol] = {
                "side": side,  # 'Buy' (Long) or 'Sell' (Short)
                "total_qty": exec_qty_dec,
                "total_entry_value": exec_value_dec,
                "total_entry_fees": exec_fee_dec,
                "first_entry_ts_ms": exec_time_ms,
                "last_entry_ts_ms": exec_time_ms,
                "entry_exec_ids": [exec_id],
                # --- Временные поля для расчета выхода ---
                "accum_exit_value": ZERO,
                "accum_exit_fees": ZERO,
                "exit_exec_ids": [],
                "last_exit_ts_ms": 0,
                "last_exit_order_type": "",
            }
            logger.debug(
                f"Opened new position: {symbol} {side} {exec_qty_dec} @ {exec_price_dec}"
            )

        else:
            # --- Обновление существующей позиции ---
            if side == pos["side"]:
                # --- Увеличение существующей позиции (докупка/допродажа) ---
                pos["total_qty"] += exec_qty_dec
                pos["total_entry_value"] += exec_value_dec
                pos["total_entry_fees"] += exec_fee_dec
                pos["last_entry_ts_ms"] = max(pos["last_entry_ts_ms"], exec_time_ms)
                pos["entry_exec_ids"].append(exec_id)
                logger.debug(
                    f"Increased position: {symbol} {side} now {pos['total_qty']}"
                )

            else:
                # --- Закрытие (частичное или полное) существующей позиции ---
                closed_qty = min(exec_qty_dec, pos["total_qty"])
                logger.debug(
                    f"Closing execution: {symbol} {side} {exec_qty_dec} vs open {pos['side']} {pos['total_qty']}. Closing {closed_qty}"
                )

                if closed_qty > ZERO:
                    # Аккумулируем данные выхода
                    pos["accum_exit_value"] += closed_qty * exec_price_dec
                    pos[
                        "accum_exit_fees"
                    ] += exec_fee_dec  # TODO: Уточнить, всегда ли комиссия за закрытие берется из execFee?
                    pos["exit_exec_ids"].append(exec_id)
                    pos["last_exit_ts_ms"] = max(pos["last_exit_ts_ms"], exec_time_ms)
                    pos["last_exit_order_type"] = order_type

                    # Рассчитываем долю закрываемой части от текущей позиции ДО уменьшения
                    # Используем для пропорционального уменьшения стоиости входа
                    proportion_closed = closed_qty / pos["total_qty"]

                    # Уменьшаем позицию
                    pos["total_qty"] -= closed_qty
                    # Уменьшаем стоимость входа пропорционально, чтобы средняя цена оставшейся части не менялась
                    pos["total_entry_value"] -= (
                        pos["total_entry_value"] * proportion_closed
                    )
                    # Уменьшаем комиссии входа пропорционально (приближенно)
                    pos["total_entry_fees"] -= (
                        pos["total_entry_fees"] * proportion_closed
                    )

                    # --- ПРОВЕРКА ПОЛНОГО ЗАКРЫТИЯ ---
                    if pos["total_qty"] <= QTY_TOLERANCE:  # Используем допуск
                        logger.info(f"Position fully closed: {symbol}")
                        # --- Формируем запись о закрытой сделке ---
                        try:
                            avg_entry_price = (
                                (pos["total_entry_value"] + pos["total_entry_fees"])
                                / (closed_qty)
                                if closed_qty > ZERO
                                else ZERO
                            )  # Восстанавливаем полную стоимость входа для этой сделки
                            avg_exit_price = (
                                pos["accum_exit_value"] / closed_qty
                                if closed_qty > ZERO
                                else ZERO
                            )

                            trade_type = "Лонг" if pos["side"] == "Buy" else "Шорт"

                            entry_dt = datetime.datetime.fromtimestamp(
                                pos["first_entry_ts_ms"] / 1000
                            )
                            exit_dt = datetime.datetime.fromtimestamp(
                                pos["last_exit_ts_ms"] / 1000
                            )

                            involved_ids = ",".join(
                                pos["entry_exec_ids"] + pos["exit_exec_ids"]
                            )

                            # Округляем числовые значения для записи в таблицу
                            closed_qty_str = str(
                                closed_qty.quantize(
                                    Decimal("0.00000001"), ROUND_HALF_UP
                                )
                            )  # Пример 8 знаков
                            avg_entry_price_str = str(
                                avg_entry_price.quantize(
                                    Decimal("0.0001"), ROUND_HALF_UP
                                )
                            )  # Пример 4 знака
                            avg_exit_price_str = str(
                                avg_exit_price.quantize(
                                    Decimal("0.0001"), ROUND_HALF_UP
                                )
                            )
                            entry_fees_str = str(
                                (
                                    pos["total_entry_fees"]
                                    + pos["total_entry_fees"] * proportion_closed
                                ).quantize(Decimal("0.0001"), ROUND_HALF_UP)
                            )  # Восстановленная комиссия
                            exit_fees_str = str(
                                pos["accum_exit_fees"].quantize(
                                    Decimal("0.0001"), ROUND_HALF_UP
                                )
                            )

                            closed_trade_data = {
                                "entry_date": entry_dt.strftime("%d.%m.%Y"),
                                "entry_time": entry_dt.strftime("%H:%M:%S"),
                                "exit_date": exit_dt.strftime("%d.%m.%Y"),
                                "exit_time": exit_dt.strftime("%H:%M:%S"),
                                "pair": symbol,
                                "type": trade_type,
                                "entry_price_avg": avg_entry_price_str,
                                "volume_coins": closed_qty_str,
                                "commission_entry": entry_fees_str,
                                "commission_exit": exit_fees_str,
                                "exit_method": pos["last_exit_order_type"],
                                "exit_price_avg": avg_exit_price_str,
                                "involved_exec_ids": involved_ids,
                            }
                            closed_trades.append(closed_trade_data)
                            logger.info(
                                f"Recorded closed trade: {symbol} {trade_type} {closed_qty_str}"
                            )

                            # Удаляем полностью закрытую позицию
                            del open_positions[symbol]

                        except Exception as format_e:
                            logger.error(
                                f"Error formatting closed trade data for {symbol}: {format_e}",
                                exc_info=True,
                            )
                            if symbol in open_positions:
                                del open_positions[
                                    symbol
                                ]  # Удаляем, чтобы избежать повторной ошибки

                    # --- Обработка "переворота" позиции ---
                    remaining_exec_qty = exec_qty_dec - closed_qty
                    if remaining_exec_qty > QTY_TOLERANCE:
                        # Если после закрытия текущей позиции в исполнении остался объем,
                        # он открывает новую позицию в противоположном направлении.
                        logger.info(
                            f"Position flipped: {symbol}. Opening new {side} position with qty {remaining_exec_qty}"
                        )
                        new_entry_value = remaining_exec_qty * exec_price_dec
                        # TODO: Как считать комиссию для переворота? Берем всю комиссию исполнения? Или пропорционально?
                        # Пока берем пропорционально оставшемуся объему
                        new_entry_fee = (
                            (exec_fee_dec / exec_qty_dec) * remaining_exec_qty
                            if exec_qty_dec > ZERO
                            else ZERO
                        )

                        open_positions[symbol] = {
                            "side": side,
                            "total_qty": remaining_exec_qty,
                            "total_entry_value": new_entry_value,
                            "total_entry_fees": new_entry_fee,
                            "first_entry_ts_ms": exec_time_ms,
                            "last_entry_ts_ms": exec_time_ms,
                            "entry_exec_ids": [exec_id],
                            "accum_exit_value": ZERO,
                            "accum_exit_fees": ZERO,
                            "exit_exec_ids": [],
                            "last_exit_ts_ms": 0,
                            "last_exit_order_type": "",
                        }

    # Логируем оставшиеся открытые позиции (если есть)
    if open_positions:
        logger.warning(
            f"Finished processing executions. {len(open_positions)} positions remain open:"
        )
        for symbol, pos_data in open_positions.items():
            logger.warning(
                f"  - {symbol}: {pos_data.get('side')} Qty: {pos_data.get('total_qty')}"
            )

    logger.info(
        f"Reconstruction complete. Found {len(closed_trades)} new closed trades to add."
    )
    return closed_trades


def add_trades_to_sheet(sheet_instance, trades_data):
    """Добавляет данные о закрытых сделках в Google Sheet."""
    if not trades_data:
        logger.info("No new closed trades to add to the sheet.")
        return 0

    logger.info(
        f"Adding {len(trades_data)} closed trades to Google Sheet '{SHEET_NAME}'..."
    )
    try:
        # 1. Найти первую пустую строку (начиная со 2-й строки, после заголовка)
        # Оптимизация: получаем только один столбец (например, A)
        # Используем UNFORMATTED_VALUE, чтобы пустые ячейки были None или ''
        col_a_values = sheet_instance.col_values(
            COL_IDX["entry_date"], value_render_option="UNFORMATTED_VALUE"
        )
        next_row = len(col_a_values) + 1
        logger.info(f"Determined next empty row to start insertion: {next_row}")

        # 2. Подготовить данные для batch_update
        updates = []
        current_row = next_row
        for trade in trades_data:
            row_updates = []
            for key, col_index in COL_IDX.items():
                # Проверяем, есть ли такое поле в данных сделки
                if key in trade:
                    value = trade[key]
                    # Создаем A1 нотацию для ячейки
                    cell_a1 = gspread.utils.rowcol_to_a1(current_row, col_index)
                    row_updates.append(
                        {
                            "range": cell_a1,
                            # Важно: [[value]] - список списков для обновления одной ячейки
                            "values": [[value]],
                        }
                    )
            updates.extend(row_updates)
            current_row += 1  # Переходим к следующей строке для следующей сделки

        # 3. Выполнить batch_update
        if updates:
            logger.info(
                f"Sending batch update request for {len(trades_data)} trades ({len(updates)} cell updates)..."
            )
            # USER_ENTERED позволяет Google Sheets правильно интерпретировать даты/числа
            sheet_instance.batch_update(updates, value_input_option="USER_ENTERED")
            logger.info(f"Successfully added {len(trades_data)} trades to the sheet.")
            return len(trades_data)
        else:
            logger.info("No cell updates were prepared (unexpected).")
            return 0

    except gspread.exceptions.APIError as e:
        logger.error(f"Google API error adding data to sheet: {e}", exc_info=True)
        # Попытка извлечь более детальное сообщение об ошибке
        error_detail = str(e)
        try:
            error_detail = e.response.json().get("error", {}).get("message", str(e))
        except:
            pass
        send_telegram_notification(
            f"❌ *Ошибка:* Ошибка Google API при записи в таблицу:\n`{error_detail}`"
        )
        return -1
    except Exception as e:
        logger.error(f"Error adding data to sheet: {e}", exc_info=True)
        send_telegram_notification(
            f"❌ *Ошибка:* Не удалось добавить сделки в Google Sheet. Детали в логах."
        )
        return -1


# === Основной блок выполнения ===
if __name__ == "__main__":
    start_run_time = time.time()
    run_dt_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"----- Script started at {run_dt_str} -----")
    logger.info(
        f"Environment: Bybit {BYBIT_ENV}, Category: {BYBIT_CATEGORY}, Fetch Days: {BYBIT_FETCH_DAYS}"
    )

    # 0. Проверка окружения (строго TESTNET для этого скрипта)
    if BYBIT_ENV != "TESTNET":
        logger.error(f"FATAL: This script is configured for TESTNET execution only.")
        logger.error(
            f"Current BYBIT_ENV is set to '{BYBIT_ENV}'. Please correct .env file or script configuration."
        )
        send_telegram_notification(
            f"❌ *Ошибка:* Скрипт настроен только для Testnet, но запущен с окружением `{BYBIT_ENV}`!"
        )
        exit(1)  # Используем ненулевой код выхода при ошибке

    # 1. Подключаемся к Google Sheets
    sheet = connect_google_sheets()
    if not sheet:
        # Уведомление уже отправлено внутри функции connect_google_sheets
        logger.error("Exiting due to Google Sheets connection failure.")
        exit(1)

    # 2. Подключаемся к Bybit (Testnet)
    bybit_session = connect_bybit()
    if not bybit_session:
        # Уведомление уже отправлено внутри функции connect_bybit
        logger.error("Exiting due to Bybit connection failure.")
        exit(1)

    # 3. Получаем ID уже существующих исполнений из таблицы
    # Это важно для предотвращения дублирования уже записанных сделок
    existing_ids = get_existing_exec_ids(sheet)

    # 4. Запрашиваем ИСПОЛНЕНИЯ с Bybit за указанный период
    end_dt = datetime.datetime.now(datetime.timezone.utc)  # Используем UTC
    start_dt = end_dt - datetime.timedelta(days=BYBIT_FETCH_DAYS)
    start_timestamp_ms = int(start_dt.timestamp() * 1000)
    end_timestamp_ms = int(end_dt.timestamp() * 1000)

    # Вызываем функцию получения исполнений
    executions = fetch_bybit_executions(
        bybit_session, start_timestamp_ms, end_timestamp_ms
    )

    if not executions:
        # Если не удалось получить исполнения, нет смысла продолжать
        logger.warning("No executions fetched from Bybit. Nothing to process.")
        # Можно отправить уведомление, если это не ожидаемо
        # send_telegram_notification(f"ℹ️ Не получено ни одного исполнения Bybit Testnet ({BYBIT_CATEGORY}) за посл. {BYBIT_FETCH_DAYS} дн.")
    else:
        # 5. Парсим ИСПОЛНЕНИЯ и реконструируем СДЕЛКИ
        # Передаем только те исполнения, ID которых еще нет в таблице (оптимизация)
        # (Хотя parse_executions_to_trades уже содержит проверку existing_ids)
        new_closed_trades = parse_executions_to_trades(executions, existing_ids)

        # 6. Добавляем новые данные (закрытые сделки) в таблицу
        added_count = add_trades_to_sheet(sheet, new_closed_trades)

        # 7. Отправляем итоговое уведомление
        if added_count > 0:
            summary_message = f"✅ Успешно добавлено *{added_count}* новых закрытых сделок Bybit Testnet ({BYBIT_CATEGORY}) в таблицу."
            logger.info(summary_message)
            send_telegram_notification(summary_message)
        elif added_count == 0 and new_closed_trades:
            # Были найдены сделки, но не добавлены (возможно, ошибка записи)
            logger.warning(
                "Trades were parsed but not added to the sheet (added_count is 0). Check logs for errors."
            )
            # Уведомление об ошибке должно было отправиться из add_trades_to_sheet
        elif added_count == 0 and not new_closed_trades:
            # Не было найдено новых ЗАКРЫТЫХ сделок для добавления
            info_message = f"ℹ️ Новых закрытых сделок Bybit Testnet ({BYBIT_CATEGORY}) для добавления не найдено за посл. {BYBIT_FETCH_DAYS} дн."
            logger.info(info_message)
            send_telegram_notification(info_message)
        else:  # added_count == -1 (ошибка)
            # Уведомление об ошибке должно было отправиться из add_trades_to_sheet
            logger.error("An error occurred while adding data to Google Sheet.")

    end_run_time = time.time()
    duration = end_run_time - start_run_time
    logger.info(f"----- Script finished in {duration:.2f} seconds -----")
