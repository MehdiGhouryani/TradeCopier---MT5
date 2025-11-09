import os
import time
import asyncio
import logging
import re
from glob import glob
from dotenv import load_dotenv
from telegram.ext import ContextTypes,Application

from telegram.constants import ParseMode
from telegram.error import TelegramError
import json
from logging.handlers import RotatingFileHandler
import sqlite3
import datetime # برای timestamp
import aiosqlite
# --- فاز ۱: راه‌اندازی لاگ‌گیری حرفه‌ای با فرمت JSON ---

class JsonFormatter(logging.Formatter):
    """
    این کلاس سفارشی، لاگ‌ها را به فرمت ساختاریافته JSON تبدیل می‌کند.
    """
    def format(self, record):
        # ایجاد یک دیکشنری پایه برای هر لاگ
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        
        # افزودن فیلدهای سفارشی و غنی‌سازی لاگ در صورت وجود
        extra_keys = ['task_name', 'entity_id', 'status', 'details', 'error']
        for key in extra_keys:
            if hasattr(record, key):
                log_record[key] = getattr(record, key)
                
        # تبدیل دیکشنری به رشته JSON
        return json.dumps(log_record, ensure_ascii=False)

# --- پیکربندی اصلی لاگ‌گیری ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # پایین‌ترین سطح برای ثبت تمام جزئیات

# ۱. لاگ‌گیری در کنسول (برای مشاهده آنی) - با فرمت ساده و خوانا
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO) # فقط پیام‌های مهم به بالا
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# ۲. لاگ‌گیری در فایل (برای ذخیره‌سازی) - با فرمت جدید JSON
file_handler = RotatingFileHandler('watcher.log', maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(JsonFormatter()) 

# حذف مدیریت‌کننده‌های قبلی برای جلوگیری از لاگ تکراری
if logger.hasHandlers():
    logger.handlers.clear()

# افزودن مدیریت‌کننده‌های جدید به لاگر
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# کاهش لاگ‌های اضافی از کتابخانه‌های دیگر
logging.getLogger('httpx').setLevel(logging.WARNING)



# --- Load Environment Variables ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")
LOG_DIRECTORY_PATH = os.getenv("LOG_DIRECTORY_PATH")
CHANNEL_ID = os.getenv("CHANNEL_ID")
ECOSYSTEM_PATH = os.getenv("ECOSYSTEM_PATH")



# --- فاز ۱، بخش دوم: مدیریت وضعیت اتمیک و بهینه ---

# State file path for stateless operation
WATCHER_STATE_PATH = os.path.join(os.path.dirname(ECOSYSTEM_PATH) if ECOSYSTEM_PATH else '.', 'watcher_state.json') if ECOSYSTEM_PATH else 'watcher_state.json'
# --- جدید: مسیر پایگاه داده آمار ---
DB_PATH = os.path.join(os.path.dirname(WATCHER_STATE_PATH), 'trade_history.db')
SOURCE_STATUS_PATH = os.path.join(os.path.dirname(WATCHER_STATE_PATH), 'source_status.json')

source_name_map = {}
state_data = {}  # دیکشنری وضعیت سراسری
state_changed = False  # فلگ برای ذخیره‌سازی دسته‌ای

source_statuses = {} 




BENIGN_ERROR_CONFIG = {
    "Market is closed": 3600,
    "error 10013": 3600,
    "Invalid Stops": 600,
    "error 130": 600,
    "Requote": 600,
    "error 10004": 600,
    "Price changed": 600,
    "error 10006": 600,
    "Failed to open source file": 300,
    "No connection": 300,
    "error 10025": 300
}

g_benign_error_last_sent = {}













# --- جدید: تابع ایجاد پایگاه داده و جدول ---
def initialize_database():
    """
    پایگاه داده SQLite و جدول تاریخچه معاملات را در صورت عدم وجود ایجاد می‌کند.
    """
    log_extra = {'entity_id': DB_PATH}
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                copy_id TEXT NOT NULL,
                source_id TEXT, -- می‌تواند از روی source_file یا state استخراج شود
                source_account_number INTEGER,
                symbol TEXT NOT NULL,
                profit REAL NOT NULL,
                source_file TEXT -- نام فایل سورس برای شناسایی منبع
            )
        ''')
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully.", extra={**log_extra, 'status': 'success'})
    except sqlite3.Error as e:
        logger.error("Failed to initialize database.", extra={**log_extra, 'error': str(e), 'status': 'failure'})
    except Exception as e:
         logger.critical("Unexpected error during database initialization.", extra={**log_extra, 'error': str(e), 'status': 'failure'})

# --- پایان تابع جدید ---




async def save_trade_to_db(trade_data: dict, db_conn: aiosqlite.Connection):
    """
    (بازنویسی شده)
    اطلاعات معامله بسته شده را به صورت ناهمزمان در پایگاه داده SQLite ذخیره می‌کند.
    """
    log_extra = {'entity_id': trade_data.get('source_ticket', 'N/A'), 'status': 'pending_save'}
    required_keys = ['copy_id', 'symbol', 'profit', 'source_file', 'source_account_number', 'source_ticket']
    if not all(key in trade_data for key in required_keys):
        logger.warning("Missing required data for saving trade to DB.", extra={**log_extra, 'details': trade_data, 'status': 'save_skipped'})
        return

    source_info = source_name_map.get(trade_data['source_file'])
    source_id = source_info['id'] if source_info else None
    source_name_for_state = source_info['name'] if source_info else trade_data['source_file']

    global state_data, state_changed
    if state_data.get(str(trade_data['source_ticket'])) != source_name_for_state:
        state_data[str(trade_data['source_ticket'])] = source_name_for_state
        state_changed = True

    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    db_id = -1

    try:
        cursor = await db_conn.execute('''
            INSERT INTO trades (timestamp, copy_id, source_id, source_account_number, symbol, profit, source_file)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            timestamp,
            trade_data['copy_id'],
            source_id,
            trade_data['source_account_number'],
            trade_data['symbol'],
            trade_data['profit'],
            trade_data['source_file']
        ))
        await db_conn.commit()
        db_id = cursor.lastrowid
        await cursor.close()
        
        logger.info(f"Trade saved to DB.", extra={**log_extra,'db_id': db_id, 'status': 'save_success'})

        if str(trade_data['source_ticket']) in state_data:
            del state_data[str(trade_data['source_ticket'])]
            state_changed = True

    except aiosqlite.Error as e:
        logger.error("Failed to save trade to DB (async).", extra={**log_extra, 'error': str(e), 'status': 'save_failure'})
    except Exception as e:
         logger.critical("Unexpected error during async save trade to DB.", extra={**log_extra, 'error': str(e), 'status': 'save_failure'})





def load_source_names():
    """
    نام‌های نمایشی و ID منابع را از فایل ecosystem.json بارگذاری می‌کند.
    ساختار source_name_map به {file_path: {'name': name, 'id': id}} تغییر می‌کند.
    """
    global source_name_map
    source_name_map = {} # پاک کردن مپ قبلی
    if not ECOSYSTEM_PATH:
        logger.warning("ECOSYSTEM_PATH not set. Skipping source names load.", extra={'status': 'skipped'})
        return
    try:
        with open(ECOSYSTEM_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)

        temp_map = {}
        for source in data.get('sources', []):
            if 'file_path' in source and 'name' in source and 'id' in source:
                temp_map[source['file_path']] = {'name': source['name'], 'id': source['id']}

        source_name_map = temp_map # فقط در صورت موفقیت کامل، متغیر سراسری را به‌روزرسانی کن
        logger.info(f"Loaded {len(source_name_map)} source names and IDs from ecosystem.json.", extra={'status': 'success'})
    except FileNotFoundError:
        logger.warning(f"ecosystem.json not found at {ECOSYSTEM_PATH}. Using empty source map.", extra={'entity_id': ECOSYSTEM_PATH})
    except json.JSONDecodeError as e:
        logger.error("JSON decode error in ecosystem.json.", extra={'error': str(e), 'status': 'failure'})
    except Exception as e:
        logger.error("Failed to load or parse ecosystem.json.", extra={'error': str(e), 'status': 'failure'})

def load_watcher_state():
    """
    وضعیت watcher را از فایل JSON با اعتبارسنجی کامل بارگذاری می‌کند.
    در صورت وجود هرگونه خطا، با یک وضعیت خالی شروع به کار می‌کند.
    """
    if not os.path.exists(WATCHER_STATE_PATH):
        logger.info(f"State file not found, starting fresh.", extra={'entity_id': WATCHER_STATE_PATH})
        return {}
    try:
        with open(WATCHER_STATE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # اعتبارسنجی: آیا داده یک دیکشنری است و آیا کلیدها و مقادیر آن رشته هستند؟
        if not isinstance(data, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in data.items()):
            raise ValueError("Invalid state format: must be a dictionary of strings.")
        logger.info(f"Loaded state with {len(data)} entries.", extra={'entity_id': WATCHER_STATE_PATH, 'status': 'success'})
        return data
    except (json.JSONDecodeError, ValueError) as e:
        logger.error("Corrupt or invalid state file. Starting with empty state.", extra={'entity_id': WATCHER_STATE_PATH, 'error': str(e), 'status': 'failure'})
        return {}
    except Exception as e:
        logger.error("Failed to load state file.", extra={'entity_id': WATCHER_STATE_PATH, 'error': str(e), 'status': 'failure'})
        return {}

def save_watcher_state(state: dict):
    """
    وضعیت watcher را به صورت اتمیک در فایل JSON ذخیره می‌کند.
    ابتدا در یک فایل موقت می‌نویسد و سپس جایگزین فایل اصلی می‌کند.
    """
    tmp_path = WATCHER_STATE_PATH + '.tmp'
    try:
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        # عملیات اتمیک: جایگزینی فایل اصلی با فایل موقت
        os.replace(tmp_path, WATCHER_STATE_PATH)
        logger.debug(f"Saved state with {len(state)} entries.", extra={'status': 'success'})
    except Exception as e:
        logger.error("Failed to save state file.", extra={'error': str(e), 'status': 'failure'})
        # اگر خطایی رخ داد، فایل موقت را حذف کن تا باقی نماند
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception as remove_e:
                logger.error(f"Failed to remove temporary state file.", extra={'error': str(remove_e)})

# --- پایان بخش مدیریت وضعیت ---



# --- فاز ۲، بخش دوم: ارسال اعلان هوشمند به تلگرام ---

# متغیرهای سراسری برای کنترل ارسال پیام‌های تکراری
last_message_hash = None
last_message_time = 0
DEDUPLICATION_COOLDOWN = 10  # (ثانیه) - از ارسال پیام‌های تکراری در این بازه زمانی جلوگیری می‌کند

async def send_telegram_alert(context: ContextTypes.DEFAULT_TYPE, message: str):
    """
    پیام‌ها را به صورت هوشمند به تلگرام ارسال می‌کند.
    - از ارسال پیام‌های تکراری جلوگیری می‌کند.
    - در صورت بروز خطای شبکه، مجدداً تلاش می‌کند.
    """
    global last_message_hash, last_message_time
    
    target_id = CHANNEL_ID if CHANNEL_ID else ADMIN_ID
    if not target_id:
        logger.warning("No target ID (CHANNEL_ID or ADMIN_ID) is set. Skipping alert.", extra={'status': 'skipped'})
        return

    # --- منطق جلوگیری از اسپم ---
    current_time = time.time()
    message_hash = hash(message)
    if message_hash == last_message_hash and (current_time - last_message_time) < DEDUPLICATION_COOLDOWN:
        logger.info("Skipping duplicate alert.", extra={'details': message[:50] + '...'})
        return
    
    # --- منطق تلاش مجدد با عقب‌نشینی نمایی (Exponential Backoff) ---
    max_retries = 3
    delay = 1.0  # شروع با ۱ ثانیه تاخیر

    for attempt in range(max_retries + 1):
        try:
            await context.bot.send_message(chat_id=target_id, text=message, parse_mode=ParseMode.MARKDOWN)
            
            # در صورت موفقیت، اطلاعات پیام را برای جلوگیری از تکرار ذخیره کن
            last_message_hash = message_hash
            last_message_time = current_time
            
            logger.info("Alert sent successfully.", extra={'target_id': target_id, 'status': 'success'})
            return # از حلقه خارج شو

        except TelegramError as e:
            log_extra = {'error': str(e), 'attempt': f"{attempt + 1}/{max_retries + 1}", 'status': 'retry_failed'}
            if attempt < max_retries:
                logger.warning(f"Failed to send alert, retrying in {delay:.1f}s...", extra=log_extra)
                await asyncio.sleep(delay)
                delay *= 2  # زمان تاخیر را دو برابر کن
            else:
                logger.error("Failed to send alert after multiple retries.", extra=log_extra)
                # اگر ارسال به کانال شکست خورد، یک بار به ادمین اطلاع بده
                if target_id == CHANNEL_ID and ADMIN_ID:
                    try:
                        fallback_msg = f"⚠️ *Channel Send Failed*\n\nOriginal message:\n{message}"
                        await context.bot.send_message(chat_id=ADMIN_ID, text=fallback_msg, parse_mode=ParseMode.MARKDOWN)
                        logger.info("Fallback alert sent to ADMIN_ID.", extra={'status': 'fallback_success'})
                    except TelegramError as e2:
                        logger.critical("Fallback to ADMIN_ID also failed.", extra={'error': str(e2), 'status': 'fallback_failed'})
                return # شکست نهایی
        except Exception as e:
            logger.critical("An unexpected error occurred in send_telegram_alert.", extra={'error': str(e)})
            return
            



open_pattern = re.compile(r'\[TRADE_OPEN\]\s+([^,]+),([^,]+),(.+?),([^,]+),([^,]+),([^,]+),(\d+)')

close_pattern = re.compile(r'\[TRADE_CLOSE\]\s+([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),(\d+)(.*)')

alert_pattern = re.compile(r'\[DD_ALERT\]\s+(.*)')
stop_pattern = re.compile(r'\[DD_STOP\]\s+(.*)')

reset_pattern = re.compile(r'\[DD_RESET\]\s+([^,]+),?(.*)')

error_pattern = re.compile(r'\[ERROR\]\s+-\s+(.*)') 

# مثال LIMIT_MAX_LOT: copy_A,TradeCopier_S1.txt,0.50,0.10
limit_max_lot_pattern = re.compile(r'\[LIMIT_MAX_LOT\]\s+([^,]+),([^,]+),([^,]+),([^,]+)')

# مثال LIMIT_MAX_TRADES: copy_A,TradeCopier_S1.txt,3,3
limit_max_trades_pattern = re.compile(r'\[LIMIT_MAX_TRADES\]\s+([^,]+),([^,]+),(\d+),(\d+)')

# مثال LIMIT_SOURCE_DD: copy_A,TradeCopier_S1.txt,-215.50,200.00,3
limit_source_dd_pattern = re.compile(r'\[LIMIT_SOURCE_DD\]\s+([^,]+),([^,]+),([^,]+),([^,]+),(\d+)')





def parse_and_format_log_line(line: str) -> tuple[str | None, dict | None]:

    line = line.strip()
    if not line:
        return None, None

    formatted_message = None
    trade_data_for_db = None

    try:
        if match := open_pattern.search(line):
            parts = [p.strip() for p in match.groups()]
            if len(parts) != 7: raise ValueError(f"Invalid OPEN format: {len(parts)} parts")
            copy_id, symbol, volume_info, price, source_ticket_str, source_file, source_account_number_str = parts
            source_account_number = int(source_account_number_str)
            source_info = source_name_map.get(source_file)
            source_display_name = source_info['name'] if source_info else source_file
            global state_data, state_changed
            if state_data.get(source_ticket_str) != source_display_name:
                state_data[source_ticket_str] = source_display_name; state_changed = True
            formatted_message = (
                f"✅ *New Position Opened*\n\n"
                f"*Source:* `{source_display_name}` (Acc: `{source_account_number}`)\n"
                f"*Copy Account:* `{copy_id}`\n*Symbol:* `{symbol}`\n"
                f"*Volume:* `{volume_info}`\n*Open Price:* `{price}`\n"
                f"*Source Ticket:* `{source_ticket_str}`"
            )

        elif match := close_pattern.search(line):
            groups = match.groups()
            
            copy_id, symbol, source_ticket_str, profit_or_reason_str, source_file, source_account_number_str = [g.strip() for g in groups[:6]]
            
            source_account_number = int(source_account_number_str)
            source_display_name = state_data.get(source_ticket_str, source_file)
            
            profit_float = 0.0
            profit_text = "N/A"
            reason_str = None
            emoji = "☑️"

            try:
                profit_float = float(profit_or_reason_str)
                profit_text = f"+${profit_float:,.2f}" if profit_float >= 0 else f"-${abs(profit_float):,.2f}"
                emoji = "☑️" if profit_float >= 0 else "🔻"
            except ValueError:
                profit_text = "N/A"
                reason_str = profit_or_reason_str
                emoji = "ℹ️"
                
            if len(groups) > 6 and groups[6] and groups[6].strip():
                extra_reason = groups[6].strip().strip('()')
                
                if reason_str:
                    reason_str = f"{reason_str} | {extra_reason}"
                else:
                    reason_str = extra_reason
            
            formatted_message = (
                f"{emoji} *Position Closed*\n\n"
                f"*Source:* `{source_display_name}` (Acc: `{source_account_number}`)\n"
                f"*Copy Account:* `{copy_id}`\n*Symbol:* `{symbol}`\n"
                f"*Profit/Loss:* `{profit_text}`\n*Source Ticket:* `{source_ticket_str}`"
            )
            
            if reason_str:
                formatted_message += f"\n*Reason:* `{reason_str}`"

            trade_data_for_db = {
                'copy_id': copy_id,
                'symbol': symbol,
                'profit': profit_float,
                'source_file': source_file,
                'source_account_number': source_account_number,
                'source_ticket': source_ticket_str
            }


            
        elif match := alert_pattern.search(line):
             parts = [p.strip() for p in match.group(1).split(',')];
             if len(parts) != 5: raise ValueError(f"Invalid ALERT format: {len(parts)} parts")
             copy_id, dd, dollar_loss, start_equity, peak_equity = parts
             formatted_message = (f"🟡 *Daily Drawdown Alert*\n\n*Account:* `{copy_id}`\n"
                                f"*Current Loss:* `%{float(dd):.2f}` `(-${float(dollar_loss):,.2f})`\n"
                                f"*Daily Start Equity:* `${float(start_equity):,.2f}`\n*Daily Peak Equity:* `${float(peak_equity):,.2f}`")
        
        elif match := stop_pattern.search(line):
             parts = [p.strip() for p in match.group(1).split(',')];
             if len(parts) != 6: raise ValueError(f"Invalid STOP format: {len(parts)} parts")
             copy_id, dd, dd_limit, dollar_loss, start_equity, peak_equity = parts
             formatted_message = (f"🔴 *Copy Stopped Due to DD Limit*\n\n*Account:* `{copy_id}`\n"
                                f"*Loss at Stop:* `%{float(dd):.2f}` `(-${float(dollar_loss):,.2f})`\n"
                                f"*Stop Threshold:* `%{float(dd_limit):,.2f}`")
        
        elif match := reset_pattern.search(line):
             copy_id = match.group(1).strip()
             details = match.group(2).strip() or "Copying re-enabled"

             formatted_message = (
                 f"✅ *Daily DD Lock Released*\n\n"
                 f"*Account:* `{copy_id}`\n"
                 f"*Status:* `{details}`"
             )
            
        elif match := error_pattern.search(line):
             error_message = match.group(1).strip()
             
             found_benign_key = None
             cooldown_period = 0
             
             for error_key, cooldown in BENIGN_ERROR_CONFIG.items():
                 if error_key in error_message:
                     found_benign_key = error_key
                     cooldown_period = cooldown
                     break
             
             if found_benign_key is None:
                 logger.warning(f"Critical error detected: {error_message}", extra={'line': line, 'status': 'critical_error_alert'})
                 formatted_message = f"🚨 *Critical Expert Error*\n\n`{error_message}`"
             
             else:
                 current_time = time.time()
                 last_sent_time = g_benign_error_last_sent.get(found_benign_key, 0)
                 
                 if (current_time - last_sent_time) > cooldown_period:
                     logger.info(f"Rate-limited benign error sending: {found_benign_key}", extra={'line': line, 'status': 'benign_error_alert'})
                     formatted_message = f"🟡 *Benign Error (Rate-Limited)*\n\n`{error_message}`"
                     g_benign_error_last_sent[found_benign_key] = current_time
                 
                 else:
                     logger.debug(f"Ignoring rate-limited benign error (in cooldown): {found_benign_key}", extra={'line': line, 'status': 'benign_error_throttled'})
                     return None, None

        elif match := limit_max_lot_pattern.search(line):
            parts = [p.strip() for p in match.groups()]
            if len(parts) != 4: raise ValueError(f"Invalid LIMIT_MAX_LOT format: {len(parts)} parts")
            copy_id, source_file, source_vol_str, limit_vol_str = parts
            source_info = source_name_map.get(source_file)
            source_display_name = source_info['name'] if source_info else source_file
            formatted_message = (
                f"🚫 *Max Lot Size Limit*\n\n"
                f"*Account:* `{copy_id}`\n"
                f"*Source:* `{source_display_name}`\n"
                f"*Details:* Trade volume `{source_vol_str}` exceeded limit `{limit_vol_str}`. Trade ignored."
            )

        elif match := limit_max_trades_pattern.search(line):
            parts = [p.strip() for p in match.groups()]
            if len(parts) != 4: raise ValueError(f"Invalid LIMIT_MAX_TRADES format: {len(parts)} parts")
            copy_id, source_file, open_trades_str, limit_trades_str = parts
            source_info = source_name_map.get(source_file)
            source_display_name = source_info['name'] if source_info else source_file
            formatted_message = (
                f"🔢 *Max Concurrent Trades Limit*\n\n"
                f"*Account:* `{copy_id}`\n"
                f"*Source:* `{source_display_name}`\n"
                f"*Details:* Limit of `{limit_trades_str}` open trades reached (`{open_trades_str}` currently open). New trade ignored."
            )

        elif match := limit_source_dd_pattern.search(line):
            parts = [p.strip() for p in match.groups()]
            if len(parts) != 5: raise ValueError(f"Invalid LIMIT_SOURCE_DD format: {len(parts)} parts")
            copy_id, source_file, current_pl_str, limit_dd_str, closed_count_str = parts
            source_info = source_name_map.get(source_file)
            source_display_name = source_info['name'] if source_info else source_file
            formatted_message = (
                f"💣 *Source Drawdown Limit Hit*\n\n"
                f"*Account:* `{copy_id}`\n"
                f"*Source:* `{source_display_name}`\n"
                f"*Details:* Floating P/L (`{current_pl_str}`) reached limit (`-{limit_dd_str}`). Closed `{closed_count_str}` position(s) from this source."
            )

    except (ValueError, IndexError, TypeError) as e:
        logger.warning(f"Malformed log line skipped: '{line}'. Error: {e}", extra={'status': 'parse_error', 'line': line})
        formatted_message = f"⚠️ *Parse Error in Log*\n`{line}`"
        trade_data_for_db = None

    return formatted_message, trade_data_for_db





async def follow_log_file(context: ContextTypes.DEFAULT_TYPE, filepath: str, db_conn: aiosqlite.Connection):
    """
    (بازنویسی شده)
    یک فایل لاگ را به صورت ناهمزمان دنبال کرده، خطوط جدید را پردازش،
    پیام تلگرام ارسال کرده و داده‌های معامله را برای ذخیره در DB (با استفاده از اتصال async) ارسال می‌کند.
    """
    task_name = asyncio.current_task().get_name()
    log_extra = {'task_name': task_name, 'entity_id': os.path.basename(filepath)}

    logger.info("Starting to watch log file.", extra=log_extra)

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            f.seek(0, 2)
            while True:
                line = f.readline()
                if not line:
                    await asyncio.sleep(1)
                    continue

                formatted_message, trade_data_for_db = parse_and_format_log_line(line)

                if formatted_message:
                    await send_telegram_alert(context, formatted_message)

                if trade_data_for_db:
                    # تغییر کلیدی: ارسال اتصال دیتابیس به تابع ذخیره‌سازی
                    await save_trade_to_db(trade_data_for_db, db_conn)

    except FileNotFoundError:
        logger.warning("Log file was not found or has been deleted. Task is stopping.", extra=log_extra)
    except asyncio.CancelledError:
        logger.info("Log file watch task has been cancelled.", extra=log_extra)
        pass
    except Exception as e:
        logger.error("An unexpected error occurred while watching log file.", extra={**log_extra, 'error': str(e), 'status': 'failure'})
        await send_telegram_alert(context, f"🚨 *Critical Watcher Error*\n\nTask `{task_name}` failed while watching `{os.path.basename(filepath)}`\nError: `{str(e)}`")




# --- فاز ۳، بخش دوم: حلقه اصلی و بررسی سلامت ---

async def batch_state_saver(state: dict):
    """
    این تسک به صورت دوره‌ای و در پس‌زمینه، وضعیت را در فایل ذخیره می‌کند.
    """
    global state_changed
    while True:
        await asyncio.sleep(10) # هر ۱۰ ثانیه یک بار اجرا می‌شود
        if state_changed:
            save_watcher_state(state)
            state_changed = False # فلگ را ریست می‌کند

async def health_checker():
    """
    این تسک به صورت دوره‌ای یک پیام سلامت در لاگ ثبت می‌کند تا از زنده بودن اسکریپت مطمئن شویم.
    """
    while True:
        await asyncio.sleep(300) # هر ۵ دقیقه یک بار
        logger.info("Watcher health check: Alive and monitoring.", extra={'status': 'healthy'})




async def source_health_check(context: ContextTypes.DEFAULT_TYPE):
    """
    به صورت دوره‌ای زمان آخرین تغییر فایل‌های سورس را بررسی کرده و
    در صورت عدم به‌روزرسانی، هشدار قطع ارتباط ارسال می‌کند.
    """
    global source_statuses
    DISCONNECT_THRESHOLD = 120 # ثانیه (۲ دقیقه)
    ALERT_COOLDOWN = 300       # ثانیه (۵ دقیقه) - جلوگیری از هشدار تکراری قطع ارتباط

    while True:
        await asyncio.sleep(60) # هر ۶۰ ثانیه یک بار چک کن
        now = time.time()
        log_extra_base = {'task_name': 'SourceHealthCheck'}

        if not source_name_map:
            logger.debug("Source name map is empty, skipping health check.", extra=log_extra_base)
            continue

        active_source_files = set(source_name_map.keys())
        checked_files = set()

        for file_path, source_info in source_name_map.items():
            source_id = source_info.get('id', 'N/A')
            source_name = source_info.get('name', file_path)
            full_path = os.path.join(LOG_DIRECTORY_PATH, file_path) if LOG_DIRECTORY_PATH else file_path # مسیر کامل فایل سورس
            log_extra = {**log_extra_base, 'entity_id': file_path, 'source_name': source_name}
            checked_files.add(file_path)

            try:
                last_modified_time = os.path.getmtime(full_path)
                time_since_update = now - last_modified_time
                current_status_info = source_statuses.get(file_path, {"status": "connected", "last_alert_time": 0})
                current_status = current_status_info["status"]

                # --- منطق قطع ارتباط ---
                if time_since_update > DISCONNECT_THRESHOLD:
                    if current_status == "connected":
                        # فقط اگر وضعیت قبلی "وصل" بود، هشدار قطع بده
                        message = f"⚠️ *Source Disconnected*\n\nSource `{source_name}` (File: `{file_path}`) has not updated in over {DISCONNECT_THRESHOLD // 60} minutes."
                        await send_telegram_alert(context, message)
                        source_statuses[file_path] = {"status": "disconnected", "last_alert_time": now}
                        logger.warning(f"Source '{source_name}' seems disconnected (no update for {time_since_update:.0f}s).", extra=log_extra)
                    elif now - current_status_info.get("last_alert_time", 0) > ALERT_COOLDOWN:
                         # اگر قبلا قطع بوده و زمان زیادی گذشته، دوباره هشدار بده
                         logger.info(f"Source '{source_name}' remains disconnected (no update for {time_since_update:.0f}s). Re-alerting.", extra=log_extra)
                         message = f"🕒 *Source Still Disconnected*\n\nSource `{source_name}` (File: `{file_path}`) remains inactive."
                         await send_telegram_alert(context, message)
                         source_statuses[file_path]["last_alert_time"] = now # زمان آخرین هشدار آپدیت شود

                # --- منطق اتصال مجدد ---
                else:
                    if current_status == "disconnected":
                        message = f"✅ *Source Reconnected*\n\nSource `{source_name}` (File: `{file_path}`) is now updating again."
                        await send_telegram_alert(context, message)
                        source_statuses[file_path] = {"status": "connected", "last_alert_time": 0} # ریست کردن وضعیت
                        logger.info(f"Source '{source_name}' reconnected.", extra=log_extra)
                    # else: وضعیت 'connected' بوده و هنوز هم هست، کاری نکن

            except FileNotFoundError:
                if file_path not in source_statuses or source_statuses[file_path]["status"] != "file_not_found":
                     # اگر فایل برای اولین بار پیدا نشد یا وضعیت قبلی چیز دیگری بود
                     message = f"❌ *Source File Not Found*\n\nFile `{file_path}` for source `{source_name}` was not found. Ensure the source EA is configured correctly."
                     await send_telegram_alert(context, message)
                     source_statuses[file_path] = {"status": "file_not_found", "last_alert_time": now}
                     logger.error(f"Source file not found: {full_path}", extra=log_extra)
            except Exception as e:
                logger.error(f"Error checking source file status for {file_path}: {e}", extra={**log_extra, 'error': str(e)})

        # --- پاک کردن سورس‌های حذف شده از ecosystem.json ---
        removed_files = set(source_statuses.keys()) - checked_files
        for removed_file in removed_files:
            del source_statuses[removed_file]
            logger.info(f"Removed '{removed_file}' from health check status (no longer in ecosystem).", extra=log_extra_base)
# --- پایان تابع جدید ---



async def save_source_statuses_periodically():
    global source_statuses
    log_extra = {'task_name': 'StatusSaver'}
    while True:
        await asyncio.sleep(15) 
        tmp_path = SOURCE_STATUS_PATH + '.tmp'
        try:
            status_to_save = {fp: info.get("status", "unknown") for fp, info in source_statuses.items()}
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(status_to_save, f, ensure_ascii=False)
            os.replace(tmp_path, SOURCE_STATUS_PATH)
            logger.debug(f"Saved {len(status_to_save)} source statuses.", extra=log_extra)
        except Exception as e:
            logger.error("Failed to save source status file.", extra={**log_extra, 'error': str(e)})
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception as remove_e:
                    logger.error(f"Failed to remove temporary status file.", extra={**log_extra, 'error': str(remove_e)})






async def main():
    """
    (بازنویسی شده)
    تابع اصلی برنامه که اتصال دیتابیس async را ایجاد کرده
    و تمام تسک‌ها را راه‌اندازی و مدیریت می‌کند.
    """
    if not all([BOT_TOKEN, ADMIN_ID, LOG_DIRECTORY_PATH]):
        logger.critical("Missing essential environment variables (BOT_TOKEN, ADMIN_ID, LOG_DIRECTORY_PATH). Exiting.")
        return

    application = Application.builder().token(BOT_TOKEN).build()
    logger.info("Log Watcher v3.4 (Async DB) started.")
    
    initialize_database()

    global state_data
    state_data = load_watcher_state()

    db_conn = None
    try:
        # --- ایجاد اتصال ناهمزمان دیتابیس ---
        try:
            db_conn = await aiosqlite.connect(DB_PATH)
            logger.info("Async Database connection established.", extra={'entity_id': DB_PATH, 'status': 'success'})
        except aiosqlite.Error as e:
            logger.critical("Failed to establish async DB connection. Watcher cannot start.", extra={'error': str(e), 'status': 'db_failure'})
            return
        # ---

        asyncio.create_task(batch_state_saver(state_data), name="StateSaver")
        asyncio.create_task(health_checker(), name="HealthChecker")
        asyncio.create_task(source_health_check(application), name="SourceHealthCheck")
        asyncio.create_task(save_source_statuses_periodically(), name="SourceStatusSaver")

        watched_slaves = {}

        while True:
            try:
                load_source_names() 

                log_pattern = os.path.join(LOG_DIRECTORY_PATH, "TradeCopier_*.log")
                all_files = glob(log_pattern)

                slaves_logs = {}
                for f in all_files:
                    match = re.search(r"TradeCopier_(.*?)_\d{4}\.\d{2}\.\d{2}\.log", os.path.basename(f))
                    if match:
                        slave_id = match.group(1)
                        if slave_id:
                            slaves_logs.setdefault(slave_id, []).append(f)

                for slave_id, files in slaves_logs.items():
                    latest_file = max(files, key=os.path.getctime)

                    if slave_id not in watched_slaves or watched_slaves[slave_id]['filepath'] != latest_file:
                        if slave_id in watched_slaves:
                            logger.info(f"Switching log file for '{slave_id}'.", extra={'entity_id': slave_id, 'details': f"From {os.path.basename(watched_slaves[slave_id]['filepath'])} to {os.path.basename(latest_file)}"})
                            watched_slaves[slave_id]['task'].cancel()

                        # --- تغییر کلیدی: ارسال db_conn به تسک ---
                        task = asyncio.create_task(follow_log_file(application, latest_file, db_conn))
                        task.set_name(f"watcher_{slave_id}")
                        watched_slaves[slave_id] = {'filepath': latest_file, 'task': task}

                await asyncio.sleep(60)

            except Exception as e:
                logger.critical("A critical error occurred in the main loop.", extra={'error': str(e), 'status': 'main_loop_failure'})
                await asyncio.sleep(60)
                
    finally:
        if db_conn:
            await db_conn.close()
            logger.info("Async Database connection closed.", extra={'status': 'shutdown'})


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Watcher stopped by user (Ctrl+C).")
    except Exception as e:
        logger.critical(f"Fatal error starting the watcher: {e}", exc_info=True)