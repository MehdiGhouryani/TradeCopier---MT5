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
# فایل لاگ به صورت خودکار پس از رسیدن به حجم ۵ مگابایت، آرشیو می‌شود.
file_handler = RotatingFileHandler('watcher.log', maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
file_handler.setLevel(logging.DEBUG) # تمام جزئیات در فایل ذخیره شود
file_handler.setFormatter(JsonFormatter()) # استفاده از فرمتر جدید

# حذف مدیریت‌کننده‌های قبلی برای جلوگیری از لاگ تکراری
if logger.hasHandlers():
    logger.handlers.clear()

# افزودن مدیریت‌کننده‌های جدید به لاگر
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# کاهش لاگ‌های اضافی از کتابخانه‌های دیگر
logging.getLogger('httpx').setLevel(logging.WARNING)

# --- پایان بخش لاگ‌گیری ---



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

source_name_map = {}
state_data = {}  # دیکشنری وضعیت سراسری
state_changed = False  # فلگ برای ذخیره‌سازی دسته‌ای

def load_source_names():
    """
    نام‌های نمایشی منابع را از فایل ecosystem.json بارگذاری می‌کند.
    این تابع با مدیریت خطای کامل نوشته شده است.
    """
    global source_name_map
    if not ECOSYSTEM_PATH:
        logger.warning("ECOSYSTEM_PATH not set. Skipping source names load.", extra={'status': 'skipped'})
        return
    try:
        with open(ECOSYSTEM_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # فقط منابعی که file_path و name دارند را استخراج می‌کند
        temp_map = {
            source['file_path']: source['name'] 
            for source in data.get('sources', []) 
            if 'file_path' in source and 'name' in source
        }
        # فقط در صورت موفقیت کامل، متغیر سراسری را به‌روزرسانی کن
        source_name_map = temp_map
        logger.info(f"Loaded {len(source_name_map)} source names from ecosystem.json.", extra={'status': 'success'})
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
            
# --- پایان بخش ارسال اعلان ---



# --- فاز ۲، بخش اول: تجزیه لاگ با Regex (Robust Parsing) ---

# الگوهای Regex از پیش کامپایل شده برای عملکرد بهتر
# این الگوها مشکل اصلی Parse Error را برای همیشه حل می‌کنند.
open_pattern = re.compile(r'\[TRADE_OPEN\]\s+(.*)')
close_pattern = re.compile(r'\[TRADE_CLOSE\]\s+(.*)')
alert_pattern = re.compile(r'\[DD_ALERT\]\s+(.*)')
stop_pattern = re.compile(r'\[DD_STOP\]\s+(.*)')
error_pattern = re.compile(r'\[ERROR\]\s+-\s+(.*)')

def parse_and_format_log_line(line: str, state: dict) -> str | None:
    """
    یک خط لاگ را با استفاده از Regex تجزیه و به یک پیام خوانا برای تلگرام تبدیل می‌کند.
    این تابع در مقابل فرمت‌های مختلف مقاوم است و خطاهای تجزیه را مدیریت می‌کند.
    """
    global state_changed

    line = line.strip()
    if not line:
        return None

    try:
        # --- پردازش لاگ TRADE_OPEN ---
        if match := open_pattern.search(line):
            # مثال: copy_A,XAUUSD,0.11 (Source:0.11,Mult:1.00),4245.71000,178661555,TradeCopier_S2.txt
            parts_str = match.group(1)
            # از Regex برای جدا کردن بخش‌ها به صورت امن استفاده می‌کنیم
            parts = re.match(r'([^,]+),\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^,]+),\s*([^,]+)', parts_str)
            if not parts or len(parts.groups()) != 6:
                raise ValueError(f"Invalid OPEN format: {parts_str}")
            
            copy_id, symbol, volume_info, price, source_ticket, source_file = [p.strip() for p in parts.groups()]
            source_name = source_name_map.get(source_file, source_file)
            
            # ذخیره نام منبع برای استفاده در لاگ TRADE_CLOSE
            if state.get(source_ticket) != source_name:
                state[source_ticket] = source_name
                state_changed = True
                
            return (
                f"✅ *New Position Opened*\n\n"
                f"*Source:* `{source_name}`\n"
                f"*Copy Account:* `{copy_id}`\n"
                f"*Symbol:* `{symbol}`\n"
                f"*Volume:* `{volume_info}`\n"
                f"*Open Price:* `{price}`\n"
                f"*Source Ticket:* `{source_ticket}`"
            )

        # --- پردازش لاگ TRADE_CLOSE ---
        elif match := close_pattern.search(line):
            parts = match.group(1).split(',')
            # فرمت جدید با نام فایل: copy_A,XAUUSD,178662307,-5.06,TradeCopier_S2.txt
            if len(parts) == 5:
                copy_id, symbol, source_ticket, profit_str, source_file = [p.strip() for p in parts]
                source_name = source_name_map.get(source_file, source_file)
            # فرمت قدیمی برای سازگاری: copy_A,XAUUSD,178662307,-5.06
            elif len(parts) == 4:
                copy_id, symbol, source_ticket, profit_str = [p.strip() for p in parts]
                # نام منبع را از وضعیت ذخیره شده می‌خوانیم
                source_name = state.get(source_ticket, 'Unknown Source')
            else:
                raise ValueError(f"Invalid CLOSE format: expected 4 or 5 parts, got {len(parts)}")
            
            profit = float(profit_str)
            profit_text = f"+${profit:,.2f}" if profit >= 0 else f"-${abs(profit):,.2f}"
            emoji = "☑️" if profit >= 0 else "🔻"
            
            # پس از بسته شدن، اطلاعات معامله را از وضعیت حذف می‌کنیم
            if source_ticket in state:
                del state[source_ticket]
                state_changed = True
                
            return (
                f"{emoji} *Position Closed*\n\n"
                f"*Source:* `{source_name}`\n"
                f"*Copy Account:* `{copy_id}`\n"
                f"*Symbol:* `{symbol}`\n"
                f"*Profit/Loss:* `{profit_text}`\n"
                f"*Source Ticket:* `{source_ticket}`"
            )

        # --- پردازش لاگ DD_ALERT ---
        elif match := alert_pattern.search(line):
            parts = [p.strip() for p in match.group(1).split(',')]
            if len(parts) != 5:
                raise ValueError(f"Invalid ALERT format: expected 5 parts, got {len(parts)}")
            copy_id, dd, dollar_loss, start_equity, peak_equity = parts
            return (
                f"🟡 *Daily Drawdown Alert*\n\n"
                f"*Account:* `{copy_id}`\n"
                f"*Current Loss:* `%{float(dd):.2f}` `(-${float(dollar_loss):,.2f})`\n"
                f"*Daily Start Equity:* `${float(start_equity):,.2f}`\n"
                f"*Daily Peak Equity:* `${float(peak_equity):,.2f}`"
            )

        # --- پردازش لاگ DD_STOP ---
        elif match := stop_pattern.search(line):
            parts = [p.strip() for p in match.group(1).split(',')]
            if len(parts) != 6:
                raise ValueError(f"Invalid STOP format: expected 6 parts, got {len(parts)}")
            copy_id, dd, dd_limit, dollar_loss, start_equity, peak_equity = parts
            return (
                f"🔴 *Copy Stopped Due to DD Limit*\n\n"
                f"*Account:* `{copy_id}`\n"
                f"*Loss at Stop:* `%{float(dd):.2f}` `(-${float(dollar_loss):,.2f})`\n"
                f"*Stop Threshold:* `%{float(dd_limit):,.2f}`"
            )

        # --- پردازش لاگ ERROR ---
        elif match := error_pattern.search(line):
            error_message = match.group(1).strip()
            # خطاهای مربوط به عدم یافتن فایل سورس را نادیده می‌گیریم تا اسپم نشود
            if "Failed to open source file" in error_message:
                logger.debug("Ignoring non-critical source file error.", extra={'details': error_message})
                return None
            return f"🚨 *Expert Error*\n\n`{error_message}`"

    except (ValueError, IndexError) as e:
        # اگر هرگونه خطایی در تجزیه رخ داد، آن را به عنوان هشدار لاگ کرده و به تلگرام ارسال می‌کنیم
        logger.warning(f"Malformed log line skipped: '{line}'. Error: {e}", extra={'status': 'parse_error'})
        return f"⚠️ *Parse Error in Log*\n`{line}`"

    return None

# --- پایان بخش تجزیه لاگ ---




# --- فاز ۳، بخش اول: نظارت ناهمزمان بر فایل‌ها ---

async def follow_log_file(context: ContextTypes.DEFAULT_TYPE, filepath: str, state: dict):
    """
    یک فایل لاگ را به صورت ناهمزمان دنبال کرده و خطوط جدید را پردازش می‌کند.
    این تابع برای مدیریت بهتر خطاها و لغو شدن تسک بهینه شده است.
    """
    task_name = asyncio.current_task().get_name()
    log_extra = {'task_name': task_name, 'entity_id': os.path.basename(filepath)}
    
    logger.info("Starting to watch log file.", extra=log_extra)
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            # رفتن به انتهای فایل برای خواندن خطوط جدید
            f.seek(0, 2)
            while True:
                line = f.readline()
                if not line:
                    await asyncio.sleep(1) # اگر خط جدیدی نبود، یک ثانیه صبر کن
                    continue
                
                # پردازش خط و دریافت پیام فرمت شده
                formatted_message = parse_and_format_log_line(line, state)
                
                if formatted_message:
                    # استفاده از تابع ارسال هوشمند جدید
                    await send_telegram_alert(context, formatted_message)

    except FileNotFoundError:
        logger.warning("Log file was not found or has been deleted. Task is stopping.", extra=log_extra)
    except asyncio.CancelledError:
        logger.info("Log file watch task has been cancelled (likely due to a new log file).", extra=log_extra)
        # این خطا طبیعی است و نیازی به اقدام خاصی ندارد
        pass
    except Exception as e:
        logger.error("An unexpected error occurred while watching log file.", extra={**log_extra, 'error': str(e), 'status': 'failure'})
        # در صورت بروز خطای ناشناخته، به ادمین اطلاع بده
        await send_telegram_alert(context, f"🚨 *Critical Watcher Error*\n\nTask `{task_name}` failed while watching `{os.path.basename(filepath)}`\nError: `{str(e)}`")

# --- پایان بخش نظارت بر فایل‌ها ---#






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

async def main():
    """
    تابع اصلی برنامه که تمام تسک‌ها را راه‌اندازی و مدیریت می‌کند.
    """
    if not all([BOT_TOKEN, ADMIN_ID, LOG_DIRECTORY_PATH]):
        logger.critical("Missing essential environment variables (BOT_TOKEN, ADMIN_ID, LOG_DIRECTORY_PATH). Exiting.")
        return

    # استفاده از ApplicationBuilder برای ساختار مدرن
    application = Application.builder().token(BOT_TOKEN).build()
    logger.info("Log Watcher v3.3 Professional Edition started.")

    global state_data
    state_data = load_watcher_state()

    # راه‌اندازی تسک‌های پس‌زمینه
    asyncio.create_task(batch_state_saver(state_data), name="StateSaver")
    asyncio.create_task(health_checker(), name="HealthChecker")

    watched_slaves = {} # دیکشنری برای نگهداری تسک‌های فعال

    while True:
        try:
            # بارگذاری مجدد نام سورس‌ها برای دریافت تغییرات
            load_source_names()

            log_pattern = os.path.join(LOG_DIRECTORY_PATH, "TradeCopier_*.log")
            all_files = glob(log_pattern)

            slaves_logs = {}
            for f in all_files:
                # پیدا کردن شناسه slave از نام فایل
                match = re.search(r"TradeCopier_(.*?)_\d{4}\.\d{2}\.\d{2}\.log", os.path.basename(f))
                if match:
                    slave_id = match.group(1)
                    # اگر slave_id خالی بود (مربوط به اکسپرت سورس)، نادیده بگیر
                    if slave_id:
                        slaves_logs.setdefault(slave_id, []).append(f)

            for slave_id, files in slaves_logs.items():
                latest_file = max(files, key=os.path.getctime)
                
                # اگر این slave برای اولین بار است که دیده می‌شود یا فایل لاگ آن تغییر کرده
                if slave_id not in watched_slaves or watched_slaves[slave_id]['filepath'] != latest_file:
                    if slave_id in watched_slaves:
                        logger.info(f"Switching log file for '{slave_id}'.", extra={'entity_id': slave_id, 'details': f"From {os.path.basename(watched_slaves[slave_id]['filepath'])} to {os.path.basename(latest_file)}"})
                        # تسک قدیمی را لغو کن
                        watched_slaves[slave_id]['task'].cancel()
                    
                    # تسک جدید برای فایل جدید ایجاد کن
                    task = asyncio.create_task(follow_log_file(application, latest_file, state_data))
                    task.set_name(f"watcher_{slave_id}")
                    watched_slaves[slave_id] = {'filepath': latest_file, 'task': task}

            await asyncio.sleep(60) # هر ۶۰ ثانیه یک بار بررسی مجدد

        except Exception as e:
            logger.critical("A critical error occurred in the main loop.", extra={'error': str(e), 'status': 'main_loop_failure'})
            await asyncio.sleep(60) # قبل از تلاش مجدد، کمی صبر کن

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Watcher stopped by user (Ctrl+C).")
    except Exception as e:
        logger.critical(f"Fatal error starting the watcher: {e}", exc_info=True)