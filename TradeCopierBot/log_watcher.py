import os
import time
import asyncio
import logging
import re
from glob import glob
from dotenv import load_dotenv
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError

# --- Basic Configuration ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('watcher.log', mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Load Environment Variables ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")
LOG_DIRECTORY_PATH = os.getenv("LOG_DIRECTORY_PATH")

# --- Telegram Bot Function ---
async def send_telegram_alert(bot: Bot, message: str):
    """Sends a formatted message to the admin."""
    if not ADMIN_ID:
        logger.warning("ADMIN_ID not set. Cannot send alert.")
        return
    try:
        await bot.send_message(
            chat_id=ADMIN_ID,
            text=message,
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"Alert sent to ADMIN_ID {ADMIN_ID}.")
    except TelegramError as e:
        logger.error(f"Failed to send alert to Telegram: {e}")

# --- Log Parsing Logic (نسخه جدید با Source/Copy) ---
def parse_and_format_log_line(line: str) -> str | None:
    line = line.strip()
    if "[TRADE_OPEN]" in line:
        try:
            parts = line.split("]")[1].strip().split(',')
            return (f"✅ *پوزیشن جدید باز شد*\n\n*حساب کپی:* `{parts[0]}`\n*نماد:* `{parts[1]}`\n*حجم:* `{parts[2]}`\n*قیمت باز شدن:* `{parts[3]}`\n*تیکت سورس:* `{parts[4]}`")
        except IndexError: return f"⚠️ *خطا در تجزیه لاگ باز شدن پوزیشن*\n`{line}`"
    elif "[TRADE_CLOSE]" in line:
        try:
            parts = line.split("]")[1].strip().split(',')
            return (f"☑️ *پوزیشن بسته شد*\n\n*حساب کپی:* `{parts[0]}`\n*نماد:* `{parts[1]}`\n*تیکت سورس:* `{parts[2]}`")
        except IndexError: return f"⚠️ *خطا در تجزیه لاگ بسته شدن پوزیشن*\n`{line}`"
    elif "[DD_ALERT]" in line:
        try:
            parts = line.split("]")[1].strip().split(',')
            return (f"🟡 *هشدار حد ضرر روزانه*\n\n*حساب:* `{parts[0]}`\n*میزان ضرر فعلی:* `%{parts[1]}`")
        except IndexError: return f"⚠️ *خطا در تجزیه لاگ هشدار DD*\n`{line}`"
    elif "[DD_STOP]" in line:
        try:
            parts = line.split("]")[1].strip().split(',')
            return (f"🔴 *توقف کپی به دلیل حد ضرر*\n\n*حساب:* `{parts[0]}`\n*ضرر در لحظه توقف:* `%{parts[1]}`\n*آستانه توقف:* `%{parts[2]}`")
        except IndexError: return f"⚠️ *خطا در تجزیه لاگ توقف DD*\n`{line}`"
    elif "[ERROR]" in line:
        error_message = line.split("[ERROR] -")[1].strip()
        return (f"🚨 *خطای اکسپرت*\n\n`{error_message}`")
    return None

# --- File Monitoring Logic (بدون تغییر) ---
async def follow_log_file(bot: Bot, filepath: str):
    """Monitors a single log file for new lines and sends alerts."""
    logger.info(f"Starting to watch log file: {filepath}")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            f.seek(0, 2)
            while True:
                line = f.readline()
                if not line:
                    await asyncio.sleep(1)
                    continue
                
                formatted_message = parse_and_format_log_line(line)
                if formatted_message:
                    await send_telegram_alert(bot, formatted_message)
    except FileNotFoundError:
        logger.warning(f"Log file was likely deleted or moved: {filepath}.")
    except asyncio.CancelledError:
        logger.info(f"Stopped watching old log file: {filepath}")
    except Exception as e:
        logger.error(f"An error occurred while watching {filepath}: {e}", exc_info=True)

# --- Main Application Logic (نسخه بهبود یافته) ---
async def main():
    if not all([BOT_TOKEN, ADMIN_ID, LOG_DIRECTORY_PATH]):
        logger.critical("FATAL: Essential environment variables are missing.")
        return

    bot = Bot(token=BOT_TOKEN)
    logger.info("Log Watcher script started.")
    
    watched_slaves = {}  # دیکشنری برای نگهداری تسک‌های هر اسلیو

    while True:
        try:
            # ۱. یافتن تمام فایل‌های لاگ موجود
            log_pattern = os.path.join(LOG_DIRECTORY_PATH, "TradeCopier_*.log")
            all_files = glob(log_pattern)
            
            # ۲. گروه‌بندی فایل‌ها بر اساس SlaveID
            slaves_logs = {}
            for f in all_files:
                # استخراج SlaveID از نام فایل
                match = re.search(r"TradeCopier_(.*?)_\d{4}\.\d{2}\.\d{2}\.log", os.path.basename(f))
                if match:
                    slave_id = match.group(1)
                    if slave_id not in slaves_logs:
                        slaves_logs[slave_id] = []
                    slaves_logs[slave_id].append(f)

            # ۳. پردازش هر اسلیو برای یافتن آخرین فایل و به‌روزرسانی تسک
            for slave_id, files in slaves_logs.items():
                latest_file = max(files, key=os.path.getctime) # یافتن جدیدترین فایل
                
                # اگر اسلیو قبلا مانیتور نمی‌شده یا فایل آن تغییر کرده
                if slave_id not in watched_slaves or watched_slaves[slave_id]['filepath'] != latest_file:
                    # اگر تسک قدیمی وجود دارد، آن را متوقف کن
                    if slave_id in watched_slaves:
                        logger.info(f"New log file detected for '{slave_id}'. Switching from {os.path.basename(watched_slaves[slave_id]['filepath'])} to {os.path.basename(latest_file)}")
                        watched_slaves[slave_id]['task'].cancel()
                    
                    # تسک جدید برای فایل جدید ایجاد کن
                    task = asyncio.create_task(follow_log_file(bot, latest_file))
                    watched_slaves[slave_id] = {'filepath': latest_file, 'task': task}
            
            await asyncio.sleep(60) # اسکن مجدد هر 60 ثانیه
        except Exception as e:
            logger.error(f"An error occurred in the main loop: {e}", exc_info=True)
            await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Log Watcher script stopped by user.")