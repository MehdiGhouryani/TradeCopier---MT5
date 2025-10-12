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
import json

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
CHANNEL_ID = os.getenv("CHANNEL_ID") # << خط جدید
ECOSYSTEM_PATH = os.getenv("ECOSYSTEM_PATH") # << خط جدید

source_name_map = {}

def load_source_names():
    """Loads the file_path -> name mapping from ecosystem.json."""
    global source_name_map
    if not ECOSYSTEM_PATH:
        logger.warning("ECOSYSTEM_PATH not set. Cannot load source names.")
        return
    try:
        with open(ECOSYSTEM_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        temp_map = {}
        for source in data.get('sources', []):
            if 'file_path' in source and 'name' in source:
                temp_map[source['file_path']] = source['name']
        
        source_name_map = temp_map
        logger.info(f"Successfully loaded {len(source_name_map)} source names.")
    except Exception as e:
        logger.error(f"Failed to load or parse ecosystem.json for source names: {e}")

# --- Telegram Bot Function ---
async def send_telegram_alert(bot: Bot, message: str):
    """Sends a formatted message, prioritizing Channel and falling back to Admin."""
    target_id = CHANNEL_ID if CHANNEL_ID else ADMIN_ID
    if not target_id:
        logger.warning("Neither CHANNEL_ID nor ADMIN_ID are set. Cannot send alert.")
        return

    try:
        await bot.send_message(
            chat_id=target_id,
            text=message,
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"Alert sent to target_id {target_id}.")
    except TelegramError as e:
        logger.error(f"Failed to send alert to primary target {target_id}: {e}")
        # Fallback to ADMIN_ID if the primary target was the channel and it failed
        if target_id == CHANNEL_ID and ADMIN_ID:
            logger.info(f"Falling back to sending alert to ADMIN_ID {ADMIN_ID}.")
            try:
                await bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"⚠️ *ارسال به کانال ناموفق بود.*\n\n{message}",
                    parse_mode=ParseMode.MARKDOWN
                )
            except TelegramError as e2:
                logger.error(f"Failed to send fallback alert to ADMIN_ID: {e2}")

# --- Log Parsing Logic (نسخه جدید با Source/Copy) ---
def parse_and_format_log_line(line: str) -> str | None:
    """Parses the new rich log formats and creates formatted messages."""
    line = line.strip()

    try:
        if "[TRADE_OPEN]" in line:
            parts = line.split("]")[1].strip().split(',')
            copy_id, symbol, volume, price, source_ticket, source_file = parts
            source_name = source_name_map.get(source_file, source_file) # Fallback to file name
            return (
                f"✅ *پوزیشن جدید باز شد*\n\n"
                f"*سورس:* `{source_name}`\n"
                f"*حساب کپی:* `{copy_id}`\n"
                f"*نماد:* `{symbol}`\n"
                f"*حجم:* `{volume}`\n"
                f"*قیمت باز شدن:* `{price}`\n"
                f"*تیکت سورس:* `{source_ticket}`"
            )

        elif "[TRADE_CLOSE]" in line:
            parts = line.split("]")[1].strip().split(',')
            copy_id, symbol, source_ticket, profit_str = parts
            profit = float(profit_str)
            profit_text = f"+${profit:,.2f}" if profit >= 0 else f"-${abs(profit):,.2f}"
            emoji = "☑️" if profit >= 0 else "🔻"
            
            # فرض می‌کنیم نام سورس در حافظه موقت موجود است (این بخش در آینده می‌تواند بهبود یابد)
            # در حال حاضر چون نام سورس در لاگ بسته شدن نیست، آن را نمایش نمی‌دهیم
            return (
                f"{emoji} *پوزیشن بسته شد*\n\n"
                f"*حساب کپی:* `{copy_id}`\n"
                f"*نماد:* `{symbol}`\n"
                f"*سود/ضرر:* `{profit_text}`\n"
                f"*تیکت سورس:* `{source_ticket}`"
            )

        elif "[DD_ALERT]" in line:
            parts = line.split("]")[1].strip().split(',')
            copy_id, dd, dollar_loss, start_equity, peak_equity = parts
            return (
                f"🟡 *هشدار حد ضرر روزانه*\n\n"
                f"*حساب:* `{copy_id}`\n"
                f"*میزان ضرر فعلی:* `%{float(dd):.2f}` `(-${float(dollar_loss):,.2f})`\n"
                f"*موجودی اولیه روز:* `${float(start_equity):,.2f}`\n"
                f"*حداکثر موجودی روز:* `${float(peak_equity):,.2f}`"
            )

        elif "[DD_STOP]" in line:
            parts = line.split("]")[1].strip().split(',')
            copy_id, dd, dd_limit, dollar_loss, start_equity, peak_equity = parts
            return (
                f"🔴 *توقف کپی به دلیل حد ضرر*\n\n"
                f"*حساب:* `{copy_id}`\n"
                f"*ضرر در لحظه توقف:* `%{float(dd):.2f}` `(-${float(dollar_loss):,.2f})`\n"
                f"*آستانه توقف:* `%{float(dd_limit):.2f}`"
            )

        elif "[ERROR]" in line:
            error_message = line.split("[ERROR] -")[1].strip()
            return f"🚨 *خطای اکسپرت*\n\n`{error_message}`"

    except (IndexError, ValueError) as e:
        logger.error(f"Error parsing log line: '{line}'. Error: {e}")
        return f"⚠️ *خطا در تجزیه لاگ*\n`{line}`"
        
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
            # فراخوانی تابع برای به‌روزرسانی نام‌ها
            load_source_names() 
            
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