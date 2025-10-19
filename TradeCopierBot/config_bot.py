import os
import logging
import json
import traceback
import sqlite3
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from telegram.error import BadRequest
from functools import wraps
import glob
from telegram.constants import ParseMode
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta






class JsonFormatter(logging.Formatter):
    """Custom formatter to output logs in JSON format."""
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        extra_keys = ['user_id', 'username', 'callback_data', 'command', 'input_for', 'action_attempt', 'status', 'entity_id', 'details', 'error']
        for key in extra_keys:
            if hasattr(record, key):
                log_record[key] = getattr(record, key)
        return json.dumps(log_record, ensure_ascii=False)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

file_handler = RotatingFileHandler('bot.log', maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(JsonFormatter())

logger.addHandler(console_handler)
logger.addHandler(file_handler)

logging.getLogger('httpx').setLevel(logging.WARNING)






load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 1717599240))
ECOSYSTEM_PATH_STR = os.getenv("ECOSYSTEM_PATH")
LOG_DIRECTORY_PATH = os.getenv("LOG_DIRECTORY_PATH")

try:
    ALLOWED_USERS = [int(uid) for uid in os.getenv("ALLOWED_USERS", "").split(",") if uid]
except (ValueError, TypeError):
    ALLOWED_USERS = []
    logger.error("Failed to parse ALLOWED_USERS from .env", extra={'status': 'failure'})

ECOSYSTEM_PATH = ""
if ECOSYSTEM_PATH_STR:
    base_dir = os.path.dirname(os.path.realpath(__file__))
    ECOSYSTEM_PATH = ECOSYSTEM_PATH_STR if os.path.isabs(ECOSYSTEM_PATH_STR) else os.path.join(base_dir, ECOSYSTEM_PATH_STR)
else:
    logger.critical("ECOSYSTEM_PATH not set", extra={'status': 'failure'})
    raise ValueError("ECOSYSTEM_PATH is missing")


DB_PATH = os.path.join(os.path.dirname(ECOSYSTEM_PATH) if ECOSYSTEM_PATH else '.', 'trade_history.db')
SOURCE_STATUS_PATH = os.path.join(os.path.dirname(ECOSYSTEM_PATH) if ECOSYSTEM_PATH else '.', 'source_status.json')


def escape_markdown_v2(text: str) -> str:
    """Escapes special characters for Telegram's MarkdownV2 format."""
    escape_chars = r'_*[]()~`>#+-=|{}.!\\'
    return ''.join(f'\\{char}' if char in escape_chars else char for char in str(text))





async def notify_admin_on_error(context: ContextTypes.DEFAULT_TYPE, function_name: str, error: Exception, **kwargs):
    """Send formatted error message to admin."""
    details = ", ".join([f"{k}='{v}'" for k, v in kwargs.items()])
    message = (
        f"🚨 *خطای ربات*\n\n"
        f"تابع: `{function_name}`\n"
        f"جزئیات: {escape_markdown_v2(details)}\n"
        f"خطا: `{escape_markdown_v2(str(error))}`"
    )
    try:
        await context.bot.send_message(chat_id=ADMIN_ID, text=message, parse_mode=ParseMode.MARKDOWN_V2)
        logger.info("Error notification sent", extra={'status': 'success', 'function': function_name})
    except Exception as e:
        logger.error("Failed to send error notification", extra={'status': 'failure', 'error': str(e)})




async def get_detailed_status_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    ecosystem = context.bot_data.get('ecosystem', {})
    if not ecosystem:
        return "> ❌ *خطا: داده‌های سیستم بارگذاری نشده‌اند\\.*"

    source_statuses = load_source_statuses()

    last_mod_time = "نامشخص"
    try:
        if ECOSYSTEM_PATH and os.path.exists(ECOSYSTEM_PATH):
             last_mod_timestamp = os.path.getmtime(ECOSYSTEM_PATH)
             last_mod_time = datetime.fromtimestamp(last_mod_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        else:
             last_mod_time = "فایل یافت نشد"
             logger.warning(f"Ecosystem path not found or not set for timestamp check: {ECOSYSTEM_PATH}")
    except Exception as e:
        last_mod_time = "خطا در خواندن"
        logger.error(f"Error getting ecosystem file modification time: {e}", exc_info=True)


    source_map = {s['file_path']: s for s in ecosystem.get('sources', []) if 'file_path' in s}
    source_id_to_filepath = {s['id']: s['file_path'] for s in ecosystem.get('sources', []) if 'id' in s and 'file_path' in s}

    status_lines = [
        f"> 🏛️ *وضعیت سیستم*",
        f"> 🕓 *آخرین به‌روزرسانی:* {escape_markdown_v2(last_mod_time)}",
        ">"
    ]
    copies = ecosystem.get('copies', [])
    if not copies:
        status_lines.append("> 🛡️ *بدون حساب کپی\\.*")
    else:
        for i, copy_account in enumerate(copies):
            status_lines.append("> ───")
            copy_id = copy_account['id']
            settings = copy_account.get('settings', {})
            dd = float(settings.get("DailyDrawdownPercent", 0))
            risk_text = escape_markdown_v2(f"{dd:.2f}%") if dd > 0 else "غیرفعال"
            flag_file_path = os.path.join(os.path.dirname(ECOSYSTEM_PATH) if ECOSYSTEM_PATH else '.', f"{copy_id}_stopped.flag")

            copy_status_emoji = "🛑" if os.path.exists(flag_file_path) else "✅"
            copy_status_text = "متوقف" if copy_status_emoji == "🛑" else "فعال"

            copy_name_escaped = escape_markdown_v2(copy_account['name'])
            header = f"> 🛡️ *حساب کپی:* {copy_name_escaped} \\({copy_status_emoji} {copy_status_text}\\)"
            status_lines.append(header)
            status_lines.append(f"> ▫️ *ریسک روزانه:* {risk_text}")
            connections = ecosystem.get('mapping', {}).get(copy_id, [])
            if not connections:
                status_lines.append("> ▫️ *اتصالات:* *بدون منبع\\.*")
            else:
                status_lines.append("> ▫️ *اتصالات:*")
                for conn in connections:
                    source_id = conn.get('source_id')
                    source_filepath = source_id_to_filepath.get(source_id)

                    if source_filepath and source_filepath in source_map:
                         source_info = source_map[source_filepath]
                         vs = conn.get('volume_settings', {})
                         mode = "Fixed" if "FixedVolume" in vs else "Multiplier"
                         value = vs.get("FixedVolume", vs.get("Multiplier", "1.0"))
                         source_name_escaped = escape_markdown_v2(source_info['name'])

                         status = source_statuses.get(source_filepath, "unknown")
                         status_emoji = "🟢"
                         if status == "disconnected":
                             status_emoji = "🔴"
                         elif status == "file_not_found":
                             status_emoji = "❓"
                         elif status == "unknown":
                              status_emoji = "⚪"

                         status_line = f">      {status_emoji} *{source_name_escaped}* ⟵ `{mode}: {escape_markdown_v2(str(value))}`" # استفاده از تورفتگی به جای └──
                         status_lines.append(status_line)
                    else:
                         status_lines.append(f">      ❓ *منبع نامعتبر ({escape_markdown_v2(source_id)})*") # استفاده از تورفتگی

            if i < len(copies) - 1:
                status_lines.append(">")
    return "\n".join(status_lines)





def load_ecosystem(application: Application) -> bool:
    """Load ecosystem data from JSON file into bot_data for caching."""
    try:
        with open(ECOSYSTEM_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        required_keys = ["sources", "copies", "mapping"]
        if not all(key in data for key in required_keys):
            raise KeyError("Ecosystem JSON missing required keys")
        application.bot_data['ecosystem'] = data
        logger.info("Ecosystem loaded", extra={'status': 'success'})
        return True
    except FileNotFoundError:
        logger.warning("Ecosystem file not found, creating empty", extra={'status': 'info', 'entity_id': ECOSYSTEM_PATH})
        with open(ECOSYSTEM_PATH, 'w', encoding='utf-8') as f:
            json.dump({"sources": [], "copies": [], "mapping": {}}, f, indent=2)
        return load_ecosystem(application)
    except json.JSONDecodeError as e:
        logger.error("Ecosystem JSON parse failed", extra={'status': 'failure', 'error': str(e)})
        return False
    except Exception as e:
        logger.error("Ecosystem load failed", extra={'status': 'failure', 'error': str(e)})
        return False



def save_ecosystem(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Save cached ecosystem data to JSON file using atomic write."""
    if 'ecosystem' not in context.bot_data:
        logger.warning("Ecosystem data not found in bot_data", extra={'status': 'failure'})
        return False
    tmp_path = ECOSYSTEM_PATH + ".tmp"
    try:
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(context.bot_data['ecosystem'], f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, ECOSYSTEM_PATH)
        logger.info("Ecosystem saved", extra={'user_id': context.user_data.get('active_user_id', 'Unknown'), 'status': 'success'})
        return True
    except Exception as e:
        logger.error("Ecosystem save failed", extra={'user_id': context.user_data.get('active_user_id', 'Unknown'), 'status': 'failure', 'error': str(e)})
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return False




def load_source_statuses() -> dict:
    if not os.path.exists(SOURCE_STATUS_PATH):
        logger.debug(f"Source status file not found at {SOURCE_STATUS_PATH}")
        return {}
    try:
        with open(SOURCE_STATUS_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            logger.warning(f"Invalid format in source status file: {SOURCE_STATUS_PATH}. Expected a dictionary.")
            return {}
        return data
    except json.JSONDecodeError as e:
        logger.warning(f"Error decoding JSON from source status file: {e}")
        return {}
    except Exception as e:
        logger.error(f"Failed to load source status file: {e}")
        return {}





def backup_ecosystem():
    """Create a backup of ecosystem.json before modifications."""
    if os.path.exists(ECOSYSTEM_PATH):
        backup_path = ECOSYSTEM_PATH + ".bak." + datetime.now().strftime('%Y%m%d%H%M%S')
        os.rename(ECOSYSTEM_PATH, backup_path)
        logger.info("Ecosystem backed up", extra={'status': 'success', 'entity_id': backup_path})



async def regenerate_all_configs(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Regenerate all configuration files for sources and copy accounts."""
    ecosystem = context.bot_data.get('ecosystem', {})
    copies = ecosystem.get('copies', [])
    all_success = True
    for copy_account in copies:
        if not await regenerate_copy_config(copy_account['id'], context):
            all_success = False
        if not await regenerate_copy_settings_config(copy_account['id'], context):
            all_success = False
    logger.info("All configs regenerated", extra={'status': 'success' if all_success else 'failure'})
    return all_success




async def regenerate_copy_config(copy_id: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Regenerates the source configuration file (.cfg) for a specific copy account.
    This final version robustly ensures the correct 5-column format to prevent parsing errors in the EA.
    """
    log_extra = {'entity_id': copy_id, 'status': 'starting'}
    logger.debug("Starting regeneration of copy config.", extra=log_extra)
    
    ecosystem = context.bot_data.get('ecosystem', {})
    connections = ecosystem.get('mapping', {}).get(copy_id, [])
    all_sources = {source['id']: source for source in ecosystem.get('sources', [])}
    
    content = ["# file_path,mode,allowed_symbols,volume_type,volume_value"]
    
    for conn in connections:
        source_id = conn.get('source_id')
        if source_id in all_sources:
            source_info = all_sources[source_id]
            
            mode = conn.get('mode', 'ALL').upper()
            
            # اگر حالت SYMBOLS نبود، این فیلد باید خالی باشد اما همچنان وجود داشته باشد.
            # این بخش کلیدی برای حل مشکل است.
            allowed_symbols = conn.get('allowed_symbols', '') if mode == 'SYMBOLS' else ''
            
            volume_settings = conn.get('volume_settings', {})
            
            if "FixedVolume" in volume_settings:
                volume_type = "FIXED"
                volume_value = volume_settings["FixedVolume"]
            else:
                volume_type = "MULTIPLIER"
                volume_value = volume_settings.get("Multiplier", 1.0)
            
            # ساختن خط با فرمت دقیق و صحیح 
            # تمام ۵ متغیر در f-string پاس داده می‌شوند تا فرمت همیشه درست باشد.
            line = f"{source_info['file_path']},{mode},{allowed_symbols},{volume_type},{volume_value}"
            content.append(line)
        else:
            logger.warning(f"Source ID '{source_id}' in mapping not in sources list. Skipping.", extra=log_extra)
            
    cfg_path = os.path.join(os.path.dirname(ECOSYSTEM_PATH), f"{copy_id}_sources.cfg")
    tmp_path = cfg_path + ".tmp"
    
    try:
        with open(tmp_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(content))
        os.replace(tmp_path, cfg_path)
        log_extra['status'] = 'success'
        logger.info("Successfully regenerated copy config file with correct format.", extra=log_extra)
        return True
    except Exception as e:
        log_extra.update({'status': 'failure', 'error': str(e)})
        logger.error("Failed during copy config regeneration.", extra=log_extra)
        await notify_admin_on_error(context, "regenerate_copy_config", e, copy_id=copy_id)
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return False


async def regenerate_copy_settings_config(copy_id: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Regenerate settings configuration file for a copy account."""
    ecosystem = context.bot_data.get('ecosystem', {})
    copy_account = next((c for c in ecosystem.get('copies', []) if c['id'] == copy_id), None)
    if not copy_account:
        logger.error("Copy account not found for config regeneration", extra={'entity_id': copy_id, 'status': 'failure'})
        return False
    settings = copy_account.get('settings', {})
    config_path = os.path.join(os.path.dirname(ECOSYSTEM_PATH), f"{copy_id}_config.txt")
    tmp_path = config_path + ".tmp"
    content = []
    if context.user_data.get('reset_stop_for_copy') == copy_id:
        content.append("ResetStop=true")
        context.user_data.pop('reset_stop_for_copy', None)
    for key, value in settings.items():
        content.append(f"{key}={value}")
    try:
        with open(tmp_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(content))
        os.replace(tmp_path, config_path)
        logger.info("Copy settings config regenerated", extra={'entity_id': copy_id, 'status': 'success'})
        return True
    except Exception as e:
        logger.error("Copy settings config regeneration failed", extra={'entity_id': copy_id, 'status': 'failure', 'error': str(e)})
        await notify_admin_on_error(context, "regenerate_copy_settings_config", e, copy_id=copy_id)
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return False



def is_user_allowed(user_id: int) -> bool:
    """Check if user is allowed to access the bot."""
    return user_id in ALLOWED_USERS



def allowed_users_only(func):
    """Decorator to log user actions and restrict access."""
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user or not is_user_allowed(user.id):
            if user:
                action_attempt = "N/A"
                if update.callback_query:
                    action_attempt = f"callback:{update.callback_query.data}"
                elif update.message and update.message.text:
                    action_attempt = f"command:{update.message.text}"
                logger.warning("Unauthorized access attempt", extra={'user_id': user.id, 'username': f"@{user.username}" if user.username else "N/A", 'action_attempt': action_attempt, 'status': 'denied'})
            unauthorized_text = "دسترسی غیرمجاز."
            if update.callback_query:
                await update.callback_query.answer(unauthorized_text, show_alert=True)
            elif update.message:
                await update.message.reply_text(unauthorized_text, parse_mode=ParseMode.MARKDOWN_V2)
            return

        context.user_data['active_user_id'] = user.id
        extra_info = {'user_id': user.id, 'status': 'start'}
        if user.username:
            extra_info['username'] = f"@{user.username}"
        message = "User action received"
        if update.callback_query:
            extra_info['callback_data'] = update.callback_query.data
            message = "Callback received"
        elif update.message and update.message.text:
            if update.message.text.startswith('/'):
                extra_info['command'] = update.message.text
                message = "Command received"
            elif context.user_data.get('waiting_for'):
                extra_info['input_for'] = context.user_data.get('waiting_for')
                message = "Text input received"
        logger.info(message, extra=extra_info)
        return await func(update, context, *args, **kwargs)
    return wrapped



def is_admin(user_id: int) -> bool:
    """Check if user is admin."""
    return user_id == ADMIN_ID



def admin_only(func):
    """Decorator to restrict access to admin only."""
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user or not is_admin(user.id):
            logger.warning("Admin-only access denied", extra={'user_id': user.id, 'username': f"@{user.username}" if user.username else "N/A", 'status': 'denied'})
            unauthorized_text = "فقط ادمین مجاز است."
            if update.callback_query:
                await update.callback_query.answer(unauthorized_text, show_alert=True)
            elif update.message:
                await update.message.reply_text(unauthorized_text, parse_mode=ParseMode.MARKDOWN_V2)
            return
        return await func(update, context, *args, **kwargs)
    return wrapped




@allowed_users_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display main menu and system status."""
    keyboard = [
        [InlineKeyboardButton("📊 وضعیت", callback_data="status")],
        [InlineKeyboardButton("📊 آمار", callback_data="statistics_menu")], # <-- دکمه جدید اضافه شد
        [InlineKeyboardButton("🛡️ حساب‌های کپی", callback_data="menu_copy_settings")],
        [InlineKeyboardButton("📊 منابع", callback_data="sources:main")],
        [InlineKeyboardButton("🔗 اتصالات", callback_data="menu_connections")],
        [InlineKeyboardButton("🔄 بازسازی فایل‌ها", callback_data="regenerate_all_files")],
        [InlineKeyboardButton("❓ راهنما", callback_data="menu_help")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    status_text = await get_detailed_status_text(context)
    if update.callback_query:
        # برای جلوگیری از خطای "Message is not modified" در هنگام رفرش وضعیت
        if update.callback_query.data == "status":
             await update.callback_query.answer("✅ وضعیت به‌روز شد")

        try:
            await update.callback_query.edit_message_text(
                status_text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN_V2
            )
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                logger.warning(f"Failed to edit message on status refresh: {e}") # لاگ هشدار به جای exception
            # else: message not modified, ignore
    else:
        await update.message.reply_text(
            status_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN_V2
        )





# --- نیازمند import های جدید در ابتدای فایل ---
from datetime import datetime, timedelta
# ---

@allowed_users_only
async def handle_statistics_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    منوی آمار با قابلیت فیلتر زمانی را نمایش داده و آمار معاملات را
    از پایگاه داده خوانده و به کاربر نمایش می‌دهد.
    """
    query = update.callback_query
    await query.answer() # اول answer بدهیم تا کاربر منتظر نماند
    user_id = update.effective_user.id
    data = query.data
    log_extra = {'user_id': user_id, 'callback_data': data}

    time_filter = "all" # پیش‌فرض: کل زمان
    if data.startswith("stats:"):
        time_filter = data.split(":")[1]

    # --- نمایش منوی انتخاب بازه زمانی ---
    if time_filter == "menu": # اگر callback_data فقط statistics_menu بود
        keyboard = [
            [InlineKeyboardButton("📊 آمار کل زمان", callback_data="stats:all")],
            [InlineKeyboardButton("📊 آمار امروز", callback_data="stats:today")],
            [InlineKeyboardButton("📊 آمار ۷ روز اخیر", callback_data="stats:7d")],
            [InlineKeyboardButton("📊 آمار ۳۰ روز اخیر", callback_data="stats:30d")],
            [InlineKeyboardButton("🔙 بازگشت به منوی اصلی", callback_data="main_menu")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
             await query.edit_message_text(
                 "لطفاً بازه زمانی مورد نظر برای نمایش آمار را انتخاب کنید:",
                 reply_markup=reply_markup,
                 parse_mode=ParseMode.MARKDOWN_V2
             )
             logger.debug("Statistics time filter menu displayed.", extra=log_extra)
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                 logger.warning(f"Failed to edit message for stats menu: {e}")
        return # پایان کار، منتظر انتخاب کاربر می‌مانیم

    # --- اگر بازه زمانی انتخاب شده بود، آمار را محاسبه و نمایش بده ---
    await query.edit_message_text("⏳ در حال محاسبه آمار برای بازه انتخابی\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2) # پیام انتظار جدید

    start_date_str = None
    end_date_str = None
    title = "📊 آمار کل معاملات" # عنوان پیش‌فرض

    now = datetime.now()
    if time_filter == "today":
        start_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        title = "📊 آمار معاملات امروز"
    elif time_filter == "7d":
        start_date = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now # تا لحظه حال
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        title = "📊 آمار معاملات ۷ روز اخیر"
    elif time_filter == "30d":
        start_date = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = now # تا لحظه حال
        start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
        end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
        title = "📊 آمار معاملات ۳۰ روز اخیر"
    # else: time_filter == "all", title پیش‌فرض باقی می‌ماند و start/end date هم None

    try:
        # ۱. خواندن اطلاعات نام‌ها (بدون تغییر)
        ecosystem = context.bot_data.get('ecosystem', {})
        source_name_lookup = {s['id']: s['name'] for s in ecosystem.get('sources', []) if 'id' in s} # اضافه کردن if 'id' in s
        copy_name_lookup = {c['id']: c['name'] for c in ecosystem.get('copies', []) if 'id' in c} # اضافه کردن if 'id' in c

        # ۲. اتصال به دیتابیس و اجرای کوئری (با شرط WHERE)
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        sql = '''
            SELECT copy_id, source_id, SUM(profit) as total_profit, COUNT(*) as trade_count
            FROM trades
        '''
        params = []
        if start_date_str and end_date_str:
            sql += " WHERE timestamp BETWEEN ? AND ?"
            params.extend([start_date_str, end_date_str])

        sql += '''
            GROUP BY copy_id, source_id
            ORDER BY copy_id, source_id
        '''

        cursor.execute(sql, params)
        results = cursor.fetchall()
        conn.close()

        # ۳. پردازش نتایج و ساخت پیام (بدون تغییر عمده)
        if not results:
            await query.edit_message_text(
                f"{title}\n\nهنوز هیچ داده‌ای برای نمایش در این بازه زمانی وجود ندارد\\.",
                # --- تغییر: بازگشت به منوی انتخاب بازه ---
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بازگشت", callback_data="statistics_menu")]]),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return

        stats_by_copy = {}
        grand_total_profit = 0
        grand_total_trades = 0

        for row in results:
            copy_id, source_id, total_profit, trade_count = row
            grand_total_profit += total_profit
            grand_total_trades += trade_count

            if copy_id not in stats_by_copy:
                stats_by_copy[copy_id] = {'total_profit': 0, 'total_trades': 0, 'sources': []}

            stats_by_copy[copy_id]['total_profit'] += total_profit
            stats_by_copy[copy_id]['total_trades'] += trade_count
            stats_by_copy[copy_id]['sources'].append({
                'source_id': source_id,
                'profit': total_profit,
                'trades': trade_count
                # 'source_file' دیگر در کوئری نیست، اگر لازم شد باید اضافه شود
            })

        # ۴. فرمت‌بندی پیام خروجی (با عنوان جدید)
        message_lines = [f"*{title}*"] # استفاده از عنوان داینامیک
        message_lines.append(f"> *مجموع سود/زیان:* `{escape_markdown_v2(f'{grand_total_profit:,.2f}')}`")
        message_lines.append(f"> *تعداد معاملات:* `{escape_markdown_v2(grand_total_trades)}`")
        message_lines.append("> \n> ─── *جزئیات بر اساس حساب کپی* ───\n>")

        for copy_id, data in stats_by_copy.items():
            copy_name = escape_markdown_v2(copy_name_lookup.get(copy_id, copy_id))
            message_lines.append(f"🛡️ *حساب:* {copy_name}")
            message_lines.append(f">  ▫️ *مجموع سود/زیان:* `{escape_markdown_v2(f'{data['total_profit']:,.2f}')}`")
            message_lines.append(f">  ▫️ *تعداد معاملات:* `{escape_markdown_v2(data['total_trades'])}`")
            message_lines.append(">  ▫️ *تفکیک منابع:*")
            if not data['sources']:
                 message_lines.append(">       └── *بدون معامله ثبت شده*")
            else:
                for source_stat in data['sources']:
                    source_name = "ناشناس یا حذف شده" # متن پیش‌فرض بهبود یافته
                    if source_stat['source_id']:
                        source_name = escape_markdown_v2(source_name_lookup.get(source_stat['source_id'], f"ID: {source_stat['source_id']}"))

                    profit_str = escape_markdown_v2(f"{source_stat['profit']:,.2f}")
                    trades_str = escape_markdown_v2(source_stat['trades'])
                    message_lines.append(f">       └── *{source_name}:* سود/زیان: `{profit_str}`, تعداد: `{trades_str}`")
            message_lines.append(">") # خط خالی

        final_message = "\n".join(message_lines)

        # ۵. ارسال پیام به تلگرام (با دکمه‌های جدید)
        # --- تغییر: دکمه به‌روزرسانی با فیلتر فعلی و بازگشت به منوی انتخاب بازه ---
        keyboard = [
             [InlineKeyboardButton("🔄 به‌روزرسانی", callback_data=f"stats:{time_filter}")], # callback_data داینامیک شد
             [InlineKeyboardButton("🔙 بازگشت به انتخاب بازه", callback_data="statistics_menu")] # بازگشت به منوی قبلی
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        try:
             await query.edit_message_text(
                 text=final_message,
                 reply_markup=reply_markup,
                 parse_mode=ParseMode.MARKDOWN_V2
             )
             log_extra['status'] = 'success'
             logger.info(f"Statistics displayed successfully for filter: {time_filter}.", extra=log_extra)
        except BadRequest as e:
             if "message is too long" in str(e).lower():
                  logger.warning(f"Statistics message too long for filter {time_filter}, sending truncated.", extra={**log_extra, 'status': 'truncated'})
                  await query.edit_message_text(
                       text=final_message[:4000] + "\n\n✂️... \\(پیام کامل نمایش داده نشد\\)",
                       reply_markup=reply_markup,
                       parse_mode=ParseMode.MARKDOWN_V2
                  )
             elif "Message is not modified" not in str(e):
                  raise
             # else: پیام تغییری نکرده، رد شو

    # ... (بخش except ها مثل قبل) ...
    except sqlite3.Error as e:
        logger.error("Database error while fetching statistics.", extra={**log_extra, 'error': str(e), 'status': 'db_error'})
        await query.edit_message_text(
            "❌ خطایی در خواندن اطلاعات از پایگاه داده رخ داد\\.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بازگشت", callback_data="statistics_menu")]]), # بازگشت به منوی آمار
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except Exception as e:
        logger.error("Unexpected error in handle_statistics_menu.", extra={**log_extra, 'error': str(e), 'status': 'failure'})
        await notify_admin_on_error(context, "handle_statistics_menu", e, time_filter=time_filter) # اضافه کردن فیلتر به گزارش خطا
        await query.edit_message_text(
            "❌ یک خطای غیرمنتظره در نمایش آمار رخ داد\\. گزارش برای ادمین ارسال شد\\.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بازگشت", callback_data="statistics_menu")]]), # بازگشت به منوی آمار
            parse_mode=ParseMode.MARKDOWN_V2
        )




@allowed_users_only
async def clean_old_logs_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clean old log files except for today's logs."""
    await update.message.reply_text("⏳ پاک‌سازی لاگ‌های قدیمی...", parse_mode=ParseMode.MARKDOWN_V2)
    if not LOG_DIRECTORY_PATH:
        await update.message.reply_text("❌ مسیر لاگ تنظیم نشده.", parse_mode=ParseMode.MARKDOWN_V2)
        logger.error("LOG_DIRECTORY_PATH not set", extra={'status': 'failure'})
        return
    try:
        today_str = datetime.now().strftime("%Y.%m.%d")
        log_pattern = os.path.join(LOG_DIRECTORY_PATH, "TradeCopier_*.log")
        all_logs = glob.glob(log_pattern)
        deleted_count = 0
        errors_count = 0
        for log_file in all_logs:
            if today_str not in os.path.basename(log_file):
                try:
                    os.remove(log_file)
                    deleted_count += 1
                    logger.info("Log file deleted", extra={'entity_id': os.path.basename(log_file), 'status': 'success'})
                except Exception as e:
                    errors_count += 1
                    logger.error("Log file deletion failed", extra={'entity_id': os.path.basename(log_file), 'status': 'failure', 'error': str(e)})
        message = f"✅ *پاک‌سازی انجام شد.*\n"
        message += f"🗑️ *حذف‌شده:* {escape_markdown_v2(deleted_count)}\n"
        if errors_count > 0:
            message += f"🚨 *خطاها:* {escape_markdown_v2(errors_count)}"
        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        await update.message.reply_text(f"❌ خطا: {escape_markdown_v2(str(e))}", parse_mode=ParseMode.MARKDOWN_V2)
        logger.error("Log cleanup failed", extra={'status': 'failure', 'error': str(e)})




@allowed_users_only
async def clean_old_backups_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Intelligently cleans up old ecosystem backup files, keeping only the 3 most recent ones.
    """
    await update.message.reply_text("⏳ در حال پاک‌سازی فایل‌های پشتیبان قدیمی\\.\\.\\.", parse_mode=ParseMode.MARKDOWN_V2)
    user_id = update.effective_user.id
    log_extra = {'user_id': user_id}

    try:
        # ساخت الگو برای پیدا کردن فایل‌های پشتیبان
        base_path = os.path.dirname(ECOSYSTEM_PATH)
        backup_pattern = os.path.join(base_path, "ecosystem.json.bak.*")
        
        backup_files = glob.glob(backup_pattern)
        
        # اگر تعداد فایل‌ها 3 یا کمتر است، نیازی به پاک‌سازی نیست
        if len(backup_files) <= 3:
            logger.info("Backup cleanup skipped: 3 or fewer backups exist.", extra=log_extra)
            await update.message.reply_text("✅ تعداد فایل‌های پشتیبان ۳ عدد یا کمتر است\\. نیازی به پاک‌سازی نیست\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        # مرتب‌سازی فایل‌ها بر اساس زمان آخرین تغییر (از جدید به قدیم)
        backup_files.sort(key=os.path.getmtime, reverse=True)
        
        # انتخاب فایل‌های قدیمی‌تر از 3 نسخه آخر برای حذف
        files_to_delete = backup_files[3:]
        
        deleted_count = 0
        errors_count = 0

        logger.info(f"Starting cleanup of {len(files_to_delete)} old backup files.", extra=log_extra)

        for file_path in files_to_delete:
            try:
                os.remove(file_path)
                deleted_count += 1
                logger.debug(f"Successfully deleted backup file: {os.path.basename(file_path)}", extra=log_extra)
            except OSError as e:
                errors_count += 1
                error_log = log_extra.copy()
                error_log['error'] = str(e)
                logger.error(f"Failed to delete backup file: {os.path.basename(file_path)}", extra=error_log)

        # ساخت و ارسال گزارش نهایی به کاربر
        message = f"✅ *عملیات پاک‌سازی پشتیبان‌ها با موفقیت انجام شد*\\.\n\n"
        message += f"🗑️ *فایل‌های حذف شده:* {deleted_count}\n"
        if errors_count > 0:
            message += f"🚨 *خطا در حذف:* {errors_count}"
            
        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN_V2)

    except Exception as e:
        log_extra['error'] = str(e)
        logger.critical("An unexpected exception occurred during backup cleanup.", extra=log_extra)
        await update.message.reply_text(f"🚨 یک خطای بحرانی در هنگام پاک‌سازی رخ داد: `{escape_markdown_v2(str(e))}`", parse_mode=ParseMode.MARKDOWN_V2)






@allowed_users_only
async def get_log_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Retrieve the latest log for a copy account."""
    args = context.args
    if not args:
        await update.message.reply_text("شناسه حساب و (اختیاری) تعداد خطوط را وارد کنید.\nمثال: `/getlog copy_A 50`", parse_mode=ParseMode.MARKDOWN_V2)
        return
    copy_id = args[0]
    num_lines = int(args[1]) if len(args) > 1 else 50
    if not LOG_DIRECTORY_PATH:
        await update.message.reply_text("❌ مسیر لاگ تنظیم نشده.", parse_mode=ParseMode.MARKDOWN_V2)
        return
    try:
        log_pattern = os.path.join(LOG_DIRECTORY_PATH, f"TradeCopier_{copy_id}_*.log")
        all_logs = glob.glob(log_pattern)
        if not all_logs:
            await update.message.reply_text(f"❌ لاگی برای *{escape_markdown_v2(copy_id)}* یافت نشد.", parse_mode=ParseMode.MARKDOWN_V2)
            return
        latest_log = max(all_logs, key=os.path.getctime)
        with open(latest_log, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            tail_lines = lines[-num_lines:] if num_lines > 0 else lines
        log_content = ''.join(tail_lines)
        if len(log_content) > 4096:
            temp_file = f"{copy_id}_log.txt"
            with open(temp_file, 'w', encoding='utf-8') as temp:
                temp.write(log_content)
            await update.message.reply_document(document=open(temp_file, 'rb'))
            os.remove(temp_file)
            logger.info("Large log file sent", extra={'entity_id': copy_id, 'status': 'success'})
        else:
            await update.message.reply_text(f"*لاگ برای* {escape_markdown_v2(copy_id)}:\n```{log_content}```", parse_mode=ParseMode.MARKDOWN_V2)
            logger.info("Inline log sent", extra={'entity_id': copy_id, 'status': 'success'})
    except Exception as e:
        await update.message.reply_text(f"❌ خطا: {escape_markdown_v2(str(e))}", parse_mode=ParseMode.MARKDOWN_V2)
        logger.error("Log retrieval failed", extra={'entity_id': copy_id, 'status': 'failure', 'error': str(e)})




@allowed_users_only
async def regenerate_all_files_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handles the regeneration of all configuration files with robust error handling and improved user feedback.
    """
    query = update.callback_query
    await query.answer(text="⏳ در حال بازسازی فایل‌ها...")
    user_id = update.effective_user.id
    log_extra = {'user_id': user_id}

    logger.info("Configuration files regeneration process initiated by user.", extra=log_extra)
    
    keyboard = [[InlineKeyboardButton("🔙 بازگشت به منوی اصلی", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        success = await regenerate_all_configs(context)
        
        if success:
            logger.info("All configuration files were regenerated successfully.", extra=log_extra)
            # ✅ اصلاحیه اصلی: نقطه انتهای جمله escape شده است
            await query.edit_message_text(
                "✅ تمام فایل‌های تنظیمات با موفقیت بازسازی شدند\\.",
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN_V2
            )
        else:
            logger.error("regenerate_all_configs function returned False.", extra=log_extra)
            await query.edit_message_text(
                "❌ در فرآیند بازسازی فایل‌ها خطایی رخ داد\\. لطفا لاگ‌ها را برای جزئیات بیشتر بررسی کنید\\.",
                reply_markup=reply_markup,
                parse_mode=ParseMode.MARKDOWN_V2
            )

    except Exception as e:
        log_extra['error'] = str(e)
        logger.critical("An unexpected exception occurred during file regeneration.", extra=log_extra)
        await query.edit_message_text(
            f"🚨 یک خطای بحرانی در هنگام بازسازی فایل‌ها رخ داد: `{escape_markdown_v2(str(e))}`",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN_V2
        )





@allowed_users_only
async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display help menu with bot usage instructions."""
    help_text = (
        "📖 *راهنمای ربات*\n\n"
        "مدیریت آسان کپی معاملات:\n\n"
        "*دستورات:*\n"
        "▫️ `/start` - منوی اصلی و وضعیت سیستم.\n"
        "▫️ `/getlog [copy_id]` - دریافت لاگ حساب (مثال: `/getlog copy_A`).\n"
        "▫️ `/clean_old_logs` - حذف لاگ‌های قدیمی.\n\n"
        "*منوها:*\n"
        "🔹 *وضعیت:* نمایش وضعیت حساب‌ها و اتصالات.\n"
        "🔹 *حساب‌های کپی:* افزودن/حذف و تنظیم ریسک.\n"
        "🔹 *منابع:* مدیریت حساب‌های منبع.\n"
        "🔹 *اتصالات:* تنظیم اتصال منابع به حساب‌ها.\n"
        "🔹 *بازسازی فایل‌ها:* بازسازی تنظیمات."
    )
    if update.callback_query:
        await update.callback_query.edit_message_text(help_text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 منوی اصلی", callback_data="main_menu")]
        ]), parse_mode=ParseMode.MARKDOWN_V2)
    else:
        await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN_V2)



async def _display_connections_for_copy(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, copy_id: str):
    """
    Helper function to display the connections menu for a specific copy account.
    This version is enhanced to show both the name and ID of sources for better clarity.
    """
    ecosystem = context.bot_data.get('ecosystem', {})
    source_map = {s['id']: s for s in ecosystem.get('sources', [])}
    copy_account = next((c for c in ecosystem.get('copies', []) if c['id'] == copy_id), None)
    
    if not copy_account:
        await query.edit_message_text("❌ حساب کپی مورد نظر یافت نشد\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    connections = ecosystem.get('mapping', {}).get(copy_id, [])
    connected_source_ids = {conn['source_id'] for conn in connections}
    
    keyboard = []
    
    # --- نمایش اتصالات موجود ---
    if not connections:
        keyboard.append([InlineKeyboardButton("این حساب به هیچ منبعی متصل نیست", callback_data="noop")])
    else:
        for conn in connections:
            source_id = conn.get('source_id')
            if source_id not in source_map:
                continue

            source_name = escape_markdown_v2(source_map[source_id]['name'])
            source_id_escaped = escape_markdown_v2(source_id)
            
            # تیتر خوانا برای هر اتصال
            header_text = f"───  اتصال به: {source_name} ({source_id_escaped}) ───"
            keyboard.append([InlineKeyboardButton(header_text, callback_data="noop")])

            # دکمه تنظیمات حجم
            vs = conn.get('volume_settings', {})
            vol_mode = "Fixed" if "FixedVolume" in vs else "Multiplier"
            vol_value = vs.get("FixedVolume", vs.get("Multiplier", 1.0))
            volume_text = f"⚙️ حجم: {vol_mode} {vol_value}"

            # دکمه حالت کپی
            copy_mode = conn.get('mode', 'ALL')
            mode_text = "🚦 حالت: "
            if copy_mode == 'ALL':
                mode_text += "همه نمادها"
            elif copy_mode == 'GOLD_ONLY':
                mode_text += "فقط طلا"
            elif copy_mode == 'SYMBOLS':
                symbols = conn.get('allowed_symbols', '')
                short_symbols = symbols[:10] + '...' if len(symbols) > 10 else symbols
                mode_text += f"خاص ({escape_markdown_v2(short_symbols) or 'خالی'})"

            keyboard.append([
                InlineKeyboardButton(volume_text, callback_data=f"conn:set_volume_type:{copy_id}:{source_id}"),
                InlineKeyboardButton(mode_text, callback_data=f"conn:set_mode_menu:{copy_id}:{source_id}"),
                InlineKeyboardButton("✂️ قطع", callback_data=f"conn:disconnect:{copy_id}:{source_id}")
            ])

    # --- نمایش منابع قابل اتصال ---
    available_sources = [s for s_id, s in source_map.items() if s_id not in connected_source_ids]
    if available_sources:
        keyboard.append([InlineKeyboardButton("─" * 20, callback_data="noop")])
        keyboard.append([InlineKeyboardButton("🔽 اتصال به یک منبع جدید 🔽", callback_data="noop")])
        for source in available_sources:
            # نمایش نام و شناسه برای منابع جدید
            connect_text = f"🔗 {escape_markdown_v2(source['name'])} ({escape_markdown_v2(source['id'])})"
            keyboard.append([InlineKeyboardButton(connect_text, callback_data=f"conn:connect:{copy_id}:{source['id']}")])

    keyboard.append([InlineKeyboardButton("🔙 بازگشت به لیست حساب‌ها", callback_data="menu_connections")])
    
    try:
        await query.edit_message_text(
            f"مدیریت اتصالات حساب *{escape_markdown_v2(copy_account['name'])}*:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            raise


        

@allowed_users_only
async def _handle_connections_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles the main connections menu, including connect, disconnect, and mode selection actions.
    """
    query = update.callback_query
    await query.answer()
    data = query.data
    parts = data.split(':')
    ecosystem = context.bot_data.get('ecosystem', {})
    user_id = update.effective_user.id
    log_extra = {'user_id': user_id, 'callback_data': data, 'status': 'processing'}

    try:
        # --- نمایش منوی اصلی اتصالات ---
        if data == "menu_connections":
            context.user_data.clear()
            logger.debug("Navigating to main connections menu", extra=log_extra)
            keyboard = []
            for copy_account in ecosystem.get('copies', []):
                connection_count = len(ecosystem.get('mapping', {}).get(copy_account['id'], []))
                button_text = f"{escape_markdown_v2(copy_account['name'])} ({connection_count} اتصال)"
                keyboard.append([InlineKeyboardButton(button_text, callback_data=f"conn:select_copy:{copy_account['id']}")])
            keyboard.append([InlineKeyboardButton("🔙 منوی اصلی", callback_data="main_menu")])
            await query.edit_message_text("مدیریت اتصالات: یک حساب کپی را انتخاب کنید:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
            return

        # --- نمایش منوی یک حساب کپی خاص ---
        if parts[1] == "select_copy":
            copy_id = parts[2]
            context.user_data['selected_copy_id'] = copy_id
            await _display_connections_for_copy(query, context, copy_id)
            return

        # --- مدیریت اتصال و قطع اتصال ---
        if parts[1] in ["connect", "disconnect"]:
            copy_id, source_id = parts[2], parts[3]
            log_extra.update({'copy_id': copy_id, 'source_id': source_id})
            
            if parts[1] == "connect":
                logger.info("Connection process initiated", extra=log_extra)
                # هنگام اتصال، یک کانفیگ پیش‌فرض با حالت ALL ایجاد می‌کنیم
                ecosystem.setdefault('mapping', {}).setdefault(copy_id, []).append({
                    'source_id': source_id, 'mode': 'ALL', 'allowed_symbols': '', 'volume_settings': {"Multiplier": 1.0}
                })
                feedback_text = "✅ اتصال با موفقیت برقرار شد"
            else:  # disconnect
                logger.info("Disconnection process initiated", extra=log_extra)
                ecosystem['mapping'][copy_id] = [c for c in ecosystem['mapping'].get(copy_id, []) if c['source_id'] != source_id]
                feedback_text = "✅ اتصال با موفقیت قطع شد"

            if save_ecosystem(context):
                await regenerate_copy_config(copy_id, context)
                await query.answer(text=feedback_text)
                log_extra['status'] = 'success'
                logger.info("Connection state changed and config regenerated.", extra=log_extra)
                await _display_connections_for_copy(query, context, copy_id)
            else:
                log_extra['status'] = 'failure'
                logger.error("Failed to save ecosystem during connection/disconnection", extra=log_extra)
                await query.answer("❌ خطا در ذخیره‌سازی تغییرات!")
            return

        # --- نمایش منوی انتخاب حالت کپی (بخش جدید) ---
        if parts[1] == "set_mode_menu":
            copy_id, source_id = parts[2], parts[3]
            log_extra.update({'copy_id': copy_id, 'source_id': source_id})
            logger.debug("Displaying copy mode selection menu", extra=log_extra)
            
            keyboard = [
                [InlineKeyboardButton("1️⃣ همه نمادها (All Symbols)", callback_data=f"conn:set_mode_action:ALL:{copy_id}:{source_id}")],
                [InlineKeyboardButton("2️⃣ فقط طلا (Gold Only)", callback_data=f"conn:set_mode_action:GOLD_ONLY:{copy_id}:{source_id}")],
                [InlineKeyboardButton("3️⃣ نمادهای خاص (Specific Symbols)", callback_data=f"conn:set_mode_action:SYMBOLS:{copy_id}:{source_id}")],
                [InlineKeyboardButton("🔙 لغو", callback_data=f"conn:select_copy:{copy_id}")]
            ]
            await query.edit_message_text(
                "لطفاً حالت کپی برای این اتصال را انتخاب کنید:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return

        # --- پردازش انتخاب حالت کپی (بخش جدید) ---
        if parts[1] == "set_mode_action":
            mode, copy_id, source_id = parts[2], parts[3], parts[4]
            log_extra.update({'copy_id': copy_id, 'source_id': source_id, 'details': {'new_mode': mode}})

            connection = next((conn for conn in ecosystem.get('mapping', {}).get(copy_id, []) if conn['source_id'] == source_id), None)
            if not connection:
                await query.answer("❌ خطا: اتصال یافت نشد!", show_alert=True)
                return

            if mode == "SYMBOLS":
                context.user_data['waiting_for'] = f"conn_symbols:{copy_id}:{source_id}"
                log_extra['state_set'] = context.user_data['waiting_for']
                logger.debug("Prompting user for allowed symbols list", extra=log_extra)
                await query.edit_message_text(
                    "لطفاً لیست نمادهای مجاز را وارد کنید\\. نمادها را با سمی‌کالن (;) از هم جدا کنید\\.\nمثال: `EURUSD;GBPUSD;XAUUSD`",
                    parse_mode=ParseMode.MARKDOWN_V2
                )
                return

            # برای ALL و GOLD_ONLY مستقیماً ذخیره می‌کنیم
            connection['mode'] = mode
            if save_ecosystem(context):
                await regenerate_copy_config(copy_id, context)
                await query.answer(f"✅ حالت کپی به '{mode}' تغییر کرد.")
                log_extra['status'] = 'success'
                logger.info("Connection copy mode updated.", extra=log_extra)
                await _display_connections_for_copy(query, context, copy_id)
            else:
                log_extra['status'] = 'failure'
                logger.error("Failed to save ecosystem after changing copy mode", extra=log_extra)
                await query.answer("❌ خطا در ذخیره‌سازی تغییرات!")
            return

    except Exception as e:
        log_extra.update({'error': str(e), 'status': 'failure'})
        logger.critical("An unexpected exception occurred in the connections menu handler.", extra=log_extra)
        await notify_admin_on_error(context, "_handle_connections_menu", e, callback_data=data)
        await query.message.reply_text("❌ یک خطای غیرمنتظره رخ داد\\. گزارش برای ادمین ارسال شد\\.", parse_mode=ParseMode.MARKDOWN_V2)




async def _display_copy_account_menu(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, copy_id: str):
    """
    Helper function to display the settings menu for a specific copy account.
    This version includes visual feedback for the pending 'ResetStop' action.
    """
    ecosystem = context.bot_data.get('ecosystem', {})
    copy_account = next((c for c in ecosystem.get('copies', []) if c['id'] == copy_id), None)
    
    if not copy_account:
        await query.edit_message_text("❌ حساب یافت نشد.", parse_mode=ParseMode.MARKDOWN_V2)
        return

    settings = copy_account.get('settings', {})
    
    # --- آماده‌سازی متن دکمه‌ها ---
    dd = float(settings.get("DailyDrawdownPercent", 0))
    dd_status_text = f"ریسک روزانه: {'🟢 فعال' if dd > 0 else '🔴 غیرفعال'}"

    copy_mode = settings.get("CopySymbolMode", "GOLD_ONLY")
    cm_text = "فقط طلا" if copy_mode == "GOLD_ONLY" else "همه نمادها"
    copy_mode_status_text = f"حالت کپی: {cm_text}"

    # ✅ هوشمندسازی دکمه ریست قفل
    # بررسی می‌کند آیا دستوری برای ریست این حساب خاص در انتظار است یا خیر
    is_reset_pending = context.user_data.get('reset_stop_for_copy') == copy_id
    reset_stop_text = "ریست قفل (در انتظار بازسازی ⏳)" if is_reset_pending else "ریست قفل (ResetStop)"

    keyboard = [
        [InlineKeyboardButton(dd_status_text, callback_data=f"setting:action:toggle_dd:{copy_id}")],
        [InlineKeyboardButton(copy_mode_status_text, callback_data=f"setting:action:copy_mode:{copy_id}")],
        [InlineKeyboardButton("تنظیم حد ضرر روزانه (%)", callback_data="setting_input_copy_DailyDrawdownPercent")],
        [InlineKeyboardButton("تنظیم حد هشدار (%)", callback_data="setting_input_copy_AlertDrawdownPercent")],
        # ✅ استفاده از متن هوشمند جدید
        [InlineKeyboardButton(reset_stop_text, callback_data=f"setting:action:reset_stop:{copy_id}")],
        [InlineKeyboardButton("🗑️ حذف حساب", callback_data=f"setting:delete:confirm:{copy_id}")],
        [InlineKeyboardButton("🔙 بازگشت به لیست", callback_data="menu_copy_settings")]
    ]
    
    try:
        await query.edit_message_text(
            text=f"تنظیمات حساب *{escape_markdown_v2(copy_account['name'])}*:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2
        )
    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.debug("Menu refresh skipped as content was unchanged.", extra={'entity_id': copy_id})
            pass
        else:
            logger.error("A BadRequest occurred while editing message", extra={'error': str(e)})
            raise

@allowed_users_only
async def _handle_copy_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle copy account settings menu with improved logic, UX, and logging."""
    query = update.callback_query
    await query.answer()
    data = query.data
    ecosystem = context.bot_data.get('ecosystem', {})
    parts = data.split(':')
    action = parts[0]
    user_id = update.effective_user.id
    log_extra = {'user_id': user_id, 'callback_data': data, 'status': 'processing'}

    # --- نمایش منوی اصلی حساب‌های کپی ---
    if action == "menu_copy_settings":
        context.user_data.clear()
        logger.debug("State cleared for copy settings menu", extra=log_extra)
        copies = ecosystem.get('copies', [])
        keyboard = []
        for c in copies:
            keyboard.append([InlineKeyboardButton(escape_markdown_v2(c['name']), callback_data=f"setting:select:{c['id']}")])
        keyboard.append([InlineKeyboardButton("➕ حساب جدید", callback_data="setting:add:start")])
        keyboard.append([InlineKeyboardButton("🔙 منوی اصلی", callback_data="main_menu")])
        await query.edit_message_text("مدیریت حساب‌های کپی: یک حساب را انتخاب کنید:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
        return

    # --- نمایش منوی تنظیمات یک حساب خاص ---
    if action == "setting" and parts[1] == "select":
        copy_id = parts[2]
        context.user_data['selected_copy_id'] = copy_id
        await _display_copy_account_menu(query, context, copy_id)
        return

    # --- مدیریت اقدامات روی یک حساب (تغییر تنظیمات) ---
    if action == "setting" and parts[1] == "action":
        sub_action = parts[2]
        copy_id = parts[3]
        copy_account = next((c for c in ecosystem.get('copies', []) if c['id'] == copy_id), None)
        if not copy_account:
            await query.edit_message_text("❌ حساب یافت نشد\\.", parse_mode=ParseMode.MARKDOWN_V2)
            return

        settings = copy_account.get('settings', {})
        feedback_text = ""

        if sub_action == "toggle_dd":
            old_dd = float(settings.get("DailyDrawdownPercent", 0))
            new_dd = 0 if old_dd > 0 else 5.0
            settings["DailyDrawdownPercent"] = new_dd
            feedback_text = "ریسک روزانه غیرفعال شد." if new_dd == 0 else "ریسک روزانه فعال شد."
            logger.info("Daily drawdown toggled", extra={'user_id': user_id, 'entity_id': copy_id, 'details': {'from': old_dd, 'to': new_dd}})

        elif sub_action == "copy_mode":
            old_mode = settings.get("CopySymbolMode", "GOLD_ONLY")
            new_mode = "ALL_SYMBOLS" if old_mode == "GOLD_ONLY" else "GOLD_ONLY"
            settings["CopySymbolMode"] = new_mode
            feedback_text = f"حالت کپی به '{'همه نمادها' if new_mode == 'ALL_SYMBOLS' else 'فقط طلا'}' تغییر کرد."
            logger.info("Copy symbol mode toggled", extra={'user_id': user_id, 'entity_id': copy_id, 'details': {'from': old_mode, 'to': new_mode}})

        elif sub_action == "reset_stop":
            context.user_data['reset_stop_for_copy'] = copy_id
            feedback_text = "دستور ریست در بازسازی بعدی اعمال می‌شود."
            logger.info("ResetStop flag set for next regeneration", extra={'user_id': user_id, 'entity_id': copy_id})

        if feedback_text:
            if save_ecosystem(context):
                await regenerate_copy_settings_config(copy_id, context)
                await query.answer(feedback_text)
                await _display_copy_account_menu(query, context, copy_id)
            else:
                log_extra.update({'status': 'failure', 'action': sub_action})
                logger.error("Ecosystem save failed after action", extra=log_extra)
                await query.answer("❌ خطا در ذخیره‌سازی تغییرات.")
        return

    # --- منطق افزودن حساب جدید (بازنویسی شده) ---
    if action == "setting" and parts[1] == "add":
        if parts[2] == "start":
            context.user_data.clear()
            
            existing_ids = {c['id'] for c in ecosystem.get('copies', [])}
            possible_ids = [f"copy_{chr(ord('A') + i)}" for i in range(10)] # Creates copy_A to copy_J
            
            new_copy_id = None
            for pid in possible_ids:
                if pid not in existing_ids:
                    new_copy_id = pid
                    break
            
            if new_copy_id is None:
                await query.edit_message_text("❌ تمام ظرفیت حساب‌های کپی (A-J) پر شده است\\.", parse_mode=ParseMode.MARKDOWN_V2)
                return

            context.user_data['temp_copy_id'] = new_copy_id
            context.user_data['waiting_for'] = 'copy_add_name'
            log_extra['state_set'] = 'copy_add_name'
            log_extra['details'] = {'new_id': new_copy_id}
            logger.debug("Prompting user for new copy account name.", extra=log_extra)
            await query.edit_message_text(f"شناسه جدید تخصیص داده شد: *{escape_markdown_v2(new_copy_id)}*\n\nلطفاً یک نام نمایشی برای این حساب وارد کنید:", parse_mode=ParseMode.MARKDOWN_V2)
        return

    # --- منطق حذف حساب ---
    if action == "setting" and parts[1] == "delete":
        sub_action = parts[2]
        copy_id = parts[3]
        if sub_action == "confirm":
            copy_name = next((c['name'] for c in ecosystem.get('copies', []) if c['id'] == copy_id), copy_id)
            keyboard = [
                [InlineKeyboardButton("✅ بله، حذف کن", callback_data=f"setting:delete:execute:{copy_id}")],
                [InlineKeyboardButton("❌ خیر، بازگشت", callback_data=f"setting:select:{copy_id}")]
            ]
            confirmation_text = f"آیا از حذف حساب *{escape_markdown_v2(copy_name)}* و تمام اتصالات آن مطمئن هستید؟ این عمل غیرقابل بازگشت است\\."
            await query.edit_message_text(confirmation_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
            return

        if sub_action == "execute":
            log_extra['entity_id'] = copy_id
            logger.info("Copy account deletion initiated", extra=log_extra)
            
            copies = ecosystem.get('copies', [])
            copy_name = next((c['name'] for c in copies if c['id'] == copy_id), copy_id)
            ecosystem['copies'] = [c for c in copies if c['id'] != copy_id]
            if copy_id in ecosystem.get('mapping', {}):
                del ecosystem['mapping'][copy_id]

            if save_ecosystem(context):
                await regenerate_all_configs(context)
                log_extra['status'] = 'success'
                logger.info("Copy account deleted successfully.", extra=log_extra)
                
                keyboard = [[InlineKeyboardButton("🔙 بازگشت به لیست حساب‌ها", callback_data="menu_copy_settings")]]
                await query.edit_message_text(text=f"✅ حساب *{escape_markdown_v2(copy_name)}* با موفقیت حذف شد\\.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
            else:
                log_extra['status'] = 'failure'
                logger.error("Copy deletion save failed", extra=log_extra)
                await query.edit_message_text("❌ خطا در هنگام حذف حساب\\. لطفا لاگ‌ها را بررسی کنید.", parse_mode=ParseMode.MARKDOWN_V2)
            return


@allowed_users_only
async def _handle_sources_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle source management menu with the new smart-add functionality."""
    query = update.callback_query
    await query.answer()
    data = query.data
    ecosystem = context.bot_data.get('ecosystem', {})
    parts = data.split(':')
    action = parts[0]
    user_id = update.effective_user.id
    log_extra = {'user_id': user_id, 'callback_data': data}

    try:
        if action == "sources" and parts[1] == "main":
            context.user_data.clear()
            logger.debug("Navigating to main sources menu", extra=log_extra)
            sources = ecosystem.get('sources', [])
            keyboard = [
                [InlineKeyboardButton(escape_markdown_v2(s['name']), callback_data=f"sources:select:{s['id']}")] for s in sources
            ]
            keyboard.append([InlineKeyboardButton("➕ منبع جدید", callback_data="sources:add:start")])
            keyboard.append([InlineKeyboardButton("🔙 منوی اصلی", callback_data="main_menu")])
            await query.edit_message_text("مدیریت منابع: یک منبع را انتخاب کنید:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
            return

        if action == "sources" and parts[1] == "select":
            source_id = parts[2]
            context.user_data['selected_source_id'] = source_id
            source = next((s for s in ecosystem.get('sources', []) if s['id'] == source_id), None)
            if not source:
                await query.edit_message_text("❌ منبع یافت نشد\\.", parse_mode=ParseMode.MARKDOWN_V2)
                return
            keyboard = [
                [InlineKeyboardButton("✏️ ویرایش نام", callback_data=f"sources:action:edit_name:{source_id}")],
                [InlineKeyboardButton("🗑️ حذف منبع", callback_data=f"sources:delete:confirm:{source_id}")],
                [InlineKeyboardButton("🔙 بازگشت به لیست", callback_data="sources:main")]
            ]
            await query.edit_message_text(f"مدیریت منبع *{escape_markdown_v2(source['name'])}*:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
            return

        if action == "sources" and parts[1] == "action" and parts[2] == "edit_name":
            source_id = parts[3]
            context.user_data['waiting_for'] = 'source_edit_name'
            log_extra['entity_id'] = source_id
            logger.debug("Prompting user for new source name", extra=log_extra)
            await query.edit_message_text("نام جدید برای منبع را وارد کنید:", parse_mode=ParseMode.MARKDOWN_V2)
            return
            
        if action == "sources" and parts[1] == "add" and parts[2] == "start":
            context.user_data.clear()
            # ✅ مرحله ۱: وضعیت جدید برای افزودن هوشمند
            context.user_data['waiting_for'] = 'source_add_smart_name'
            logger.debug("Prompting user for new source display name (smart add)", extra=log_extra)
            # ✅ مرحله ۲: پرسیدن فقط نام نمایشی
            await query.edit_message_text("لطفا نام نمایشی برای منبع جدید را وارد کنید:", parse_mode=ParseMode.MARKDOWN_V2)
            return

        if action == "sources" and parts[1] == "delete":
            # (این بخش بدون تغییر باقی می‌ماند چون از قبل اصلاح شده است)
            sub_action = parts[2]
            source_id = parts[3]
            log_extra['entity_id'] = source_id
            source = next((s for s in ecosystem.get('sources', []) if s['id'] == source_id), None)
            source_name = source['name'] if source else source_id

            if sub_action == "confirm":
                keyboard = [
                    [InlineKeyboardButton("✅ بله، حذف کن", callback_data=f"sources:delete:execute:{source_id}")],
                    [InlineKeyboardButton("❌ خیر، بازگشت", callback_data=f"sources:select:{source_id}")]
                ]
                confirmation_text = f"آیا از حذف منبع *{escape_markdown_v2(source_name)}* و تمام اتصالات آن مطمئن هستید؟ این عمل غیرقابل بازگشت است\\."
                await query.edit_message_text(confirmation_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
                return
                
            if sub_action == "execute":
                logger.info("Source deletion process initiated", extra=log_extra)
                backup_ecosystem()
                ecosystem['sources'] = [s for s in ecosystem.get('sources', []) if s['id'] != source_id]
                mapping = ecosystem.get('mapping', {})
                for copy_id in list(mapping.keys()):
                    mapping[copy_id] = [conn for conn in mapping[copy_id] if conn['source_id'] != source_id]
                if save_ecosystem(context):
                    await regenerate_all_configs(context)
                    logger.info("Source and its connections deleted successfully", extra=log_extra)
                    keyboard = [[InlineKeyboardButton("🔙 بازگشت به لیست منابع", callback_data="sources:main")]]
                    await query.edit_message_text(text=f"✅ منبع *{escape_markdown_v2(source_name)}* با موفقیت حذف شد\\.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN_V2)
                else:
                    logger.error("Failed to save ecosystem after source deletion", extra=log_extra)
                    await query.edit_message_text("❌ خطا در هنگام حذف منبع\\. لطفا لاگ‌ها را بررسی کنید\\.", parse_mode=ParseMode.MARKDOWN_V2)
                return
    
    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.debug("Skipping message edit: content is identical.", extra=log_extra)
            pass
        else:
            log_extra['error'] = str(e)
            logger.error("A BadRequest occurred in sources menu handler", extra=log_extra)
            raise







# ==============================================================================
#  TEXT INPUT HANDLER & PROCESSORS (REFACTORED)
# ==============================================================================

async def _process_source_smart_add(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, ecosystem: dict, log_extra: dict):
    """Processes the display name to smartly create a new source."""
    if not text:
        await update.message.reply_text("❌ نام نمی‌تواند خالی باشد\\. لطفاً یک نام معتبر وارد کنید:", parse_mode=ParseMode.MARKDOWN_V2)
        return False

    max_num = 0
    sources = ecosystem.get('sources', [])
    for s in sources:
        if s['id'].startswith("source_"):
            try:
                num = int(s['id'].split('_')[1])
                if num > max_num:
                    max_num = num
            except (ValueError, IndexError):
                continue

    new_num = max_num + 1
    new_source = {
        "id": f"source_{new_num}",
        "name": text,
        "file_path": f"TradeCopier_S{new_num}.txt",
        "config_file": f"source_{new_num}_config.txt"
    }

    ecosystem.setdefault('sources', []).append(new_source)
    if not save_ecosystem(context):
        raise IOError("Failed to save ecosystem after smart-adding source")

    log_extra.update({'entity_id': new_source['id'], 'details': new_source})
    logger.info("New source smart-added successfully", extra=log_extra)
    
    success_message = (
        f"✅ منبع *{escape_markdown_v2(new_source['name'])}* با موفقیت ساخته شد\\.\n\n"
        f"▫️ شناسه: `{escape_markdown_v2(new_source['id'])}`\n"
        f"▫️ فایل مسیر: `{escape_markdown_v2(new_source['file_path'])}`"
    )
    await update.message.reply_text(success_message, parse_mode=ParseMode.MARKDOWN_V2)
    return True

async def _process_source_edit_name(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, ecosystem: dict, log_extra: dict):
    if not text:
        await update.message.reply_text("❌ نام نمی‌تواند خالی باشد\\. لطفاً یک نام معتبر وارد کنید:", parse_mode=ParseMode.MARKDOWN_V2)
        return False
    source_id = context.user_data.get('selected_source_id')
    if not source_id:
        raise KeyError("'selected_source_id' not found in user_data. Please re-select the source.")
    source_to_edit = next((s for s in ecosystem.get('sources', []) if s['id'] == source_id), None)
    if not source_to_edit:
        await update.message.reply_text("❌ منبع مورد نظر یافت نشد\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return True
    old_name = source_to_edit['name']
    source_to_edit['name'] = text
    if not save_ecosystem(context):
        source_to_edit['name'] = old_name # Rollback change on failure
        raise IOError("Failed to save ecosystem after editing source name")
    log_extra.update({'entity_id': source_id, 'details': {'from': old_name, 'to': text}})
    logger.info("Source name updated successfully", extra=log_extra)
    await update.message.reply_text("✅ نام منبع با موفقیت تغییر کرد\\.", parse_mode=ParseMode.MARKDOWN_V2)
    return True

async def _process_copy_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, ecosystem: dict, log_extra: dict):
    if not text:
        await update.message.reply_text("❌ نام نمی‌تواند خالی باشد\\. لطفاً یک نام معتبر وارد کنید:", parse_mode=ParseMode.MARKDOWN_V2)
        return False
    copy_id = context.user_data['temp_copy_id']
    new_copy = {'id': copy_id, 'name': text, 'settings': {"DailyDrawdownPercent": 5.0, "AlertDrawdownPercent": 4.0}}
    ecosystem.setdefault('copies', []).append(new_copy)
    ecosystem.setdefault('mapping', {})[copy_id] = []
    if not save_ecosystem(context):
        raise IOError("Failed to save ecosystem after adding copy account")
    await regenerate_copy_settings_config(copy_id, context)
    await regenerate_copy_config(copy_id, context)
    log_extra['entity_id'] = copy_id
    logger.info("New copy account added successfully", extra=log_extra)
    await update.message.reply_text(f"✅ حساب کپی *{escape_markdown_v2(text)}* با موفقیت افزوده شد\\.", parse_mode=ParseMode.MARKDOWN_V2)
    return True

async def _process_copy_setting_value(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, ecosystem: dict, log_extra: dict):
    waiting_for = context.user_data.get('waiting_for', '')
    setting_key = waiting_for.replace("copy_", "")
    copy_id = context.user_data.get('selected_copy_id')
    if not copy_id:
        raise KeyError("'selected_copy_id' not found. Please re-select the copy account.")
    try:
        value = float(text)
        if value < 0: raise ValueError("Value cannot be negative.")
    except ValueError:
        await update.message.reply_text("❌ ورودی نامعتبر است\\. لطفاً یک عدد مثبت وارد کنید \\(مثال: 4\\.5\\)\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return False
    copy_account = next((c for c in ecosystem.get('copies', []) if c['id'] == copy_id), None)
    if copy_account:
        copy_account.setdefault('settings', {})[setting_key] = value
        if not save_ecosystem(context):
            raise IOError(f"Failed to save ecosystem after updating {setting_key}")
        await regenerate_copy_settings_config(copy_id, context)
        log_extra.update({'entity_id': copy_id, 'details': {'setting': setting_key, 'value': value}})
        logger.info("Copy setting updated successfully", extra=log_extra)
        await update.message.reply_text(f"✅ مقدار *{escape_markdown_v2(setting_key)}* با موفقیت به‌روزرسانی شد\\.", parse_mode=ParseMode.MARKDOWN_V2)
    else:
        await update.message.reply_text("❌ حساب کپی مورد نظر یافت نشد\\.", parse_mode=ParseMode.MARKDOWN_V2)
    return True

async def _process_conn_volume_value(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, ecosystem: dict, log_extra: dict):
    _, vol_type, copy_id, source_id = context.user_data.get('waiting_for', ':::').split(':')
    try:
        value = float(text)
        if value <= 0: raise ValueError("Value must be a positive number.")
    except ValueError:
        await update.message.reply_text("❌ ورودی نامعتبر است\\. لطفاً یک عدد بزرگتر از صفر وارد کنید\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return False
    connection = next((conn for conn in ecosystem.get('mapping', {}).get(copy_id, []) if conn['source_id'] == source_id), None)
    if not connection:
        await update.message.reply_text("❌ اتصال مورد نظر یافت نشد\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return True
    volume_key = "Multiplier" if vol_type == "mult" else "FixedVolume"
    connection['volume_settings'] = {volume_key: value}
    if not save_ecosystem(context):
        raise IOError("Failed to save ecosystem after updating volume settings")
    await regenerate_copy_config(copy_id, context)
    log_extra.update({'copy_id': copy_id, 'source_id': source_id, 'details': {'type': vol_type, 'value': value}})
    logger.info("Connection volume updated successfully", extra=log_extra)
    await update.message.reply_text("✅ حجم اتصال با موفقیت تنظیم شد\\.", parse_mode=ParseMode.MARKDOWN_V2)
    await _display_connections_for_copy(update.callback_query or update.message, context, copy_id)
    return True

async def _process_conn_symbols(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, ecosystem: dict, log_extra: dict):
    """Processes the list of allowed symbols for a connection."""
    _, copy_id, source_id = context.user_data.get('waiting_for', '::').split(':')
    if not text:
        await update.message.reply_text("❌ لیست نمادها نمی‌تواند خالی باشد\\. لطفاً حداقل یک نماد وارد کنید\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return False

    # پاک‌سازی و فرمت‌بندی ورودی
    symbols = [s.strip().upper() for s in text.split(';') if s.strip()]
    if not symbols:
        await update.message.reply_text("❌ فرمت ورودی نامعتبر است\\. لطفاً نمادها را با سمی‌کالن جدا کنید\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return False
    
    formatted_symbols = ";".join(symbols)

    connection = next((conn for conn in ecosystem.get('mapping', {}).get(copy_id, []) if conn['source_id'] == source_id), None)
    if not connection:
        await update.message.reply_text("❌ اتصال مورد نظر یافت نشد\\.", parse_mode=ParseMode.MARKDOWN_V2)
        return True

    connection['mode'] = 'SYMBOLS'
    connection['allowed_symbols'] = formatted_symbols

    if not save_ecosystem(context):
        raise IOError("Failed to save ecosystem after updating allowed symbols")
    
    await regenerate_copy_config(copy_id, context)
    log_extra.update({'copy_id': copy_id, 'source_id': source_id, 'details': {'mode': 'SYMBOLS', 'symbols': formatted_symbols}})
    logger.info("Connection allowed symbols updated successfully", extra=log_extra)
    await update.message.reply_text(f"✅ حالت کپی به 'نمادهای خاص' با لیست زیر تغییر کرد:\n`{escape_markdown_v2(formatted_symbols)}`", parse_mode=ParseMode.MARKDOWN_V2)
    
    # برای بازگشت به منو، نیاز به یک query داریم. چون در اینجا نداریم، به کاربر پیام می‌دهیم.
    keyboard = [[InlineKeyboardButton("🔙 بازگشت به اتصالات", callback_data=f"conn:select_copy:{copy_id}")]]
    await update.message.reply_text("برای ادامه، به منوی اتصالات بازگردید:", reply_markup=InlineKeyboardMarkup(keyboard))
    return True

# --- Dispatcher Dictionary ---
STATE_HANDLERS = {
    "source_add_smart_name": _process_source_smart_add,
    "source_edit_name": _process_source_edit_name,
    "copy_add_name": _process_copy_add_name,
}

# --- Main Text Input Handler ---
@allowed_users_only
async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    waiting_for = context.user_data.get('waiting_for')
    if not waiting_for:
        return

    text = update.message.text.strip()
    ecosystem = context.bot_data.get('ecosystem', {})
    user_id = update.effective_user.id
    log_extra = {'user_id': user_id, 'state': waiting_for, 'text_received': text, 'status': 'processing'}
    
    handler = STATE_HANDLERS.get(waiting_for)
    if not handler:
        if waiting_for.startswith("copy_"):
            handler = _process_copy_setting_value
        elif waiting_for.startswith("conn_volume:"):
            handler = _process_conn_volume_value
        elif waiting_for.startswith("conn_symbols:"): # <-- بخش جدید
            handler = _process_conn_symbols

    should_return_to_main_menu = False
    try:
        if handler:
            should_return_to_main_menu = await handler(update, context, text, ecosystem=ecosystem, log_extra=log_extra)
        else:
            logger.warning("No handler found for an active 'waiting_for' state. Clearing state.", extra=log_extra)
            should_return_to_main_menu = True
    except (KeyError, IOError, Exception) as e:
        error_message = f"❌ یک خطای غیرمنتظره در پردازش ورودی رخ داد\\."
        await update.message.reply_text(error_message, parse_mode=ParseMode.MARKDOWN_V2)
        log_extra.update({'error': str(e), 'status': 'failure'})
        logger.error("An exception occurred during text input processing.", extra=log_extra)
        await notify_admin_on_error(context, "handle_text_input", e, waiting_for=waiting_for)
        should_return_to_main_menu = True
    finally:
        if should_return_to_main_menu:
            context.user_data.clear()
            logger.debug("State cleared after text input processing.", extra={'user_id': user_id})
            # به جای فراخوانی start، به کاربر اجازه می‌دهیم خودش با دکمه‌ها ادامه دهد
            # این کار از ارسال پیام‌های ناخواسته جلوگیری می‌کند.





async def callback_handler_for_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handles all callback queries that lead to a user providing text input.
    This now includes the multi-step process for setting a connection's volume.
    """
    query = update.callback_query
    await query.answer()
    data = query.data
    parts = data.split(':')
    user_id = update.effective_user.id
    log_extra = {'user_id': user_id, 'callback_data': data}

    try:
        # --- Handler for Copy Account Settings Input ---
        if data.startswith("setting_input_copy_"):
            setting_key = data.replace("setting_input_copy_", "")
            context.user_data['waiting_for'] = f"copy_{setting_key}"
            log_extra['state_set'] = context.user_data['waiting_for']
            logger.debug("Prompting user for copy account setting value", extra=log_extra)
            await query.edit_message_text(
                f"لطفا مقدار جدید برای *{escape_markdown_v2(setting_key)}* را وارد کنید \\(مثال: 4\\.5\\):",
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return

        # --- Handler for Connection Volume Settings (Multi-step) ---
        # Step 1: User clicks the 'Volume' button, show type selection menu.
        if data.startswith("conn:set_volume_type:"):
            copy_id, source_id = parts[2], parts[3]
            log_extra.update({'copy_id': copy_id, 'source_id': source_id})
            logger.debug("Displaying volume type selection menu", extra=log_extra)
            
            keyboard = [
                [InlineKeyboardButton("ضریب (Multiplier)", callback_data=f"conn:set_volume_value:mult:{copy_id}:{source_id}")],
                [InlineKeyboardButton("حجم ثابت (Fixed)", callback_data=f"conn:set_volume_value:fixed:{copy_id}:{source_id}")],
                [InlineKeyboardButton("🔙 لغو", callback_data=f"conn:select_copy:{copy_id}")]
            ]
            await query.edit_message_text(
                "نوع حجم برای این اتصال را انتخاب کنید:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return

        # Step 2: User selects a volume type, prompt for the numeric value.
        if data.startswith("conn:set_volume_value:"):
            vol_type, copy_id, source_id = parts[2], parts[3], parts[4]
            context.user_data['waiting_for'] = f"conn_volume:{vol_type}:{copy_id}:{source_id}"
            
            log_extra.update({'copy_id': copy_id, 'source_id': source_id, 'state_set': context.user_data['waiting_for']})
            logger.debug("Prompting user for connection volume value", extra=log_extra)

            prompt = "لطفا مقدار **ضریب** را وارد کنید \\(مثال: 1\\.5\\):" if vol_type == "mult" else "لطفا مقدار **حجم ثابت** را وارد کنید \\(مثال: 0\\.1\\):"
            await query.edit_message_text(prompt, parse_mode=ParseMode.MARKDOWN_V2)
            return

    except BadRequest as e:
        if "Message is not modified" in str(e):
            logger.debug("Skipping message edit in text input handler: content is identical.", extra=log_extra)
            pass
        else:
            log_extra['error'] = str(e)
            logger.error("A BadRequest occurred in text input handler", extra=log_extra)
            raise
    except Exception as e:
        log_extra['error'] = str(e)
        logger.error("An unexpected error occurred in text input handler.", extra=log_extra)
        await query.edit_message_text("❌ یک خطای غیرمنتظره رخ داد\\. لطفا لاگ‌ها را بررسی کنید\\.", parse_mode=ParseMode.MARKDOWN_V2)



async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors and send detailed report to admin."""
    logger.error("Update handling failed", extra={'status': 'failure', 'error': str(context.error)})
    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = "".join(tb_list)
    update_str = update.to_dict() if isinstance(update, Update) else str(update)
    user_data_str = json.dumps(context.user_data, indent=2, ensure_ascii=False) if context.user_data else "Empty"
    header = "> 🚨 *خطای ربات*\n\n"
    update_info = f"> *به‌روزرسانی:*\n> ```json\n{escape_markdown_v2(str(update_str))}\n> ```\n"
    user_data_info = f"> *داده‌های کاربر:*\n> ```json\n{escape_markdown_v2(user_data_str)}\n> ```\n"
    traceback_info = f"> *ردیابی:*\n> ```\n{escape_markdown_v2(tb_string)}\n> ```"
    full_message = header + update_info + user_data_info + traceback_info
    MAX_MESSAGE_LENGTH = 4096
    if len(full_message) <= MAX_MESSAGE_LENGTH:
        try:
            await context.bot.send_message(chat_id=ADMIN_ID, text=full_message, parse_mode=ParseMode.MARKDOWN_V2)
        except Exception as e:
            logger.error("Error notification send failed", extra={'status': 'failure', 'error': str(e)})
    else:
        try:
            await context.bot.send_message(chat_id=ADMIN_ID, text=header, parse_mode=ParseMode.MARKDOWN_V2)
            with open("error_traceback.txt", "w", encoding="utf-8") as f:
                f.write(f"Update Info:\n{update_str}\n\nUser Data:\n{user_data_str}\n\nTraceback:\n{tb_string}")
            with open("error_traceback.txt", "rb") as f:
                await context.bot.send_document(chat_id=ADMIN_ID, document=f, caption="جزئیات خطا پیوست شد.")
            os.remove("error_traceback.txt")
        except Exception as e:
            logger.error("Error document send failed", extra={'status': 'failure', 'error': str(e)})





async def cleanup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """A scheduled job that automatically cleans up old ecosystem backup files."""
    log_extra = {'job_name': 'backup_cleanup'}
    logger.info("Automatic backup cleanup job started.", extra=log_extra)

    try:
        base_path = os.path.dirname(ECOSYSTEM_PATH)
        backup_pattern = os.path.join(base_path, "ecosystem.json.bak.*")
        backup_files = glob.glob(backup_pattern)

        if len(backup_files) <= 3:
            logger.info("Backup cleanup job skipped: 3 or fewer backups exist.", extra=log_extra)
            return

        backup_files.sort(key=os.path.getmtime, reverse=True)
        files_to_delete = backup_files[3:]
        
        deleted_count = 0
        errors_count = 0

        for file_path in files_to_delete:
            try:
                os.remove(file_path)
                deleted_count += 1
            except OSError as e:
                errors_count += 1
                error_log = log_extra.copy()
                error_log['error'] = str(e)
                logger.error(f"Failed to delete backup file during scheduled job: {os.path.basename(file_path)}", extra=error_log)

        # ارسال گزارش به ادمین در صورت انجام عملیات
        if deleted_count > 0 or errors_count > 0:
            message = f"🤖 *گزارش پاک‌سازی خودکار پشتیبان‌ها*\n\n"
            message += f"🗑️ *فایل‌های حذف شده:* {deleted_count}\n"
            if errors_count > 0:
                message += f"🚨 *خطا در حذف:* {errors_count}"
            
            await context.bot.send_message(chat_id=ADMIN_ID, text=message, parse_mode=ParseMode.MARKDOWN_V2)
            logger.info(f"Automatic backup cleanup finished. Deleted: {deleted_count}, Errors: {errors_count}", extra=log_extra)

    except Exception as e:
        log_extra['error'] = str(e)
        logger.critical("A critical error occurred in the automatic backup cleanup job.", extra=log_extra)
        await context.bot.send_message(chat_id=ADMIN_ID, text=f"🚨 *خطای بحرانی در جاب پاک‌سازی خودکار*:\n`{escape_markdown_v2(str(e))}`", parse_mode=ParseMode.MARKDOWN_V2)




def main() -> None:
    """Initialize and run the bot."""
    if not all([BOT_TOKEN, ECOSYSTEM_PATH, ALLOWED_USERS, LOG_DIRECTORY_PATH]):
        logger.critical("Missing critical environment variables", extra={'status': 'failure'})
        return
        
    application = Application.builder().token(BOT_TOKEN).build()
    
    if not load_ecosystem(application):
        logger.critical("Ecosystem load failed, stopping bot", extra={'status': 'failure'})
        return
    

# ✅ --- Scheduling the Automatic Job ---
    job_queue = application.job_queue
    # محاسبه ۷ روز به ثانیه (7 * 24 * 60 * 60)
    seven_days_in_seconds = 604800
    job_queue.run_repeating(cleanup_job, interval=seven_days_in_seconds, name="weekly_backup_cleanup")
    

    
    # --- Handler Registrations ---
    # Command Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("getlog", get_log_handler))
    application.add_handler(CommandHandler("clean_old_logs", clean_old_logs_handler))
    application.add_handler(CommandHandler("cleanbackups", clean_old_backups_handler))

    
    application.add_handler(CallbackQueryHandler(
        callback_handler_for_text_input, 
        pattern="^setting_input_|^conn:set_volume_type:|^conn:set_volume_value:"
    ))

    # General Menu Handlers
    application.add_handler(CallbackQueryHandler(start, pattern="^main_menu$"))
    application.add_handler(CallbackQueryHandler(start, pattern="^status$"))
    application.add_handler(CallbackQueryHandler(regenerate_all_files_handler, pattern="^regenerate_all_files$"))
    application.add_handler(CallbackQueryHandler(help_handler, pattern="^menu_help$"))
    application.add_handler(CallbackQueryHandler(_handle_connections_menu, pattern="^menu_connections$|^conn:"))
    application.add_handler(CallbackQueryHandler(_handle_copy_settings_menu, pattern="^menu_copy_settings$|^setting:"))
    application.add_handler(CallbackQueryHandler(_handle_sources_menu, pattern="^sources:"))
    application.add_handler(CallbackQueryHandler(handle_statistics_menu, pattern="^statistics_menu$|^stats:"))
    # Message Handler for text input (must be one of the last)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))
    
    # Error handler (must be last)
    application.add_error_handler(error_handler)
    
    logger.info("Bot started successfully", extra={'status': 'success'})
    
    # Run the bot
    application.run_polling()
if __name__ == "__main__":
    main()