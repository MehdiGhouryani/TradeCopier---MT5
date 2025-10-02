import os
import logging
import json
import traceback 
import html   
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from functools import wraps
import glob 
from datetime import datetime 
from telegram.constants import ParseMode

# --- Initial logging and environment variable setup ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log', mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logging.getLogger('httpx').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = 1717599240  
ECOSYSTEM_PATH_STR = os.getenv("ECOSYSTEM_PATH")
LOG_DIRECTORY_PATH = os.getenv("LOG_DIRECTORY_PATH")

try:
    ALLOWED_USERS = [int(uid) for uid in os.getenv("ALLOWED_USERS", "").split(",") if uid]
except (ValueError, TypeError):
    ALLOWED_USERS = []
    logger.error("Error reading ALLOWED_USERS.")

ECOSYSTEM_PATH = ""
if ECOSYSTEM_PATH_STR:
    base_dir = os.path.dirname(os.path.realpath(__file__))
    ECOSYSTEM_PATH = ECOSYSTEM_PATH_STR if os.path.isabs(ECOSYSTEM_PATH_STR) else os.path.join(base_dir, ECOSYSTEM_PATH_STR)
else:
    logger.critical("ECOSYSTEM_PATH environment variable not set.")

# -------------------------------------------------------------------
# تابع کمکی برای ارسال پیام خطا به ادمین
# -------------------------------------------------------------------
async def notify_admin_on_error(context: ContextTypes.DEFAULT_TYPE, function_name: str, error: Exception, **kwargs):
    """یک پیام خطای فرمت‌بندی شده به ادمین ارسال می‌کند."""
    details = ", ".join([f"{k}='{v}'" for k, v in kwargs.items()])
    message = (
        f"🚨 *خطای بحرانی در ربات*\n\n"
        f"هنگام اجرای تابع `{function_name}` خطایی رخ داد.\n\n"
        f"▫️ *جزئیات:* {details}\n"
        f"▫️ *متن خطا:* `{str(error)}`\n\n"
        f"لطفاً برای اطلاعات کامل‌تر، لاگ‌های ربات را بررسی کنید."
    )
    try:
        await context.bot.send_message(chat_id=ADMIN_ID, text=message, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"FATAL: Failed to send critical error notification to admin: {e}")

# --- Ecosystem Helper Functions ---

def load_ecosystem(application: Application) -> bool:
    """Loads the ecosystem data from the JSON file into bot_data for caching."""
    try:
        with open(ECOSYSTEM_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Schema validation for the new structure
        required_keys = ["sources", "copies", "mapping"]
        if not all(key in data for key in required_keys):
            raise KeyError("Ecosystem JSON is missing required keys: sources, copies, mapping.")
        application.bot_data['ecosystem'] = data
        logger.info("Ecosystem data loaded and cached successfully.")
        return True
    except FileNotFoundError:
        logger.error(f"Ecosystem file not found at {ECOSYSTEM_PATH}. Please create it.")
        with open(ECOSYSTEM_PATH, 'w', encoding='utf-8') as f:
            json.dump({"sources": [], "copies": [], "mapping": {}}, f, indent=2)
        logger.info(f"Created a blank ecosystem file at {ECOSYSTEM_PATH}.")
        return load_ecosystem(application)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding ecosystem JSON: {e}. The file might be empty or malformed.", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading ecosystem JSON: {e}", exc_info=True)
        return False

def save_ecosystem(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Saves the cached ecosystem data back to the JSON file using an atomic write."""
    if 'ecosystem' not in context.bot_data:
        return False
    tmp_path = ECOSYSTEM_PATH + ".tmp"
    try:
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(context.bot_data['ecosystem'], f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, ECOSYSTEM_PATH)
        logger.info("Ecosystem data saved successfully.")
        return True
    except Exception as e:
        logger.error(f"Error saving ecosystem file: {e}", exc_info=True)
        return False

def regenerate_all_configs(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Regenerates ALL config files for all sources and copies."""
    ecosystem = context.bot_data.get('ecosystem', {})
    copies = ecosystem.get('copies', [])
    
    # Regenerate all copy connections and settings files
    for copy_account in copies:
        regenerate_copy_config(copy_account['id'], context)
        regenerate_copy_settings_config(copy_account['id'], context)
        
    # Regenerate all source volume files
    regenerate_source_volume_configs(context)
    logger.info("All configuration files have been regenerated.")
    return True

async def regenerate_copy_config(copy_id: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    (نسخه اتمی) فایل کانفیگ اتصالات (_sources.cfg) را برای یک حساب کپی بازسازی می‌کند.
    """
    ecosystem = context.bot_data.get('ecosystem', {})
    connected_source_ids = ecosystem.get('mapping', {}).get(copy_id, [])
    all_sources = {source_account['id']: source_account for source_account in ecosystem.get('sources', [])}
    content = [f"{all_sources[s_id]['file_path']},{all_sources[s_id]['config_file']}" for s_id in connected_source_ids if s_id in all_sources]
    
    cfg_path = os.path.join(os.path.dirname(ECOSYSTEM_PATH), f"{copy_id}_sources.cfg")
    tmp_path = cfg_path + ".tmp"
    
    try:
        with open(tmp_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(content))
        os.replace(tmp_path, cfg_path)
        logger.info(f"Successfully regenerated connections config for copy '{copy_id}'.")
        return True
    except Exception as e:
        logger.error(f"Failed to regenerate connections for '{copy_id}': {e}", exc_info=True)
        await notify_admin_on_error(context, "regenerate_copy_config", e, copy_id=copy_id)
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return False

async def regenerate_copy_settings_config(copy_id: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    (نسخه اتمی) فایل تنظیمات (_config.txt) را برای یک حساب کپی بازسازی می‌کند.
    """
    ecosystem = context.bot_data.get('ecosystem', {})
    copy_account = next((s for s in ecosystem.get('copies', []) if s['id'] == copy_id), None)
    if not copy_account:
        logger.error(f"Cannot regenerate settings: Copy account with id '{copy_id}' not found.")
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
        logger.info(f"Successfully regenerated settings config for copy '{copy_id}'.")
        return True
    except Exception as e:
        logger.error(f"Failed to regenerate settings for '{copy_id}': {e}", exc_info=True)
        await notify_admin_on_error(context, "regenerate_copy_settings_config", e, copy_id=copy_id)
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return False

async def regenerate_source_volume_configs(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    (نسخه اتمی) تمام فایل‌های تنظیمات حجم سورس‌ها را بازسازی می‌کند.
    """
    ecosystem = context.bot_data.get('ecosystem', {})
    all_success = True
    
    for source_account in ecosystem.get('sources', []):
        source_id = source_account.get('id', 'N/A')
        config_file = source_account.get('config_file')
        
        if not config_file:
            logger.warning(f"Skipping source '{source_id}' due to missing 'config_file'.")
            continue

        cfg_path = os.path.join(os.path.dirname(ECOSYSTEM_PATH), config_file)
        tmp_path = cfg_path + ".tmp"
        
        vs = source_account.get('volume_settings', {})
        content = []
        if "FixedVolume" in vs:
            content.append(f"FixedVolume={vs['FixedVolume']}")
        elif "Multiplier" in vs:
            content.append(f"Multiplier={vs['Multiplier']}")
            
        try:
            with open(tmp_path, 'w', encoding='utf-8') as f:
                f.write("\n".join(content))
            os.replace(tmp_path, cfg_path)
            logger.info(f"Successfully regenerated volume config for source '{source_id}'.")
        except Exception as e:
            logger.error(f"Failed to regenerate volume config for '{source_id}': {e}", exc_info=True)
            await notify_admin_on_error(context, "regenerate_source_volume_configs", e, source_id=source_id)
            all_success = False
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
                
    return all_success

def is_user_allowed(user_id: int) -> bool:
    """Checks if a user ID is in the allowed list."""
    return user_id in ALLOWED_USERS

def allowed_users_only(func):
    """
    (Decorator) Restricts access to handlers to allowed users only.
    If a user is not authorized, it notifies them and logs the attempt.
    """
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user or not is_user_allowed(user.id):
            if user:
                logger.warning(
                    f"Unauthorized access denied for user_id={user.id} (Username: @{user.username})."
                )
                
            unauthorized_text = "شما اجازه دسترسی به این ربات را ندارید."
            
            if update.callback_query:
                await update.callback_query.answer(unauthorized_text, show_alert=True)
            elif update.message:
                await update.message.reply_text(unauthorized_text)
            
            return
            
        return await func(update, context, *args, **kwargs)
        
    return wrapped

# --- Handlers ---

@allowed_users_only
async def clean_old_logs_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /clean_old_logs command to delete log files from previous days."""
    await update.message.reply_text("⏳ در حال بررسی و پاکسازی فایل‌های لاگ قدیمی...")

    if not LOG_DIRECTORY_PATH:
        await update.message.reply_text("❌ خطا: مسیر پوشه لاگ‌ها در سرور تنظیم نشده است.")
        logger.error("LOG_DIRECTORY_PATH environment variable is not set.")
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
                    logger.info(f"Deleted old log file: {log_file}")
                except Exception as e:
                    errors_count += 1
                    logger.error(f"Failed to delete log file {log_file}: {e}")
        
        message = f"✅ عملیات پاکسازی با موفقیت انجام شد.\n\n"
        message += f"🗑️ *تعداد فایل‌های پاک شده:* {deleted_count}\n"
        if errors_count > 0:
            message += f"🚨 *تعداد خطاها در پاکسازی:* {errors_count}"
            
        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        await update.message.reply_text(f"❌ یک خطای پیش‌بینی نشده در حین پاکسازی رخ داد: {e}")
        logger.error(f"Error in clean_old_logs_handler: {e}", exc_info=True)

@allowed_users_only
async def get_log_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /getlog command to fetch the latest log file for a copy."""
    args = context.args
    if not args:
        await update.message.reply_text(
            "فرمت دستور اشتباه است.\n"
            "استفاده صحیح: `/getlog <copy_id> [تعداد_خطوط]`\n"
            "مثال: `/getlog copy_A 50`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    copy_id = args[0]
    num_lines = int(args[1]) if len(args) > 1 and args[1].isdigit() else 20

    if not LOG_DIRECTORY_PATH:
        await update.message.reply_text("❌ خطا: مسیر پوشه لاگ‌ها در سرور تنظیم نشده است.")
        logger.error("LOG_DIRECTORY_PATH environment variable is not set.")
        return

    try:
        log_pattern = os.path.join(LOG_DIRECTORY_PATH, f"TradeCopier_{copy_id}_*.log")
        list_of_files = glob.glob(log_pattern)
        
        if not list_of_files:
            await update.message.reply_text(f"هیچ فایل لاگی برای کپی `{copy_id}` یافت نشد.", parse_mode=ParseMode.MARKDOWN)
            return

        latest_file = max(list_of_files, key=os.path.getctime)
        
        with open(latest_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        last_lines = lines[-num_lines:]
        
        if not last_lines:
            await update.message.reply_text(f"فایل لاگ `{os.path.basename(latest_file)}` خالی است.", parse_mode=ParseMode.MARKDOWN)
            return

        message = f"📄 *آخرین {len(last_lines)} خط از لاگ برای `{copy_id}`*\n"
        message += f"*فایل:* `{os.path.basename(latest_file)}`\n\n"
        message += "```\n"
        message += "".join(last_lines)
        message += "```"

        MAX_MESSAGE_LENGTH = 4096
        if len(message) > MAX_MESSAGE_LENGTH:
            await update.message.reply_text(
                f"📄 *آخرین {len(last_lines)} خط از لاگ برای `{copy_id}`*\n"
                f"*فایل:* `{os.path.basename(latest_file)}`\n\n"
                "محتوای لاگ بیش از حد طولانی است و به صورت فایل متنی ارسال می‌شود."
            )
            log_output_path = os.path.join(os.path.dirname(__file__), f"log_{copy_id}.txt")
            with open(log_output_path, "w", encoding="utf-8") as f:
                f.write("".join(last_lines))
            await update.message.reply_document(document=open(log_output_path, 'rb'))
            os.remove(log_output_path)
        else:
            await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        await update.message.reply_text(f"❌ یک خطای پیش‌بینی نشده در خواندن فایل لاگ رخ داد: {e}")
        logger.error(f"Error in get_log_handler: {e}", exc_info=True)

async def get_detailed_status_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    """Creates a detailed and formatted string of the entire system status."""
    ecosystem = context.bot_data.get('ecosystem', {})
    if not ecosystem:
        return "❌ **خطا: اطلاعات سیستم بارگذاری نشده است.**"

    source_map = {source_account['id']: source_account['name'] for source_account in ecosystem.get('sources', [])}
    status_lines = ["**-- 🍃 وضعیت کامل سیستم --**"]

    status_lines.append("\n**📊 حساب‌های سورس**")
    sources = ecosystem.get('sources', [])
    if not sources:
        status_lines.append("  - هیچ سورس‌ای تعریف نشده است.")
    else:
        for source_account in sources:
            vs = source_account.get('volume_settings', {})
            mode = "حجم ثابت" if "FixedVolume" in vs else "ضریب"
            value = vs.get("FixedVolume", vs.get("Multiplier", "N/A"))
            status_lines.append(f"  - `{source_account['name']}`: *{mode} = {value}*")

    status_lines.append("\n**🛡️ حساب‌های کپی**")
    copies = ecosystem.get('copies', [])
    if not copies:
        status_lines.append("  - هیچ حساب کپی تعریف نشده است.")
    else:
        for copy_account in copies:
            settings = copy_account.get('settings', {})
            dd = float(settings.get("DailyDrawdownPercent", 0))
            risk_status = f"فعال ({dd}%)" if dd > 0 else "غیرفعال"
            copy_mode = "تمام نمادها" if settings.get("CopySymbolMode", "GOLD_ONLY") == "ALL" else "فقط طلا"
            
            connected_ids = ecosystem.get('mapping', {}).get(copy_account['id'], [])
            connected_names = [source_map.get(sid, sid) for sid in connected_ids]
            connections_text = ", ".join(f"`{name}`" for name in connected_names) if connected_names else "_به هیچ سورس‌ای متصل نیست_"

            status_lines.append(f"\n  - **{copy_account['name']}** (`{copy_account['id']}`)")
            status_lines.append(f"    - ریسک: *{risk_status}* | کپی: *{copy_mode}*")
            status_lines.append(f"    - اتصالات: {connections_text}")
    
    return "\n".join(status_lines)

@allowed_users_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the main menu with a detailed status."""
    status_text = await get_detailed_status_text(context)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⛓️ مدیریت اتصالات", callback_data="menu_connections")],
        [InlineKeyboardButton("🛡️ تنظیمات حساب‌های کپی", callback_data="menu_copy_settings")],
        [InlineKeyboardButton("📊 تنظیمات حجم سورس‌ها", callback_data="menu_volume_settings")],
        [InlineKeyboardButton("🔄 بازتولید تمام فایل‌ها", callback_data="regenerate_all_files")],
        [InlineKeyboardButton("ℹ️ راهنما", callback_data="menu_help")],
    ])
    
    message = f"{status_text}\n\nلطفاً بخش مورد نظر را برای مدیریت انتخاب کنید:"
    
    if update.callback_query:
        await update.callback_query.edit_message_text(message, reply_markup=keyboard, parse_mode='Markdown')
    else:
        await update.message.reply_text(message, reply_markup=keyboard, parse_mode='Markdown')

@allowed_users_only
async def regenerate_all_files_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """(نسخه async) تمام فایل‌های کانفیگ را بازسازی می‌کند."""
    query = update.callback_query
    await query.answer("⏳ در حال بازتولید تمام فایل‌های کانفیگ...")
    
    ecosystem = context.bot_data.get('ecosystem', {})
    copies = ecosystem.get('copies', [])
    all_success = True

    for copy_account in copies:
        if not await regenerate_copy_config(copy_account['id'], context) or \
           not await regenerate_copy_settings_config(copy_account['id'], context):
            all_success = False
    
    if not await regenerate_source_volume_configs(context):
        all_success = False
    
    if all_success:
        logger.info("All configuration files have been regenerated successfully.")
        await query.answer("✅ تمام فایل‌ها با موفقیت بازتولید شدند!", show_alert=True)
    else:
        logger.error("An error occurred during the regeneration of all config files.")
        await query.answer("❌ خطا در بازتولید برخی از فایل‌ها! لاگ‌ها را بررسی کنید.", show_alert=True)

async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the help message and a back button."""
    query = update.callback_query
    await query.answer()
    
    help_text = """
===============

*⛓️ اتصالات:*
انتخاب حساب کپی و فعال/غیرفعال کردن اتصال سورس‌ها.

*🛡️ تنظیمات حساب‌های کپی:*
تنظیم DD، حالت کپی (طلا/همه) و ریست کردن قفل اکسپرت.

*📊 حجم سورس‌ها:*
تنظیم نحوه کپی حجم برای هر سورس (حجم ثابت / ضریب).

*🔄 بازتولید فایل‌ها:*
همگام‌سازی تمام اکسپرت‌ها با آخرین تغییرات ربات.

----
`/getlog <ID>`: مشاهده لاگ یک حساب کپی.

`/clean_old_logs`: پاک کردن لاگ‌های قدیمی.
"""
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 بازگشت به منوی اصلی", callback_data="main_menu")]
    ])
    
    await query.edit_message_text(
        text=help_text,
        reply_markup=keyboard,
        parse_mode=ParseMode.MARKDOWN
    )

async def _handle_connections_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the connection management flow."""
    query = update.callback_query
    await query.answer()
    data = query.data
    ecosystem = context.bot_data.get('ecosystem', {})

    parts = data.split(':')
    action = parts[0]
    
    if action == "menu_connections":
        copies = ecosystem.get('copies', [])
        keyboard = [[InlineKeyboardButton(f"{c['name']} ({c['id']})", callback_data=f"conn:select:{c['id']}")] for c in copies]
        keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data="main_menu")])
        await query.edit_message_text("یک حساب کپی را برای مدیریت اتصالات انتخاب کنید:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if action == "conn" and parts[1] == "select":
        copy_id = parts[2]
        sources = ecosystem.get('sources', [])
        connected = ecosystem.get('mapping', {}).get(copy_id, [])
        copy_name = next((c['name'] for c in ecosystem.get('copies', []) if c['id'] == copy_id), copy_id)
        keyboard = [[InlineKeyboardButton(f"{'✅' if s['id'] in connected else '❌'} {s['name']}", callback_data=f"conn:toggle:{copy_id}:{s['id']}")] for s in sources]
        keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data="menu_connections")])
        await query.edit_message_text(f"اتصالات حساب کپی **{copy_name}**:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

    if action == "conn" and parts[1] == "toggle":
        copy_id = parts[2]
        source_id = parts[3]
        source_name = next((s['name'] for s in ecosystem.get('sources', []) if s['id'] == source_id), source_id)
        keyboard = [[InlineKeyboardButton("✅ بله", callback_data=f"conn:confirm:{copy_id}:{source_id}"), InlineKeyboardButton("❌ لغو", callback_data=f"conn:select:{copy_id}")]]
        await query.edit_message_text(f"آیا از تغییر اتصال به **{source_name}** مطمئن هستید؟", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

    if action == "conn" and parts[1] == "confirm":
        copy_id = parts[2]
        source_id = parts[3]
        
        await query.answer("⏳ در حال به‌روزرسانی...")
        mapping = ecosystem.get('mapping', {})
        connected = mapping.get(copy_id, [])
        if source_id in connected:
            connected.remove(source_id)
        else:
            connected.append(source_id)
        
        context.bot_data['ecosystem']['mapping'][copy_id] = connected
        
        if save_ecosystem(context) and await regenerate_copy_config(copy_id, context):
            await query.answer("✅ انجام شد!")
        else:
            await query.answer("❌ خطا!")
        
        sources = ecosystem.get('sources', [])
        connected = ecosystem.get('mapping', {}).get(copy_id, [])
        copy_name = next((c['name'] for c in ecosystem.get('copies', []) if c['id'] == copy_id), copy_id)
        keyboard = [[InlineKeyboardButton(f"{'✅' if s['id'] in connected else '❌'} {s['name']}", callback_data=f"conn:toggle:{copy_id}:{s['id']}")] for s in sources]
        keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data="menu_connections")])
        await query.edit_message_text(f"اتصالات حساب کپی **{copy_name}**:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def _handle_copy_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the per-copy settings flow."""
    query = update.callback_query
    await query.answer()
    data = query.data
    ecosystem = context.bot_data.get('ecosystem', {})
    
    parts = data.split(':')
    action = parts[0]

    if action == "menu_copy_settings":
        copies = ecosystem.get('copies', [])
        keyboard = [[InlineKeyboardButton(f"{c['name']} ({c['id']})", callback_data=f"setting:select:{c['id']}")] for c in copies]
        keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data="main_menu")])
        await query.edit_message_text("یک حساب کپی را برای مدیریت تنظیمات انتخاب کنید:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if action == "setting" and parts[1] == "select":
        copy_id = parts[2]
        context.user_data['selected_copy_id'] = copy_id
        copy_account = next((c for c in ecosystem.get('copies', []) if c['id'] == copy_id), None)
        if not copy_account:
            await query.edit_message_text("❌ خطا: حساب کپی یافت نشد.")
            return

        settings = copy_account.get('settings', {})
        dd = float(settings.get("DailyDrawdownPercent", 0))
        cm_text = "فقط طلا" if settings.get("CopySymbolMode", "GOLD_ONLY") == "GOLD_ONLY" else "تمام نمادها"
        keyboard = [
            [InlineKeyboardButton(f"{'❌ غیرفعال' if dd > 0 else '✅ فعال'} کردن ریسک", callback_data=f"setting:action:toggle_dd")],
            [InlineKeyboardButton(f" حالت کپی: {cm_text}", callback_data=f"setting:action:copy_mode")],
            [InlineKeyboardButton("تنظیم حد ضرر (DD %)", callback_data="setting_input_copy_DailyDrawdownPercent")],
            [InlineKeyboardButton("تنظیم حد هشدار (%)", callback_data="setting_input_copy_AlertDrawdownPercent")],
            [InlineKeyboardButton("ریست کردن قفل (RESET)", callback_data=f"setting:action:reset_stop")],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="menu_copy_settings")]
        ]
        await query.edit_message_text(f"تنظیمات حساب کپی **{copy_account['name']}**:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return
        
    copy_id = context.user_data.get('selected_copy_id')
    if not copy_id:
        return
    copy_account = next((c for c in ecosystem.get('copies', []) if c['id'] == copy_id), None)
    if not copy_account:
        await query.edit_message_text("❌ خطا: حساب کپی یافت نشد.")
        return
    
    if action == "setting" and parts[1] == "action":
        sub_action = parts[2]
        should_save = True
        if sub_action == "toggle_dd":
            copy_account['settings']['DailyDrawdownPercent'] = 0.0 if float(copy_account['settings'].get("DailyDrawdownPercent", 0)) > 0 else 4.7
        elif sub_action == "copy_mode":
            should_save = False
            keyboard = [[InlineKeyboardButton("کپی تمام نمادها", callback_data=f"setting:set_copy:ALL"), InlineKeyboardButton("کپی فقط طلا", callback_data=f"setting:set_copy:GOLD_ONLY")], [InlineKeyboardButton("🔙 بازگشت", callback_data=f"setting:select:{copy_id}")]]
            await query.edit_message_text("حالت کپی نماد را انتخاب کنید:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        elif sub_action == "reset_stop":
            context.user_data['reset_stop_for_copy'] = copy_id
            await query.answer("دستور ریست برای حساب کپی ارسال شد.", show_alert=True)
        
        if should_save:
            if save_ecosystem(context) and await regenerate_copy_settings_config(copy_id, context):
                await query.answer("✅ انجام شد!")
        
        settings = copy_account.get('settings', {})
        dd = float(settings.get("DailyDrawdownPercent", 0))
        cm_text = "فقط طلا" if settings.get("CopySymbolMode", "GOLD_ONLY") == "GOLD_ONLY" else "تمام نمادها"
        keyboard = [
            [InlineKeyboardButton(f"{'❌ غیرفعال' if dd > 0 else '✅ فعال'} کردن ریسک", callback_data=f"setting:action:toggle_dd")],
            [InlineKeyboardButton(f" حالت کپی: {cm_text}", callback_data=f"setting:action:copy_mode")],
            [InlineKeyboardButton("تنظیم حد ضرر (DD %)", callback_data="setting_input_copy_DailyDrawdownPercent")],
            [InlineKeyboardButton("تنظیم حد هشدار (%)", callback_data="setting_input_copy_AlertDrawdownPercent")],
            [InlineKeyboardButton("ریست کردن قفل (RESET)", callback_data=f"setting:action:reset_stop")],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="menu_copy_settings")]
        ]
        await query.edit_message_text(f"تنظیمات حساب کپی **{copy_account['name']}**:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return
        
    if action == "setting" and parts[1] == "set_copy":
        mode = parts[2]
        copy_account['settings']['CopySymbolMode'] = mode
        if save_ecosystem(context) and await regenerate_copy_settings_config(copy_id, context):
            await query.answer(f"✅ حالت کپی به {mode} تغییر یافت")
        
        settings = copy_account.get('settings', {})
        dd = float(settings.get("DailyDrawdownPercent", 0))
        cm_text = "فقط طلا" if settings.get("CopySymbolMode", "GOLD_ONLY") == "GOLD_ONLY" else "تمام نمادها"
        keyboard = [
            [InlineKeyboardButton(f"{'❌ غیرفعال' if dd > 0 else '✅ فعال'} کردن ریسک", callback_data=f"setting:action:toggle_dd")],
            [InlineKeyboardButton(f" حالت کپی: {cm_text}", callback_data=f"setting:action:copy_mode")],
            [InlineKeyboardButton("تنظیم حد ضرر (DD %)", callback_data="setting_input_copy_DailyDrawdownPercent")],
            [InlineKeyboardButton("تنظیم حد هشدار (%)", callback_data="setting_input_copy_AlertDrawdownPercent")],
            [InlineKeyboardButton("ریست کردن قفل (RESET)", callback_data=f"setting:action:reset_stop")],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="menu_copy_settings")]
        ]
        await query.edit_message_text(f"تنظیمات حساب کپی **{copy_account['name']}**:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def _handle_volume_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the source volume settings flow."""
    query = update.callback_query
    await query.answer()
    data = query.data
    ecosystem = context.bot_data.get('ecosystem', {})

    parts = data.split(':')
    action = parts[0]

    if action == "menu_volume_settings":
        sources = ecosystem.get('sources', [])
        keyboard = [[InlineKeyboardButton(f"{s['name']} ({s['id']})", callback_data=f"vol:select:{s['id']}")] for s in sources]
        keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data="main_menu")])
        await query.edit_message_text("یک سورس را برای تنظیم حجم انتخاب کنید:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if action == "vol" and parts[1] == "select":
        source_id = parts[2]
        context.user_data['selected_source_id'] = source_id
        source_account = next((s for s in ecosystem.get('sources', []) if s['id'] == source_id), None)
        if not source_account:
            await query.edit_message_text("❌ خطا: سورس یافت نشد.")
            return

        vs = source_account.get('volume_settings', {})
        mode = "FixedVolume" if "FixedVolume" in vs else "Multiplier"
        value = vs.get(mode, "N/A")
        keyboard = [[InlineKeyboardButton("حجم ثابت (Fixed)", callback_data="vol_input_source_FixedVolume"), InlineKeyboardButton("ضریب (Multiplier)", callback_data="vol_input_source_Multiplier")], [InlineKeyboardButton("🔙 بازگشت", callback_data="menu_volume_settings")]]
        await query.edit_message_text(f"سورس: **{source_account['name']}**\nوضعیت: `{mode}={value}`\n\nحالت حجم را انتخاب کنید:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles all numerical inputs for settings."""
    if not is_user_allowed(update.effective_user.id):
        return
    waiting_for = context.user_data.get('waiting_for')
    if not waiting_for:
        return
    
    try:
        value = float(update.message.text)
        ecosystem = context.bot_data['ecosystem']
        
        if waiting_for.startswith("copy_"):
            key = waiting_for.replace("copy_", "")
            copy_id = context.user_data.get('selected_copy_id')
            copy_account = next((c for c in ecosystem.get('copies', []) if c['id'] == copy_id), None)
            if not copy_account:
                raise Exception("حساب کپی انتخاب شده یافت نشد.")
            copy_account['settings'][key] = round(value, 2)
            if save_ecosystem(context) and await regenerate_copy_settings_config(copy_id, context):
                await update.message.reply_text("✅ تنظیمات حساب کپی ذخیره شد.")
            else:
                raise Exception("خطا در ذخیره سازی.")
        
        elif waiting_for.startswith("source_"):
            key = waiting_for.replace("source_", "")
            source_id = context.user_data.get('selected_source_id')
            source_account = next((s for s in ecosystem.get('sources', []) if s['id'] == source_id), None)
            if not source_account:
                raise Exception("سورس انتخاب شده یافت نشد.")
            source_account['volume_settings'] = {key: round(value, 2)}
            if save_ecosystem(context) and await regenerate_source_volume_configs(context):
                await update.message.reply_text("✅ تنظیمات حجم ذخیره شد.")
            else:
                raise Exception("خطا در ذخیره سازی.")

        context.user_data.clear()
        await start(update, context)
    except (ValueError, TypeError):
        await update.message.reply_text("❌ خطا: لطفاً فقط یک مقدار عددی معتبر (مثال: 4.7 یا 0.1) وارد کنید.")
    except Exception as e:
        await update.message.reply_text(f"❌ خطا: {e}")
        logger.error(f"Error in handle_text_input: {e}", exc_info=True)

async def text_input_trigger(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sets the state to wait for a text input."""
    query = update.callback_query
    await query.answer()
    data = query.data
    
    waiting_for_map = {
        "setting_input_copy_DailyDrawdownPercent": ("copy_DailyDrawdownPercent", "درصد حد ضرر روزانه را وارد کنید (مثال: 4.7):"),
        "setting_input_copy_AlertDrawdownPercent": ("copy_AlertDrawdownPercent", "درصد هشدار را وارد کنید (مثال: 4.0):"),
        "vol_input_source_FixedVolume": ("source_FixedVolume", "مقدار حجم ثابت را وارد کنید (مثال: 0.1):"),
        "vol_input_source_Multiplier": ("source_Multiplier", "مقدار ضریب را وارد کنید (مثال: 1.5):"),
    }
    if data in waiting_for_map:
        key, prompt = waiting_for_map[data]
        context.user_data['waiting_for'] = key
        await query.edit_message_text(prompt)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Logs the error and sends a telegram message to notify the admin."""
    logger.error("Exception while handling an update:", exc_info=context.error)

    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = "".join(tb_list)

    update_str = update.to_dict() if isinstance(update, Update) else str(update)
    message = (
        f"An exception was raised while handling an update\n\n"
        f"<b>Update:</b>\n<pre>{html.escape(json.dumps(update_str, indent=2, ensure_ascii=False))}</pre>\n\n"
        f"<b>User Data:</b>\n<pre>{html.escape(str(context.user_data))}</pre>\n\n"
        f"<b>Error:</b>\n<pre>{html.escape(tb_string)}</pre>"
    )
    
    MAX_MESSAGE_LENGTH = 4096
    for i in range(0, len(message), MAX_MESSAGE_LENGTH):
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=message[i:i + MAX_MESSAGE_LENGTH],
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Failed to send error message to admin: {e}")

def main() -> None:
    if not all([BOT_TOKEN, ECOSYSTEM_PATH, ALLOWED_USERS, LOG_DIRECTORY_PATH]):
        logger.critical("FATAL: Critical environment variables are not set. Check your .env file.")
        return
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    if not load_ecosystem(application):
        logger.critical("FATAL: Could not load initial ecosystem data. Bot will not start.")
        return

    # دستورات اصلی
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("getlog", get_log_handler)) 
    application.add_handler(CommandHandler("clean_old_logs", clean_old_logs_handler))
    # Handlers for main menu buttons
    application.add_handler(CallbackQueryHandler(start, pattern="^main_menu$"))
    application.add_handler(CallbackQueryHandler(regenerate_all_files_handler, pattern="^regenerate_all_files$"))
    application.add_handler(CallbackQueryHandler(help_handler, pattern="^menu_help$"))
    
    # Handlers for specific logic sections
    application.add_handler(CallbackQueryHandler(_handle_connections_menu, pattern="^menu_connections$|^conn:"))
    application.add_handler(CallbackQueryHandler(_handle_copy_settings_menu, pattern="^menu_copy_settings$|^setting:"))
    application.add_handler(CallbackQueryHandler(_handle_volume_menu, pattern="^menu_volume_settings$|^vol:"))

    # Handlers for text input state
    application.add_handler(CallbackQueryHandler(text_input_trigger, pattern="^setting_input_|^vol_input_"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))
    
    # ثبت handler برای مدیریت خطا
    application.add_error_handler(error_handler)
    
    logger.info("Bot is running...")
    application.run_polling()

if __name__ == "__main__":
    main()