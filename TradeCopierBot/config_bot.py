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
            
            connections = ecosystem.get('mapping', {}).get(copy_account['id'], [])
            connected_names = [source_map.get(conn['source_id'], conn['source_id']) for conn in connections]
            connections_text = ", ".join(f"`{name}`" for name in connected_names) if connected_names else "_به هیچ سورس‌ای متصل نیست_"

            status_lines.append(f"\n  - **{copy_account['name']}** (`{copy_account['id']}`)")
            status_lines.append(f"    - ریسک: *{risk_status}*")
            status_lines.append(f"    - اتصالات: {connections_text}")
    
    return "\n".join(status_lines)

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

async def regenerate_all_configs(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Regenerates ALL config files for all sources and copies."""
    ecosystem = context.bot_data.get('ecosystem', {})
    copies = ecosystem.get('copies', [])
    
    # Regenerate all copy connections and settings files
    for copy_account in copies:
        await regenerate_copy_config(copy_account['id'], context)
        await regenerate_copy_settings_config(copy_account['id'], context)
        
    # Regenerate all source volume files
    await regenerate_source_volume_configs(context)
    logger.info("All configuration files have been regenerated.")
    return True

async def regenerate_copy_config(copy_id: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    (نسخه اتمی) فایل کانفیگ اتصالات (_sources.cfg) را برای یک حساب کپی بازسازی می‌کند.
    """
    ecosystem = context.bot_data.get('ecosystem', {})
    connections = ecosystem.get('mapping', {}).get(copy_id, [])
    all_sources = {source_account['id']: source_account for source_account in ecosystem.get('sources', [])}
    content = []
    for conn in connections:
        s_id = conn.get('source_id')
        if s_id in all_sources:
            mode = conn.get('mode', 'ALL')
            allowed_symbols = conn.get('allowed_symbols', '') if mode == 'SYMBOLS' else ''
            line = f"{all_sources[s_id]['file_path']},{all_sources[s_id]['config_file']},{mode},{allowed_symbols}"
            content.append(line)
    
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
        await update.message.reply_text("لطفاً ID حساب کپی و (اختیاری) تعداد خطوط را وارد کنید. مثال: /getlog copy_A 50")
        return

    copy_id = args[0]
    num_lines = int(args[1]) if len(args) > 1 else 50  # پیش‌فرض 50 خط آخر

    if not LOG_DIRECTORY_PATH:
        await update.message.reply_text("❌ خطا: مسیر پوشه لاگ‌ها تنظیم نشده است.")
        return

    try:
        log_pattern = os.path.join(LOG_DIRECTORY_PATH, f"TradeCopier_{copy_id}_*.log")
        all_logs = glob.glob(log_pattern)
        if not all_logs:
            await update.message.reply_text(f"❌ لاگی برای {copy_id} یافت نشد.")
            return

        latest_log = max(all_logs, key=os.path.getctime)
        with open(latest_log, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            tail_lines = lines[-num_lines:] if num_lines > 0 else lines

        log_content = ''.join(tail_lines)
        if len(log_content) > 4096:  # محدودیت پیام تلگرام
            temp_file = f"{copy_id}_log.txt"
            with open(temp_file, 'w', encoding='utf-8') as temp:
                temp.write(log_content)
            await update.message.reply_document(document=open(temp_file, 'rb'))
            os.remove(temp_file)
        else:
            await update.message.reply_text(f"لاگ {copy_id}:\n```{log_content}```", parse_mode=ParseMode.MARKDOWN)

    except Exception as e:
        await update.message.reply_text(f"❌ خطا در دریافت لاگ: {e}")
        logger.error(f"Error in get_log_handler: {e}", exc_info=True)

@allowed_users_only
async def regenerate_all_files_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the 'بازسازی تمام فایل‌ها' button."""
    query = update.callback_query
    await query.answer()
    if await regenerate_all_configs(context): 
        await query.edit_message_text("✅ تمام فایل‌های کانفیگ با موفقیت بازسازی شدند.")
    else:
        await query.edit_message_text("❌ خطا در بازسازی فایل‌ها. لطفاً لاگ‌ها را بررسی کنید.")

@allowed_users_only
async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the 'راهنما' button."""
    query = update.callback_query
    await query.answer()
    help_text = (
        "راهنمای ربات مدیریت اکوسیستم:\n\n"
        "/start - شروع ربات و نمایش منو اصلی\n"
        "/getlog [copy_id] [lines] - دریافت آخرین لاگ برای یک حساب کپی (پیش‌فرض 50 خط)\n"
        "/clean_old_logs - پاکسازی لاگ‌های قدیمی\n\n"
        "منوها:\n"
        "- مدیریت اتصالات: اتصال سورس به کپی\n"
        "- تنظیمات حساب‌های کپی: مدیریت ریسک و حالت کپی\n"
        "- تنظیمات حجم سورس‌ها: Fixed یا Multiplier\n"
        "- بازسازی فایل‌ها: تولید مجدد تمام کانفیگ‌ها"
    )
    keyboard = [[InlineKeyboardButton("🔙 بازگشت", callback_data="main_menu")]]
    await query.edit_message_text(help_text, reply_markup=InlineKeyboardMarkup(keyboard))

@allowed_users_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays the main menu with a detailed status."""
    status_text = await get_detailed_status_text(context)
    keyboard = [
        [InlineKeyboardButton("⛓️ مدیریت اتصالات", callback_data="menu_connections")],
        [InlineKeyboardButton("🛡️ تنظیمات حساب‌های کپی", callback_data="menu_copy_settings")],
        [InlineKeyboardButton("📊 تنظیمات حجم سورس‌ها", callback_data="menu_volume_settings")],
        [InlineKeyboardButton("🔄 بازتولید تمام فایل‌ها", callback_data="regenerate_all_files")],
        [InlineKeyboardButton("ℹ️ راهنما", callback_data="menu_help")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    message = f"{status_text}\n\nلطفاً بخش مورد نظر را برای مدیریت انتخاب کنید:"
    
    if update.callback_query:
        await update.callback_query.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')




# --- کل تابع فعلی را حذف کرده و این نسخه را جایگزین کنید ---
async def _handle_connections_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the connections management flow with advanced symbol filtering."""
    query = update.callback_query
    await query.answer()
    data = query.data
    ecosystem = context.bot_data.get('ecosystem', {})

    # START: Auto-migration for old mapping structure
    for copy_id_key, connections_list in ecosystem.get('mapping', {}).items():
        if connections_list and isinstance(connections_list[0], str):
            logger.warning(f"Old mapping structure detected for '{copy_id_key}'. Migrating to new structure...")
            new_connections = [{'source_id': src_id, 'mode': 'ALL'} for src_id in connections_list]
            ecosystem['mapping'][copy_id_key] = new_connections
            save_ecosystem(context)
            break
    # END: Auto-migration

    parts = data.split(':')
    action = parts[0]

    # --- نمایش منوی اصلی اتصالات (انتخاب حساب کپی) ---
    if action == "menu_connections":
        copies = ecosystem.get('copies', [])
        keyboard = [[InlineKeyboardButton(f"{c['name']} ({c['id']})", callback_data=f"conn:select_copy:{c['id']}")] for c in copies]
        keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data="main_menu")])
        await query.edit_message_text("یک حساب کپی را انتخاب کنید:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # --- نمایش لیست سورس‌ها برای یک حساب کپی ---
    if action == "conn" and parts[1] == "select_copy":
        copy_id = parts[2]
        context.user_data['selected_copy_id'] = copy_id
        copy_account = next((c for c in ecosystem.get('copies', []) if c['id'] == copy_id), None)
        if not copy_account:
            await query.edit_message_text("❌ خطا: حساب کپی یافت نشد.")
            return

        sources = ecosystem.get('sources', [])
        connections = ecosystem.get('mapping', {}).get(copy_id, [])
        connected_sources = {conn['source_id']: conn for conn in connections}

        keyboard = []
        for s in sources:
            conn = connected_sources.get(s['id'])
            status = f"متصل - {conn.get('mode', 'ALL')}" if conn else "غیرمتصل"
            keyboard.append([InlineKeyboardButton(f"{s['name']} ({s['id']}) - {status}", callback_data=f"conn:manage_source:{s['id']}")])
        keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data="menu_connections")])
        await query.edit_message_text(f"اتصالات حساب کپی **{copy_account['name']}**:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

    # --- نمایش منوی مدیریت برای یک سورس خاص ---
    if action == "conn" and parts[1] == "manage_source":
        source_id = parts[2]
        context.user_data['selected_source_id'] = source_id
        copy_id = context.user_data.get('selected_copy_id')
        connections = ecosystem.get('mapping', {}).get(copy_id, [])
        is_connected = any(c['source_id'] == source_id for c in connections)

        toggle_text = "غیرفعال کردن اتصال" if is_connected else "فعال کردن اتصال"
        keyboard = [
            [InlineKeyboardButton(toggle_text, callback_data=f"conn:action:toggle_connection:{source_id}")]
        ]
        if is_connected:
            keyboard.append([InlineKeyboardButton("تغییر حالت کپی", callback_data=f"conn:action:change_mode:{source_id}")])
        keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data=f"conn:select_copy:{copy_id}")])
        await query.edit_message_text(f"مدیریت اتصال سورس {source_id}:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # --- منطق فعال/غیرفعال کردن اتصال ---
    if action == "conn" and parts[1] == "action" and parts[2] == "toggle_connection":
        source_id = parts[3]
        copy_id = context.user_data.get('selected_copy_id')
        connections = ecosystem.get('mapping', {}).get(copy_id, [])
        conn_index = next((i for i, c in enumerate(connections) if c['source_id'] == source_id), None)

        if conn_index is not None:
            del connections[conn_index]
        else:
            connections.append({'source_id': source_id, 'mode': 'ALL'})

        if save_ecosystem(context) and await regenerate_copy_config(copy_id, context):
            await query.answer("✅ اتصال به‌روزرسانی شد.")
        else:
            await query.answer("❌ خطا در ذخیره‌سازی.")
        
        # بازسازی و نمایش منوی لیست سورس‌ها
        query.data = f"conn:select_copy:{copy_id}"
        await _handle_connections_menu(update, context)
        return

    # --- نمایش منوی تغییر حالت کپی ---
    if action == "conn" and parts[1] == "action" and parts[2] == "change_mode":
        source_id = parts[3]
        keyboard = [
            [InlineKeyboardButton("کپی همه نمادها (ALL)", callback_data=f"conn:set_mode:{source_id}:ALL")],
            [InlineKeyboardButton("فقط طلا (GOLD_ONLY)", callback_data=f"conn:set_mode:{source_id}:GOLD_ONLY")],
            [InlineKeyboardButton("نمادهای خاص (SYMBOLS)", callback_data=f"conn:set_mode:{source_id}:SYMBOLS")],
            [InlineKeyboardButton("🔙 بازگشت", callback_data=f"conn:manage_source:{source_id}")]
        ]
        await query.edit_message_text("حالت کپی را انتخاب کنید:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # --- منطق تنظیم حالت کپی ---
    if action == "conn" and parts[1] == "set_mode":
        source_id = parts[2]
        mode = parts[3]
        copy_id = context.user_data.get('selected_copy_id')
        conn = next((c for c in ecosystem.get('mapping', {}).get(copy_id, []) if c['source_id'] == source_id), None)

        if conn:
            conn['mode'] = mode
            if mode != 'SYMBOLS':
                conn.pop('allowed_symbols', None)
            
            if save_ecosystem(context) and await regenerate_copy_config(copy_id, context):
                await query.answer(f"✅ حالت به {mode} تغییر یافت.")
            else:
                await query.answer("❌ خطا در ذخیره‌سازی.")

        if mode == 'SYMBOLS':
            context.user_data['waiting_for'] = 'symbols'
            await query.edit_message_text("لطفاً لیست نمادهای مورد نظر را با **سمی‌کالن ( ; )** از هم جدا کرده و ارسال کنید. مثال: `EURUSD;GBPUSD`", parse_mode='Markdown')
            return

        # بازسازی و نمایش منوی مدیریت سورس
        query.data = f"conn:manage_source:{source_id}"
        await _handle_connections_menu(update, context)
        return



async def _handle_copy_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        await query.edit_message_text("یک حساب کپی را برای تنظیم انتخاب کنید:", reply_markup=InlineKeyboardMarkup(keyboard))
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


# --- این تابع کامل و بهبودیافته را کپی و جایگزین تابع قبلی کنید ---
async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handles all text inputs from the user, including numerical settings and symbol lists,
    with improved error handling and state management.
    """
    if not is_user_allowed(update.effective_user.id):
        return

    waiting_for = context.user_data.get('waiting_for')
    if not waiting_for:
        return

    text = update.message.text.strip()
    ecosystem = context.bot_data.get('ecosystem', {})
    should_return_to_main_menu = True

    try:
        # --- بخش مدیریت تنظیمات عددی ---
        if waiting_for.startswith("copy_") or waiting_for.startswith("source_"):
            try:
                value = float(text)
            except ValueError:
                await update.message.reply_text("❌ خطا: ورودی باید یک عدد باشد. مثال: 4.7 یا 0.1")
                return # منتظر ورودی صحیح بعدی می‌مانیم

            if waiting_for.startswith("copy_"):
                key = waiting_for.replace("copy_", "")
                copy_id = context.user_data.get('selected_copy_id')
                item = next((c for c in ecosystem.get('copies', []) if c['id'] == copy_id), None)
                if not item:
                    raise KeyError("حساب کپی انتخاب شده یافت نشد.")
                item['settings'][key] = round(value, 2)
                if save_ecosystem(context) and await regenerate_copy_settings_config(copy_id, context):
                    await update.message.reply_text(f"✅ تنظیمات `{key}` برای `{copy_id}` ذخیره شد.")
                else:
                    raise IOError("خطا در ذخیره سازی یا بازسازی فایل کانفیگ حساب کپی.")

            elif waiting_for.startswith("source_"):
                key = waiting_for.replace("source_", "")
                source_id = context.user_data.get('selected_source_id')
                item = next((s for s in ecosystem.get('sources', []) if s['id'] == source_id), None)
                if not item:
                    raise KeyError("سورس انتخاب شده یافت نشد.")
                item['volume_settings'] = {key: round(value, 2)}
                if save_ecosystem(context) and await regenerate_source_volume_configs(context):
                    await update.message.reply_text(f"✅ تنظیمات حجم برای `{source_id}` ذخیره شد.")
                else:
                    raise IOError("خطا در ذخیره سازی یا بازسازی فایل کانفیگ سورس.")

        # --- بخش مدیریت لیست نمادها ---
        elif waiting_for == "symbols":
            copy_id = context.user_data.get('selected_copy_id')
            source_id = context.user_data.get('selected_source_id')
            if not copy_id or not source_id:
                raise KeyError("اطلاعات حساب کپی یا سورس در حافظه موقت یافت نشد.")

            symbols = [sym.strip().upper() for sym in text.split(';') if sym.strip()]
            allowed_symbols_str = ';'.join(symbols)

            conn = next((c for c in ecosystem.get('mapping', {}).get(copy_id, []) if c['source_id'] == source_id), None)
            if not conn:
                raise KeyError(f"اتصال بین {copy_id} و {source_id} یافت نشد.")
            
            conn['allowed_symbols'] = allowed_symbols_str
            if save_ecosystem(context) and await regenerate_copy_config(copy_id, context):
                await update.message.reply_text(f"✅ لیست نمادها برای اتصال `{source_id}` به `{copy_id}` با موفقیت ذخیره شد.")
            else:
                raise IOError("خطا در ذخیره سازی یا بازسازی فایل کانفیگ اتصالات.")
        
        else:
            # اگر ربات منتظر ورودی ناشناخته‌ای بود
            logger.warning(f"Unknown 'waiting_for' state: {waiting_for}")
            should_return_to_main_menu = False


    except KeyError as e:
        await update.message.reply_text(f"❌ خطای منطقی: {e}. لطفاً دوباره از منوی اصلی شروع کنید.")
        logger.error(f"KeyError in handle_text_input: {e}", exc_info=True)
        should_return_to_main_menu = True
    except IOError as e:
        await update.message.reply_text(f"❌ خطای فایل: {e}. لطفاً لاگ‌های سرور را بررسی کنید.")
        logger.error(f"IOError in handle_text_input: {e}", exc_info=True)
        should_return_to_main_menu = True
    except Exception as e:
        await update.message.reply_text(f"❌ یک خطای پیش‌بینی نشده رخ داد: {e}")
        logger.error(f"Unhandled exception in handle_text_input: {e}", exc_info=True)
        should_return_to_main_menu = True

    finally:
        # در هر صورت، چه موفقیت‌آمیز چه ناموفق، وضعیت را پاک کرده و به منوی اصلی بازگرد
        if should_return_to_main_menu:
            context.user_data.clear()
            await start(update, context)

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