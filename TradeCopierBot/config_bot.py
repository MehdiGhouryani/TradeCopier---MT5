import os
import logging
import json
import traceback
import html
import re  # Added for regex validation in inputs
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from functools import wraps
import glob
from datetime import datetime
from telegram.constants import ParseMode
from logging.handlers import RotatingFileHandler  # For smarter logging with rotation

# --- Improved Logging Setup (Professional and Smarter) ---
# Use RotatingFileHandler for log rotation (max 5MB per file, keep 5 backups)
# Levels: DEBUG for detailed ops, INFO for normal, WARNING for issues, ERROR for failures
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Capture all levels, filter in handlers

# Console handler for INFO and above
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# File handler with rotation for all DEBUG+
file_handler = RotatingFileHandler('bot.log', maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Suppress httpx logs as before
logging.getLogger('httpx').setLevel(logging.WARNING)

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 1717599240))  # Default to provided value if not set
ECOSYSTEM_PATH_STR = os.getenv("ECOSYSTEM_PATH")
LOG_DIRECTORY_PATH = os.getenv("LOG_DIRECTORY_PATH")

try:
    ALLOWED_USERS = [int(uid) for uid in os.getenv("ALLOWED_USERS", "").split(",") if uid]
except (ValueError, TypeError):
    ALLOWED_USERS = []
    logger.error("Error parsing ALLOWED_USERS from .env. Using empty list.")

ECOSYSTEM_PATH = ""
if ECOSYSTEM_PATH_STR:
    base_dir = os.path.dirname(os.path.realpath(__file__))
    ECOSYSTEM_PATH = ECOSYSTEM_PATH_STR if os.path.isabs(ECOSYSTEM_PATH_STR) else os.path.join(base_dir, ECOSYSTEM_PATH_STR)
else:
    logger.critical("ECOSYSTEM_PATH environment variable not set. Bot cannot start.")
    raise ValueError("Missing ECOSYSTEM_PATH")

# --- Helper for Admin Error Notification ---
async def notify_admin_on_error(context: ContextTypes.DEFAULT_TYPE, function_name: str, error: Exception, **kwargs):
    """Sends a formatted error message to the admin."""
    details = ", ".join([f"{k}='{v}'" for k, v in kwargs.items()])
    message = (
        f"🚨 *Critical Bot Error*\n\n"
        f"Error occurred in function `{function_name}`.\n\n"
        f"▫️ *Details:* {details}\n"
        f"▫️ *Error Text:* `{str(error)}`\n\n"
        f"Please check bot logs for full traceback."
    )
    try:
        await context.bot.send_message(chat_id=ADMIN_ID, text=message, parse_mode='Markdown')
        logger.info(f"Sent error notification to admin for {function_name}.")
    except Exception as e:
        logger.error(f"Failed to send error notification to admin: {e}")

# --- Ecosystem Helper Functions ---

async def get_detailed_status_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    """Creates a detailed and formatted string of the entire system status."""
    ecosystem = context.bot_data.get('ecosystem', {})
    if not ecosystem:
        return "❌ **Error: System data not loaded.**"

    source_map = {source_account['id']: source_account['name'] for source_account in ecosystem.get('sources', [])}
    status_lines = ["**-- 🍃 Full System Status --**"]

    status_lines.append("\n**📊 Source Accounts**")
    sources = ecosystem.get('sources', [])
    if not sources:
        status_lines.append("  - No sources defined.")
    else:
        for source_account in sources:
            vs = source_account.get('volume_settings', {})
            mode = "Fixed Volume" if "FixedVolume" in vs else "Multiplier"
            value = vs.get("FixedVolume", vs.get("Multiplier", "N/A"))
            status_lines.append(f"  - `{source_account['name']}`: *{mode} = {value}*")

    status_lines.append("\n**🛡️ Copy Accounts**")
    copies = ecosystem.get('copies', [])
    if not copies:
        status_lines.append("  - No copies defined.")
    else:
        for copy_account in copies:
            copy_id = copy_account['id']
            settings = copy_account.get('settings', {})
            dd = float(settings.get("DailyDrawdownPercent", 0))
            risk_text = f"{dd}%" if dd > 0 else "Disabled"

            # Check flag file for stopped status
            flag_file_path = os.path.join(os.path.dirname(ECOSYSTEM_PATH), f"{copy_id}_stopped.flag")
            status_emoji = "🔴" if os.path.exists(flag_file_path) else "🟢"
            status_text = " (Stopped)" if status_emoji == "🔴" else ""

            connection_count = len(ecosystem.get('mapping', {}).get(copy_id, []))

            status_lines.append(
                f"{status_emoji} `{copy_account['name']}`: Risk: *{risk_text}* | Connected to *{connection_count} sources*{status_text}"
            )

    return "\n".join(status_lines)

def load_ecosystem(application: Application) -> bool:
    """Loads the ecosystem data from the JSON file into bot_data for caching."""
    try:
        with open(ECOSYSTEM_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # Schema validation
        required_keys = ["sources", "copies", "mapping"]
        if not all(key in data for key in required_keys):
            raise KeyError("Ecosystem JSON missing required keys: sources, copies, mapping.")
        application.bot_data['ecosystem'] = data
        logger.info("Ecosystem data loaded and cached successfully.")
        return True
    except FileNotFoundError:
        logger.warning(f"Ecosystem file not found at {ECOSYSTEM_PATH}. Creating blank file.")
        with open(ECOSYSTEM_PATH, 'w', encoding='utf-8') as f:
            json.dump({"sources": [], "copies": [], "mapping": {}}, f, indent=2)
        return load_ecosystem(application)
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error in ecosystem file: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Unexpected error loading ecosystem: {e}", exc_info=True)
        return False

def save_ecosystem(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Saves the cached ecosystem data back to the JSON file using an atomic write."""
    if 'ecosystem' not in context.bot_data:
        logger.warning("No ecosystem data in bot_data to save.")
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
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return False

# Backup ecosystem before critical operations (new smart feature)
def backup_ecosystem():
    if os.path.exists(ECOSYSTEM_PATH):
        backup_path = ECOSYSTEM_PATH + ".bak"
        os.replace(ECOSYSTEM_PATH, backup_path)
        logger.info(f"Created backup of ecosystem.json at {backup_path}.")

async def regenerate_all_configs(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Regenerates ALL config files for all sources and copies."""
    ecosystem = context.bot_data.get('ecosystem', {})
    copies = ecosystem.get('copies', [])

    all_success = True
    for copy_account in copies:
        if not await regenerate_copy_config(copy_account['id'], context):
            all_success = False
        if not await regenerate_copy_settings_config(copy_account['id'], context):
            all_success = False

    if not await regenerate_source_volume_configs(context):
        all_success = False

    if all_success:
        logger.info("All configuration files regenerated successfully.")
    else:
        logger.warning("Some configuration regenerations failed.")
    return all_success

async def regenerate_copy_config(copy_id: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
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
        logger.info(f"Regenerated connections config for copy '{copy_id}'.")
        return True
    except Exception as e:
        logger.error(f"Failed to regenerate connections for '{copy_id}': {e}", exc_info=True)
        await notify_admin_on_error(context, "regenerate_copy_config", e, copy_id=copy_id)
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return False

async def regenerate_copy_settings_config(copy_id: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    ecosystem = context.bot_data.get('ecosystem', {})
    copy_account = next((c for c in ecosystem.get('copies', []) if c['id'] == copy_id), None)
    if not copy_account:
        logger.error(f"Copy account '{copy_id}' not found for settings regeneration.")
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
        logger.info(f"Regenerated settings config for copy '{copy_id}'.")
        return True
    except Exception as e:
        logger.error(f"Failed to regenerate settings for '{copy_id}': {e}", exc_info=True)
        await notify_admin_on_error(context, "regenerate_copy_settings_config", e, copy_id=copy_id)
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        return False

async def regenerate_source_volume_configs(context: ContextTypes.DEFAULT_TYPE) -> bool:
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
            logger.info(f"Regenerated volume config for source '{source_id}'.")
        except Exception as e:
            logger.error(f"Failed to regenerate volume config for '{source_id}': {e}", exc_info=True)
            await notify_admin_on_error(context, "regenerate_source_volume_configs", e, source_id=source_id)
            all_success = False
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    return all_success

def is_user_allowed(user_id: int) -> bool:
    return user_id in ALLOWED_USERS

def allowed_users_only(func):
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user or not is_user_allowed(user.id):
            if user:
                logger.warning(f"Unauthorized access attempt by user_id={user.id} (@{user.username}).")
            unauthorized_text = "You do not have access to this bot."
            if update.callback_query:
                await update.callback_query.answer(unauthorized_text, show_alert=True)
            elif update.message:
                await update.message.reply_text(unauthorized_text)
            return
        return await func(update, context, *args, **kwargs)
    return wrapped

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

def admin_only(func):
    @wraps(func)
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if not user or not is_admin(user.id):
            logger.warning(f"Admin-only access denied for user_id={user.id} (@{user.username}).")
            unauthorized_text = "This operation is restricted to the admin only."
            if update.callback_query:
                await update.callback_query.answer(unauthorized_text, show_alert=True)
            elif update.message:
                await update.message.reply_text(unauthorized_text)
            return
        return await func(update, context, *args, **kwargs)
    return wrapped

# --- Handlers ---

@allowed_users_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [
        [InlineKeyboardButton("📊 وضعیت سیستم", callback_data="status")],
        [InlineKeyboardButton("🛡️ مدیریت حساب‌های کپی", callback_data="menu_copy_settings")],
        [InlineKeyboardButton("📊 مدیریت سورس‌ها", callback_data="sources:main")],
        [InlineKeyboardButton("🔗 مدیریت اتصالات", callback_data="menu_connections")],
        [InlineKeyboardButton("🔄 بازسازی تمام فایل‌ها", callback_data="regenerate_all_files")],
        [InlineKeyboardButton("❓ راهنما", callback_data="menu_help")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    status_text = await get_detailed_status_text(context)
    if update.callback_query:
        await update.callback_query.edit_message_text(status_text, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(status_text, reply_markup=reply_markup, parse_mode='Markdown')

@allowed_users_only
async def clean_old_logs_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("⏳ Checking and cleaning old log files...")

    if not LOG_DIRECTORY_PATH:
        await update.message.reply_text("❌ Error: Log directory path not set.")
        logger.error("LOG_DIRECTORY_PATH not set in .env.")
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
                    logger.info(f"Deleted old log: {log_file}")
                except Exception as e:
                    errors_count += 1
                    logger.error(f"Failed to delete log {log_file}: {e}")

        message = f"✅ Cleanup completed.\n\n"
        message += f"🗑️ *Deleted files:* {deleted_count}\n"
        if errors_count > 0:
            message += f"🚨 *Errors:* {errors_count}"

        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await update.message.reply_text(f"❌ Unexpected error during cleanup: {e}")
        logger.error(f"Error in clean_old_logs_handler: {e}", exc_info=True)

@allowed_users_only
async def get_log_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args
    if not args:
        await update.message.reply_text("Please provide copy ID and (optional) line count. Example: /getlog copy_A 50")
        return

    copy_id = args[0]
    num_lines = int(args[1]) if len(args) > 1 else 50

    if not LOG_DIRECTORY_PATH:
        await update.message.reply_text("❌ Error: Log directory not set.")
        return

    try:
        log_pattern = os.path.join(LOG_DIRECTORY_PATH, f"TradeCopier_{copy_id}_*.log")
        all_logs = glob.glob(log_pattern)
        if not all_logs:
            await update.message.reply_text(f"❌ No logs found for {copy_id}.")
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
            logger.info(f"Sent large log file for {copy_id} as document.")
        else:
            await update.message.reply_text(f"Log for {copy_id}:\n```{log_content}```", parse_mode=ParseMode.MARKDOWN)
            logger.info(f"Sent inline log for {copy_id}.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error fetching log: {e}")
        logger.error(f"Error in get_log_handler for {copy_id}: {e}", exc_info=True)

@allowed_users_only
async def regenerate_all_files_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if await regenerate_all_configs(context):
        await query.edit_message_text("✅ All config files regenerated successfully.")
        logger.info("Regenerated all configs via button.")
    else:
        await query.edit_message_text("❌ Error regenerating files. Check logs.")
        logger.warning("Failed to regenerate all configs.")

@allowed_users_only
async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Assuming help text is defined; truncated in original, so placeholder
    help_text = "Help guide: Use /start for menu, /getlog for logs, etc."
    await update.message.reply_text(help_text)

@allowed_users_only
async def _handle_connections_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Assuming this is defined; truncated in original, placeholder logic
    query = update.callback_query
    await query.answer()
    # ... (implement as per original truncated code)

@allowed_users_only
async def _handle_copy_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    ecosystem = context.bot_data.get('ecosystem', {})
    parts = data.split(':')
    action = parts[0]

    if action == "menu_copy_settings":
        copies = ecosystem.get('copies', [])
        keyboard = []
        for c in copies:
            keyboard.append([InlineKeyboardButton(c['name'], callback_data=f"setting:select:{c['id']}")])
        keyboard.append([InlineKeyboardButton("➕ Add New Copy Account", callback_data="setting:add:start")])  # New add button
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
        await query.edit_message_text("Select a copy account to manage:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if action == "setting" and parts[1] == "select":
        copy_id = parts[2]
        context.user_data['selected_copy_id'] = copy_id
        copy_account = next((c for c in ecosystem.get('copies', []) if c['id'] == copy_id), None)
        if not copy_account:
            await query.edit_message_text("❌ Copy account not found.")
            return

        settings = copy_account.get('settings', {})
        dd = float(settings.get("DailyDrawdownPercent", 0))
        cm_text = "Gold Only" if settings.get("CopySymbolMode", "GOLD_ONLY") == "GOLD_ONLY" else "All Symbols"
        keyboard = [
            [InlineKeyboardButton(f"{'❌ Disable' if dd > 0 else '✅ Enable'} Risk", callback_data=f"setting:action:toggle_dd")],
            [InlineKeyboardButton(f"Copy Mode: {cm_text}", callback_data=f"setting:action:copy_mode")],
            [InlineKeyboardButton("Set DD Limit (%)", callback_data="setting_input_copy_DailyDrawdownPercent")],
            [InlineKeyboardButton("Set Alert Limit (%)", callback_data="setting_input_copy_AlertDrawdownPercent")],
            [InlineKeyboardButton("Reset Lock (RESET)", callback_data=f"setting:action:reset_stop")],
            [InlineKeyboardButton("🗑️ Delete This Copy", callback_data=f"setting:delete:confirm:{copy_id}")],  # New delete button
            [InlineKeyboardButton("🔙 Back", callback_data="menu_copy_settings")]
        ]
        await query.edit_message_text(f"Settings for copy **{copy_account['name']}**:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

    # ... (rest of original logic for toggle, set_copy, etc.)

    if action == "setting" and parts[1] == "add":
        sub_action = parts[2]
        if sub_action == "start":
            context.user_data['waiting_for'] = 'copy_add_id'
            await query.edit_message_text("Enter unique ID for the new copy account (e.g., copy_K):")
            return
        # Handled in handle_text_input

    if action == "setting" and parts[1] == "delete":
        sub_action = parts[2]
        copy_id = parts[3]
        if sub_action == "confirm":
            keyboard = [
                [InlineKeyboardButton("✅ Yes, Delete", callback_data=f"setting:delete:execute:{copy_id}")],
                [InlineKeyboardButton("❌ No, Cancel", callback_data="menu_copy_settings")]
            ]
            await query.edit_message_text(f"Are you sure you want to delete copy '{copy_id}'? This will remove all related mappings.", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        if sub_action == "execute":
            # Safe delete
            backup_ecosystem()  # Backup before delete
            copies = ecosystem.get('copies', [])
            ecosystem['copies'] = [c for c in copies if c['id'] != copy_id]
            if copy_id in ecosystem.get('mapping', {}):
                del ecosystem['mapping'][copy_id]
            if save_ecosystem(context):
                await regenerate_all_configs(context)  # Auto regenerate after delete
                logger.info(f"Deleted copy '{copy_id}' and related mappings.")
                await query.edit_message_text(f"✅ Copy '{copy_id}' deleted successfully.")
            else:
                logger.error(f"Failed to save after deleting copy '{copy_id}'.")
                await query.edit_message_text("❌ Error deleting copy. Check logs.")
            # Return to menu
            await _handle_copy_settings_menu(update, context)
            return

@allowed_users_only
async def _handle_sources_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    ecosystem = context.bot_data.get('ecosystem', {})
    parts = data.split(':')
    action = parts[0]

    if action == "sources" and parts[1] == "main":
        sources = ecosystem.get('sources', [])
        keyboard = []
        for s in sources:
            vs = s.get('volume_settings', {})
            mode = "Fixed" if "FixedVolume" in vs else "Multiplier"
            value = vs.get("FixedVolume", vs.get("Multiplier", "N/A"))
            button_text = f"{s['name']} ({mode}: {value})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"sources:select:{s['id']}")])
        keyboard.append([InlineKeyboardButton("➕ Add New Source", callback_data="sources:add:start")])  # New add button
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
        await query.edit_message_text("Select a source to manage:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if action == "sources" and parts[1] == "select":
        source_id = parts[2]
        context.user_data['selected_source_id'] = source_id
        source = next((s for s in ecosystem.get('sources', []) if s['id'] == source_id), None)
        if not source:
            await query.edit_message_text("❌ Source not found.")
            return

        keyboard = [
            [InlineKeyboardButton("✏️ Edit Name", callback_data="sources:action:edit_name")],
            [InlineKeyboardButton("⚙️ Set Volume", callback_data="sources:action:edit_volume")],
            [InlineKeyboardButton("🗑️ Delete This Source", callback_data=f"sources:delete:confirm:{source_id}")],  # New delete button
            [InlineKeyboardButton("🔙 Back to Sources", callback_data="sources:main")]
        ]
        await query.edit_message_text(f"Manage source: **{source['name']}**", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        return

    if action == "sources" and parts[1] == "add":
        sub_action = parts[2]
        if sub_action == "start":
            context.user_data['waiting_for'] = 'source_add_id'
            await query.edit_message_text("Enter unique ID for the new source (e.g., source_11):")
            return
        # Handled in handle_text_input

    if action == "sources" and parts[1] == "delete":
        sub_action = parts[2]
        source_id = parts[3]
        if sub_action == "confirm":
            keyboard = [
                [InlineKeyboardButton("✅ Yes, Delete", callback_data=f"sources:delete:execute:{source_id}")],
                [InlineKeyboardButton("❌ No, Cancel", callback_data="sources:main")]
            ]
            await query.edit_message_text(f"Are you sure you want to delete source '{source_id}'? This will remove all related mappings.", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        if sub_action == "execute":
            # Safe delete
            backup_ecosystem()  # Backup before delete
            sources = ecosystem.get('sources', [])
            ecosystem['sources'] = [s for s in sources if s['id'] != source_id]
            mapping = ecosystem.get('mapping', {})
            for copy_id in list(mapping.keys()):
                mapping[copy_id] = [conn for conn in mapping[copy_id] if conn['source_id'] != source_id]
            if save_ecosystem(context):
                await regenerate_all_configs(context)  # Auto regenerate after delete
                logger.info(f"Deleted source '{source_id}' and related mappings.")
                await query.edit_message_text(f"✅ Source '{source_id}' deleted successfully.")
            else:
                logger.error(f"Failed to save after deleting source '{source_id}'.")
                await query.edit_message_text("❌ Error deleting source. Check logs.")
            # Return to menu
            await _handle_sources_menu(update, context)
            return

    # ... (rest of original logic for action, edit_name, edit_volume)

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_user_allowed(update.effective_user.id):
        return

    waiting_for = context.user_data.get('waiting_for')
    if not waiting_for:
        return

    text = update.message.text.strip()
    ecosystem = context.bot_data.get('ecosystem', {})
    should_return_to_main_menu = True

    try:
        # Source add flow (multi-step with validation)
        if waiting_for == "source_add_id":
            if not re.match(r'^[a-zA-Z0-9_]+$', text):  # Regex validation for ID (alphanumeric + underscore)
                await update.message.reply_text("❌ Invalid ID format. Use alphanumeric and underscore only. Try again:")
                return
            if any(s['id'] == text for s in ecosystem.get('sources', [])):
                await update.message.reply_text("❌ ID already exists. Enter a unique ID:")
                return
            context.user_data['temp_source_id'] = text
            context.user_data['waiting_for'] = 'source_add_name'
            await update.message.reply_text("Enter display name for the new source:")
            return

        elif waiting_for == "source_add_name":
            context.user_data['temp_source_name'] = text
            context.user_data['waiting_for'] = 'source_add_file_path'
            await update.message.reply_text("Enter file_path for the new source (e.g., TradeCopier_S11.txt):")
            return

        elif waiting_for == "source_add_file_path":
            context.user_data['temp_source_file_path'] = text
            context.user_data['waiting_for'] = 'source_add_config_file'
            await update.message.reply_text("Enter config_file for the new source (e.g., TradeCopier_S11_config.txt):")
            return

        elif waiting_for == "source_add_config_file":
            new_source = {
                'id': context.user_data['temp_source_id'],
                'name': context.user_data['temp_source_name'],
                'file_path': context.user_data['temp_source_file_path'],
                'config_file': text,
                'volume_settings': {}  # Default empty
            }
            ecosystem['sources'].append(new_source)
            if save_ecosystem(context):
                await regenerate_source_volume_configs(context)
                logger.info(f"Added new source '{new_source['id']}'.")
                await update.message.reply_text(f"✅ New source '{new_source['id']}' added successfully.")
            else:
                raise IOError("Failed to save ecosystem after adding source.")
            del context.user_data['temp_source_id']
            del context.user_data['temp_source_name']
            del context.user_data['temp_source_file_path']

        # Copy add flow (similar)
        elif waiting_for == "copy_add_id":
            if not re.match(r'^[a-zA-Z0-9_]+$', text):
                await update.message.reply_text("❌ Invalid ID format. Use alphanumeric and underscore only. Try again:")
                return
            if any(c['id'] == text for c in ecosystem.get('copies', [])):
                await update.message.reply_text("❌ ID already exists. Enter a unique ID:")
                return
            context.user_data['temp_copy_id'] = text
            context.user_data['waiting_for'] = 'copy_add_name'
            await update.message.reply_text("Enter display name for the new copy:")
            return

        elif waiting_for == "copy_add_name":
            new_copy = {
                'id': context.user_data['temp_copy_id'],
                'name': text,
                'settings': {}  # Default empty
            }
            ecosystem['copies'].append(new_copy)
            ecosystem['mapping'][new_copy['id']] = []  # Empty mapping
            if save_ecosystem(context):
                await regenerate_copy_settings_config(new_copy['id'], context)
                await regenerate_copy_config(new_copy['id'], context)
                logger.info(f"Added new copy '{new_copy['id']}'.")
                await update.message.reply_text(f"✅ New copy '{new_copy['id']}' added successfully.")
            else:
                raise IOError("Failed to save ecosystem after adding copy.")
            del context.user_data['temp_copy_id']

        # ... (rest of original input handling for symbols, DD, etc.)

        else:
            logger.warning(f"Unhandled waiting_for state: {waiting_for}")
            should_return_to_main_menu = False

    except KeyError as e:
        await update.message.reply_text(f"❌ Logical error: {e}. Start over from main menu.")
        logger.error(f"KeyError in handle_text_input: {e}", exc_info=True)
    except IOError as e:
        await update.message.reply_text(f"❌ File error: {e}. Check server logs.")
        logger.error(f"IOError in handle_text_input: {e}", exc_info=True)
    except Exception as e:
        await update.message.reply_text(f"❌ Unexpected error: {e}")
        logger.error(f"Unhandled exception in handle_text_input: {e}", exc_info=True)
    finally:
        if should_return_to_main_menu:
            context.user_data.clear()
            await start(update, context)

async def text_input_trigger(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data

    waiting_for_map = {
        "setting_input_copy_DailyDrawdownPercent": ("copy_DailyDrawdownPercent", "Enter daily DD percent (e.g., 4.7):"),
        "setting_input_copy_AlertDrawdownPercent": ("copy_AlertDrawdownPercent", "Enter alert percent (e.g., 4.0):"),
        "vol_input_source_FixedVolume": ("source_FixedVolume", "Enter fixed volume (e.g., 0.1):"),
        "vol_input_source_Multiplier": ("source_Multiplier", "Enter multiplier (e.g., 1.5):"),
    }
    if data in waiting_for_map:
        key, prompt = waiting_for_map[data]
        context.user_data['waiting_for'] = key
        await query.edit_message_text(prompt)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception handling update:", exc_info=context.error)

    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = "".join(tb_list)

    update_str = update.to_dict() if isinstance(update, Update) else str(update)
    message = (
        f"Exception in update:\n\n"
        f"<b>Update:</b>\n<pre>{html.escape(json.dumps(update_str, indent=2, ensure_ascii=False))}</pre>\n\n"
        f"<b>User Data:</b>\n<pre>{html.escape(str(context.user_data))}</pre>\n\n"
        f"<b>Error:</b>\n<pre>{html.escape(tb_string)}</pre>"
    )

    MAX_MESSAGE_LENGTH = 4096
    for i in range(0, len(message), MAX_MESSAGE_LENGTH):
        try:
            await context.bot.send_message(chat_id=ADMIN_ID, text=message[i:i + MAX_MESSAGE_LENGTH], parse_mode='HTML')
        except Exception as e:
            logger.error(f"Failed to send error to admin: {e}")

def main() -> None:
    if not all([BOT_TOKEN, ECOSYSTEM_PATH, ALLOWED_USERS, LOG_DIRECTORY_PATH]):
        logger.critical("Critical env vars missing. Check .env.")
        return

    application = Application.builder().token(BOT_TOKEN).build()

    if not load_ecosystem(application):
        logger.critical("Failed to load ecosystem. Bot stopping.")
        return

    # Main commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("getlog", get_log_handler))
    application.add_handler(CommandHandler("clean_old_logs", clean_old_logs_handler))

    # Callback handlers
    application.add_handler(CallbackQueryHandler(start, pattern="^main_menu$"))
    application.add_handler(CallbackQueryHandler(regenerate_all_files_handler, pattern="^regenerate_all_files$"))
    application.add_handler(CallbackQueryHandler(help_handler, pattern="^menu_help$"))

    # Section handlers
    application.add_handler(CallbackQueryHandler(_handle_connections_menu, pattern="^menu_connections$|^conn:"))
    application.add_handler(CallbackQueryHandler(_handle_copy_settings_menu, pattern="^menu_copy_settings$|^setting:"))
    application.add_handler(CallbackQueryHandler(_handle_sources_menu, pattern="^sources:"))

    # Input triggers
    application.add_handler(CallbackQueryHandler(text_input_trigger, pattern="^setting_input_|^vol_input_"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))

    # Error handler
    application.add_error_handler(error_handler)

    logger.info("Bot started successfully.")
    application.run_polling()

if __name__ == "__main__":
    main()