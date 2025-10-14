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
from logging.handlers import RotatingFileHandler  # For professional log rotation

# --- Professional Logging Setup (Smarter with Rotation) ---
# Rotate watcher.log: max 5MB, keep 5 backups
# Levels: DEBUG for traces, INFO for ops, WARNING for issues, ERROR for failures
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Console handler for INFO+
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# Rotating file handler for DEBUG+
file_handler = RotatingFileHandler('watcher.log', maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s [Task: %(task_name)s]'))  # Added task_name for async debug

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# --- Load Environment Variables ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")
LOG_DIRECTORY_PATH = os.getenv("LOG_DIRECTORY_PATH")
CHANNEL_ID = os.getenv("CHANNEL_ID")
ECOSYSTEM_PATH = os.getenv("ECOSYSTEM_PATH")

# State file path for stateless operation
WATCHER_STATE_PATH = os.path.join(os.path.dirname(ECOSYSTEM_PATH) if ECOSYSTEM_PATH else '.', 'watcher_state.json') if ECOSYSTEM_PATH else 'watcher_state.json'

source_name_map = {}
state_data = {}  # Global state dict: source_ticket -> source_name
state_changed = False  # Flag for batch save

def load_source_names():
    """Loads file_path -> name mapping from ecosystem.json."""
    global source_name_map
    if not ECOSYSTEM_PATH:
        logger.warning("ECOSYSTEM_PATH not set. Skipping source names load.")
        return
    try:
        with open(ECOSYSTEM_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        temp_map = {source['file_path']: source['name'] for source in data.get('sources', []) if 'file_path' in source and 'name' in source}
        source_name_map = temp_map
        logger.info(f"Loaded {len(source_name_map)} source names from ecosystem.json.")
    except FileNotFoundError:
        logger.warning(f"ecosystem.json not found at {ECOSYSTEM_PATH}. Using empty source map.")
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error in ecosystem.json: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Failed to load ecosystem.json: {e}", exc_info=True)

def load_watcher_state():
    """Loads watcher state from JSON file, with validation."""
    if not os.path.exists(WATCHER_STATE_PATH):
        logger.info(f"State file {WATCHER_STATE_PATH} not found. Starting with empty state.")
        return {}
    try:
        with open(WATCHER_STATE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in data.items()):
            raise ValueError("Invalid state format: must be dict[str, str].")
        logger.info(f"Loaded state with {len(data)} entries from {WATCHER_STATE_PATH}.")
        return data
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Corrupt state file {WATCHER_STATE_PATH}: {e}. Starting with empty state.", exc_info=True)
        return {}
    except Exception as e:
        logger.error(f"Failed to load state file: {e}", exc_info=True)
        return {}

def save_watcher_state(state):
    """Saves watcher state to JSON file atomically."""
    tmp_path = WATCHER_STATE_PATH + '.tmp'
    try:
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp_path, WATCHER_STATE_PATH)
        logger.debug(f"Saved state with {len(state)} entries to {WATCHER_STATE_PATH}.")
    except Exception as e:
        logger.error(f"Failed to save state file: {e}", exc_info=True)
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

# --- Telegram Alert Function (Unchanged but with better logging) ---
async def send_telegram_alert(bot: Bot, message: str):
    target_id = CHANNEL_ID if CHANNEL_ID else ADMIN_ID
    if not target_id:
        logger.warning("No target ID set for alert. Skipping.")
        return
    try:
        await bot.send_message(chat_id=target_id, text=message, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"Alert sent to {target_id}: {message[:50]}...")
    except TelegramError as e:
        logger.error(f"Failed to send to primary {target_id}: {e}")
        if target_id == CHANNEL_ID and ADMIN_ID:
            try:
                fallback_msg = f"⚠️ *Channel send failed.*\n\n{message}"
                await bot.send_message(chat_id=ADMIN_ID, text=fallback_msg, parse_mode=ParseMode.MARKDOWN)
                logger.info(f"Fallback alert sent to ADMIN_ID.")
            except TelegramError as e2:
                logger.error(f"Failed fallback to ADMIN_ID: {e2}")

# --- Robust Log Parsing (with Regex, State, and Backward Compat) ---
def parse_and_format_log_line(line: str, state: dict) -> str | None:
    line = line.strip()
    if not line:
        return None

    # Regex patterns for each log type (more robust than split)
    open_pattern = re.compile(r'\[TRADE_OPEN\] (.*)')
    close_pattern = re.compile(r'\[TRADE_CLOSE\] (.*)')
    alert_pattern = re.compile(r'\[DD_ALERT\] (.*)')
    stop_pattern = re.compile(r'\[DD_STOP\] (.*)')
    error_pattern = re.compile(r'\[ERROR\] - (.*)')

    try:
        if match := open_pattern.search(line):
            parts = match.group(1).split(',')
            if len(parts) != 6:
                raise ValueError(f"Invalid OPEN format: expected 6 parts, got {len(parts)}")
            copy_id, symbol, volume, price, source_ticket, source_file = [p.strip() for p in parts]
            source_name = source_name_map.get(source_file, source_file)
            # Update state
            state[source_ticket] = source_name
            global state_changed
            state_changed = True
            return (
                f"✅ *New Position Opened*\n\n"
                f"*Source:* `{source_name}`\n"
                f"*Copy Account:* `{copy_id}`\n"
                f"*Symbol:* `{symbol}`\n"
                f"*Volume:* `{volume}`\n"
                f"*Open Price:* `{price}`\n"
                f"*Source Ticket:* `{source_ticket}`"
            )

        elif match := close_pattern.search(line):
            parts = match.group(1).split(',')
            if len(parts) == 5:  # New format with source_file
                copy_id, symbol, source_ticket, profit_str, source_file = [p.strip() for p in parts]
                source_name = source_name_map.get(source_file, source_file)
            elif len(parts) == 4:  # Old format fallback
                copy_id, symbol, source_ticket, profit_str = [p.strip() for p in parts]
                source_name = state.get(source_ticket, 'Unknown Source')
            else:
                raise ValueError(f"Invalid CLOSE format: expected 4 or 5 parts, got {len(parts)}")
            profit = float(profit_str)
            profit_text = f"+${profit:,.2f}" if profit >= 0 else f"-${abs(profit):,.2f}"
            emoji = "☑️" if profit >= 0 else "🔻"
            # Remove from state
            if source_ticket in state:
                del state[source_ticket]
                global state_changed
                state_changed = True
            return (
                f"{emoji} *Position Closed*\n\n"
                f"*Source:* `{source_name}`\n"  # Now always available
                f"*Copy Account:* `{copy_id}`\n"
                f"*Symbol:* `{symbol}`\n"
                f"*Profit/Loss:* `{profit_text}`\n"
                f"*Source Ticket:* `{source_ticket}`"
            )

        elif match := alert_pattern.search(line):
            parts = match.group(1).split(',')
            if len(parts) != 5:
                raise ValueError(f"Invalid ALERT format: expected 5 parts, got {len(parts)}")
            copy_id, dd, dollar_loss, start_equity, peak_equity = [p.strip() for p in parts]
            return (
                f"🟡 *Daily Drawdown Alert*\n\n"
                f"*Account:* `{copy_id}`\n"
                f"*Current Loss:* `%{float(dd):.2f}` `(-${float(dollar_loss):,.2f})`\n"
                f"*Daily Start Equity:* `${float(start_equity):,.2f}`\n"
                f"*Daily Peak Equity:* `${float(peak_equity):,.2f}`"
            )

        elif match := stop_pattern.search(line):
            parts = match.group(1).split(',')
            if len(parts) != 6:
                raise ValueError(f"Invalid STOP format: expected 6 parts, got {len(parts)}")
            copy_id, dd, dd_limit, dollar_loss, start_equity, peak_equity = [p.strip() for p in parts]
            return (
                f"🔴 *Copy Stopped Due to DD Limit*\n\n"
                f"*Account:* `{copy_id}`\n"
                f"*Loss at Stop:* `%{float(dd):.2f}` `(-${float(dollar_loss):,.2f})`\n"
                f"*Stop Threshold:* `%{float(dd_limit):.2f}`"
            )

        elif match := error_pattern.search(line):
            error_message = match.group(1).strip()
            return f"🚨 *Expert Error*\n\n`{error_message}`"

    except (ValueError, IndexError) as e:
        logger.warning(f"Malformed log line skipped: '{line}'. Error: {e}")
        return f"⚠️ *Parse Error in Log*\n`{line}`"

    return None

# --- File Monitoring (with better error handling) ---
async def follow_log_file(bot: Bot, filepath: str, state: dict):
    logger.info(f"Starting watch on log: {filepath}", extra={'task_name': asyncio.current_task().get_name()})
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            f.seek(0, 2)  # Go to end
            while True:
                line = f.readline()
                if not line:
                    await asyncio.sleep(1)
                    continue
                formatted = parse_and_format_log_line(line, state)
                if formatted:
                    await send_telegram_alert(bot, formatted)
    except FileNotFoundError:
        logger.warning(f"Log file deleted/moved: {filepath}. Task stopping.")
    except asyncio.CancelledError:
        logger.info(f"Task cancelled for old log: {filepath}")
    except Exception as e:
        logger.error(f"Unexpected error watching {filepath}: {e}", exc_info=True)

# --- Batch State Saver (periodic) ---
async def batch_state_saver(state: dict):
    global state_changed
    while True:
        if state_changed:
            save_watcher_state(state)
            state_changed = False
            logger.debug("Batch saved state.")
        await asyncio.sleep(10)  # Every 10s check

# --- Health Check (periodic log/alive) ---
async def health_checker():
    while True:
        logger.info("Watcher health: Alive and monitoring.")
        # Optional: if no activity >1h, send alert (implement if needed)
        await asyncio.sleep(300)  # Every 5min

# --- Main Logic ---
async def main():
    if not all([BOT_TOKEN, ADMIN_ID, LOG_DIRECTORY_PATH]):
        logger.critical("Missing essential env vars. Exiting.")
        return

    bot = Bot(token=BOT_TOKEN)
    logger.info("Log Watcher v3.2 started.")

    global state_data
    state_data = load_watcher_state()

    # Periodic tasks
    asyncio.create_task(batch_state_saver(state_data))
    asyncio.create_task(health_checker())

    watched_slaves = {}

    while True:
        try:
            load_source_names()  # Hot-reload every loop (60s)

            log_pattern = os.path.join(LOG_DIRECTORY_PATH, "TradeCopier_*.log")
            all_files = glob(log_pattern)

            slaves_logs = {}
            for f in all_files:
                match = re.search(r"TradeCopier_(.*?)_\d{4}\.\d{2}\.\d{2}\.log", os.path.basename(f))
                if match:
                    slave_id = match.group(1)
                    slaves_logs.setdefault(slave_id, []).append(f)

            for slave_id, files in slaves_logs.items():
                latest_file = max(files, key=os.path.getctime)
                if slave_id not in watched_slaves or watched_slaves[slave_id]['filepath'] != latest_file:
                    if slave_id in watched_slaves:
                        logger.info(f"Switching log for '{slave_id}': {os.path.basename(watched_slaves[slave_id]['filepath'])} -> {os.path.basename(latest_file)}")
                        watched_slaves[slave_id]['task'].cancel()
                    task = asyncio.create_task(follow_log_file(bot, latest_file, state_data))
                    task.set_name(f"watcher_{slave_id}")  # For log extra
                    watched_slaves[slave_id] = {'filepath': latest_file, 'task': task}

            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"Main loop error: {e}", exc_info=True)
            await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Watcher stopped by user.")
    except Exception as e:
        logger.critical(f"Fatal error starting watcher: {e}", exc_info=True)