import os
import sys
import json
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, time
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz

# --- Configuration Constants ---
XML_SOURCE_URL = "[https://raw.githubusercontent.com/globetvapp/epg/refs/heads/main/Brazil/brazil1.xml](https://raw.githubusercontent.com/globetvapp/epg/refs/heads/main/Brazil/brazil1.xml)"
CHANNEL_LIST_FILE = "__channel.txt"
OUTPUT_DIR_TODAY = os.path.join("schedule", "today")
OUTPUT_DIR_TOMORROW = os.path.join("schedule", "tomorrow")
TARGET_TIMEZONE = pytz.timezone('America/Sao_Paulo')
MAX_WORKERS = 10  # Optimal for file I/O operations

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def setup_environment():
    """
    Ensures that the necessary output directories exist.
    """
    for directory in:
        os.makedirs(directory, exist_ok=True)
    logger.info("Environment setup complete. Output directories ready.")

def load_target_channels():
    """
    Reads the whitelist of channel IDs from __channel.txt.
    Returns a set of strings for O(1) lookup.
    """
    if not os.path.exists(CHANNEL_LIST_FILE):
        logger.error(f"Channel list file '{CHANNEL_LIST_FILE}' not found.")
        return set()
    
    with open(CHANNEL_LIST_FILE, 'r', encoding='utf-8') as f:
        # Strip whitespace and ignore empty lines
        channels = {line.strip() for line in f if line.strip()}
    
    logger.info(f"Loaded {len(channels)} target channels.")
    return channels

def parse_xmltv_date(date_str):
    """
    Parses an XMLTV date string (YYYYMMDDhhmmss +/-hhmm) into a 
    timezone-aware datetime object localized to Sao Paulo.
    """
    if not date_str:
        return None
    try:
        # The %z directive handles the offset (+0000 or -0300)
        # XMLTV dates are typically "20251218013000 +0000"
        dt_aware = datetime.strptime(date_str.strip(), '%Y%m%d%H%M%S %z')
        # Convert to target timezone (Sao Paulo)
        return dt_aware.astimezone(TARGET_TIMEZONE)
    except ValueError:
        try:
             # Fallback for formats without space before timezone
            dt_aware = datetime.strptime(date_str.strip(), '%Y%m%d%H%M%S%z')
            return dt_aware.astimezone(TARGET_TIMEZONE)
        except ValueError as e:
            logger.warning(f"Date parsing failed for string '{date_str}': {e}")
            return None

def format_ampm(dt_obj):
    """Formats a datetime object to 'HH:MM AM/PM' string."""
    return dt_obj.strftime('%I:%M %p')

def get_node_text(element, tag_name):
    """Safely extracts text from a child XML node."""
    node = element.find(tag_name)
    return node.text if node is not None else ""

def process_channel(channel_id, programmes, target_dates):
    """
    The core logic unit. Processes all programmes for a single channel.
    Splits shows across midnight and sorts them into 'today' or 'tomorrow'.
    
    Args:
        channel_id (str): The unique ID of the channel.
        programmes (list): List of XML 'programme' elements.
        target_dates (dict): specific datetime boundaries for today/tomorrow.
    """
    
    # Initialize containers
    schedules = {
        'today':,
        'tomorrow':
    }
    
    today_date_str = target_dates['today_date'].strftime('%Y-%m-%d')
    tomorrow_date_str = target_dates['tomorrow_date'].strftime('%Y-%m-%d')

    # Boundaries
    today_start = target_dates['today_start']
    today_end = target_dates['today_end']      # 23:59:59 today
    tomorrow_start = target_dates['tomorrow_start'] # 00:00:00 tomorrow
    tomorrow_end = target_dates['tomorrow_end']

    for prog in programmes:
        start_str = prog.get('start')
        stop_str = prog.get('stop')
        
        start_dt = parse_xmltv_date(start_str)
        end_dt = parse_xmltv_date(stop_str)
        
        if not start_dt or not end_dt:
            continue

        # Base show object
        # show_logo handles the "blank if not available" requirement
        icon_node = prog.find('icon')
        show_logo = icon_node.get('src') if icon_node is not None else ""
        
        show_info = {
            "show_name": get_node_text(prog, 'title'),
            "show_logo": show_logo,
            "episode_description": get_node_text(prog, 'desc'),
            # Times to be filled dynamically
        }

        # --- LOGIC FOR TODAY ---
        # Check if the show overlaps with Today's 24h window
        if start_dt <= today_end and end_dt >= today_start:
            # Determine effective start/end for the 'Today' file
            # If show started yesterday, clip start to today_start (12:00 AM)
            eff_start = max(start_dt, today_start)
            # If show ends tomorrow, clip end to today_end (11:59 PM)
            eff_end = min(end_dt, today_end)
            
            # Create entry
            entry = show_info.copy()
            entry['start_time'] = format_ampm(eff_start)
            entry['end_time'] = format_ampm(eff_end)
            schedules['today'].append(entry)

        # --- LOGIC FOR TOMORROW ---
        # Check if the show overlaps with Tomorrow's 24h window
        if start_dt <= tomorrow_end and end_dt >= tomorrow_start:
            # Determine effective start/end for the 'Tomorrow' file
            # If show started today (and crosses midnight), clip start to tomorrow_start (12:00 AM)
            eff_start = max(start_dt, tomorrow_start)
            # If show ends day after tomorrow, clip end to tomorrow_end
            eff_end = min(end_dt, tomorrow_end)
            
            # Create entry
            entry = show_info.copy()
            entry['start_time'] = format_ampm(eff_start)
            entry['end_time'] = format_ampm(eff_end)
            schedules['tomorrow'].append(entry)

    # Write JSON files if data exists
    # Case insensitive filename requirement: channel_id is typically used as is, 
    # but prompt requested e.g. Record-TV.json. Using channel_id directly as requested in logic.
    safe_filename = channel_id.replace('/', '_') + ".json"

    if schedules['today']:
        file_path = os.path.join(OUTPUT_DIR_TODAY, safe_filename)
        output_data = {
            "channel": channel_id,
            "date": today_date_str,
            "schedule": schedules['today']
        }
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

    if schedules['tomorrow']:
        file_path = os.path.join(OUTPUT_DIR_TOMORROW, safe_filename)
        output_data = {
            "channel": channel_id,
            "date": tomorrow_date_str,
            "schedule": schedules['tomorrow']
        }
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)

def main():
    setup_environment()
    
    # 1. Fetch Data
    logger.info("Downloading EPG XML...")
    try:
        resp = requests.get(XML_SOURCE_URL)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.critical(f"Failed to download XML: {e}")
        sys.exit(1)

    # 2. Parse XML
    logger.info("Parsing XML Tree...")
    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        logger.critical(f"XML Parsing failed: {e}")
        sys.exit(1)

    # 3. Filter and Group
    target_channels = load_target_channels()
    if not target_channels:
        logger.warning("No channels defined in __channel.txt. Exiting.")
        return

    # Group programmes by channel ID
    channel_map = {cid: for cid in target_channels}
    
    # Efficiently gather programmes only for target channels
    for prog in root.findall('programme'):
        cid = prog.get('channel')
        # The XML channel ID might match exactly
        if cid in channel_map:
            channel_map[cid].append(prog)

    # 4. Define Time Boundaries (Sao Paulo)
    # Get current time in Sao Paulo
    now = datetime.now(TARGET_TIMEZONE)
    today_date = now.date()
    tomorrow_date = today_date + timedelta(days=1)
    
    # Create precise datetime boundaries
    # Today starts at 00:00:00, ends at 23:59:59.999999
    today_start = TARGET_TIMEZONE.localize(datetime.combine(today_date, time.min))
    today_end = TARGET_TIMEZONE.localize(datetime.combine(today_date, time.max))
    
    # Tomorrow starts at 00:00:00 (next day), ends at 23:59:59.999999
    tomorrow_start = TARGET_TIMEZONE.localize(datetime.combine(tomorrow_date, time.min))
    tomorrow_end = TARGET_TIMEZONE.localize(datetime.combine(tomorrow_date, time.max))

    target_dates = {
        'today_date': today_date,
        'tomorrow_date': tomorrow_date,
        'today_start': today_start,
        'today_end': today_end,
        'tomorrow_start': tomorrow_start,
        'tomorrow_end': tomorrow_end
    }

    # 5. Parallel Execution
    logger.info(f"Processing {len(target_channels)} channels with {MAX_WORKERS} threads...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures =
        for cid, progs in channel_map.items():
            if progs: # Only process if there are programmes
                futures.append(executor.submit(process_channel, cid, progs, target_dates))
        
        # Wait for completion
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Thread execution error: {e}")

    logger.info("EPG Update Complete.")

if __name__ == "__main__":
    main()
