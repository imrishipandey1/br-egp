#!/usr/bin/env python3
"""
EPG XML Scraper - Extracts channel schedules from EPG XML and saves to JSON
Supports parallel processing for multiple channels
"""

import requests
import xml.etree.ElementTree as ET
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
EPG_URL = "https://raw.githubusercontent.com/globetvapp/epg/refs/heads/main/Brazil/brazil1.xml"
CHANNELS_FILE = "__channels.txt"
OUTPUT_DIR_TODAY = "schedule/today"
OUTPUT_DIR_TOMORROW = "schedule/tomorrow"
TIMEZONE = pytz.timezone('America/Sao_Paulo')  # Brazil - São Paulo timezone

def create_directories():
    """Create output directories if they don't exist"""
    Path(OUTPUT_DIR_TODAY).mkdir(parents=True, exist_ok=True)
    Path(OUTPUT_DIR_TOMORROW).mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directories created/verified: {OUTPUT_DIR_TODAY}, {OUTPUT_DIR_TOMORROW}")

def read_channels_file():
    """Read channel names/IDs from __channels.txt"""
    if not os.path.exists(CHANNELS_FILE):
        logger.error(f"Channel file '{CHANNELS_FILE}' not found!")
        return []
    
    with open(CHANNELS_FILE, 'r', encoding='utf-8') as f:
        channels = [line.strip() for line in f if line.strip()]
    
    logger.info(f"Loaded {len(channels)} channels from {CHANNELS_FILE}")
    return channels

def fetch_epg_xml():
    """Fetch EPG XML from URL"""
    logger.info(f"Fetching EPG XML from {EPG_URL}")
    try:
        response = requests.get(EPG_URL, timeout=30)
        response.raise_for_status()
        logger.info(f"EPG XML fetched successfully ({len(response.content)} bytes)")
        return ET.fromstring(response.content)
    except Exception as e:
        logger.error(f"Failed to fetch EPG XML: {e}")
        raise

def parse_xmltv_time(time_str):
    """
    Parse XMLTV time format: '20251218013000 +0000'
    Returns datetime object in UTC
    """
    try:
        # Remove timezone info and parse
        time_part = time_str.split()[0] if ' ' in time_str else time_str
        # Format: YYYYMMDDhhmmss
        dt_utc = datetime.strptime(time_part, '%Y%m%d%H%M%S')
        dt_utc = pytz.UTC.localize(dt_utc)
        return dt_utc
    except Exception as e:
        logger.warning(f"Failed to parse time '{time_str}': {e}")
        return None

def format_brazilian_time(dt_utc):
    """Convert UTC datetime to São Paulo time and format"""
    if dt_utc is None:
        return None
    
    dt_br = dt_utc.astimezone(TIMEZONE)
    # Format: HH:MM AM/PM
    return dt_br.strftime('%I:%M %p').lstrip('0').replace(' 0', ' ')

def format_brazilian_date(dt_utc):
    """Convert UTC datetime to São Paulo date"""
    if dt_utc is None:
        return None
    
    dt_br = dt_utc.astimezone(TIMEZONE)
    return dt_br.strftime('%Y-%m-%d')

def get_today_tomorrow_dates():
    """Get today and tomorrow dates in Brazilian timezone"""
    now = datetime.now(TIMEZONE)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)
    
    return {
        'today': today,
        'tomorrow': tomorrow,
        'today_str': today.strftime('%Y-%m-%d'),
        'tomorrow_str': tomorrow.strftime('%Y-%m-%d')
    }

def normalize_channel_id(channel_name):
    """Convert channel name to channel ID format (with .br suffix if not present)"""
    channel_name = channel_name.strip()
    # If it doesn't end with .br, add it for searching in XML
    if not channel_name.endswith('.br'):
        # Try both with and without .br
        return [f"{channel_name}.br", channel_name]
    return [channel_name]

def sanitize_filename(filename):
    """Convert channel name to safe filename"""
    # Replace spaces and special chars with dashes
    filename = filename.replace(' ', '-').replace('.br', '')
    # Remove any other special characters
    filename = ''.join(c if c.isalnum() or c == '-' else '' for c in filename)
    return filename.lower()

def scrape_channel_schedule(args):
    """
    Scrape schedule for a single channel
    Args: (root_element, channel_name, dates_dict)
    """
    root, channel_name, dates = args
    
    try:
        logger.info(f"Processing channel: {channel_name}")
        
        # Try to find the channel with different variations
        channel_ids = normalize_channel_id(channel_name)
        programmes = []
        
        for channel_id in channel_ids:
            programmes = root.findall(f".//programme[@channel='{channel_id}']")
            if programmes:
                logger.info(f"Found {len(programmes)} programmes for {channel_id}")
                break
        
        if not programmes:
            logger.warning(f"No programmes found for {channel_name} (tried: {channel_ids})")
            return {
                'status': 'no_data',
                'channel': channel_name
            }
        
        # Filter and organize by date
        today_schedule = []
        tomorrow_schedule = []
        
        for prog in programmes:
            start_str = prog.get('start')
            stop_str = prog.get('stop')
            
            start_dt = parse_xmltv_time(start_str)
            stop_dt = parse_xmltv_time(stop_str)
            
            if not start_dt or not stop_dt:
                continue
            
            # Get Brazilian date
            prog_date = format_brazilian_date(start_dt)
            
            # Extract show details
            title = prog.find('title')
            show_name = title.text if title is not None and title.text else "Unknown"
            
            desc = prog.find('desc')
            show_desc = desc.text if desc is not None and desc.text else ""
            
            icon = prog.find('icon')
            show_logo = icon.get('src') if icon is not None else ""
            
            # Format times
            start_time = format_brazilian_time(start_dt)
            end_time = format_brazilian_time(stop_dt)
            
            schedule_item = {
                "show_name": show_name,
                "show_logo": show_logo,
                "start_time": start_time,
                "end_time": end_time,
                "episode_description": show_desc
            }
            
            # Categorize by date
            if prog_date == dates['today_str']:
                today_schedule.append((start_dt, schedule_item))
            elif prog_date == dates['tomorrow_str']:
                tomorrow_schedule.append((start_dt, schedule_item))
        
        # Sort by start time and remove the sort key
        today_schedule.sort(key=lambda x: x[0])
        tomorrow_schedule.sort(key=lambda x: x[0])
        
        today_schedule = [item for _, item in today_schedule]
        tomorrow_schedule = [item for _, item in tomorrow_schedule]
        
        # Get clean channel name
        clean_channel_name = channel_name.replace('.br', '').strip()
        
        # Prepare output
        result = {
            'status': 'success',
            'channel': clean_channel_name,
            'today': {
                'filename': f"{OUTPUT_DIR_TODAY}/{sanitize_filename(channel_name)}.json",
                'data': {
                    "channel": clean_channel_name,
                    "date": dates['today_str'],
                    "schedule": today_schedule
                },
                'count': len(today_schedule)
            },
            'tomorrow': {
                'filename': f"{OUTPUT_DIR_TOMORROW}/{sanitize_filename(channel_name)}.json",
                'data': {
                    "channel": clean_channel_name,
                    "date": dates['tomorrow_str'],
                    "schedule": tomorrow_schedule
                },
                'count': len(tomorrow_schedule)
            }
        }
        
        logger.info(f"✓ {clean_channel_name}: {len(today_schedule)} today, {len(tomorrow_schedule)} tomorrow")
        return result
        
    except Exception as e:
        logger.error(f"Error processing channel {channel_name}: {e}")
        return {
            'status': 'error',
            'channel': channel_name,
            'error': str(e)
        }

def save_json(filepath, data):
    """Save data to JSON file"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved: {filepath}")
    except Exception as e:
        logger.error(f"Failed to save {filepath}: {e}")

def main():
    logger.info("=" * 60)
    logger.info("EPG Scraper Started")
    logger.info("=" * 60)
    
    # Create directories
    create_directories()
    
    # Read channels
    channels = read_channels_file()
    if not channels:
        logger.error("No channels to process!")
        return
    
    # Fetch EPG XML
    root = fetch_epg_xml()
    
    # Get dates
    dates = get_today_tomorrow_dates()
    logger.info(f"Today: {dates['today_str']}, Tomorrow: {dates['tomorrow_str']}")
    
    # Prepare arguments for parallel processing
    args_list = [(root, channel, dates) for channel in channels]
    
    # Process channels in parallel
    results = []
    max_workers = min(8, len(channels))  # Use up to 8 threads
    logger.info(f"Starting parallel processing with {max_workers} workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(scrape_channel_schedule, args): args[1] for args in args_list}
        
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
                
                # Save JSON files if successful
                if result['status'] == 'success':
                    save_json(result['today']['filename'], result['today']['data'])
                    save_json(result['tomorrow']['filename'], result['tomorrow']['data'])
                    
            except Exception as e:
                logger.error(f"Future execution error: {e}")
    
    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("PROCESSING SUMMARY")
    logger.info("=" * 60)
    
    success_count = sum(1 for r in results if r['status'] == 'success')
    no_data_count = sum(1 for r in results if r['status'] == 'no_data')
    error_count = sum(1 for r in results if r['status'] == 'error')
    
    logger.info(f"Total Channels: {len(channels)}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"No Data: {no_data_count}")
    logger.info(f"Errors: {error_count}")
    
    total_today = sum(r['today']['count'] for r in results if r['status'] == 'success')
    total_tomorrow = sum(r['tomorrow']['count'] for r in results if r['status'] == 'success')
    
    logger.info(f"\nTotal Programmes - Today: {total_today}, Tomorrow: {total_tomorrow}")
    logger.info("=" * 60)
    logger.info("EPG Scraper Completed")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
