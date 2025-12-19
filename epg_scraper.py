#!/usr/bin/env python3
"""
EPG Scraper - Production-ready TV schedule extractor
Extracts, filters, and converts EPG data with proper timezone handling
"""

import xml.etree.ElementTree as ET
import json
import os
import re
from datetime import datetime, timedelta, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple
from zoneinfo import ZoneInfo
import urllib.request
import sys

# Constants
EPG_URL = "https://raw.githubusercontent.com/globetvapp/epg/refs/heads/main/Brazil/brazil1.xml"
CHANNEL_FILE = "__channel.txt"
OUTPUT_DIR = "schedule"
TARGET_TIMEZONE = ZoneInfo("America/Sao_Paulo")
UTC_TIMEZONE = ZoneInfo("UTC")


def log(message: str, level: str = "INFO"):
    """Thread-safe logging"""
    timestamp = datetime.now(TARGET_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}", flush=True)


def load_channels() -> List[str]:
    """Load channel IDs from __channel.txt"""
    try:
        with open(CHANNEL_FILE, 'r', encoding='utf-8') as f:
            channels = [line.strip() for line in f if line.strip()]
        log(f"Loaded {len(channels)} channels from {CHANNEL_FILE}")
        return channels
    except FileNotFoundError:
        log(f"Channel file {CHANNEL_FILE} not found", "ERROR")
        sys.exit(1)


def sanitize_filename(channel_id: str) -> str:
    """
    Convert channel ID to filename
    Record TV.br -> Record-TV.json
    """
    # Remove .br suffix
    name = re.sub(r'\.br$', '', channel_id, flags=re.IGNORECASE)
    # Replace spaces with hyphens
    name = name.replace(' ', '-')
    return f"{name}.json"


def parse_xmltv_time(time_str: str) -> datetime:
    """
    Parse XMLTV timestamp: 20251218013000 +0000
    Returns datetime in UTC
    """
    # Extract date/time and timezone parts
    dt_part = time_str[:14]  # 20251218013000
    
    # Parse the datetime
    dt = datetime.strptime(dt_part, "%Y%m%d%H%M%S")
    
    # Apply timezone (always UTC in source)
    dt = dt.replace(tzinfo=UTC_TIMEZONE)
    
    return dt


def convert_to_local(dt: datetime) -> datetime:
    """Convert UTC datetime to Sao Paulo timezone"""
    return dt.astimezone(TARGET_TIMEZONE)


def format_time_12h(dt: datetime) -> str:
    """Format datetime to 12-hour format: 01:30 AM"""
    return dt.strftime("%I:%M %p").lstrip('0')


def format_date(dt: datetime) -> str:
    """Format date: 2025-12-18"""
    return dt.strftime("%Y-%m-%d")


def get_day_boundaries(date_obj: datetime) -> Tuple[datetime, datetime]:
    """
    Get midnight to 11:59:59.999999 PM boundaries for a given date
    Returns (start, end) in local timezone
    """
    date_only = date_obj.date()
    
    # Start: midnight (00:00:00)
    start = datetime.combine(date_only, time.min)
    start = start.replace(tzinfo=TARGET_TIMEZONE)
    
    # End: 23:59:59.999999
    end = datetime.combine(date_only, time.max)
    end = end.replace(tzinfo=TARGET_TIMEZONE)
    
    return start, end


def get_show_effective_date(show_start: datetime) -> datetime:
    """
    Determine which day a show belongs to based on its start time.
    A show belongs to the day it starts in (local timezone).
    """
    return show_start.date()


def download_epg() -> str:
    """Download EPG XML file"""
    log(f"Downloading EPG from {EPG_URL}")
    try:
        with urllib.request.urlopen(EPG_URL, timeout=30) as response:
            content = response.read().decode('utf-8')
        log(f"Downloaded {len(content)} bytes")
        return content
    except Exception as e:
        log(f"Failed to download EPG: {e}", "ERROR")
        sys.exit(1)


def process_channel(
    channel_id: str,
    xml_content: str,
    today_date: datetime,
    tomorrow_date: datetime
) -> Tuple[str, bool]:
    """
    Process a single channel and generate today/tomorrow schedules
    Returns (channel_id, success)
    """
    try:
        log(f"Processing channel: {channel_id}")
        
        # Parse XML and find programmes for this channel
        root = ET.fromstring(xml_content)
        programmes = root.findall(f".//programme[@channel='{channel_id}']")
        
        if not programmes:
            log(f"No programmes found for {channel_id}", "WARNING")
            return channel_id, False
        
        log(f"Found {len(programmes)} programmes for {channel_id}")
        
        # Get day boundaries
        today_start, today_end = get_day_boundaries(today_date)
        tomorrow_start, tomorrow_end = get_day_boundaries(tomorrow_date)
        
        # Target dates for comparison
        today_target = today_date.date()
        tomorrow_target = tomorrow_date.date()
        
        # Collect shows for each day
        today_shows = []
        tomorrow_shows = []
        
        for prog in programmes:
            try:
                # Parse times
                start_utc = parse_xmltv_time(prog.get('start'))
                stop_utc = parse_xmltv_time(prog.get('stop'))
                
                # Convert to local timezone
                start_local = convert_to_local(start_utc)
                stop_local = convert_to_local(stop_utc)
                
                # Determine which day this show belongs to based on START time
                show_date = start_local.date()
                
                # Extract show details
                title = prog.find('title')
                desc = prog.find('desc')
                
                show_name = title.text if title is not None and title.text else "Unknown"
                episode_desc = desc.text if desc is not None and desc.text else ""
                
                # Process for TODAY
                if show_date == today_target:
                    # Show starts today
                    adj_start = start_local
                    adj_end = stop_local
                    
                    # If show ends after midnight (into tomorrow), truncate to 11:59 PM
                    if adj_end.date() > today_target:
                        adj_end = today_end
                    
                    # Ensure times are within today's boundary
                    if adj_start < today_start:
                        adj_start = today_start
                    if adj_end > today_end:
                        adj_end = today_end
                    
                    today_shows.append({
                        "show_name": show_name,
                        "show_logo": "",
                        "start_time": format_time_12h(adj_start),
                        "end_time": format_time_12h(adj_end),
                        "episode_description": episode_desc
                    })
                
                # Process for TOMORROW
                elif show_date == tomorrow_target:
                    # Show starts tomorrow
                    adj_start = start_local
                    adj_end = stop_local
                    
                    # If show ends after midnight (into day after tomorrow), truncate to 11:59 PM
                    if adj_end.date() > tomorrow_target:
                        adj_end = tomorrow_end
                    
                    # Ensure times are within tomorrow's boundary
                    if adj_start < tomorrow_start:
                        adj_start = tomorrow_start
                    if adj_end > tomorrow_end:
                        adj_end = tomorrow_end
                    
                    tomorrow_shows.append({
                        "show_name": show_name,
                        "show_logo": "",
                        "start_time": format_time_12h(adj_start),
                        "end_time": format_time_12h(adj_end),
                        "episode_description": episode_desc
                    })
                
                # Special case: Show started YESTERDAY but ends today
                elif show_date < today_target and stop_local.date() >= today_target:
                    # Include in today's schedule starting from midnight
                    adj_start = today_start
                    adj_end = stop_local
                    
                    # Ensure end time doesn't exceed today
                    if adj_end > today_end:
                        adj_end = today_end
                    
                    today_shows.append({
                        "show_name": show_name,
                        "show_logo": "",
                        "start_time": format_time_12h(adj_start),
                        "end_time": format_time_12h(adj_end),
                        "episode_description": episode_desc
                    })
                
                # Special case: Show started TODAY but ends in TOMORROW
                # This is already handled above (truncated to 11:59 PM for today)
                # But we also need to add it to TOMORROW starting from midnight
                elif show_date == today_target and stop_local.date() > tomorrow_target:
                    # Add continuation to tomorrow
                    adj_start = tomorrow_start
                    adj_end = stop_local
                    
                    # Ensure end time doesn't exceed tomorrow
                    if adj_end > tomorrow_end:
                        adj_end = tomorrow_end
                    
                    tomorrow_shows.append({
                        "show_name": show_name,
                        "show_logo": "",
                        "start_time": format_time_12h(adj_start),
                        "end_time": format_time_12h(adj_end),
                        "episode_description": episode_desc
                    })
                    
            except Exception as e:
                log(f"Error processing programme in {channel_id}: {e}", "WARNING")
                continue
        
        # Generate output filename
        filename = sanitize_filename(channel_id)
        display_name = re.sub(r'\.br$', '', channel_id, flags=re.IGNORECASE)
        
        # Save TODAY schedule
        if today_shows:
            today_data = {
                "channel": display_name,
                "date": format_date(today_date),
                "schedule": today_shows
            }
            save_schedule(filename, "today", today_data)
            log(f"Saved {len(today_shows)} shows for {channel_id} (today)")
        else:
            log(f"No shows for {channel_id} today", "WARNING")
        
        # Save TOMORROW schedule
        if tomorrow_shows:
            tomorrow_data = {
                "channel": display_name,
                "date": format_date(tomorrow_date),
                "schedule": tomorrow_shows
            }
            save_schedule(filename, "tomorrow", tomorrow_data)
            log(f"Saved {len(tomorrow_shows)} shows for {channel_id} (tomorrow)")
        else:
            log(f"No shows for {channel_id} tomorrow", "WARNING")
        
        return channel_id, True
        
    except Exception as e:
        log(f"Failed to process {channel_id}: {e}", "ERROR")
        import traceback
        log(traceback.format_exc(), "ERROR")
        return channel_id, False


def save_schedule(filename: str, day_type: str, data: Dict):
    """Save schedule JSON to appropriate directory"""
    output_path = Path(OUTPUT_DIR) / day_type / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def main():
    """Main execution function"""
    log("=" * 60)
    log("EPG Scraper Started")
    log("=" * 60)
    
    # Get current time in local timezone
    now_local = datetime.now(TARGET_TIMEZONE)
    today_local = now_local
    tomorrow_local = now_local + timedelta(days=1)
    
    log(f"Current time (Sao Paulo): {now_local.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    log(f"Processing schedules for: {format_date(today_local)} (today) and {format_date(tomorrow_local)} (tomorrow)")
    
    # Load channels
    channels = load_channels()
    
    # Download EPG
    xml_content = download_epg()
    
    # Create output directories
    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    (Path(OUTPUT_DIR) / "today").mkdir(exist_ok=True)
    (Path(OUTPUT_DIR) / "tomorrow").mkdir(exist_ok=True)
    
    # Process channels in parallel
    log(f"Processing {len(channels)} channels in parallel...")
    
    successful = 0
    failed = 0
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all tasks
        futures = {
            executor.submit(
                process_channel,
                channel_id,
                xml_content,
                today_local,
                tomorrow_local
            ): channel_id
            for channel_id in channels
        }
        
        # Collect results
        for future in as_completed(futures):
            channel_id, success = future.result()
            if success:
                successful += 1
            else:
                failed += 1
    
    # Summary
    log("=" * 60)
    log(f"EPG Scraper Completed")
    log(f"Successful: {successful}/{len(channels)}")
    log(f"Failed: {failed}/{len(channels)}")
    log("=" * 60)
    
    # Exit with error code if any failed
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()