#!/usr/bin/env python3
“””
EPG Scraper - Production-ready TV schedule extractor
Extracts, filters, and converts EPG data with proper timezone handling
“””

import xml.etree.ElementTree as ET
import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional, Tuple
from zoneinfo import ZoneInfo
import urllib.request
import sys

# Constants

EPG_URL = “https://raw.githubusercontent.com/globetvapp/epg/refs/heads/main/Brazil/brazil1.xml”
CHANNEL_FILE = “__channel.txt”
OUTPUT_DIR = “schedule”
TARGET_TIMEZONE = ZoneInfo(“America/Sao_Paulo”)
UTC_TIMEZONE = ZoneInfo(“UTC”)

# Date boundaries in local timezone

MIDNIGHT = “12:00 AM”
END_OF_DAY = “11:59 PM”

def log(message: str, level: str = “INFO”):
“”“Thread-safe logging”””
timestamp = datetime.now(TARGET_TIMEZONE).strftime(”%Y-%m-%d %H:%M:%S”)
print(f”[{timestamp}] [{level}] {message}”, flush=True)

def load_channels() -> List[str]:
“”“Load channel IDs from __channel.txt”””
try:
with open(CHANNEL_FILE, ‘r’, encoding=‘utf-8’) as f:
channels = [line.strip() for line in f if line.strip()]
log(f”Loaded {len(channels)} channels from {CHANNEL_FILE}”)
return channels
except FileNotFoundError:
log(f”Channel file {CHANNEL_FILE} not found”, “ERROR”)
sys.exit(1)

def sanitize_filename(channel_id: str) -> str:
“””
Convert channel ID to filename
Record TV.br -> Record-TV.json
“””
# Remove .br suffix
name = re.sub(r’.br$’, ‘’, channel_id, flags=re.IGNORECASE)
# Replace spaces with hyphens
name = name.replace(’ ’, ‘-’)
return f”{name}.json”

def parse_xmltv_time(time_str: str) -> datetime:
“””
Parse XMLTV timestamp: 20251218013000 +0000
Returns datetime in UTC
“””
# Extract date/time and timezone parts
dt_part = time_str[:14]  # 20251218013000
tz_part = time_str[15:]   # +0000

```
# Parse the datetime
dt = datetime.strptime(dt_part, "%Y%m%d%H%M%S")

# Apply timezone (always UTC in source)
dt = dt.replace(tzinfo=UTC_TIMEZONE)

return dt
```

def convert_to_local(dt: datetime) -> datetime:
“”“Convert UTC datetime to Sao Paulo timezone”””
return dt.astimezone(TARGET_TIMEZONE)

def format_time_12h(dt: datetime) -> str:
“”“Format datetime to 12-hour format: 01:30 AM”””
return dt.strftime(”%I:%M %p”).lstrip(‘0’)

def format_date(dt: datetime) -> str:
“”“Format date: 2025-12-18”””
return dt.strftime(”%Y-%m-%d”)

def get_day_boundaries(reference_date: datetime) -> Tuple[datetime, datetime]:
“””
Get midnight to 11:59:59.999999 PM boundaries for a given date
Returns (start, end) in local timezone
“””
date_only = reference_date.date()

```
# Start: midnight (00:00:00)
start = datetime.combine(date_only, datetime.min.time())
start = start.replace(tzinfo=TARGET_TIMEZONE)

# End: 23:59:59.999999
end = datetime.combine(date_only, datetime.max.time())
end = end.replace(tzinfo=TARGET_TIMEZONE)

return start, end
```

def should_include_in_day(
show_start: datetime,
show_end: datetime,
day_start: datetime,
day_end: datetime
) -> bool:
“””
Check if a show overlaps with the day boundary (12:00 AM - 11:59 PM)
“””
# Show must overlap with the day window
return not (show_end <= day_start or show_start > day_end)

def adjust_show_times(
show_start: datetime,
show_end: datetime,
day_start: datetime,
day_end: datetime
) -> Tuple[datetime, datetime]:
“””
Adjust show times to fit within day boundaries
- If show starts before day: set to 12:00 AM
- If show ends after day: set to 11:59 PM
“””
adjusted_start = max(show_start, day_start)
adjusted_end = min(show_end, day_end)

```
return adjusted_start, adjusted_end
```

def download_epg() -> str:
“”“Download EPG XML file”””
log(f”Downloading EPG from {EPG_URL}”)
try:
with urllib.request.urlopen(EPG_URL, timeout=30) as response:
content = response.read().decode(‘utf-8’)
log(f”Downloaded {len(content)} bytes”)
return content
except Exception as e:
log(f”Failed to download EPG: {e}”, “ERROR”)
sys.exit(1)

def process_channel(
channel_id: str,
xml_content: str,
today_local: datetime,
tomorrow_local: datetime
) -> Tuple[str, bool]:
“””
Process a single channel and generate today/tomorrow schedules
Returns (channel_id, success)
“””
try:
log(f”Processing channel: {channel_id}”)

```
    # Parse XML and find programmes for this channel
    root = ET.fromstring(xml_content)
    programmes = root.findall(f".//programme[@channel='{channel_id}']")
    
    if not programmes:
        log(f"No programmes found for {channel_id}", "WARNING")
        return channel_id, False
    
    log(f"Found {len(programmes)} programmes for {channel_id}")
    
    # Get day boundaries
    today_start, today_end = get_day_boundaries(today_local)
    tomorrow_start, tomorrow_end = get_day_boundaries(tomorrow_local)
    
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
            
            # Extract show details
            title = prog.find('title')
            desc = prog.find('desc')
            
            show_name = title.text if title is not None and title.text else "Unknown"
            episode_desc = desc.text if desc is not None and desc.text else ""
            
            # Check if show belongs to TODAY
            if should_include_in_day(start_local, stop_local, today_start, today_end):
                adj_start, adj_end = adjust_show_times(
                    start_local, stop_local, today_start, today_end
                )
                
                today_shows.append({
                    "show_name": show_name,
                    "show_logo": "",
                    "start_time": format_time_12h(adj_start),
                    "end_time": format_time_12h(adj_end),
                    "episode_description": episode_desc
                })
            
            # Check if show belongs to TOMORROW
            if should_include_in_day(start_local, stop_local, tomorrow_start, tomorrow_end):
                adj_start, adj_end = adjust_show_times(
                    start_local, stop_local, tomorrow_start, tomorrow_end
                )
                
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
            "date": format_date(today_local),
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
            "date": format_date(tomorrow_local),
            "schedule": tomorrow_shows
        }
        save_schedule(filename, "tomorrow", tomorrow_data)
        log(f"Saved {len(tomorrow_shows)} shows for {channel_id} (tomorrow)")
    else:
        log(f"No shows for {channel_id} tomorrow", "WARNING")
    
    return channel_id, True
    
except Exception as e:
    log(f"Failed to process {channel_id}: {e}", "ERROR")
    return channel_id, False
```

def save_schedule(filename: str, day_type: str, data: Dict):
“”“Save schedule JSON to appropriate directory”””
output_path = Path(OUTPUT_DIR) / day_type / filename
output_path.parent.mkdir(parents=True, exist_ok=True)

```
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2, ensure_ascii=False)
```

def main():
“”“Main execution function”””
log(”=” * 60)
log(“EPG Scraper Started”)
log(”=” * 60)

```
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
```

if **name** == “**main**”:
main()