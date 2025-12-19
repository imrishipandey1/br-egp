#!/usr/bin/env python3
"""
EPG Scraper Script - Strict Interval Clamping Version

Fetches EPG XML from the specified URL, parses it using streaming iterparse to minimize memory usage,
filters programmes for channels listed in channels.txt, converts UTC times to America/Sao_Paulo timezone,
clamps each programme interval to daily windows (00:00 - 23:59 local) without splitting,
validates clamped duration > 0, and outputs structured JSON files for today and tomorrow schedules per channel.

Key Rules Enforced:
- Each programme produces AT MOST ONE entry per day.
- Clamp: final_start = max(prog_start, day_start), final_end = min(prog_end, day_end)
- Skip if final_start >= final_end (zero/negative duration).
- No artificial segments or duplicates within a day.
- Handles midnight-crossing by separate clamping per day (appears in both if spans).

Requirements:
- Python 3.11+
- requests, lxml (for XML parsing)
- zoneinfo (built-in)
- concurrent.futures (built-in)
- json, os, datetime (built-in)

Usage:
python epg_scraper.py
Assumes channels.txt exists in the repo root.
Outputs to schedule/today/ and schedule/tomorrow/
"""

import requests
import io
from lxml import etree
from datetime import datetime, timedelta, time, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict
import os
import json
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any

# Constants
EPG_URL = "https://raw.githubusercontent.com/globetvapp/epg/refs/heads/main/Brazil/brazil1.xml"
CHANNELS_FILE = "channels.txt"
OUTPUT_BASE = "schedule"
SAO_PAULO_TZ = ZoneInfo("America/Sao_Paulo")
UTC_TZ = timezone.utc
TIME_FORMAT = "%I:%M %p"  # 12-hour with AM/PM (e.g., 01:30 AM, 10:45 PM)


def parse_time(time_str: str) -> datetime:
    """
    Parse EPG time string like '20251218013000 +0000' to UTC datetime.
    Format: YYYYMMDDHHMMSS %z
    """
    return datetime.strptime(time_str, "%Y%m%d%H%M%S %z")


def read_channels() -> set:
    """
    Read channel IDs from channels.txt, one per line, stripped.
    Ignores blank lines.
    Returns set for O(1) lookups.
    """
    if not os.path.exists(CHANNELS_FILE):
        raise FileNotFoundError(f"{CHANNELS_FILE} not found. Create it with channel IDs, one per line.")
    
    with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
        channels = {line.strip() for line in f if line.strip()}
    
    if not channels:
        print("Warning: No channels listed in channels.txt.")
    
    return channels


def fetch_and_parse_programmes(channels: set) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch EPG XML via requests, parse with lxml iterparse (streaming) to collect programmes per channel.
    Only processes programmes for channels in the set.
    Clears elements to manage memory.
    """
    response = requests.get(EPG_URL)
    response.raise_for_status()
    
    programmes = defaultdict(list)
    
    context = etree.iterparse(
        io.BytesIO(response.content),
        events=("end",),
        tag="programme",
    )
    
    for event, elem in context:
        channel_id = elem.get("channel")
        if channel_id in channels:
            start_str = elem.get("start")
            stop_str = elem.get("stop")
            if start_str and stop_str:
                try:
                    start_utc = parse_time(start_str)
                    stop_utc = parse_time(stop_str)
                    
                    # Ensure start < stop (basic sanity)
                    if start_utc >= stop_utc:
                        print(f"Warning: Invalid programme duration for {channel_id}, skipping.")
                        elem.clear()
                        continue
                    
                    title_elem = elem.find("title")
                    title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""
                    
                    desc_elem = elem.find("desc")
                    desc = desc_elem.text.strip() if desc_elem is not None and desc_elem.text else ""
                    
                    programmes[channel_id].append({
                        "start": start_utc,
                        "stop": stop_utc,
                        "title": title,
                        "desc": desc,
                    })
                except ValueError as e:
                    print(f"Warning: Skipping invalid programme time for channel {channel_id}: {e}")
        
        # Clear element and children to free memory (streaming)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    
    del context  # Ensure cleanup
    
    return dict(programmes)  # Convert to regular dict


def get_daily_windows() -> tuple[datetime, datetime, datetime, datetime, Any, Any]:
    """
    Get start/end windows for today and tomorrow in Sao Paulo TZ.
    Today/Tomorrow: 00:00:00 to 23:59:59.999999 local.
    Returns: today_start, today_end, tomorrow_start, tomorrow_end, today_date, tomorrow_date
    """
    now_local = datetime.now(SAO_PAULO_TZ)
    today_date = now_local.date()
    tomorrow_date = today_date + timedelta(days=1)
    
    today_start = datetime.combine(today_date, time.min, tzinfo=SAO_PAULO_TZ)
    today_end = datetime.combine(today_date, time(23, 59, 59, 999999), tzinfo=SAO_PAULO_TZ)
    tomorrow_start = datetime.combine(tomorrow_date, time.min, tzinfo=SAO_PAULO_TZ)
    tomorrow_end = datetime.combine(tomorrow_date, time(23, 59, 59, 999999), tzinfo=SAO_PAULO_TZ)
    
    return today_start, today_end, tomorrow_start, tomorrow_end, today_date, tomorrow_date


def process_channel(
    channel_id: str,
    programmes: List[Dict[str, Any]],
    today_start: datetime,
    today_end: datetime,
    tomorrow_start: datetime,
    tomorrow_end: datetime,
    today_date: Any,
    tomorrow_date: Any,
) -> None:
    """
    Process programmes for one channel: clamp each to windows (once per day), validate duration,
    format, sort by clamped start time, write JSONs.
    Enforces: AT MOST ONE entry per programme per day. No splitting.
    Handles Cases A/B/C via clamping (midnight-crossing appears clamped in both days if valid).
    Skips channels with no data.
    """
    if not programmes:
        print(f"Warning: No programmes found for channel {channel_id}. Skipping.")
        return
    
    display_name = channel_id.replace(".br", "")
    filename = display_name.replace(" ", "-").lower() + ".json"  # Lowercase, case insensitive as per rules
    
    # Sort programmes by UTC start time (chronological order)
    programmes.sort(key=lambda p: p["start"])
    
    # Define windows as list for loop
    windows = [
        (today_start, today_end, today_date, "today"),
        (tomorrow_start, tomorrow_end, tomorrow_date, "tomorrow"),
    ]
    
    for day_start, day_end, day_date_obj, dir_name in windows:
        schedule_entries = []
        
        for prog in programmes:
            # Step 1: Convert to local TZ
            local_start = prog["start"].astimezone(SAO_PAULO_TZ)
            local_end = prog["stop"].astimezone(SAO_PAULO_TZ)
            
            # Step 2: Clamp ONCE per day (no split)
            clamped_start = max(local_start, day_start)
            clamped_end = min(local_end, day_end)
            
            # Step 3: HARD VALIDATION - skip if zero/negative duration
            if clamped_start >= clamped_end:
                continue  # Do not save; enforces no fake/empty entries
            
            # Step 4: Format times
            start_str = clamped_start.strftime(TIME_FORMAT)
            end_str = clamped_end.strftime(TIME_FORMAT)
            
            # Build entry (one per valid clamp)
            entry = {
                "show_name": prog["title"],
                "show_logo": "",  # Always empty if unavailable
                "start_time": start_str,
                "end_time": end_str,
                "episode_description": prog["desc"],
            }
            schedule_entries.append((clamped_start, entry))  # For sorting by clamped start
        
        # Sort by clamped start time (maintain order, no duplicates)
        schedule_entries.sort(key=lambda x: x[0])
        schedule = [entry for _, entry in schedule_entries]
        
        # Build JSON data
        data = {
            "channel": display_name,
            "date": day_date_obj.isoformat(),  # YYYY-MM-DD
            "schedule": schedule,
        }
        
        # Write to file
        output_dir = os.path.join(OUTPUT_BASE, dir_name)
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, filename)
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Generated {output_file} with {len(schedule)} entries.")


def main():
    """Main execution: read channels, fetch/parse XML, process channels in parallel, output JSONs."""
    channels = read_channels()
    if not channels:
        print("No channels to process. Exiting.")
        return
    
    print(f"Processing {len(channels)} channels...")
    programmes_by_channel = fetch_and_parse_programmes(channels)
    
    today_start, today_end, tomorrow_start, tomorrow_end, today_date, tomorrow_date = get_daily_windows()
    
    # Parallel processing: one thread per channel (thread-safe, no shared state)
    with ThreadPoolExecutor(max_workers=min(10, len(channels))) as executor:
        futures = [
            executor.submit(
                process_channel,
                channel_id,
                programmes_by_channel.get(channel_id, []),
                today_start, today_end,
                tomorrow_start, tomorrow_end,
                today_date, tomorrow_date,
            )
            for channel_id in channels
        ]
        # Wait for completion, propagate exceptions
        for future in futures:
            future.result()
    
    print("EPG scraping completed successfully.")


if __name__ == "__main__":
    main()