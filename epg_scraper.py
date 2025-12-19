#!/usr/bin/env python3
"""
EPG Scraper Script

Fetches EPG XML from the specified URL, parses it using streaming iterparse to minimize memory usage,
filters programmes for channels listed in channels.txt, converts UTC times to America/Sao_Paulo timezone,
clips programmes to daily windows (12:00 AM - 11:59 PM local), handles midnight-crossing shows by truncation/splitting,
and outputs structured JSON files for today and tomorrow schedules per channel.

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
TIME_FORMAT = "%I:%M %p"  # 12-hour with AM/PM, leading zero for hours 01-09


def parse_time(time_str: str) -> datetime:
    """
    Parse EPG time string like '20251218013000 +0000' to UTC datetime.
    Format: YYYYMMDDHHMMSS %z
    """
    return datetime.strptime(time_str, "%Y%m%d%H%M%S %z")


def read_channels() -> set:
    """
    Read channel IDs from channels.txt, one per line, stripped.
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
        no_network=False,  # Not needed, but safe
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
        
        # Clear element and children to free memory
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    
    del context  # Ensure cleanup
    
    return dict(programmes)  # Convert to regular dict


def get_daily_windows() -> tuple[datetime, datetime, datetime, datetime]:
    """
    Get start/end windows for today and tomorrow in Sao Paulo TZ.
    Today: current local date 00:00:00 to 23:59:59.999999
    Tomorrow: next day same.
    """
    now_local = datetime.now(SAO_PAULO_TZ)
    today_date = now_local.date()
    tomorrow_date = today_date + timedelta(days=1)
    
    today_start = datetime.combine(today_date, time.min, tzinfo=SAO_PAULO_TZ)
    today_end = datetime.combine(today_date, time.max, tzinfo=SAO_PAULO_TZ)
    tomorrow_start = datetime.combine(tomorrow_date, time.min, tzinfo=SAO_PAULO_TZ)
    tomorrow_end = datetime.combine(tomorrow_date, time.max, tzinfo=SAO_PAULO_TZ)
    
    return today_start, today_end, tomorrow_start, tomorrow_end, today_date, tomorrow_date


def process_channel(
    channel_id: str,
    programmes: List[Dict[str, Any]],
    today_start: datetime,
    today_end: datetime,
    tomorrow_start: datetime,
    tomorrow_end: datetime,
    today_date,
    tomorrow_date,
) -> None:
    """
    Process programmes for one channel: clip to windows, format, sort by start time, write JSONs.
    Handles truncation for midnight-crossing (Cases A, B, C).
    Skips zero-duration clips.
    Writes to schedule/today/ and schedule/tomorrow/
    """
    if not programmes:
        print(f"Warning: No programmes found for channel {channel_id}. Skipping.")
        return
    
    display_name = channel_id.replace(".br", "")
    filename = display_name.replace(" ", "-") + ".json"
    
    # Sort programmes by UTC start time (should be chronological)
    programmes.sort(key=lambda p: p["start"])
    
    windows = [
        (today_start, today_end, today_date, "today"),
        (tomorrow_start, tomorrow_end, tomorrow_date, "tomorrow"),
    ]
    
    for window_start, window_end, window_date_obj, dir_name in windows:
        schedule_entries = []
        
        for prog in programmes:
            local_start = prog["start"].astimezone(SAO_PAULO_TZ)
            local_end = prog["stop"].astimezone(SAO_PAULO_TZ)
            
            clip_start = max(local_start, window_start)
            clip_end = min(local_end, window_end)
            
            if clip_start < clip_end:
                start_str = clip_start.strftime(TIME_FORMAT)
                end_str = clip_end.strftime(TIME_FORMAT)
                
                entry = {
                    "show_name": prog["title"],
                    "show_logo": "",
                    "start_time": start_str,
                    "end_time": end_str,
                    "episode_description": prog["desc"],
                }
                schedule_entries.append((clip_start, entry))
        
        # Sort by clip start time
        schedule_entries.sort(key=lambda x: x[0])
        schedule = [entry for _, entry in schedule_entries]
        
        data = {
            "channel": display_name,
            "date": window_date_obj.isoformat(),
            "schedule": schedule,
        }
        
        output_dir = os.path.join(OUTPUT_BASE, dir_name)
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, filename)
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Generated {output_file} with {len(schedule)} entries.")


def main():
    """Main execution: parse, process in parallel, output JSONs."""
    channels = read_channels()
    if not channels:
        print("No channels to process. Exiting.")
        return
    
    print(f"Processing {len(channels)} channels...")
    programmes_by_channel = fetch_and_parse_programmes(channels)
    
    today_start, today_end, tomorrow_start, tomorrow_end, today_date, tomorrow_date = get_daily_windows()
    
    # Parallel processing per channel
    with ThreadPoolExecutor(max_workers=min(10, len(channels))) as executor:
        futures = [
            executor.submit(
                process_channel,
                channel_id,
                programmes_by_channel.get(channel_id, []),
                today_start,
                today_end,
                tomorrow_start,
                tomorrow_end,
                today_date,
                tomorrow_date,
            )
            for channel_id in channels
        ]
        for future in futures:
            future.result()  # Wait for all, raise any exceptions
    
    print("EPG scraping completed.")


if __name__ == "__main__":
    main()