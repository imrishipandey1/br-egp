#!/usr/bin/env python3
import os
import sys
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor
import xml.etree.ElementTree as ET

def read_channel_list(path="channel.txt"):
    """Read channel IDs (with .br) from channel.txt."""
    channels = []
    try:
        with open(path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                channels.append(line)
    except FileNotFoundError:
        print(f"Error: {path} not found.", file=sys.stderr)
        sys.exit(1)
    return channels

def fetch_epg_xml(url, dest="epg.xml"):
    """Download the EPG XML to a local file (streaming to save memory)."""
    try:
        import requests
    except ImportError:
        print("Missing requests library. Please install it.", file=sys.stderr)
        sys.exit(1)
    try:
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    except Exception as e:
        print(f"Error fetching XML from {url}: {e}", file=sys.stderr)
        sys.exit(1)

def parse_and_group(epg_file, valid_channels):
    """
    Stream-parse the XML and group programmes by channel.
    Only channels in valid_channels (with .br suffix) are kept.
    """
    programmes_by_channel = {ch: [] for ch in valid_channels}
    context = ET.iterparse(epg_file, events=("end",))
    for event, elem in context:
        if elem.tag == "programme":
            ch = elem.attrib.get("channel", "")
            if ch not in programmes_by_channel:
                elem.clear()
                continue
            # Extract data
            start = elem.attrib.get("start")
            stop = elem.attrib.get("stop")
            title = elem.findtext("title") or ""
            desc = elem.findtext("desc") or ""
            programmes_by_channel[ch].append({
                "start": start,
                "stop": stop,
                "title": title,
                "desc": desc
            })
            elem.clear()
    return programmes_by_channel

def process_channel_schedule(channel_id, programmes, saopaulo_tz, today_date, tomorrow_date):
    """
    Compute 'today' and 'tomorrow' schedule entries for one channel.
    Returns {'today': [...], 'tomorrow': [...]} where each list has shows
    with clipped start/end datetime objects.
    """
    schedules = {"today": [], "tomorrow": []}
    # Define day boundaries (00:00 to 23:59:59) in local time
    today_start = datetime.combine(today_date, datetime.min.time()).replace(tzinfo=saopaulo_tz)
    today_end   = today_start.replace(hour=23, minute=59, second=59)
    tomorrow_start = today_start + timedelta(days=1)
    tomorrow_end   = tomorrow_start.replace(hour=23, minute=59, second=59)

    for prog in programmes:
        # Parse original UTC times
        try:
            dt_start = datetime.strptime(prog["start"], "%Y%m%d%H%M%S %z")
            dt_end   = datetime.strptime(prog["stop"],  "%Y%m%d%H%M%S %z")
        except:
            continue
        # Convert to Sao Paulo time
        dt_start_local = dt_start.astimezone(saopaulo_tz)
        dt_end_local   = dt_end.astimezone(saopaulo_tz)

        # Clip for today
        eff_start = max(dt_start_local, today_start)
        eff_end   = min(dt_end_local, today_end)
        if eff_start < eff_end:
            schedules["today"].append({
                "show_name": prog["title"],
                "show_logo": "",
                "start": eff_start,
                "end": eff_end,
                "episode_description": prog["desc"]
            })
        # Clip for tomorrow
        eff_start = max(dt_start_local, tomorrow_start)
        eff_end   = min(dt_end_local, tomorrow_end)
        if eff_start < eff_end:
            schedules["tomorrow"].append({
                "show_name": prog["title"],
                "show_logo": "",
                "start": eff_start,
                "end": eff_end,
                "episode_description": prog["desc"]
            })

    # Sort by start time
    schedules["today"].sort(key=lambda x: x["start"])
    schedules["tomorrow"].sort(key=lambda x: x["start"])
    return schedules

def write_schedule_json(channel_id, shows, output_dir, date_val):
    """
    Write a JSON file for one channel and one date.
    - channel_id: full ID with .br
    - shows: list of dicts with 'start', 'end', 'show_name', etc.
    - output_dir: either "schedule/today" or "schedule/tomorrow"
    - date_val: date object for the schedule.
    """
    # Prepare filename and display name
    display_name = channel_id[:-3] if channel_id.endswith(".br") else channel_id
    filename = display_name.replace(" ", "-") + ".json"
    path = os.path.join(output_dir, filename)

    data = {
        "channel": display_name,
        "date": date_val.strftime("%Y-%m-%d"),
        "schedule": []
    }
    for show in shows:
        start_str = show["start"].strftime("%I:%M %p")
        end_str   = show["end"].strftime("%I:%M %p")
        entry = {
            "show_name": show["show_name"],
            "show_logo": show["show_logo"],
            "start_time": start_str,
            "end_time": end_str,
            "episode_description": show["episode_description"]
        }
        data["schedule"].append(entry)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def main():
    CHANNEL_FILE = "channel.txt"
    EPG_URL = "https://raw.githubusercontent.com/globetvapp/epg/refs/heads/main/Brazil/brazil1.xml"

    # 1. Read list of channels to process
    valid_channels = read_channel_list(CHANNEL_FILE)
    if not valid_channels:
        print("No channels to process.", file=sys.stderr)
        sys.exit(0)

    # 2. Download the EPG XML file (streaming)
    fetch_epg_xml(EPG_URL, dest="epg.xml")

    # 3. Parse and group programmes by channel
    programmes_by_channel = parse_and_group("epg.xml", valid_channels)

    # 4. Prepare output directories
    today_dir = os.path.join("schedule", "today")
    tomorrow_dir = os.path.join("schedule", "tomorrow")
    os.makedirs(today_dir, exist_ok=True)
    os.makedirs(tomorrow_dir, exist_ok=True)

    # 5. Determine 'today' and 'tomorrow' dates in Sao Paulo
    sao_tz = ZoneInfo("America/Sao_Paulo")
    now_sp = datetime.now(sao_tz)
    today_date = now_sp.date()
    tomorrow_date = (now_sp + timedelta(days=1)).date()

    # 6. Process each channel in parallel
    def process_channel(ch_id):
        shows = programmes_by_channel.get(ch_id, [])
        schedules = process_channel_schedule(ch_id, shows, sao_tz, today_date, tomorrow_date)
        write_schedule_json(ch_id, schedules["today"], today_dir, today_date)
        write_schedule_json(ch_id, schedules["tomorrow"], tomorrow_dir, tomorrow_date)

    with ThreadPoolExecutor() as executor:
        executor.map(process_channel, valid_channels)

if __name__ == "__main__":
    main()