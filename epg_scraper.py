import xml.etree.ElementTree as ET
import requests
import json
import os
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor

EPG_URL = "https://raw.githubusercontent.com/globetvapp/epg/refs/heads/main/Brazil/brazil1.xml"
TIMEZONE = ZoneInfo("America/Sao_Paulo")
UTC = ZoneInfo("UTC")

TODAY = datetime.now(TIMEZONE).date()
TOMORROW = TODAY + timedelta(days=1)

def safe_filename(name):
    return name.replace(" ", "-").replace(".", "").strip()

def parse_xml():
    r = requests.get(EPG_URL, timeout=60)
    r.raise_for_status()
    return ET.fromstring(r.content)

def parse_time(value):
    dt = datetime.strptime(value[:14], "%Y%m%d%H%M%S")
    return dt.replace(tzinfo=UTC).astimezone(TIMEZONE)

def extract_day_segment(start, end, day):
    day_start = datetime.combine(day, time(0, 0), TIMEZONE)
    day_end = datetime.combine(day, time(23, 59), TIMEZONE)

    if end <= day_start or start >= day_end:
        return None

    return max(start, day_start), min(end, day_end)

def format_time(dt):
    return dt.strftime("%I:%M %p").lstrip("0")

def build_schedule(root, channel_id, day):
    schedule = []

    for p in root.findall("programme"):
        if p.attrib.get("channel") != channel_id:
            continue

        start = parse_time(p.attrib["start"])
        end = parse_time(p.attrib["stop"])

        segment = extract_day_segment(start, end, day)
        if not segment:
            continue

        seg_start, seg_end = segment

        schedule.append({
            "show_name": p.findtext("title", "").strip(),
            "show_logo": "",
            "start_time": format_time(seg_start),
            "end_time": format_time(seg_end),
            "episode_description": p.findtext("desc", "").strip()
        })

    return schedule

def process_channel(channel_id, root):
    channel_name = channel_id.replace(".br", "").strip()
    filename = safe_filename(channel_name) + ".json"

    for day, folder in [(TODAY, "today"), (TOMORROW, "tomorrow")]:
        schedule = build_schedule(root, channel_id, day)
        if not schedule:
            continue

        os.makedirs(f"schedule/{folder}", exist_ok=True)

        with open(f"schedule/{folder}/{filename}", "w", encoding="utf-8") as f:
            json.dump({
                "channel": channel_name,
                "date": str(day),
                "schedule": schedule
            }, f, ensure_ascii=False, indent=2)

def main():
    with open("__channel.txt") as f:
        channels = [x.strip() for x in f if x.strip()]

    root = parse_xml()

    with ThreadPoolExecutor(max_workers=8) as executor:
        for ch in channels:
            executor.submit(process_channel, ch, root)

if __name__ == "__main__":
    main()
