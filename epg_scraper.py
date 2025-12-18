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
    response = requests.get(EPG_URL, timeout=60)
    response.raise_for_status()
    return ET.fromstring(response.content)

def parse_time(value):
    dt = datetime.strptime(value[:14], "%Y%m%d%H%M%S")
    return dt.replace(tzinfo=UTC).astimezone(TIMEZONE)

def clamp_time(start, end, day):
    day_start = datetime.combine(day, time(0, 0), TIMEZONE)
    day_end = datetime.combine(day, time(23, 59), TIMEZONE)

    start = max(start, day_start)
    end = min(end, day_end)

    if start >= end:
        return None
    return start, end

def format_time(dt):
    return dt.strftime("%I:%M %p").lstrip("0")

def build_schedule(root, channel_id, day):
    schedule = []

    for p in root.findall("programme"):
        if p.attrib.get("channel") != channel_id:
            continue

        start = parse_time(p.attrib["start"])
        end = parse_time(p.attrib["stop"])

        clamped = clamp_time(start, end, day)
        if not clamped:
            continue

        start, end = clamped

        schedule.append({
            "show_name": p.findtext("title", "").strip(),
            "show_logo": "",
            "start_time": format_time(start),
            "end_time": format_time(end),
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

        data = {
            "channel": channel_name,
            "date": str(day),
            "schedule": schedule
        }

        os.makedirs(f"schedule/{folder}", exist_ok=True)
        with open(f"schedule/{folder}/{filename}", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    with open("__channel.txt") as f:
        channels = [line.strip() for line in f if line.strip()]

    root = parse_xml()

    with ThreadPoolExecutor(max_workers=8) as executor:
        for ch in channels:
            executor.submit(process_channel, ch, root)

if __name__ == "__main__":
    main()
