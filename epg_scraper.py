import requests
import json
import os
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://mi.tv/br/async/channel"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}

os.makedirs("schedule/today", exist_ok=True)
os.makedirs("schedule/tomorrow", exist_ok=True)

LOG_FILE = "epg.log"


def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}\n")


def fetch_html(url):
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.text


def parse_shows(html):
    soup = BeautifulSoup(html, "html.parser")
    shows = []

    for li in soup.select("ul.broadcasts li"):
        time_el = li.select_one(".time")
        title_el = li.select_one("h2")
        desc_el = li.select_one(".synopsis")
        cat_el = li.select_one(".sub-title")
        img_el = li.select_one(".image")

        if not time_el or not title_el:
            continue

        start_time = time_el.text.strip()
        category = cat_el.text.strip() if cat_el else ""

        logo = ""
        if img_el and "background-image" in img_el.get("style", ""):
            match = re.search(r"url\('(.+?)'\)", img_el["style"])
            if match:
                logo = match.group(1)

        shows.append({
            "show_name": title_el.text.strip(),
            "start_time": start_time,
            "show_logo": logo,
            "show_category": category,
            "episode_description": desc_el.text.strip() if desc_el else ""
        })

    return shows


def time_to_minutes(time_str):
    """Convert HH:MM to minutes since midnight"""
    h, m = map(int, time_str.split(":"))
    return h * 60 + m


def build_schedule_with_end_times(shows):
    """Add end times based on next show's start time"""
    schedule = []
    
    for i, show in enumerate(shows):
        if i + 1 < len(shows):
            end_time = shows[i + 1]["start_time"]
        else:
            # Last show: add 30 minutes
            start_mins = time_to_minutes(show["start_time"])
            end_mins = start_mins + 30
            end_time = f"{end_mins // 60:02d}:{end_mins % 60:02d}"
        
        schedule.append({
            "show_name": show["show_name"],
            "show_logo": show["show_logo"],
            "show_category": show["show_category"],
            "start_time": show["start_time"],
            "end_time": end_time,
            "episode_description": show["episode_description"]
        })
    
    return schedule


def split_schedule_by_cutoff(shows, cutoff_time="05:47"):
    """
    Split shows into before and after cutoff time.
    Shows from 00:00 to cutoff go to 'early_morning'
    Shows from cutoff onwards go to 'daytime'
    """
    cutoff_mins = time_to_minutes(cutoff_time)
    
    early_morning = []  # 00:00 - 05:46
    daytime = []         # 05:47 - 23:59
    
    for show in shows:
        show_mins = time_to_minutes(show["start_time"])
        
        if show_mins < cutoff_mins:
            early_morning.append(show)
        else:
            daytime.append(show)
    
    return early_morning, daytime


def process_channel(channel):
    try:
        log(f"START channel: {channel}")

        # Fetch all three days
        html_yesterday = fetch_html(f"{BASE_URL}/{channel}/ontem/330")
        html_today = fetch_html(f"{BASE_URL}/{channel}/330")
        html_tomorrow = fetch_html(f"{BASE_URL}/{channel}/amanha/330")

        shows_yesterday = parse_shows(html_yesterday)
        shows_today = parse_shows(html_today)
        shows_tomorrow = parse_shows(html_tomorrow)

        # Split yesterday's schedule: we only want early morning (00:00-05:46)
        yesterday_early, _ = split_schedule_by_cutoff(shows_yesterday)
        
        # Split today's schedule: early morning + daytime
        today_early, today_daytime = split_schedule_by_cutoff(shows_today)
        
        # Split tomorrow's schedule: we only want daytime (05:47+)
        _, tomorrow_daytime = split_schedule_by_cutoff(shows_tomorrow)

        # Build TODAY's complete schedule:
        # Early morning from yesterday + Daytime from today
        today_complete = yesterday_early + today_daytime
        
        # Build TOMORROW's complete schedule:
        # Early morning from today + Daytime from tomorrow
        tomorrow_complete = today_early + tomorrow_daytime

        # Add end times
        today_schedule = build_schedule_with_end_times(today_complete)
        tomorrow_schedule = build_schedule_with_end_times(tomorrow_complete)

        # Get today's and tomorrow's dates
        today_date = datetime.now()
        tomorrow_date = today_date + timedelta(days=1)

        # Prepare filename and channel name
        filename = channel.lower().replace("_", "-") + ".json"
        channel_name = channel.replace("-", " ").title()

        # Save TODAY's schedule
        if not today_schedule:
            log(f"SKIPPED today → {channel} (no shows found)")
        else:
            with open(f"schedule/today/{filename}", "w", encoding="utf-8") as f:
                json.dump({
                    "channel": channel_name,
                    "date": today_date.strftime("%d/%m/%Y"),
                    "schedule": today_schedule
                }, f, ensure_ascii=False, indent=2)
            log(f"SAVED today → schedule/today/{filename} ({len(today_schedule)} shows)")

        # Save TOMORROW's schedule
        if not tomorrow_schedule:
            log(f"SKIPPED tomorrow → {channel} (no shows found)")
        else:
            with open(f"schedule/tomorrow/{filename}", "w", encoding="utf-8") as f:
                json.dump({
                    "channel": channel_name,
                    "date": tomorrow_date.strftime("%d/%m/%Y"),
                    "schedule": tomorrow_schedule
                }, f, ensure_ascii=False, indent=2)
            log(f"SAVED tomorrow → schedule/tomorrow/{filename} ({len(tomorrow_schedule)} shows)")

    except Exception as e:
        log(f"FAILED channel: {channel} | {e}")


def main():
    open(LOG_FILE, "w", encoding="utf-8").close()
    log("EPG Scraper started")

    with open("channel.txt", "r", encoding="utf-8") as f:
        channels = [c.strip() for c in f if c.strip()]

    log(f"Processing {len(channels)} channels")

    with ThreadPoolExecutor(max_workers=6) as executor:
        executor.map(process_channel, channels)

    log("EPG Scraper completed")


if __name__ == "__main__":
    main()
