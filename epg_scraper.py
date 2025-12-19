import requests
import json
import os
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, time
from concurrent.futures import ThreadPoolExecutor
import pytz

BASE_URL = "https://mi.tv/br/async/channel"
TIMEZONE = pytz.timezone("America/Sao_Paulo")

START_DAY = time(5, 30)
END_DAY = time(23, 59)
MIDNIGHT = time(0, 0)
END_NIGHT = time(5, 29)

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

os.makedirs("schedule/today", exist_ok=True)
os.makedirs("schedule/tomorrow", exist_ok=True)

LOG_FILE = "epg.log"


def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} | {msg}\n")


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


def build_schedule(shows, target_date):
    schedule = []

    for i, show in enumerate(shows):
        start_t = datetime.strptime(show["start_time"], "%H:%M").time()
        start_dt = TIMEZONE.localize(datetime.combine(target_date, start_t))

        if i + 1 < len(shows):
            next_t = datetime.strptime(shows[i + 1]["start_time"], "%H:%M").time()
            end_dt = TIMEZONE.localize(datetime.combine(target_date, next_t))
        else:
            end_dt = start_dt + timedelta(minutes=30)

        schedule.append({
            "show_name": show["show_name"],
            "show_logo": show["show_logo"],
            "show_category": show["show_category"],
            "start_time": start_dt.strftime("%H:%M"),
            "end_time": end_dt.strftime("%H:%M"),
            "episode_description": show["episode_description"]
        })

    return schedule


def filter_by_time(schedule, start_time, end_time):
    return [
        s for s in schedule
        if start_time <= datetime.strptime(s["start_time"], "%H:%M").time() <= end_time
    ]


def process_channel(channel):
    try:
        log(f"START channel: {channel}")

        html_y = fetch_html(f"{BASE_URL}/{channel}/ontem/330")
        html_t = fetch_html(f"{BASE_URL}/{channel}/330")
        html_tm = fetch_html(f"{BASE_URL}/{channel}/amanha/330")

        shows_y = parse_shows(html_y)
        shows_t = parse_shows(html_t)
        shows_tm = parse_shows(html_tm)

        today_date = datetime.now(TIMEZONE).date()
        tomorrow_date = today_date + timedelta(days=1)

        today_schedule = (
            filter_by_time(build_schedule(shows_y, today_date), MIDNIGHT, END_NIGHT)
            + filter_by_time(build_schedule(shows_t, today_date), START_DAY, END_DAY)
        )

        tomorrow_schedule = (
            filter_by_time(build_schedule(shows_t, tomorrow_date), MIDNIGHT, END_NIGHT)
            + filter_by_time(build_schedule(shows_tm, tomorrow_date), START_DAY, END_DAY)
        )

        filename = channel.lower().replace("_", "-") + ".json"
        channel_name = channel.replace("-", " ").title()

        # -------- NEW LOGIC (ONLY CHANGE) -------- #

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

    with open("channel.txt", "r", encoding="utf-8") as f:
        channels = [c.strip() for c in f if c.strip()]

    with ThreadPoolExecutor(max_workers=6) as executor:
        executor.map(process_channel, channels)


if __name__ == "__main__":
    main()
