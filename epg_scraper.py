import requests
import json
import os
import re
from bs4 import BeautifulSoup
from datetime import datetime

BASE_URL = "https://mi.tv/br/async/channel"
LOG_FILE = "epg.log"

TODAY_DIR = "schedule/today"
TOMORROW_DIR = "schedule/tomorrow"

os.makedirs(TODAY_DIR, exist_ok=True)
os.makedirs(TOMORROW_DIR, exist_ok=True)

# ---------------- LOGGING ----------------

def log(msg):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

open(LOG_FILE, "w").close()  # overwrite log every run

# ---------------- HELPERS ----------------

MONTHS_PT = {
    "janeiro": "01", "fevereiro": "02", "março": "03",
    "abril": "04", "maio": "05", "junho": "06",
    "julho": "07", "agosto": "08", "setembro": "09",
    "outubro": "10", "novembro": "11", "dezembro": "12"
}

def parse_date(h1_text):
    # Sexta 23 de janeiro
    match = re.search(r"(\d{1,2}) de ([a-zç]+)", h1_text.lower())
    if not match:
        return ""
    day = match.group(1).zfill(2)
    month = MONTHS_PT.get(match.group(2), "01")
    year = datetime.now().year
    return f"{day}/{month}/{year}"

def time_to_minutes(t):
    h, m = map(int, t.split(":"))
    return h * 60 + m

# ---------------- FETCH ----------------

def fetch_page(url):
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.text

def parse_schedule(html):
    soup = BeautifulSoup(html, "html.parser")

    channel = soup.select_one(".channel-info img")
    channel_name = channel["title"].strip()

    h1 = soup.select_one(".channel-info h1 span")
    date = parse_date(h1.text.strip()) if h1 else ""

    items = []

    broadcasts = soup.select("ul.broadcasts li")
    for li in broadcasts:
        time_el = li.select_one("span.time")
        title_el = li.select_one("h2")
        cat_el = li.select_one("span.sub-title")
        desc_el = li.select_one("p.synopsis")
        img_el = li.select_one(".image")

        if not time_el or not title_el:
            continue

        img_url = ""
        if img_el and "background-image" in img_el.get("style", ""):
            img_url = re.search(r"url\('(.+?)'\)", img_el["style"]).group(1)

        items.append({
            "start_time": time_el.text.strip(),
            "show_name": title_el.text.strip(),
            "show_category": cat_el.text.strip() if cat_el else "",
            "episode_description": desc_el.text.strip() if desc_el else "",
            "show_logo": img_url
        })

    return channel_name, date, items

# ---------------- MERGE LOGIC ----------------

def build_day_schedule(primary, secondary):
    merged = primary + secondary
    merged.sort(key=lambda x: time_to_minutes(x["start_time"]))

    for i in range(len(merged)):
        if i + 1 < len(merged):
            merged[i]["end_time"] = merged[i + 1]["start_time"]
        else:
            merged[i]["end_time"] = "23:59"

    return merged

# ---------------- MAIN ----------------

def run():
    with open("channel.txt", "r", encoding="utf-8") as f:
        channels = [c.strip() for c in f if c.strip()]

    for slug in channels:
        try:
            yesterday_url = f"{BASE_URL}/{slug}/ontem/330"
            today_url = f"{BASE_URL}/{slug}/330"
            tomorrow_url = f"{BASE_URL}/{slug}/amanha/330"

            ch_y, d_y, y_items = parse_schedule(fetch_page(yesterday_url))
            ch_t, d_t, t_items = parse_schedule(fetch_page(today_url))
            ch_tm, d_tm, tm_items = parse_schedule(fetch_page(tomorrow_url))

            today_schedule = build_day_schedule(y_items, t_items)
            tomorrow_schedule = build_day_schedule(t_items, tm_items)

            today_json = {
                "channel": ch_t,
                "date": d_t,
                "schedule": today_schedule
            }

            tomorrow_json = {
                "channel": ch_tm,
                "date": d_tm,
                "schedule": tomorrow_schedule
            }

            with open(f"{TODAY_DIR}/{slug}.json", "w", encoding="utf-8") as f:
                json.dump(today_json, f, ensure_ascii=False, indent=2)

            with open(f"{TOMORROW_DIR}/{slug}.json", "w", encoding="utf-8") as f:
                json.dump(tomorrow_json, f, ensure_ascii=False, indent=2)

            log(f"SUCCESS: {slug}")

        except Exception as e:
            log(f"FAILED: {slug} | {str(e)}")

if __name__ == "__main__":
    run()