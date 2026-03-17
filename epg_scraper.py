import requests
from bs4 import BeautifulSoup
import datetime
import pytz
import os
import json
import re

# --- CONFIGURATION ---
CHANNEL_FILE = "channel.txt"
LOG_FILE = "epg.log"
OUTPUT_DIR = "schedule"
TIMEZONE = pytz.timezone('America/Sao_Paulo')

URL_TEMPLATES = {
    "yesterday": "https://mi.tv/br/async/channel/{slug}/ontem/-180",
    "today":     "https://mi.tv/br/async/channel/{slug}/-180",
    "tomorrow":  "https://mi.tv/br/async/channel/{slug}/amanha/-180"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"
}

def log(message):
    """Writes to the log file and console."""
    timestamp = datetime.datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{timestamp}] {message}"
    print(msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def get_soup(url):
    """Fetches a URL and returns a BeautifulSoup object."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        log(f"ERROR fetching {url}: {e}")
        return None

def parse_page(soup):
    """
    Parses a single HTML page into a list of show dictionaries.
    Also extracts the channel display name.
    """
    if not soup:
        return None, []

    channel_name = "Unknown"
    info_div = soup.find("div", class_="channel-info")
    if info_div:
        img_tag = info_div.find("img")
        if img_tag and img_tag.get("title"):
            channel_name = img_tag.get("title")

    schedule_items = []
    ul = soup.find("ul", class_="broadcasts")
    if not ul:
        return channel_name, []

    lis = ul.find_all("li")
    for li in lis:
        try:
            h2 = li.find("h2")
            show_name = h2.get_text(strip=True) if h2 else ""

            time_span = li.find("span", class_="time")
            start_time = time_span.get_text(strip=True) if time_span else ""

            sub_title = li.find("span", class_="sub-title")
            category = sub_title.get_text(strip=True) if sub_title else ""

            p_synopsis = li.find("p", class_="synopsis")
            desc = p_synopsis.get_text(strip=True) if p_synopsis else ""

            logo_url = ""
            img_div = li.find("div", class_="image")
            if img_div and img_div.has_attr("style"):
                style_text = img_div["style"]
                match = re.search(r"url\('?(.*?)'?\)", style_text)
                if match:
                    logo_url = match.group(1)

            if start_time and show_name:
                # To save space, only add keys if they contain data
                item = {
                    "show": show_name,
                    "start": start_time
                }
                if logo_url: item["logo"] = logo_url
                if category: item["category"] = category
                if desc: item["episode"] = desc
                
                schedule_items.append(item)

        except Exception as e:
            log(f"Warning: Failed to parse a list item: {e}")
            continue

    return channel_name, schedule_items

def split_schedule_at_midnight(schedule_list):
    """
    Splits a list into [Part1 (Day), Part2 (Next Morning)].
    Detects when time drops (e.g. 23:59 -> 00:00).
    """
    if not schedule_list:
        return [], []

    split_index = len(schedule_list)

    for i in range(1, len(schedule_list)):
        prev_time_str = schedule_list[i-1]['start']
        curr_time_str = schedule_list[i]['start']
        
        try:
            prev_hour = int(prev_time_str.split(':')[0])
            curr_hour = int(curr_time_str.split(':')[0])
            
            if curr_hour < prev_hour:
                split_index = i
                break
        except:
            continue
            
    part_1 = schedule_list[:split_index]
    part_2 = schedule_list[split_index:]
    return part_1, part_2

def calculate_end_times(current_day_list, next_day_first_item=None):
    """
    Sets end_time = start_time of the NEXT show.
    """
    for i in range(len(current_day_list)):
        if i < len(current_day_list) - 1:
            current_day_list[i]['end'] = current_day_list[i+1]['start']
        else:
            if next_day_first_item:
                current_day_list[i]['end'] = next_day_first_item['start']
    return current_day_list

def save_json(filename, channel_name, combined_schedule):
    """Saves the combined data to a single highly-compressed JSON file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    file_path = os.path.join(OUTPUT_DIR, f"{filename}.json")
    
    final_json = {
        "channel": channel_name,
        "schedules": combined_schedule
    }
    
    with open(file_path, "w", encoding="utf-8") as f:
        # separators=(',', ':') removes all spaces, making the file as small as possible
        json.dump(final_json, f, ensure_ascii=False, separators=(',', ':'))
    log(f"Saved: {file_path}")

# --- MAIN EXECUTION ---
def main():
    with open(LOG_FILE, "w") as f:
        f.write(f"Starting Scraper run at {datetime.datetime.now(TIMEZONE)}\n")

    if not os.path.exists(CHANNEL_FILE):
        log(f"Error: {CHANNEL_FILE} not found.")
        return

    with open(CHANNEL_FILE, "r") as f:
        channels = [line.strip() for line in f if line.strip()]

    today_dt = datetime.datetime.now(TIMEZONE)
    tomorrow_dt = today_dt + datetime.timedelta(days=1)
    
    date_str_today = today_dt.strftime("%d/%m/%Y")
    date_str_tomorrow = tomorrow_dt.strftime("%d/%m/%Y")

    for slug in channels:
        log(f"Processing channel: {slug}")
        
        soup_yest = get_soup(URL_TEMPLATES["yesterday"].format(slug=slug))
        soup_today = get_soup(URL_TEMPLATES["today"].format(slug=slug))
        soup_tom = get_soup(URL_TEMPLATES["tomorrow"].format(slug=slug))

        name_y, list_y = parse_page(soup_yest)
        name_t, list_t = parse_page(soup_today)
        name_tm, list_tm = parse_page(soup_tom)

        channel_name = name_t if name_t != "Unknown" else slug

        _, yest_post_midnight = split_schedule_at_midnight(list_y)
        today_day_part, today_post_midnight = split_schedule_at_midnight(list_t)
        tom_day_part, _ = split_schedule_at_midnight(list_tm)

        full_today_schedule = yest_post_midnight + today_day_part
        full_tomorrow_schedule = today_post_midnight + tom_day_part

        first_show_tm = full_tomorrow_schedule[0] if full_tomorrow_schedule else None
        full_today_schedule = calculate_end_times(full_today_schedule, first_show_tm)
        full_tomorrow_schedule = calculate_end_times(full_tomorrow_schedule, None)

        # Combine schedules into a single dictionary
        combined_schedule = {}
        if full_today_schedule:
            combined_schedule[date_str_today] = full_today_schedule
        if full_tomorrow_schedule:
            combined_schedule[date_str_tomorrow] = full_tomorrow_schedule

        if combined_schedule:
            save_json(slug, channel_name, combined_schedule)
        else:
            log(f"Warning: No schedule found for {slug} (Today or Tomorrow)")

    log("Scraper finished.")

if __name__ == "__main__":
    main()
