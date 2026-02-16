import requests
from bs4 import BeautifulSoup
import json
import datetime
import os
import re
import sys

# --- CONFIGURATION ---
BASE_URL = "https://mi.tv/br/async/channel/{}/{}/-180"
CHANNELS_FILE = "channel.txt"
LOG_FILE = "epg.log"
OUTPUT_DIR = "schedule"

# Ensure output directories exist
os.makedirs(os.path.join(OUTPUT_DIR, "today"), exist_ok=True)
os.makedirs(os.path.join(OUTPUT_DIR, "tomorrow"), exist_ok=True)

def log_message(message):
    """Writes to both console and log file."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_msg = f"[{timestamp}] {message}"
    print(formatted_msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(formatted_msg + "\n")

def get_soup(url):
    """Fetches URL and returns BeautifulSoup object."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        log_message(f"Error fetching {url}: {e}")
        return None

def parse_schedule_items(soup, base_date):
    """
    Parses the HTML list items and assigns a rough datetime.
    Returns a list of dictionaries.
    """
    items = []
    if not soup:
        return items

    # Find the channel name from the page title or image alt
    channel_name_tag = soup.find("div", class_="channel-info")
    channel_name = "Unknown"
    if channel_name_tag:
        img = channel_name_tag.find("img")
        if img and img.get("title"):
            channel_name = img.get("title")

    broadcasts = soup.find("ul", class_="broadcasts")
    if not broadcasts:
        return items

    lis = broadcasts.find_all("li")
    
    current_date = base_date
    last_time_obj = None

    for li in lis:
        # Extract Start Time
        time_span = li.find("span", class_="time")
        if not time_span:
            continue
        time_str = time_span.get_text(strip=True)
        
        # Convert to time object
        try:
            t = datetime.datetime.strptime(time_str, "%H:%M").time()
        except ValueError:
            continue

        # --- ROLLOVER LOGIC ---
        # If current time is less than previous time (e.g., 00:00 < 23:00), 
        # we have crossed into the next day.
        if last_time_obj and t < last_time_obj:
            current_date += datetime.timedelta(days=1)
        
        last_time_obj = t
        
        # Build full datetime
        dt = datetime.datetime.combine(current_date, t)

        # Extract other details
        show_name = li.find("h2").get_text(strip=True) if li.find("h2") else ""
        
        # Extract Category
        category = li.find("span", class_="sub-title").get_text(strip=True) if li.find("span", class_="sub-title") else ""
        
        # Extract Description
        desc = li.find("p", class_="synopsis").get_text(strip=True) if li.find("p", class_="synopsis") else ""
        
        # Extract Logo from style attribute
        logo_url = ""
        img_div = li.find("div", class_="image")
        if img_div and img_div.has_attr("style"):
            style_attr = img_div["style"]
            # Regex to find url('...')
            match = re.search(r"url\('?(.*?)'?\)", style_attr)
            if match:
                logo_url = match.group(1)

        items.append({
            "datetime": dt, # Used for sorting/filtering
            "channel": channel_name,
            "show_name": show_name,
            "show_logo": logo_url,
            "show_category": category,
            "start_time": time_str,
            "episode_description": desc
        })

    return items

def process_channel(channel_slug):
    log_message(f"Processing: {channel_slug}")
    
    today_date = datetime.date.today()
    yesterday_date = today_date - datetime.timedelta(days=1)
    tomorrow_date = today_date + datetime.timedelta(days=1)

    # 1. Fetch all three URLs
    # Structure: BASE_URL.format(slug, day_string)
    # Note: 'ontem' = yesterday, '' (empty) = today, 'amanha' = tomorrow
    
    url_yesterday = BASE_URL.format(channel_slug, "ontem")
    url_today = BASE_URL.format(channel_slug, "") # Empty string for today path
    url_tomorrow = BASE_URL.format(channel_slug, "amanha")

    # Pass the 'base_date' assuming the broadcast starts on that day
    # (The parse function handles the rollover to the next day)
    items_y = parse_schedule_items(get_soup(url_yesterday), yesterday_date)
    items_t = parse_schedule_items(get_soup(url_today), today_date)
    items_tom = parse_schedule_items(get_soup(url_tomorrow), tomorrow_date)

    # 2. Stitch them together
    all_items = items_y + items_t + items_tom
    
    # Sort just in case (essential for calculating end_time)
    all_items.sort(key=lambda x: x["datetime"])

    # 3. Calculate End Times
    for i in range(len(all_items)):
        current = all_items[i]
        if i < len(all_items) - 1:
            next_item = all_items[i + 1]
            current["end_time"] = next_item["start_time"]
        else:
            # Last item: unknown end time, leave empty or estimate
            current["end_time"] = "" 

    # 4. Filter and Save (Today and Tomorrow)
    save_json(all_items, today_date, "today", channel_slug)
    save_json(all_items, tomorrow_date, "tomorrow", channel_slug)

def save_json(all_items, target_date, folder_name, channel_slug):
    """Filters items strictly for target_date and saves JSON."""
    
    filtered_schedule = []
    channel_name_display = "Unknown"

    for item in all_items:
        # Strict Calendar Filter: 00:00 to 23:59
        if item["datetime"].date() == target_date:
            channel_name_display = item["channel"] # Capture channel name
            
            filtered_schedule.append({
                "show_name": item["show_name"],
                "show_logo": item["show_logo"],
                "show_category": item["show_category"],
                "start_time": item["start_time"],
                "end_time": item["end_time"],
                "episode_description": item["episode_description"]
            })

    if not filtered_schedule:
        log_message(f"Warning: No shows found for {folder_name} ({target_date}) for {channel_slug}")
        return

    output_data = {
        "channel": channel_name_display,
        "date": target_date.strftime("%d/%m/%Y"),
        "schedule": filtered_schedule
    }

    file_path = os.path.join(OUTPUT_DIR, folder_name, f"{channel_slug}.json")
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        log_message(f"Saved {folder_name} schedule for {channel_slug}")
    except Exception as e:
        log_message(f"Failed to save JSON for {channel_slug}: {e}")

def main():
    # Initialize log
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"EPG Scraper Run: {datetime.datetime.now()}\n")

    if not os.path.exists(CHANNELS_FILE):
        log_message(f"Error: {CHANNELS_FILE} not found.")
        return

    with open(CHANNELS_FILE, "r") as f:
        channels = [line.strip() for line in f if line.strip()]

    for channel in channels:
        try:
            process_channel(channel)
        except Exception as e:
            log_message(f"Critical error processing {channel}: {e}")

if __name__ == "__main__":
    main()
