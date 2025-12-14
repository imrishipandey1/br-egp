import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
import re

# São Paulo timezone
SP_TZ = pytz.timezone('America/Sao_Paulo')

def get_current_date_sp():
    """Get current date in São Paulo timezone"""
    return datetime.now(SP_TZ)

def parse_time(time_str, reference_date):
    """Parse time string and return datetime object"""
    time_obj = datetime.strptime(time_str.strip(), "%H:%M")
    return reference_date.replace(hour=time_obj.hour, minute=time_obj.minute, second=0, microsecond=0)

def fetch_schedule(url, retries=3):
    """Fetch schedule from URL with retry logic"""
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            if attempt == retries - 1:
                print(f"Failed to fetch {url} after {retries} attempts: {e}")
                return None
            print(f"Retry {attempt + 1} for {url}")
    return None

def extract_schedule(html, date_ref):
    """Extract schedule data from HTML"""
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'html.parser')
    broadcasts = soup.find('ul', class_='broadcasts')
    
    if not broadcasts:
        return []
    
    shows = []
    items = broadcasts.find_all('li')
    
    for item in items:
        try:
            # Extract show name
            h2 = item.find('h2')
            show_name = h2.get_text(strip=True) if h2 else None
            
            # Extract start time
            time_span = item.find('span', class_='time')
            start_time = time_span.get_text(strip=True) if time_span else None
            
            # Extract logo
            image_div = item.find('div', class_='image')
            logo = None
            if image_div and image_div.get('style'):
                style = image_div.get('style')
                match = re.search(r"url\('([^']+)'\)", style)
                if match:
                    logo = match.group(1)
            
            # Extract category
            sub_title = item.find('span', class_='sub-title')
            category = sub_title.get_text(strip=True) if sub_title else None
            
            # Extract description
            synopsis = item.find('p', class_='synopsis')
            description = synopsis.get_text(strip=True) if synopsis else None
            
            if show_name and start_time:
                shows.append({
                    'name': show_name,
                    'start_time': start_time,
                    'logo': logo,
                    'category': category,
                    'description': description
                })
        except Exception as e:
            print(f"Error extracting show data: {e}")
            continue
    
    return shows

def process_schedule(shows, date_ref):
    """Process schedule and calculate end times"""
    if not shows:
        return []
    
    processed = []
    
    for i, show in enumerate(shows):
        start_dt = parse_time(show['start_time'], date_ref)
        
        # Calculate end time
        if i < len(shows) - 1:
            end_dt = parse_time(shows[i + 1]['start_time'], date_ref)
            # Handle time crossing midnight
            if end_dt <= start_dt:
                end_dt += timedelta(days=1)
        else:
            # Last show - set to 23:59
            end_dt = date_ref.replace(hour=23, minute=59, second=0, microsecond=0)
        
        processed.append({
            'name': show['name'],
            'logo': show['logo'],
            'start_time': start_dt.strftime('%H:%M'),
            'end_time': end_dt.strftime('%H:%M'),
            'category': show['category'],
            'description': show['description']
        })
    
    return processed

def separate_by_day(schedule, cutoff_hour=6):
    """Separate schedule into current day and next day based on cutoff"""
    current_day = []
    next_day = []
    
    for show in schedule:
        hour = int(show['start_time'].split(':')[0])
        
        # Shows before cutoff hour belong to next day
        if hour < cutoff_hour:
            next_day.append(show)
        else:
            current_day.append(show)
    
    return current_day, next_day

def scrape_channel(line):
    """Scrape single channel"""
    try:
        parts = line.strip().split('=')
        if len(parts) != 2:
            print(f"Invalid line format: {line}")
            return None
        
        urls_part = parts[0].strip()
        channel_name = parts[1].strip()
        
        urls = [u.strip() for u in urls_part.split(',')]
        if len(urls) != 2:
            print(f"Expected 2 URLs for {channel_name}, got {len(urls)}")
            return None
        
        today_url, tomorrow_url = urls
        channel_id = channel_name.lower().replace(' ', '-')
        
        print(f"Processing: {channel_name}")
        
        # Get current date in SP timezone
        now = get_current_date_sp()
        today = now.date()
        tomorrow = today + timedelta(days=1)
        
        # Fetch schedules
        today_html = fetch_schedule(today_url)
        tomorrow_html = fetch_schedule(tomorrow_url)
        
        # Extract schedules
        today_shows = extract_schedule(today_html, now.replace(hour=0, minute=0, second=0, microsecond=0))
        tomorrow_shows = extract_schedule(tomorrow_html, now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
        
        if not today_shows and not tomorrow_shows:
            print(f"No schedule found for {channel_name}")
            return None
        
        # Process schedules
        today_processed = process_schedule(today_shows, now.replace(hour=0, minute=0, second=0, microsecond=0))
        tomorrow_processed = process_schedule(tomorrow_shows, now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))
        
        # Separate by day (handle early morning shows)
        today_final, today_overflow = separate_by_day(today_processed)
        tomorrow_final, tomorrow_overflow = separate_by_day(tomorrow_processed)
        
        # Merge overflow from today into tomorrow
        today_schedule = today_final + today_overflow
        
        # Filter tomorrow to only include up to 23:59
        tomorrow_schedule = [s for s in tomorrow_final if s['end_time'] <= '23:59']
        
        # Create JSON structure
        data = {
            'channel': channel_name,
            'today': {
                'date': today.strftime('%Y-%m-%d'),
                'schedule': today_schedule
            },
            'tomorrow': {
                'date': tomorrow.strftime('%Y-%m-%d'),
                'schedule': tomorrow_schedule
            }
        }
        
        return (channel_id, data)
        
    except Exception as e:
        print(f"Error processing channel {line}: {e}")
        return None

def main():
    # Read channels file
    if not os.path.exists('channel.txt'):
        print("channel.txt not found!")
        return
    
    with open('channel.txt', 'r', encoding='utf-8') as f:
        lines = [line for line in f.readlines() if line.strip()]
    
    # Create schedule directory
    os.makedirs('schedule', exist_ok=True)
    
    # Process channels in parallel (max 10 threads for GitHub free tier)
    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(scrape_channel, line): line for line in lines}
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)
    
    # Save JSON files
    for channel_id, data in results:
        filepath = os.path.join('schedule', f'{channel_id}.json')
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Saved: {filepath}")
    
    print(f"\nCompleted! Processed {len(results)} channels")

if __name__ == '__main__':
    main()
