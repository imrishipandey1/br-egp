import xml.etree.ElementTree as ET
import requests
from datetime import datetime, timedelta
import pytz
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# Constants
EPG_URL = "https://raw.githubusercontent.com/globetvapp/epg/refs/heads/main/Brazil/brazil1.xml"
BRAZIL_TZ = pytz.timezone('America/Sao_Paulo')
UTC_TZ = pytz.UTC

def parse_epg_time(time_str):
    """Parse EPG time format: 20251218013000 +0000 to datetime object"""
    # Remove timezone offset
    time_part = time_str.split()[0]
    # Parse: YYYYMMDDHHmmss
    dt = datetime.strptime(time_part, "%Y%m%d%H%M%S")
    # Set as UTC
    dt = UTC_TZ.localize(dt)
    # Convert to Brazilian time
    return dt.astimezone(BRAZIL_TZ)

def format_time_12h(dt):
    """Format datetime to 12-hour format with AM/PM"""
    return dt.strftime("%I:%M %p").lstrip('0')

def get_channel_name(channel_id):
    """Remove .br extension from channel ID"""
    return channel_id.replace('.br', '').strip()

def get_date_range_brazil(day_offset=0):
    """Get start and end datetime for a day in Brazilian timezone"""
    now_brazil = datetime.now(BRAZIL_TZ)
    target_date = now_brazil.date() + timedelta(days=day_offset)
    
    # Start: 00:00:00 of target date
    start_dt = BRAZIL_TZ.localize(datetime.combine(target_date, datetime.min.time()))
    # End: 23:59:59 of target date
    end_dt = BRAZIL_TZ.localize(datetime.combine(target_date, datetime.max.time()))
    
    return start_dt, end_dt, target_date

def parse_epg_for_channel(xml_content, channel_id, day_offset=0):
    """Parse EPG XML and extract schedule for a specific channel"""
    root = ET.fromstring(xml_content)
    
    start_dt, end_dt, target_date = get_date_range_brazil(day_offset)
    
    schedule = []
    
    # Find all programme elements for this channel
    for programme in root.findall(f".//programme[@channel='{channel_id}']"):
        try:
            # Parse times
            start_time_str = programme.get('start')
            stop_time_str = programme.get('stop')
            
            if not start_time_str or not stop_time_str:
                continue
            
            start_time = parse_epg_time(start_time_str)
            end_time = parse_epg_time(stop_time_str)
            
            # Check if the show airs during the target day
            # Include if: show starts OR ends during target day, OR spans across the entire day
            show_airs_during_day = (
                (start_time <= end_dt and end_time >= start_dt)
            )
            
            if not show_airs_during_day:
                continue
            
            # Adjust times to fit within the target day
            display_start = max(start_time, start_dt)
            display_end = min(end_time, end_dt)
            
            # Get show details
            title_elem = programme.find('title')
            desc_elem = programme.find('desc')
            
            show_name = title_elem.text if title_elem is not None else "Unknown"
            description = desc_elem.text if desc_elem is not None else ""
            
            schedule.append({
                "show_name": show_name,
                "show_logo": "",
                "start_time": format_time_12h(display_start),
                "end_time": format_time_12h(display_end),
                "episode_description": description,
                "_sort_time": display_start  # For sorting
            })
        except Exception as e:
            print(f"Error parsing programme for {channel_id}: {e}")
            continue
    
    # Sort by start time
    schedule.sort(key=lambda x: x['_sort_time'])
    
    # Remove sort key before returning
    for item in schedule:
        del item['_sort_time']
    
    return schedule, target_date

def process_channel(xml_content, channel_id, day_offset, output_dir):
    """Process a single channel and save to JSON"""
    try:
        channel_name = get_channel_name(channel_id)
        day_label = "today" if day_offset == 0 else "tomorrow"
        print(f"Processing {channel_name} for {day_label}...")
        
        schedule, target_date = parse_epg_for_channel(xml_content, channel_id, day_offset)
        
        if not schedule:
            print(f"No schedule found for {channel_name} on {target_date}")
            return False
        
        # Create output data
        output_data = {
            "channel": channel_name,
            "date": target_date.strftime("%Y-%m-%d"),
            "schedule": schedule
        }
        
        # Create filename (replace spaces with hyphens, case insensitive)
        filename = re.sub(r'\s+', '-', channel_name) + '.json'
        filepath = os.path.join(output_dir, filename)
        
        # Save to JSON
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        print(f"✓ Saved {channel_name} schedule to {filepath} ({len(schedule)} shows)")
        return True
        
    except Exception as e:
        print(f"✗ Error processing {channel_id}: {e}")
        return False

def load_channels(channel_file='__channel.txt'):
    """Load channel IDs from file"""
    if not os.path.exists(channel_file):
        print(f"Channel file {channel_file} not found!")
        return []
    
    with open(channel_file, 'r', encoding='utf-8') as f:
        channels = [line.strip() for line in f if line.strip()]
    
    print(f"Loaded {len(channels)} channels from {channel_file}")
    return channels

def main():
    # Create output directories
    os.makedirs('schedule/today', exist_ok=True)
    os.makedirs('schedule/tomorrow', exist_ok=True)
    
    # Load channels
    channels = load_channels()
    if not channels:
        print("No channels to process!")
        return
    
    # Fetch EPG XML once
    try:
        xml_content = fetch_epg_xml()
        print(f"EPG data fetched successfully ({len(xml_content)} bytes)")
    except Exception as e:
        print(f"Failed to fetch EPG data: {e}")
        return
    
    # Process all channels in parallel
    tasks = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all tasks
        for channel_id in channels:
            # Today
            tasks.append(executor.submit(process_channel, xml_content, channel_id, 0, 'schedule/today'))
            # Tomorrow
            tasks.append(executor.submit(process_channel, xml_content, channel_id, 1, 'schedule/tomorrow'))
        
        # Wait for completion
        successful = 0
        failed = 0
        for future in as_completed(tasks):
            if future.result():
                successful += 1
            else:
                failed += 1
    
    print(f"\n{'='*60}")
    print(f"Processing complete!")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
