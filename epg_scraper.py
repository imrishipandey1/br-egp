import xml.etree.ElementTree as ET
import json
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request
import urllib.parse
from zoneinfo import ZoneInfo

EPG_URL = "https://raw.githubusercontent.com/globetvapp/epg/refs/heads/main/Brazil/brazil1.xml"
CHANNEL_FILE = "channel.txt"
OUTPUT_DIR = "schedule"
LOGO_BASE_URL = "https://cdn-er-images.online.meo.pt/eemstb/ImageHandler.ashx"

def download_epg():
    """Download EPG XML file"""
    print("Downloading EPG data...")
    with urllib.request.urlopen(EPG_URL) as response:
        return response.read()

def parse_time(time_str):
    """Convert XML time format to Brazilian datetime"""
    # Format: 20251216063000 +0000
    dt_part = time_str.split()[0]
    dt = datetime.strptime(dt_part, "%Y%m%d%H%M%S")
    # Assume UTC, convert to Sao Paulo time
    dt_utc = dt.replace(tzinfo=ZoneInfo("UTC"))
    dt_br = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
    return dt_br

def format_brazilian_datetime(dt):
    """Format datetime in Brazilian format: DD/MM/YYYY HH:MM:SS"""
    return dt.strftime("%d/%m/%Y %H:%M:%S")

def generate_logo_url(show_title, channel_call_letter):
    """Generate logo URL with proper UTF-8 encoding"""
    encoded_title = urllib.parse.quote(show_title, safe='')
    
    params = {
        'progTitle': encoded_title,
        'chCallLetter': channel_call_letter,
        'profile': '16_9',
        'appSource': 'GuiaTV',
        'width': '160',
        'stb': 'retina2x'
    }
    
    query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
    return f"{LOGO_BASE_URL}?{query_string}"

def get_channel_id_from_name(channel_name, root):
    """Find channel ID that starts with the given channel name"""
    for channel in root.findall('.//channel'):
        channel_id = channel.get('id', '')
        # Remove extension and compare
        channel_name_from_id = channel_id.rsplit('.', 1)[0]
        if channel_name_from_id == channel_name:
            return channel_id
    return None

def process_channel(channel_name, xml_content):
    """Process a single channel and generate JSON"""
    try:
        print(f"Processing channel: {channel_name}")
        
        # Parse XML
        root = ET.fromstring(xml_content)
        
        # Find channel ID
        channel_id = get_channel_id_from_name(channel_name, root)
        if not channel_id:
            print(f"Channel ID not found for: {channel_name}")
            return None
        
        print(f"Found channel ID: {channel_id}")
        
        # Get today and tomorrow in Brazilian timezone
        now_br = datetime.now(ZoneInfo("America/Sao_Paulo"))
        today_start = now_br.replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow_end = (today_start + timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Extract channel call letter (remove .br extension)
        channel_call_letter = channel_id.rsplit('.', 1)[0]
        
        # Find all programmes for this channel
        programmes = []
        for programme in root.findall('.//programme'):
            if programme.get('channel') == channel_id:
                start_time = parse_time(programme.get('start'))
                end_time = parse_time(programme.get('stop'))
                
                # Only include today and tomorrow
                if today_start <= start_time < tomorrow_end:
                    title_elem = programme.find('title')
                    desc_elem = programme.find('desc')
                    
                    title = title_elem.text if title_elem is not None and title_elem.text else ""
                    description = desc_elem.text if desc_elem is not None and desc_elem.text else ""
                    
                    # Generate logo URL
                    logo_url = generate_logo_url(title, channel_call_letter) if title else ""
                    
                    programmes.append({
                        "show_name": title,
                        "show_logo": logo_url,
                        "start_time": format_brazilian_datetime(start_time),
                        "end_time": format_brazilian_datetime(end_time),
                        "description": description
                    })
        
        # Sort by start time
        programmes.sort(key=lambda x: datetime.strptime(x['start_time'], "%d/%m/%Y %H:%M:%S"))
        
        # Create JSON structure
        result = {
            "channel_name": channel_name,
            "channel_id": channel_id,
            "schedule": programmes
        }
        
        # Save to JSON file
        filename = channel_name.lower().replace(' ', '-') + '.json'
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"Saved {len(programmes)} programmes for {channel_name} to {filename}")
        return filename
        
    except Exception as e:
        print(f"Error processing channel {channel_name}: {str(e)}")
        return None

def main():
    """Main function to process all channels"""
    try:
        # Download EPG data
        xml_content = download_epg()
        
        # Read channel list
        if not os.path.exists(CHANNEL_FILE):
            print(f"Error: {CHANNEL_FILE} not found")
            return
        
        with open(CHANNEL_FILE, 'r', encoding='utf-8') as f:
            channels = [line.strip() for line in f if line.strip()]
        
        print(f"Found {len(channels)} channels to process")
        
        # Process channels in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(process_channel, channel, xml_content): channel 
                for channel in channels
            }
            
            for future in as_completed(futures):
                channel = futures[future]
                try:
                    result = future.result()
                    if result:
                        print(f"✓ Completed: {channel}")
                    else:
                        print(f"✗ Failed: {channel}")
                except Exception as e:
                    print(f"✗ Exception for {channel}: {str(e)}")
        
        print("\nAll channels processed successfully!")
        
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
