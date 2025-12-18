#!/usr/bin/env python3
"""
EPG Scraper - Fetches TV schedules from EPG XML and saves to JSON
Converts UTC to São Paulo timezone and filters 24-hour schedules
"""

import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import pytz
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Timezone configuration
UTC = pytz.UTC
SAO_PAULO_TZ = pytz.timezone('America/Sao_Paulo')

class EPGScraper:
    def __init__(self, epg_url: str, channels_file: str):
        self.epg_url = epg_url
        self.channels_file = channels_file
        self.epg_data = None
        self.channels = []
        
    def fetch_epg(self) -> bool:
        """Fetch EPG XML from URL"""
        try:
            logger.info(f"Fetching EPG from {self.epg_url}")
            response = requests.get(self.epg_url, timeout=30)
            response.raise_for_status()
            self.epg_data = ET.fromstring(response.content)
            logger.info("EPG fetched successfully")
            return True
        except Exception as e:
            logger.error(f"Error fetching EPG: {e}")
            return False
    
    def load_channels(self) -> bool:
        """Load channel list from file"""
        try:
            if not os.path.exists(self.channels_file):
                logger.warning(f"Channels file not found: {self.channels_file}")
                return False
            
            with open(self.channels_file, 'r', encoding='utf-8') as f:
                self.channels = [line.strip() for line in f if line.strip()]
            
            logger.info(f"Loaded {len(self.channels)} channels from {self.channels_file}")
            return True
        except Exception as e:
            logger.error(f"Error loading channels: {e}")
            return False
    
    def parse_datetime(self, dt_string: str) -> Optional[datetime]:
        """
        Parse datetime from EPG format: '20251218013000 +0000'
        Returns UTC datetime object
        """
        try:
            # Remove timezone info from string
            dt_clean = dt_string.split()[0]
            # Parse: YYYYMMDDHHMMSS
            dt = datetime.strptime(dt_clean, '%Y%m%d%H%M%S')
            # Make it timezone aware (UTC)
            dt_utc = UTC.localize(dt)
            return dt_utc
        except Exception as e:
            logger.warning(f"Error parsing datetime '{dt_string}': {e}")
            return None
    
    def format_time_br(self, dt: datetime) -> str:
        """Format time in Brazilian format (HH:MM)"""
        return dt.strftime('%H:%M')
    
    def format_date_br(self, dt: datetime) -> str:
        """Format date in Brazilian format (YYYY-MM-DD)"""
        return dt.strftime('%Y-%m-%d')
    
    def get_24h_schedule(self, channel_id: str, target_date: datetime) -> List[Dict]:
        """
        Get 24-hour schedule (00:00-23:59) for a specific channel and date
        Handles shows that span midnight and cross-day boundaries
        """
        schedule = []
        
        # Define target date range (00:00 to 23:59:59 in São Paulo timezone)
        target_date_sp = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        target_date_sp = SAO_PAULO_TZ.localize(target_date_sp)
        target_date_end_sp = target_date_sp + timedelta(hours=24)
        
        # Convert to UTC for comparison with EPG data
        target_date_utc = target_date_sp.astimezone(UTC)
        target_date_end_utc = target_date_end_sp.astimezone(UTC)
        
        try:
            # Find all programmes for this channel
            for programme in self.epg_data.findall('programme'):
                if programme.get('channel') != channel_id:
                    continue
                
                # Parse start and stop times
                start_str = programme.get('start')
                stop_str = programme.get('stop')
                
                if not start_str or not stop_str:
                    continue
                
                start_utc = self.parse_datetime(start_str)
                stop_utc = self.parse_datetime(stop_str)
                
                if not start_utc or not stop_utc:
                    continue
                
                # Convert to São Paulo timezone for display
                start_sp = start_utc.astimezone(SAO_PAULO_TZ)
                stop_sp = stop_utc.astimezone(SAO_PAULO_TZ)
                
                # Check if programme overlaps with target date
                # Include if: start is before target_date_end AND stop is after target_date_start
                if start_utc >= target_date_end_utc or stop_utc <= target_date_utc:
                    continue
                
                # Extract show information
                title = programme.find('title')
                show_name = title.text if title is not None else "Unknown"
                
                desc = programme.find('desc')
                episode_description = desc.text if desc is not None else ""
                
                # Logo not available in this EPG format
                show_logo = ""
                
                # Determine actual start and end times for this date
                # Start time: max of programme start and target date start
                actual_start = max(start_sp, target_date_sp)
                
                # End time: min of programme stop and target date end
                actual_end = min(stop_sp, target_date_end_sp)
                
                # Only include if there's actual content for this day
                if actual_start < actual_end:
                    entry = {
                        "show_name": show_name,
                        "show_logo": show_logo,
                        "start_time": self.format_time_br(actual_start),
                        "end_time": self.format_time_br(actual_end),
                        "episode_description": episode_description
                    }
                    schedule.append(entry)
        
        except Exception as e:
            logger.error(f"Error getting schedule for channel {channel_id}: {e}")
        
        return schedule
    
    def format_channel_name(self, channel_id: str) -> str:
        """
        Format channel name for filename
        Input: 'Record TV.br' -> Output: 'Record-TV'
        """
        # Remove .br or similar extensions
        name = channel_id.rsplit('.', 1)[0] if '.' in channel_id else channel_id
        # Replace spaces and special chars with dash
        name = name.replace(' ', '-').replace('.', '-').lower()
        return name
    
    def scrape_channel(self, channel_id: str, today: datetime, tomorrow: datetime) -> Dict:
        """Scrape schedule for a single channel"""
        try:
            logger.info(f"Processing channel: {channel_id}")
            
            # Get 24-hour schedules
            today_schedule = self.get_24h_schedule(channel_id, today)
            tomorrow_schedule = self.get_24h_schedule(channel_id, tomorrow)
            
            # Extract channel display name
            channel_display = channel_id.rsplit('.', 1)[0] if '.' in channel_id else channel_id
            
            # Format filenames
            channel_filename = self.format_channel_name(channel_id)
            
            result = {
                'channel_id': channel_id,
                'channel_filename': channel_filename,
                'channel_display': channel_display,
                'today': {
                    'data': {
                        'channel': channel_display,
                        'date': self.format_date_br(today),
                        'schedule': today_schedule
                    },
                    'count': len(today_schedule)
                },
                'tomorrow': {
                    'data': {
                        'channel': channel_display,
                        'date': self.format_date_br(tomorrow),
                        'schedule': tomorrow_schedule
                    },
                    'count': len(tomorrow_schedule)
                }
            }
            
            logger.info(f"✓ {channel_id}: {result['today']['count']} shows today, {result['tomorrow']['count']} shows tomorrow")
            return result
        
        except Exception as e:
            logger.error(f"Error processing channel {channel_id}: {e}")
            return None
    
    def save_json(self, directory: str, filename: str, data: Dict) -> bool:
        """Save data to JSON file"""
        try:
            Path(directory).mkdir(parents=True, exist_ok=True)
            filepath = os.path.join(directory, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved: {filepath}")
            return True
        except Exception as e:
            logger.error(f"Error saving to {filepath}: {e}")
            return False
    
    def run(self, max_workers: int = 5) -> bool:
        """Run the scraper with parallel threading"""
        try:
            # Fetch EPG and channels
            if not self.fetch_epg() or not self.load_channels():
                return False
            
            # Calculate dates in São Paulo timezone
            now_sp = datetime.now(SAO_PAULO_TZ)
            today_sp = now_sp.replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow_sp = today_sp + timedelta(days=1)
            
            logger.info(f"Today (São Paulo): {self.format_date_br(today_sp)}")
            logger.info(f"Tomorrow (São Paulo): {self.format_date_br(tomorrow_sp)}")
            logger.info(f"Starting parallel processing with {max_workers} workers...")
            
            # Process channels in parallel
            results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self.scrape_channel, ch, today_sp, tomorrow_sp): ch
                    for ch in self.channels
                }
                
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        results.append(result)
            
            # Save JSON files
            saved_count = 0
            for result in results:
                channel_filename = result['channel_filename']
                
                # Save today's schedule
                today_file = f"{channel_filename}.json"
                if self.save_json('schedule/today', today_file, result['today']['data']):
                    saved_count += 1
                
                # Save tomorrow's schedule
                tomorrow_file = f"{channel_filename}.json"
                if self.save_json('schedule/tomorrow', tomorrow_file, result['tomorrow']['data']):
                    saved_count += 1
            
            logger.info(f"✓ Successfully saved {saved_count} JSON files")
            logger.info("EPG scraping completed successfully!")
            return True
        
        except Exception as e:
            logger.error(f"Fatal error in scraper: {e}")
            return False


def main():
    """Main entry point"""
    EPG_URL = "https://raw.githubusercontent.com/globetvapp/epg/refs/heads/main/Brazil/brazil1.xml"
    CHANNELS_FILE = "__channels.txt"
    
    scraper = EPGScraper(EPG_URL, CHANNELS_FILE)
    success = scraper.run(max_workers=5)
    
    exit(0 if success else 1)


if __name__ == "__main__":
    main()
