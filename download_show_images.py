import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
from io import BytesIO
from PIL import Image

BASE_UPLOAD_URL = "https://programacaotvhoje.com/wp-content/uploads/downloaded-images"
MAX_THREADS = 15
TIMEOUT = 20
ROOT_DIR = os.getcwd()
SCHEDULE_DIR = os.path.join(ROOT_DIR, "schedule")
DOWNLOAD_DIR = os.path.join(ROOT_DIR, "downloaded-images")

def webp_filename(url):
    name = os.path.basename(urlparse(url).path)
    base, _ = os.path.splitext(name)
    return f"{base}.webp"

def download_and_convert(task):
    url, save_path = task
    try:
        r = requests.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        image = Image.open(BytesIO(r.content)).convert("RGB")
        
        # Compress to keep under 10 KB
        max_size_kb = 10
        max_size_bytes = max_size_kb * 1024
        quality = 85
        min_quality = 20
        
        while quality >= min_quality:
            buffer = BytesIO()
            image.save(buffer, "WEBP", quality=quality)
            size = buffer.tell()
            
            if size <= max_size_bytes:
                buffer.seek(0)
                with open(save_path, "wb") as f:
                    f.write(buffer.read())
                return True, url
            
            quality -= 5
        
        # If still too large, save with minimum quality
        image.save(save_path, "WEBP", quality=min_quality)
        return True, url
        
    except Exception:
        return False, url

def process_json(json_path, day):
    channel_slug = os.path.splitext(os.path.basename(json_path))[0]
    output_dir = os.path.join(DOWNLOAD_DIR, channel_slug, day)
    os.makedirs(output_dir, exist_ok=True)
    
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    unique_urls = {}
    download_tasks = []
    
    for show in data.get("schedule", []):
        logo_url = show.get("show_logo", "").strip()
        if not logo_url:
            continue
        
        if logo_url not in unique_urls:
            filename = webp_filename(logo_url)
            local_path = os.path.join(output_dir, filename)
            unique_urls[logo_url] = filename
            
            if not os.path.exists(local_path):
                download_tasks.append((logo_url, local_path))
        
        show["show_logo"] = (
            f"{BASE_UPLOAD_URL}/{channel_slug}/{day}/{unique_urls[logo_url]}"
        )
    
    if download_tasks:
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = [executor.submit(download_and_convert, t) for t in download_tasks]
            for _ in as_completed(futures):
                pass
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    for day in ["today", "tomorrow"]:
        day_dir = os.path.join(SCHEDULE_DIR, day)
        if not os.path.isdir(day_dir):
            continue
        
        for file in os.listdir(day_dir):
            if file.endswith(".json"):
                process_json(os.path.join(day_dir, file), day)

if __name__ == "__main__":
    main()
