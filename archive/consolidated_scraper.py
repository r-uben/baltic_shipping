#!/usr/bin/env python3
"""
Consolidated Baltic Shipping vessel scraper - Single file solution
Simple iteration through IMO numbers with checksum validation, 404 detection, and LLM extraction
"""
import asyncio
import aiohttp
import json
import os
from pathlib import Path
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor

# Configuration
START_IMO = 1000000
END_IMO = 9999999
MAX_WORKERS = 12  # For 15-core system, leave some headroom
RESUME_FILE = 'scraper_resume.json'
DATA_DIR = 'data/vessels'
BASE_URL = 'https://www.balticshipping.com/vessel/imo/{}'
LLM_MODEL = 'gpt-oss:20b'
BATCH_SIZE = 100  # Process in batches for progress tracking

# Statistics
stats = {
    'checked': 0,
    'valid_imos': 0,
    'found': 0,
    'extracted': 0,
    'errors': 0,
    'skipped_404': 0,
    'start_time': time.time()
}

def is_valid_imo(imo: int) -> bool:
    """Validate IMO checksum - filters 90% of invalid numbers locally"""
    imo_str = str(imo)
    if len(imo_str) != 7:
        return False
    checksum = sum(int(imo_str[i]) * (7 - i) for i in range(6)) % 10
    return checksum == int(imo_str[6])

def get_file_path(imo: int) -> Path:
    """Get hierarchical file path for IMO to prevent filesystem overload"""
    imo_str = str(imo)
    # Create hierarchy: data/vessels/1/0/0/1000074.json
    dir_path = Path(DATA_DIR) / imo_str[0] / imo_str[1] / imo_str[2]
    return dir_path / f'{imo}.json'

def file_exists(imo: int) -> bool:
    """Check if we already have this vessel's data"""
    return get_file_path(imo).exists()

async def check_vessel_exists(session: aiohttp.ClientSession, imo: int) -> bool:
    """Fast check if vessel exists (not 404) - uses HEAD request for speed"""
    url = BASE_URL.format(imo)
    try:
        async with session.head(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
            if response.status == 404:
                return False
            # Also do a quick GET to check for soft 404s
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                text = await resp.text()
                if 'vessel not found' in text.lower() or 'no vessel' in text.lower():
                    return False
                return True
    except Exception:
        return False  # Network errors = skip

async def extract_with_llm(session: aiohttp.ClientSession, imo: int) -> dict:
    """Extract vessel data using LLM via Ollama API"""
    try:
        # First get the page content
        url = BASE_URL.format(imo)
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
            html = await response.text()
        
        # Prepare LLM prompt
        prompt = f"""Extract vessel data from this Baltic Shipping page for IMO {imo}.
        Return ONLY a JSON object with these fields:
        - imo: IMO number
        - mmsi: MMSI number
        - name: vessel name
        - flag: flag country
        - type: vessel type
        - length: length in meters
        - breadth: breadth in meters
        - dwt: deadweight tonnage
        - built: year built
        - description: brief description
        
        If a field is not found, use null. Return only valid JSON, no extra text.
        
        HTML Content:
        {html[:10000]}  # Limit to first 10k chars to avoid token limits
        """
        
        # Call Ollama API
        async with session.post(
            'http://localhost:11434/api/generate',
            json={
                'model': LLM_MODEL,
                'prompt': prompt,
                'stream': False,
                'format': 'json'
            },
            timeout=aiohttp.ClientTimeout(total=60)
        ) as response:
            if response.status == 200:
                result = await response.json()
                data = json.loads(result['response'])
                data['imo'] = str(imo)  # Ensure IMO is set
                data['scraped_at'] = datetime.now().isoformat()
                return data
    except Exception as e:
        print(f"[ERROR] LLM extraction failed for IMO {imo}: {str(e)[:100]}")
        return None

async def process_imo(semaphore: asyncio.Semaphore, session: aiohttp.ClientSession, imo: int):
    """Process single IMO: validate, check existence, extract if exists"""
    async with semaphore:
        stats['checked'] += 1
        
        # Step 1: Validate IMO checksum
        if not is_valid_imo(imo):
            return  # Invalid checksum, skip
        
        stats['valid_imos'] += 1
        
        # Step 2: Check if already scraped
        if file_exists(imo):
            stats['extracted'] += 1  # Already have it
            return
        
        # Step 3: Check if vessel exists (404 check)
        exists = await check_vessel_exists(session, imo)
        if not exists:
            stats['skipped_404'] += 1
            return
        
        # Step 4: Vessel exists! Extract with LLM
        print(f"[FOUND] IMO {imo} exists, extracting...")
        stats['found'] += 1
        
        data = await extract_with_llm(session, imo)
        if data:
            # Save to file
            file_path = get_file_path(imo)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            stats['extracted'] += 1
            print(f"[SAVED] IMO {imo}: {data.get('name', 'Unknown')} ({data.get('flag', 'Unknown')})")
        else:
            stats['errors'] += 1

def load_resume_point():
    """Load last processed IMO for resume capability"""
    if os.path.exists(RESUME_FILE):
        try:
            with open(RESUME_FILE, 'r') as f:
                resume_data = json.load(f)
                return resume_data.get('last_imo', START_IMO)
        except:
            return START_IMO
    return START_IMO

def save_resume_point(last_imo: int):
    """Save progress for resume capability"""
    resume_data = {
        'last_imo': last_imo,
        'stats': stats,
        'timestamp': datetime.now().isoformat()
    }
    with open(RESUME_FILE, 'w') as f:
        json.dump(resume_data, f, indent=2)

def print_stats():
    """Print current statistics"""
    elapsed = time.time() - stats['start_time']
    rate = stats['checked'] / elapsed if elapsed > 0 else 0
    
    print(f"""
    ========== Statistics ==========
    Checked: {stats['checked']:,}
    Valid IMOs: {stats['valid_imos']:,}
    Found vessels: {stats['found']:,}
    Extracted: {stats['extracted']:,}
    404/Not found: {stats['skipped_404']:,}
    Errors: {stats['errors']:,}
    
    Rate: {rate:.1f} IMOs/sec
    Hit rate: {(stats['found'] / stats['valid_imos'] * 100) if stats['valid_imos'] > 0 else 0:.2f}%
    Time elapsed: {elapsed/60:.1f} minutes
    ================================
    """)

async def main():
    """Main scraping loop"""
    # Load resume point
    start_imo = load_resume_point()
    print(f"Starting from IMO {start_imo:,} (target: {END_IMO:,})")
    
    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    
    # Create HTTP session with connection pool
    connector = aiohttp.TCPConnector(limit=MAX_WORKERS)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Process in batches
        current_imo = start_imo
        while current_imo <= END_IMO:
            # Create batch of tasks
            batch_end = min(current_imo + BATCH_SIZE, END_IMO + 1)
            tasks = []
            
            for imo in range(current_imo, batch_end):
                task = process_imo(semaphore, session, imo)
                tasks.append(task)
            
            # Execute batch
            await asyncio.gather(*tasks)
            
            # Update progress
            current_imo = batch_end
            save_resume_point(current_imo - 1)
            
            # Print stats every 1000 IMOs
            if stats['checked'] % 1000 == 0:
                print_stats()
    
    # Final stats
    print("\n[COMPLETE] Final statistics:")
    print_stats()

if __name__ == '__main__':
    print("""
    =====================================
    Baltic Shipping Consolidated Scraper
    =====================================
    
    Configuration:
    - Range: {:,} to {:,}
    - Workers: {}
    - LLM Model: {}
    - Data directory: {}
    
    Process:
    1. Validate IMO checksum (instant)
    2. Check if vessel exists (0.5 sec)
    3. Extract with LLM if exists (~45 sec)
    4. Save to hierarchical JSON structure
    
    Press Ctrl+C to pause (resume supported)
    =====================================
    """.format(START_IMO, END_IMO, MAX_WORKERS, LLM_MODEL, DATA_DIR))
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[PAUSED] Progress saved. Run again to resume.")
        print_stats()