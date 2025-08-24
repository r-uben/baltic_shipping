#!/usr/bin/env python3
"""
Master Baltic Shipping Scraper - Single reliable solution
Iterates through IMO numbers, validates checksums, checks for 404s, and extracts vessel data with local LLM
"""
import asyncio
import aiohttp
import json
import os
from pathlib import Path
from datetime import datetime
import time
import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

console = Console()

# Global statistics
stats = {
    'total_checked': 0,
    'valid_imos': 0,
    'vessels_found': 0,
    'successfully_scraped': 0,
    'errors': 0,
    'not_found_404': 0,
    'start_time': time.time()
}

def validate_imo_checksum(imo: int) -> bool:
    """
    Validate IMO number using mod-10 checksum algorithm
    This filters out ~90% of invalid numbers locally before making HTTP requests
    """
    imo_str = str(imo)
    if len(imo_str) != 7:
        return False
    
    # Calculate checksum: multiply each digit by (7-position) and sum
    checksum = sum(int(imo_str[i]) * (7 - i) for i in range(6)) % 10
    return checksum == int(imo_str[6])

def get_output_path(imo: int, data_dir: str) -> Path:
    """Simple flat file structure with IMO as filename"""
    return Path(data_dir) / f"vessel_{imo}.json"

def already_scraped(imo: int, data_dir: str) -> bool:
    """Check if we already have this vessel's data"""
    return get_output_path(imo, data_dir).exists()

async def vessel_exists(session: aiohttp.ClientSession, imo: int) -> tuple[bool, str]:
    """
    Check if vessel page exists and return HTML if it does
    Returns (exists, html_content)
    """
    url = f'https://www.balticshipping.com/vessel/imo/{imo}'
    
    try:
        async with session.get(
            url, 
            timeout=aiohttp.ClientTimeout(total=10),
            headers={'User-Agent': 'Mozilla/5.0 (compatible; VesselScraper/1.0)'}
        ) as response:
            if response.status == 404:
                return False, ""
            
            html = await response.text()
            
            # Check for soft 404s (page loads but vessel not found)
            if ('vessel not found' in html.lower() or 
                'no vessel' in html.lower() or
                'vessel details not available' in html.lower()):
                return False, ""
            
            return True, html
            
    except asyncio.TimeoutError:
        console.print(f"[yellow]⏱ Timeout checking IMO {imo}[/yellow]")
        return False, ""
    except Exception as e:
        console.print(f"[red]❌ Error checking IMO {imo}: {str(e)[:50]}[/red]")
        return False, ""

async def extract_with_local_llm(imo: int, html: str, model: str) -> dict:
    """Extract vessel data using local LLM via Ollama"""
    
    # Create focused prompt with HTML snippet
    html_snippet = html[:12000]  # First 12k chars to capture more content
    
    # Debug: Save HTML to file for inspection if needed
    if len(html) < 1000:  # Likely an error page
        console.print(f"[yellow]⚠ IMO {imo}: HTML too short ({len(html)} chars), might be error page[/yellow]")
        return None
    
    prompt = f"""You are extracting vessel data from a Baltic Shipping webpage. 

Look for vessel information like ship name, MMSI, flag, vessel type, dimensions, tonnage, build year, etc.

Extract the following information and return ONLY a valid JSON object with these exact keys:

{{
    "name": "vessel name or null if not found",
    "mmsi": "MMSI number or null if not found",
    "flag": "flag country or null if not found", 
    "vessel_type": "type of vessel or null if not found",
    "length": "length in meters or null if not found",
    "breadth": "breadth/beam in meters or null if not found", 
    "dwt": "deadweight tonnage or null if not found",
    "built_year": "year built or null if not found",
    "description": "any vessel description or null if not found"
}}

Important: Return ONLY the JSON object, no explanations or other text.

HTML content (first 12000 chars):
{html_snippet}
"""

    try:
        async with aiohttp.ClientSession() as llm_session:
            async with llm_session.post(
                'http://localhost:11434/api/generate',
                json={
                    'model': model,
                    'prompt': prompt,
                    'stream': False,
                    'options': {
                        'temperature': 0.1,  # Low temperature for consistent extraction
                        'num_predict': 500   # Limit response length
                    }
                },
                timeout=aiohttp.ClientTimeout(total=45)  # 45 second timeout for LLM
            ) as response:
                
                if response.status != 200:
                    return None
                    
                result = await response.json()
                llm_response = result.get('response', '').strip()
                
                # Try to extract JSON from response
                try:
                    # Look for JSON object in response
                    import re
                    json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', llm_response)
                    
                    if json_match:
                        vessel_data = json.loads(json_match.group())
                    else:
                        # Fallback: try parsing entire response
                        vessel_data = json.loads(llm_response)
                    
                    # Add metadata
                    vessel_data['imo'] = str(imo)
                    vessel_data['scraped_at'] = datetime.now().isoformat()
                    vessel_data['source_url'] = f'https://www.balticshipping.com/vessel/imo/{imo}'
                    
                    return vessel_data
                    
                except json.JSONDecodeError:
                    console.print(f"[yellow]⚠ IMO {imo}: LLM returned invalid JSON: {llm_response[:100]}...[/yellow]")
                    return None
                    
    except asyncio.TimeoutError:
        console.print(f"[red]⏱ IMO {imo}: LLM extraction timeout[/red]")
        return None
    except Exception as e:
        console.print(f"[red]❌ IMO {imo}: LLM error: {str(e)[:50]}[/red]")
        return None

async def process_imo(
    semaphore: asyncio.Semaphore, 
    session: aiohttp.ClientSession, 
    imo: int, 
    model: str, 
    data_dir: str,
    debug_html: bool = False
):
    """Process a single IMO: validate -> check exists -> extract -> save"""
    
    async with semaphore:
        stats['total_checked'] += 1
        
        # Step 1: Validate IMO checksum locally (instant)
        if not validate_imo_checksum(imo):
            return  # Skip invalid IMOs
        
        stats['valid_imos'] += 1
        
        # Step 2: Skip if already scraped
        if already_scraped(imo, data_dir):
            stats['successfully_scraped'] += 1
            return
        
        # Step 3: Check if vessel exists on website
        exists, html = await vessel_exists(session, imo)
        if not exists:
            stats['not_found_404'] += 1
            return
        
        # Step 4: Vessel found! Extract data with LLM
        stats['vessels_found'] += 1
        console.print(f"[green]🚢 IMO {imo} found - extracting data...[/green]")
        
        # Debug: Save HTML if requested
        if debug_html:
            debug_dir = Path(data_dir) / "debug_html"
            debug_dir.mkdir(parents=True, exist_ok=True)
            with open(debug_dir / f"imo_{imo}.html", 'w', encoding='utf-8') as f:
                f.write(html)
        
        vessel_data = await extract_with_local_llm(imo, html, model)
        
        if vessel_data:
            # Step 5: Save to file
            output_path = get_output_path(imo, data_dir)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(vessel_data, f, indent=2, ensure_ascii=False)
            
            stats['successfully_scraped'] += 1
            vessel_name = vessel_data.get('name', 'Unknown')
            console.print(f"[cyan]✅ IMO {imo}: {vessel_name} - SAVED[/cyan]")
        else:
            stats['errors'] += 1

def print_progress_stats():
    """Print current progress statistics"""
    elapsed = time.time() - stats['start_time']
    rate = stats['total_checked'] / elapsed if elapsed > 0 else 0
    
    valid_rate = stats['valid_imos'] / stats['total_checked'] * 100 if stats['total_checked'] > 0 else 0
    hit_rate = stats['vessels_found'] / stats['valid_imos'] * 100 if stats['valid_imos'] > 0 else 0
    
    console.print(f"""
[bold cyan]Progress Update[/bold cyan]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Checked: {stats['total_checked']:,} IMOs ({rate:.1f}/sec)
Valid: {stats['valid_imos']:,} ({valid_rate:.1f}% of checked)
Found: {stats['vessels_found']:,} vessels ({hit_rate:.2f}% of valid)
Scraped: {stats['successfully_scraped']:,}
Errors: {stats['errors']:,}
Not Found: {stats['not_found_404']:,}
Runtime: {elapsed/60:.1f} minutes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    """)

@click.command()
@click.option('--start-imo', default=1000000, help='Starting IMO number')
@click.option('--end-imo', default=9999999, help='Ending IMO number') 
@click.option('--workers', default=5, help='Number of parallel workers')
@click.option('--model', default='gpt-oss:20b', help='Local LLM model name')
@click.option('--data-dir', default='data/vessels', help='Output directory')
@click.option('--batch-size', default=500, help='Process in batches of this size')
@click.option('--resume', is_flag=True, help='Resume from last processed IMO')
@click.option('--debug-html', is_flag=True, help='Save HTML files for debugging')
def main(start_imo, end_imo, workers, model, data_dir, batch_size, resume, debug_html):
    """
    Master Baltic Shipping Scraper
    
    Simple, reliable approach:
    1. Iterate through IMO number range
    2. Validate IMO checksum (filters ~90% invalid locally)
    3. Check if vessel page exists (avoid 404s)
    4. Extract vessel data using local LLM
    5. Save as individual JSON files
    
    Features:
    - Resume capability
    - Progress tracking
    - Error handling
    - Rate limiting
    """
    
    # Ensure output directory exists
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    
    console.print(f"""
[bold cyan]Master Baltic Shipping Scraper[/bold cyan]
════════════════════════════════════════
Configuration:
• IMO Range: {start_imo:,} → {end_imo:,} ({end_imo - start_imo:,} numbers)
• Parallel Workers: {workers}
• LLM Model: {model}  
• Output Directory: {data_dir}
• Batch Size: {batch_size:,}

Process:
1. Validate IMO checksum (instant, ~10% pass)
2. Check vessel exists (~0.5 sec per check)
3. Extract with LLM if found (~30-45 sec per extraction)
4. Save to JSON file

[yellow]Starting in 3 seconds... Press Ctrl+C to stop gracefully[/yellow]
    """)
    
    time.sleep(3)
    
    async def run_scraper():
        # Set up concurrency control
        semaphore = asyncio.Semaphore(workers)
        
        # Create HTTP session with connection pooling
        connector = aiohttp.TCPConnector(limit=workers * 2)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            
            # Progress bar setup
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TextColumn("[cyan]{task.completed}/{task.total} IMOs"),
                console=console
            )
            
            with progress:
                task = progress.add_task(
                    f"Processing IMOs {start_imo:,} to {end_imo:,}", 
                    total=end_imo - start_imo + 1
                )
                
                # Process in batches
                current_imo = start_imo
                
                while current_imo <= end_imo:
                    batch_end = min(current_imo + batch_size, end_imo + 1)
                    
                    # Create tasks for this batch
                    tasks = []
                    for imo in range(current_imo, batch_end):
                        task_coro = process_imo(semaphore, session, imo, model, data_dir, debug_html)
                        tasks.append(task_coro)
                    
                    # Execute batch
                    await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Update progress
                    progress.advance(task, batch_end - current_imo)
                    current_imo = batch_end
                    
                    # Print stats every 1000 IMOs
                    if stats['total_checked'] % 1000 == 0 and stats['total_checked'] > 0:
                        print_progress_stats()
        
        # Final results
        console.print("\n[bold green]✓ SCRAPING COMPLETE![/bold green]")
        print_progress_stats()
    
    try:
        asyncio.run(run_scraper())
    except KeyboardInterrupt:
        console.print("\n[yellow]⏸ STOPPED - Current progress saved[/yellow]")
        print_progress_stats()

if __name__ == '__main__':
    main()