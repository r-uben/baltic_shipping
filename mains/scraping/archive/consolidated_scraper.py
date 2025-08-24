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
import click
from rich.console import Console
from rich.progress import Progress

console = Console()

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

def get_file_path(imo: int, data_dir: str) -> Path:
    """Get hierarchical file path for IMO to prevent filesystem overload"""
    imo_str = str(imo)
    # Create hierarchy: data/vessels/1/0/0/1000074.json
    dir_path = Path(data_dir) / imo_str[0] / imo_str[1] / imo_str[2]
    return dir_path / f'{imo}.json'

def file_exists(imo: int, data_dir: str) -> bool:
    """Check if we already have this vessel's data"""
    return get_file_path(imo, data_dir).exists()

async def check_vessel_exists(session: aiohttp.ClientSession, imo: int) -> bool:
    """Fast check if vessel exists (not 404) - uses HEAD request for speed"""
    url = f'https://www.balticshipping.com/vessel/imo/{imo}'
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

async def extract_with_llm(session: aiohttp.ClientSession, imo: int, model: str) -> dict:
    """Extract vessel data using LLM via Ollama API"""
    try:
        # Suppress the verbose LLM scraper output
        import sys
        import io
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        
        try:
            # Use the existing LLM scraper which works
            from src.baltic_shipping.llm_intelligent_scraper import LLMIntelligentScraper
            scraper = LLMIntelligentScraper(ollama_model=model)
            
            # This uses the working implementation
            data = await scraper.scrape_vessel_comprehensive(imo)
        finally:
            sys.stdout = old_stdout
        
        if data:
            # Extract just the essential fields from the comprehensive data
            combined = data.get('combined_data', {})
            clean_data = {
                'imo': str(imo),
                'mmsi': combined.get('MMSI'),
                'name': combined.get('Vessel name') or combined.get('name'),
                'flag': combined.get('Flag'),
                'type': combined.get('Vessel type') or combined.get('type'),
                'length': combined.get('Length') or combined.get('length'),
                'breadth': combined.get('Breadth') or combined.get('breadth'),
                'dwt': combined.get('DWT') or combined.get('Deadweight'),
                'built': combined.get('Year of built') or combined.get('built'),
                'description': combined.get('Description'),
                'scraped_at': datetime.now().isoformat()
            }
            return clean_data
        return None
            
    except Exception as e:
        console.print(f"[red]❌ LLM failed for IMO {imo}: {str(e)[:50]}[/red]")
        return None

async def process_imo(semaphore: asyncio.Semaphore, session: aiohttp.ClientSession, imo: int, data_dir: str, model: str):
    """Process single IMO: validate, check existence, extract if exists"""
    async with semaphore:
        stats['checked'] += 1
        
        # Step 1: Validate IMO checksum
        if not is_valid_imo(imo):
            return  # Invalid checksum, skip
        
        stats['valid_imos'] += 1
        
        # Step 2: Check if already scraped
        if file_exists(imo, data_dir):
            stats['extracted'] += 1  # Already have it
            return
        
        # Step 3: Check if vessel exists (404 check)
        exists = await check_vessel_exists(session, imo)
        if not exists:
            stats['skipped_404'] += 1
            return
        
        # Step 4: Vessel exists! Extract with LLM
        stats['found'] += 1
        console.print(f"[yellow]⚡ IMO {imo} exists - starting LLM extraction (45s)...[/yellow]")
        
        data = await extract_with_llm(session, imo, model)
        if data:
            # Save to file
            file_path = get_file_path(imo, data_dir)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            stats['extracted'] += 1
            console.print(f"[green]✅ IMO {imo}: {data.get('name', 'Unknown')} ({data.get('flag', 'Unknown')}) - SAVED[/green]")
        else:
            stats['errors'] += 1
            console.print(f"[red]❌ IMO {imo}: Extraction failed[/red]")

def load_resume_point(resume_file: str, start: int):
    """Load last processed IMO for resume capability"""
    if os.path.exists(resume_file):
        try:
            with open(resume_file, 'r') as f:
                resume_data = json.load(f)
                return resume_data.get('last_imo', start)
        except:
            return start
    return start

def save_resume_point(resume_file: str, last_imo: int):
    """Save progress for resume capability"""
    resume_data = {
        'last_imo': last_imo,
        'stats': stats,
        'timestamp': datetime.now().isoformat()
    }
    with open(resume_file, 'w') as f:
        json.dump(resume_data, f, indent=2)

def print_stats():
    """Print current statistics"""
    elapsed = time.time() - stats['start_time']
    rate = stats['checked'] / elapsed if elapsed > 0 else 0
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    
    console.print(f"""
    [bold cyan]━━━━━━━━━ Statistics ━━━━━━━━━[/bold cyan]
    [white]Checked:[/white] {stats['checked']:,} IMOs
    [white]Valid:[/white] {stats['valid_imos']:,} ({stats['valid_imos']/stats['checked']*100 if stats['checked'] > 0 else 0:.1f}% pass checksum)
    [green]Found:[/green] {stats['found']:,} vessels exist
    [cyan]Extracted:[/cyan] {stats['extracted']:,} saved
    [yellow]Not found:[/yellow] {stats['skipped_404']:,} (404s)
    [red]Errors:[/red] {stats['errors']:,}
    
    [white]Speed:[/white] {rate:.1f} IMOs/sec
    [white]Hit rate:[/white] {(stats['found'] / stats['valid_imos'] * 100) if stats['valid_imos'] > 0 else 0:.2f}% of valid IMOs
    [white]Time:[/white] {hours}h {minutes}m
    [bold cyan]━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[/bold cyan]
    """)

@click.command()
@click.option('--start', default=1000000, help='Start IMO number')
@click.option('--end', default=9999999, help='End IMO number')
@click.option('--workers', default=12, help='Number of parallel workers')
@click.option('--model', default='gpt-oss:20b', help='LLM model to use')
@click.option('--data-dir', default='data/vessels', help='Directory to save vessel data')
@click.option('--batch-size', default=100, help='Process IMOs in batches of this size')
@click.option('--no-resume', is_flag=True, help='Start fresh, ignore any saved progress')
def main(start, end, workers, model, data_dir, batch_size, no_resume):
    """
    Consolidated Baltic Shipping vessel scraper
    
    Simple approach:
    1. Iterate through IMO numbers
    2. Validate checksum locally
    3. Check if vessel exists (not 404)
    4. Extract with LLM if exists
    5. Save as individual JSON files
    
    Resume capability included - press Ctrl+C to pause.
    """
    
    console.print(f"""
    [bold cyan]=====================================
    Baltic Shipping Consolidated Scraper
    =====================================[/bold cyan]
    
    Configuration:
    - Range: {start:,} to {end:,}
    - Workers: {workers}
    - LLM Model: {model}
    - Data directory: {data_dir}
    - Batch size: {batch_size}
    
    Process:
    1. Validate IMO checksum (instant)
    2. Check if vessel exists (0.5 sec)
    3. Extract with LLM if exists (~45 sec)
    4. Save to hierarchical JSON structure
    
    [yellow]Press Ctrl+C to pause (resume supported)[/yellow]
    """)
    
    async def run():
        # Setup resume
        resume_file = f'{data_dir}_resume.json'
        if no_resume:
            start_imo = start
        else:
            start_imo = load_resume_point(resume_file, start)
            if start_imo > start:
                console.print(f"[yellow]Resuming from IMO {start_imo:,}[/yellow]")
        
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(workers)
        
        # Create HTTP session with connection pool
        connector = aiohttp.TCPConnector(limit=workers)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Process in batches with progress bar
            with Progress() as progress:
                task = progress.add_task(f"Processing IMOs {start_imo:,} to {end:,}", total=end-start_imo+1)
                
                current_imo = start_imo
                while current_imo <= end:
                    # Create batch of tasks
                    batch_end = min(current_imo + batch_size, end + 1)
                    tasks = []
                    
                    for imo in range(current_imo, batch_end):
                        t = process_imo(semaphore, session, imo, data_dir, model)
                        tasks.append(t)
                    
                    # Execute batch
                    await asyncio.gather(*tasks)
                    
                    # Update progress
                    progress.advance(task, batch_end - current_imo)
                    current_imo = batch_end
                    save_resume_point(resume_file, current_imo - 1)
                    
                    # Print stats every 1000 IMOs
                    if stats['checked'] % 1000 == 0:
                        print_stats()
        
        # Final stats
        console.print("\n[green]✓ COMPLETE![/green]")
        print_stats()
    
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        console.print("\n[yellow]PAUSED - Progress saved. Run again to resume.[/yellow]")
        print_stats()

if __name__ == '__main__':
    main()