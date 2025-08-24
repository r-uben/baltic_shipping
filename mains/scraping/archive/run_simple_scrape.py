#!/usr/bin/env python3
"""
Simple sequential IMO scraper - no fancy sampling, just iterate and check
"""
import asyncio
import json
from pathlib import Path
from datetime import datetime
import click
from rich.console import Console
from rich.progress import Progress
import aiofiles

console = Console()

def is_valid_imo(imo: int) -> bool:
    """Validate IMO checksum"""
    s = str(imo)
    if len(s) != 7: 
        return False
    checksum = sum(int(s[i]) * (7 - i) for i in range(6)) % 10
    return checksum == int(s[6])

class SimpleScraper:
    def __init__(self, workers=10, model='gpt-oss:20b'):
        self.workers = workers
        self.model = model
        self.semaphore = asyncio.Semaphore(workers)
        self.found_count = 0
        self.checked_count = 0
        
    async def process_imo(self, imo: int, output_file):
        """Process single IMO: check validity, existence, and extract if exists"""
        async with self.semaphore:
            # Step 1: Check checksum
            if not is_valid_imo(imo):
                return None  # Invalid checksum, skip
            
            # Step 2: Check if vessel exists
            from playwright.async_api import async_playwright
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                url = f"https://www.balticshipping.com/vessel/imo/{imo}"
                try:
                    response = await page.goto(url, timeout=5000, wait_until='domcontentloaded')
                    
                    # Check 404
                    if response.status == 404:
                        await browser.close()
                        self.checked_count += 1
                        return None  # No vessel, skip
                    
                    # Check content for vessel data
                    content = await page.content()
                    if 'vessel not found' in content.lower() or 'no vessel' in content.lower():
                        await browser.close()
                        self.checked_count += 1
                        return None  # Soft 404, skip
                    
                    await browser.close()
                    
                    # Step 3: Vessel exists! Extract data with LLM
                    console.print(f"[green]✓ Found vessel: IMO {imo} - extracting data...[/green]")
                    
                    from src.baltic_shipping.llm_intelligent_scraper import LLMIntelligentScraper
                    scraper = LLMIntelligentScraper(ollama_model=self.model)
                    
                    data = await scraper.scrape_vessel_comprehensive(imo)
                    if data:
                        # Save as individual JSON file in hierarchical structure
                        # e.g., data/vessels/9/0/0/9000074.json
                        imo_str = str(imo)
                        dir_path = Path(f"data/vessels/{imo_str[0]}/{imo_str[1]}/{imo_str[2]}")
                        dir_path.mkdir(parents=True, exist_ok=True)
                        
                        individual_file = dir_path / f"{imo}.json"
                        async with aiofiles.open(individual_file, 'w') as f:
                            await f.write(json.dumps(data, indent=2))
                        
                        # Also append to JSONL for batch processing
                        async with aiofiles.open(output_file, 'a') as f:
                            await f.write(json.dumps(data) + '\n')
                        
                        self.found_count += 1
                        self.checked_count += 1
                        
                        # Show what we got
                        combined = data.get('combined_data', {})
                        console.print(f"  → {combined.get('name', 'Unknown')} ({combined.get('flag', 'Unknown')}) - {len(combined)} fields")
                        console.print(f"  → Saved to {individual_file}")
                        return data
                    
                except Exception as e:
                    console.print(f"[yellow]Error checking IMO {imo}: {str(e)[:50]}[/yellow]")
                    await browser.close()
                
                self.checked_count += 1
                return None
    
    async def scan_range(self, start: int, end: int):
        """Simple sequential scan with parallel workers"""
        # Create output file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"data/vessels_simple_{timestamp}.jsonl"
        
        # Create checkpoint file for resume capability
        checkpoint_file = f"data/checkpoint_simple_{timestamp}.json"
        
        console.print(f"Output: {output_file}")
        console.print(f"Checkpoint: {checkpoint_file}")
        
        # Process in batches for progress tracking
        batch_size = self.workers * 10
        total_range = end - start
        
        with Progress() as progress:
            task = progress.add_task(f"Scanning IMOs {start:,} to {end:,}", total=total_range)
            
            for batch_start in range(start, end, batch_size):
                batch_end = min(batch_start + batch_size, end)
                batch_imos = list(range(batch_start, batch_end))
                
                # Process batch in parallel
                tasks = [self.process_imo(imo, output_file) for imo in batch_imos]
                await asyncio.gather(*tasks)
                
                # Update progress
                progress.advance(task, len(batch_imos))
                
                # Show stats periodically
                if self.checked_count % 100 == 0:
                    hit_rate = (self.found_count / self.checked_count * 100) if self.checked_count > 0 else 0
                    progress.console.print(
                        f"  Stats: {self.checked_count:,} checked, {self.found_count:,} found ({hit_rate:.1f}%)"
                    )
                
                # Save checkpoint
                checkpoint = {
                    'last_imo': batch_end,
                    'found_count': self.found_count,
                    'checked_count': self.checked_count,
                    'timestamp': datetime.now().isoformat()
                }
                async with aiofiles.open(checkpoint_file, 'w') as f:
                    await f.write(json.dumps(checkpoint, indent=2))

@click.command()
@click.option('--start', default=9000000, help='Start IMO')
@click.option('--end', default=9001000, help='End IMO')
@click.option('--workers', default=10, help='Parallel workers')
@click.option('--model', default='gpt-oss:20b', help='LLM model for extraction')
def main(start, end, workers, model):
    """
    Simple sequential IMO scraper
    
    How it works:
    1. Iterate through IMO range
    2. Check checksum (skip if invalid)
    3. Check if vessel exists (skip if 404)
    4. Extract data with LLM (if vessel exists)
    5. Save to JSONL file immediately
    
    No sampling, no complex logic - just check every number!
    """
    
    console.print(f"""
    [bold cyan]Simple Sequential Scraper[/bold cyan]
    Range: {start:,} - {end:,}
    Workers: {workers}
    Model: {model}
    
    Process:
    1. Check IMO checksum (instant)
    2. Check vessel exists (0.5s)
    3. Extract if exists ({model})
    4. Save to JSONL
    """)
    
    # Estimate time
    total_numbers = end - start
    valid_imos = total_numbers // 10  # ~10% have valid checksum
    estimated_vessels = valid_imos * 0.03  # ~3% are real vessels
    
    check_time = (valid_imos * 0.5) / workers / 60  # minutes for checking
    extract_time = (estimated_vessels * 45) / 60  # minutes for extraction (sequential)
    
    console.print(f"""
    [yellow]Estimates:[/yellow]
    - Total numbers: {total_numbers:,}
    - Valid checksums: ~{valid_imos:,}
    - Expected vessels: ~{int(estimated_vessels):,}
    - Check time: ~{check_time:.1f} minutes
    - Extract time: ~{extract_time:.1f} minutes
    - Total time: ~{(check_time + extract_time):.1f} minutes
    """)
    
    if not click.confirm("Continue?"):
        return
    
    async def run():
        scraper = SimpleScraper(workers=workers, model=model)
        await scraper.scan_range(start, end)
        
        console.print(f"""
        [green]✓ Complete![/green]
        
        Final Statistics:
        - Checked: {scraper.checked_count:,} IMOs
        - Found: {scraper.found_count:,} vessels
        - Hit rate: {(scraper.found_count/scraper.checked_count*100):.1f}%
        """)
    
    asyncio.run(run())

if __name__ == "__main__":
    main()