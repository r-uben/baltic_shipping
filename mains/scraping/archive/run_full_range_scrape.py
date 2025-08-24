#!/usr/bin/env python3
"""
Full-range IMO scraper with resume capability
Designed to scrape ALL vessels (IMO 1000000-9999999)
"""
import asyncio
import json
from pathlib import Path
from datetime import datetime, timedelta
import click
from rich.console import Console
from rich.progress import Progress
from rich.table import Table
import aiofiles
import time

console = Console()

def is_valid_imo(imo: int) -> bool:
    """Validate IMO checksum"""
    s = str(imo)
    if len(s) != 7: 
        return False
    checksum = sum(int(s[i]) * (7 - i) for i in range(6)) % 10
    return checksum == int(s[6])

class FullRangeScraper:
    def __init__(self, workers=50, extract_workers=1, model='gpt-oss:20b'):
        self.workers = workers  # For checking vessels
        self.extract_workers = extract_workers  # For LLM extraction
        self.model = model
        self.check_semaphore = asyncio.Semaphore(workers)
        self.extract_semaphore = asyncio.Semaphore(extract_workers)
        
        # Statistics
        self.stats = {
            'checked': 0,
            'found': 0,
            'extracted': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        # Checkpoint file for resume
        self.checkpoint_file = Path("data/full_scrape_checkpoint.json")
        self.found_queue = asyncio.Queue()  # Queue of IMOs to extract
        
    async def load_checkpoint(self):
        """Load checkpoint for resume capability"""
        if self.checkpoint_file.exists():
            async with aiofiles.open(self.checkpoint_file, 'r') as f:
                content = await f.read()
                return json.loads(content)
        return None
    
    async def save_checkpoint(self, current_imo: int):
        """Save checkpoint periodically"""
        checkpoint = {
            'last_imo': current_imo,
            'stats': self.stats,
            'timestamp': datetime.now().isoformat()
        }
        async with aiofiles.open(self.checkpoint_file, 'w') as f:
            await f.write(json.dumps(checkpoint, indent=2))
    
    async def quick_check(self, imo: int) -> bool:
        """Check if vessel exists (0.5s)"""
        async with self.check_semaphore:
            from playwright.async_api import async_playwright
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                url = f"https://www.balticshipping.com/vessel/imo/{imo}"
                try:
                    response = await page.goto(url, timeout=5000, wait_until='domcontentloaded')
                    
                    if response.status == 404:
                        await browser.close()
                        return False
                    
                    content = await page.content()
                    exists = 'vessel not found' not in content.lower() and 'no vessel' not in content.lower()
                    
                    await browser.close()
                    return exists
                    
                except Exception:
                    await browser.close()
                    return False
    
    async def extract_vessel(self, imo: int):
        """Extract vessel data with LLM"""
        async with self.extract_semaphore:
            try:
                from src.baltic_shipping.llm_intelligent_scraper import LLMIntelligentScraper
                scraper = LLMIntelligentScraper(ollama_model=self.model)
                
                data = await scraper.scrape_vessel_comprehensive(imo)
                if data:
                    # Save individual file
                    imo_str = str(imo)
                    dir_path = Path(f"data/vessels_full/{imo_str[0]}/{imo_str[1]}/{imo_str[2]}")
                    dir_path.mkdir(parents=True, exist_ok=True)
                    
                    individual_file = dir_path / f"{imo}.json"
                    async with aiofiles.open(individual_file, 'w') as f:
                        await f.write(json.dumps(data, indent=2))
                    
                    self.stats['extracted'] += 1
                    return data
                    
            except Exception as e:
                console.print(f"[red]Extract error IMO {imo}: {str(e)[:50]}[/red]")
                self.stats['errors'] += 1
                return None
    
    async def extraction_worker(self):
        """Worker that continuously extracts from queue"""
        while True:
            try:
                imo = await self.found_queue.get()
                if imo is None:  # Poison pill
                    break
                    
                console.print(f"[cyan]Extracting IMO {imo}...[/cyan]")
                await self.extract_vessel(imo)
                
            except Exception as e:
                console.print(f"[red]Extraction worker error: {e}[/red]")
    
    async def check_and_queue(self, imo: int):
        """Check vessel and queue for extraction if exists"""
        if not is_valid_imo(imo):
            return
        
        self.stats['checked'] += 1
        
        exists = await self.quick_check(imo)
        if exists:
            self.stats['found'] += 1
            await self.found_queue.put(imo)
            console.print(f"[green]✓ Found IMO {imo} (queue size: {self.found_queue.qsize()})[/green]")
    
    async def run_full_scrape(self, start: int, end: int, resume: bool = True):
        """Run the full scrape with resume capability"""
        # Check for resume
        if resume:
            checkpoint = await self.load_checkpoint()
            if checkpoint:
                start = checkpoint['last_imo'] + 1
                self.stats = checkpoint['stats']
                console.print(f"[yellow]Resuming from IMO {start}[/yellow]")
                console.print(f"Previous stats: {self.stats['found']} found, {self.stats['extracted']} extracted")
        
        # Start extraction workers
        extract_tasks = [
            asyncio.create_task(self.extraction_worker()) 
            for _ in range(self.extract_workers)
        ]
        
        # Main checking loop
        batch_size = self.workers * 100
        checkpoint_interval = 10000  # Save checkpoint every 10k IMOs
        
        with Progress() as progress:
            task = progress.add_task(f"Checking IMOs {start:,} to {end:,}", total=end-start)
            
            for batch_start in range(start, end, batch_size):
                batch_end = min(batch_start + batch_size, end)
                batch_imos = list(range(batch_start, batch_end))
                
                # Check batch in parallel
                check_tasks = [self.check_and_queue(imo) for imo in batch_imos]
                await asyncio.gather(*check_tasks)
                
                progress.advance(task, len(batch_imos))
                
                # Save checkpoint periodically
                if batch_end % checkpoint_interval == 0:
                    await self.save_checkpoint(batch_end)
                    
                    # Show statistics
                    elapsed = time.time() - self.stats['start_time']
                    rate = self.stats['checked'] / elapsed if elapsed > 0 else 0
                    eta_seconds = (end - batch_end) / rate if rate > 0 else 0
                    eta = timedelta(seconds=int(eta_seconds))
                    
                    progress.console.print(f"""
                    [bold]Progress Report[/bold]
                    Checked: {self.stats['checked']:,}
                    Found: {self.stats['found']:,} ({self.stats['found']/self.stats['checked']*100:.2f}%)
                    Extracted: {self.stats['extracted']:,}
                    Queue: {self.found_queue.qsize()}
                    Rate: {rate:.1f} IMOs/sec
                    ETA: {eta}
                    """)
        
        # Wait for extraction queue to empty
        console.print("[yellow]Waiting for extraction queue to finish...[/yellow]")
        while not self.found_queue.empty():
            await asyncio.sleep(1)
            console.print(f"Queue remaining: {self.found_queue.qsize()}")
        
        # Stop extraction workers
        for _ in range(self.extract_workers):
            await self.found_queue.put(None)
        
        await asyncio.gather(*extract_tasks)
        
        # Final statistics
        elapsed = time.time() - self.stats['start_time']
        console.print(f"""
        [green]✓ COMPLETE![/green]
        
        Total time: {timedelta(seconds=int(elapsed))}
        Checked: {self.stats['checked']:,}
        Found: {self.stats['found']:,}
        Extracted: {self.stats['extracted']:,}
        Errors: {self.stats['errors']:,}
        
        Hit rate: {self.stats['found']/self.stats['checked']*100:.2f}%
        Check rate: {self.stats['checked']/elapsed:.1f} IMOs/sec
        Extract rate: {self.stats['extracted']/elapsed:.3f} vessels/sec
        """)

@click.command()
@click.option('--start', default=1000000, help='Start IMO')
@click.option('--end', default=9999999, help='End IMO')
@click.option('--check-workers', default=50, help='Parallel workers for checking')
@click.option('--extract-workers', default=1, help='Parallel LLM extraction workers')
@click.option('--model', default='gpt-oss:20b', help='LLM model')
@click.option('--no-resume', is_flag=True, help='Start fresh, ignore checkpoint')
def main(start, end, check_workers, extract_workers, model, no_resume):
    """
    Full-range IMO scraper with resume capability
    
    Designed for scraping the ENTIRE Baltic Shipping database.
    
    Features:
    - Resume capability (automatic checkpoint every 10k IMOs)
    - Separate workers for checking and extraction
    - Queue-based extraction (doesn't block checking)
    - Hierarchical file storage
    - Real-time statistics and ETA
    
    Time estimates for full range (1M-10M):
    - 50 check workers + 1 extract worker: ~14 days
    - 100 check workers + 2 extract workers: ~7 days
    """
    
    console.print(f"""
    [bold cyan]Full-Range Vessel Scraper[/bold cyan]
    
    Range: {start:,} - {end:,}
    Total numbers: {end-start:,}
    Valid IMOs: ~{(end-start)//10:,}
    Expected vessels: ~{(end-start)//10 * 0.03:,.0f}
    
    Workers:
    - Checking: {check_workers} parallel
    - Extraction: {extract_workers} parallel
    - Model: {model}
    
    Resume: {not no_resume}
    
    [yellow]Time Estimates:[/yellow]
    - Check time: ~{(end-start)//10 * 0.5 / check_workers / 3600:.1f} hours
    - Extract time: ~{(end-start)//10 * 0.03 * 45 / extract_workers / 3600:.1f} hours
    - Total: ~{max((end-start)//10 * 0.5 / check_workers / 3600, (end-start)//10 * 0.03 * 45 / extract_workers / 3600):.1f} hours
    """)
    
    if not click.confirm("Start full scrape?"):
        return
    
    async def run():
        scraper = FullRangeScraper(
            workers=check_workers,
            extract_workers=extract_workers,
            model=model
        )
        await scraper.run_full_scrape(start, end, resume=not no_resume)
    
    asyncio.run(run())

if __name__ == "__main__":
    main()