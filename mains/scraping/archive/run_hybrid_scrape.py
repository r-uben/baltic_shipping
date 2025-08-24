#!/usr/bin/env python3
"""
Hybrid scraping: Fast model to find vessels, then gpt-oss for quality extraction
"""
import asyncio
import json
from pathlib import Path
from datetime import datetime
import click
from rich.console import Console
from rich.table import Table

console = Console()

class HybridScraper:
    def __init__(self):
        self.found_vessels = []
        self.not_found = []
        
    async def fast_check(self, imo: int) -> bool:
        """Quick check if vessel exists using fast model or basic fetch"""
        from playwright.async_api import async_playwright
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            url = f"https://www.balticshipping.com/vessel/imo/{imo}"
            try:
                await page.goto(url, timeout=10000)
                content = await page.content()
                
                # Quick check for vessel existence
                if 'not found' in content.lower() or 'no vessel' in content.lower():
                    await browser.close()
                    return False
                    
                # Check for actual vessel data
                if 'IMO number' in content or 'MMSI' in content:
                    await browser.close()
                    return True
                    
            except:
                pass
            
            await browser.close()
            return False
    
    async def fast_scan_range(self, start: int, end: int, workers: int = 10):
        """Scan range with multiple workers to find valid vessels"""
        semaphore = asyncio.Semaphore(workers)
        
        async def check_with_limit(imo):
            async with semaphore:
                exists = await self.fast_check(imo)
                if exists:
                    self.found_vessels.append(imo)
                    console.print(f"[green]✓ Found vessel: IMO {imo}[/green]")
                else:
                    self.not_found.append(imo)
                return exists
        
        # Create all tasks
        tasks = [check_with_limit(imo) for imo in range(start, end + 1)]
        
        # Run with progress
        from rich.progress import Progress
        with Progress() as progress:
            task = progress.add_task(f"Fast scanning IMOs {start}-{end}", total=len(tasks))
            
            for coro in asyncio.as_completed(tasks):
                await coro
                progress.advance(task)
        
        return self.found_vessels
    
    async def quality_extract(self, imos: list, model: str = 'gpt-oss:20b'):
        """Extract detailed data from found vessels using gpt-oss"""
        from src.baltic_shipping.llm_intelligent_scraper import LLMIntelligentScraper
        
        scraper = LLMIntelligentScraper(ollama_model=model)
        results = []
        
        console.print(f"\n[cyan]Extracting detailed data from {len(imos)} vessels with {model}[/cyan]")
        
        for imo in imos:
            console.print(f"Processing IMO {imo}...")
            result = await scraper.scrape_vessel_comprehensive(imo)
            if result:
                results.append(result)
        
        return results

@click.command()
@click.option('--start', default=9000000, help='Start IMO')
@click.option('--end', default=9000100, help='End IMO')
@click.option('--fast-workers', default=10, help='Workers for fast scanning')
@click.option('--quality-model', default='gpt-oss:20b', help='Model for quality extraction')
def main(start, end, fast_workers, quality_model):
    """
    Hybrid approach: Fast scan + Quality extraction
    
    Phase 1: Quickly scan IMO range to find valid vessels (10 parallel workers)
    Phase 2: Use gpt-oss to extract detailed data from found vessels
    
    This is MUCH faster than checking every IMO with gpt-oss!
    """
    
    async def run():
        scraper = HybridScraper()
        
        # Phase 1: Fast scan
        console.print(f"""
        [bold cyan]Phase 1: Fast Vessel Detection[/bold cyan]
        Range: {start:,} - {end:,}
        Workers: {fast_workers}
        """)
        
        found = await scraper.fast_scan_range(start, end, fast_workers)
        
        # Statistics
        stats = Table(title="Scan Results")
        stats.add_column("Metric", style="cyan")
        stats.add_column("Value", style="green")
        stats.add_row("Total Scanned", str(end - start + 1))
        stats.add_row("Vessels Found", str(len(found)))
        stats.add_row("Not Found", str(len(scraper.not_found)))
        stats.add_row("Hit Rate", f"{len(found)/(end-start+1)*100:.1f}%")
        console.print(stats)
        
        if not found:
            console.print("[yellow]No vessels found in range[/yellow]")
            return
        
        # Phase 2: Quality extraction
        console.print(f"""
        [bold cyan]Phase 2: Quality Data Extraction[/bold cyan]
        Vessels to process: {len(found)}
        Model: {quality_model}
        """)
        
        results = await scraper.quality_extract(found, quality_model)
        
        # Save results
        if results:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"data/hybrid_vessels_{timestamp}.json"
            
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
            
            console.print(f"[green]✓ Saved {len(results)} vessels to {output_file}[/green]")
    
    asyncio.run(run())

if __name__ == "__main__":
    main()