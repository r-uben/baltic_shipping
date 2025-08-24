#!/usr/bin/env python3
"""
Clean IMO scraper - just get the essential vessel data
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

class CleanScraper:
    def __init__(self, workers=10, model='gpt-oss:20b'):
        self.workers = workers
        self.model = model
        self.semaphore = asyncio.Semaphore(workers)
        self.found_count = 0
        self.checked_count = 0
        
    async def extract_vessel_clean(self, imo: int):
        """Extract just the essential vessel data"""
        from playwright.async_api import async_playwright
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            url = f"https://www.balticshipping.com/vessel/imo/{imo}"
            
            try:
                await page.goto(url, timeout=30000)
                await page.wait_for_load_state('networkidle')
                
                # Get the main page content
                content = await page.content()
                
                # Use LLM to extract just the essential fields
                from src.baltic_shipping.llm_intelligent_scraper import LLMIntelligentScraper
                scraper = LLMIntelligentScraper(ollama_model=self.model)
                
                # Simple prompt for clean extraction
                prompt = f"""Extract vessel data for IMO {imo}. Return ONLY these fields as JSON:
                - imo
                - mmsi  
                - name
                - flag
                - type
                - length
                - breadth
                - description
                
                If a field is not found, use null. Return only the JSON object, no extra text."""
                
                # Query LLM directly
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    response = await session.post(
                        'http://localhost:11434/api/generate',
                        json={
                            'model': self.model,
                            'prompt': prompt + "\n\nHTML Content:\n" + content[:10000],  # Limit content
                            'stream': False,
                            'format': 'json'
                        },
                        timeout=aiohttp.ClientTimeout(total=60)
                    )
                    
                    if response.status == 200:
                        result = await response.json()
                        vessel_data = json.loads(result['response'])
                        
                        # Ensure IMO is set
                        vessel_data['imo'] = str(imo)
                        
                        await browser.close()
                        return vessel_data
                    
            except Exception as e:
                console.print(f"[red]Error extracting IMO {imo}: {str(e)[:50]}[/red]")
            
            await browser.close()
            return None
        
    async def process_imo(self, imo: int):
        """Process single IMO: check and extract if exists"""
        async with self.semaphore:
            # Step 1: Check checksum
            if not is_valid_imo(imo):
                return None
            
            # Step 2: Quick existence check
            from playwright.async_api import async_playwright
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                url = f"https://www.balticshipping.com/vessel/imo/{imo}"
                try:
                    response = await page.goto(url, timeout=5000, wait_until='domcontentloaded')
                    
                    if response.status == 404:
                        await browser.close()
                        self.checked_count += 1
                        return None
                    
                    content = await page.content()
                    if 'vessel not found' in content.lower() or 'no vessel' in content.lower():
                        await browser.close()
                        self.checked_count += 1
                        return None
                    
                    await browser.close()
                    
                    # Step 3: Extract clean data
                    console.print(f"[green]✓ Found vessel: IMO {imo}[/green]")
                    
                    data = await self.extract_vessel_clean(imo)
                    if data:
                        self.found_count += 1
                        self.checked_count += 1
                        
                        # Save as individual JSON
                        imo_str = str(imo)
                        dir_path = Path(f"data/vessels_clean/{imo_str[0]}/{imo_str[1]}/{imo_str[2]}")
                        dir_path.mkdir(parents=True, exist_ok=True)
                        
                        individual_file = dir_path / f"{imo}.json"
                        async with aiofiles.open(individual_file, 'w') as f:
                            await f.write(json.dumps(data, indent=2))
                        
                        console.print(f"  → {data.get('name', 'Unknown')} - {data.get('flag', 'Unknown')}")
                        return data
                    
                except Exception as e:
                    console.print(f"[yellow]Error checking IMO {imo}: {str(e)[:30]}[/yellow]")
                    await browser.close()
                
                self.checked_count += 1
                return None
    
    async def scan_range(self, start: int, end: int):
        """Scan IMO range"""
        batch_size = self.workers * 10
        all_results = []
        
        with Progress() as progress:
            task = progress.add_task(f"Scanning IMOs {start:,} to {end:,}", total=end-start)
            
            for batch_start in range(start, end, batch_size):
                batch_end = min(batch_start + batch_size, end)
                batch_imos = list(range(batch_start, batch_end))
                
                tasks = [self.process_imo(imo) for imo in batch_imos]
                results = await asyncio.gather(*tasks)
                
                for result in results:
                    if result:
                        all_results.append(result)
                
                progress.advance(task, len(batch_imos))
                
                if self.checked_count % 100 == 0 and self.checked_count > 0:
                    hit_rate = (self.found_count / self.checked_count * 100)
                    progress.console.print(
                        f"  Stats: {self.checked_count:,} checked, {self.found_count:,} found ({hit_rate:.1f}%)"
                    )
        
        # Save summary
        if all_results:
            summary_file = f"data/vessels_clean_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            async with aiofiles.open(summary_file, 'w') as f:
                await f.write(json.dumps(all_results, indent=2))
            console.print(f"[green]Summary saved to {summary_file}[/green]")
        
        return all_results

@click.command()
@click.option('--start', default=9000000, help='Start IMO')
@click.option('--end', default=9001000, help='End IMO')
@click.option('--workers', default=10, help='Parallel workers')
@click.option('--model', default='gpt-oss:20b', help='LLM model')
def main(start, end, workers, model):
    """
    Clean IMO scraper - just the essential data
    
    Output format (per vessel):
    {
        "imo": "9000003",
        "mmsi": "525909090",
        "name": "ADHAR",
        "flag": "Indonesia",
        "type": "Tug boat",
        "length": "2 m",
        "breadth": "2 m",
        "description": "..."
    }
    """
    
    console.print(f"""
    [bold cyan]Clean Vessel Scraper[/bold cyan]
    Range: {start:,} - {end:,}
    Workers: {workers}
    Model: {model}
    
    Output: Clean JSON with 8 essential fields only
    """)
    
    async def run():
        scraper = CleanScraper(workers=workers, model=model)
        results = await scraper.scan_range(start, end)
        
        console.print(f"""
        [green]✓ Complete![/green]
        
        Found: {len(results)} vessels
        Checked: {scraper.checked_count} IMOs
        Hit rate: {(len(results)/scraper.checked_count*100 if scraper.checked_count else 0):.1f}%
        """)
    
    asyncio.run(run())

if __name__ == "__main__":
    main()