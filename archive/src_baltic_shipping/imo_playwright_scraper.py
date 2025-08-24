"""
IMO-based vessel scraper using Playwright for JavaScript-rendered pages
"""
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import json
from typing import Dict, List, Optional, Tuple
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.text import Text
from datetime import datetime
import re

console = Console()

class IMOPlaywrightScraper:
    """Scraper that fetches vessels directly by IMO number using Playwright"""
    
    def __init__(self, output_dir: str = "data", max_concurrent: int = 5):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.base_url = "https://www.balticshipping.com/vessel/imo/"
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
    async def fetch_vessel(self, page, imo: int) -> Tuple[int, Optional[Dict]]:
        """Fetch a single vessel by IMO number"""
        async with self.semaphore:
            url = f"{self.base_url}{imo}"
            try:
                await page.goto(url, timeout=30000, wait_until='networkidle')
                await page.wait_for_load_state("domcontentloaded")
                
                # Check if vessel exists
                content = await page.content()
                if "404" in content or "not found" in content.lower():
                    return imo, None
                
                vessel_data = await self.parse_vessel_page(page, imo, url)
                return imo, vessel_data
                
            except Exception as e:
                console.print(f"[red]Error fetching IMO {imo}: {str(e)[:50]}[/red]")
                return imo, None
    
    async def parse_vessel_page(self, page, imo: int, url: str) -> Optional[Dict]:
        """Parse vessel data from page"""
        content = await page.content()
        soup = BeautifulSoup(content, 'html.parser')
        
        vessel_data = {
            'IMO number': str(imo),
            'source_url': url
        }
        
        # Extract vessel name from h1
        h1 = soup.find('h1')
        if h1:
            vessel_data['Vessel name'] = h1.text.strip()
        
        # Extract data from info sections
        info_sections = soup.find_all('div', class_='info-section')
        for section in info_sections:
            rows = section.find_all('div', class_='row')
            for row in rows:
                label = row.find('div', class_='label')
                value = row.find('div', class_='value')
                if label and value:
                    key = label.text.strip().rstrip(':')
                    val = value.text.strip()
                    if key and val and val != '-':
                        vessel_data[key] = val
        
        # Alternative: Look for table with vessel info
        tables = soup.find_all('table')
        for table in tables:
            for row in table.find_all('tr'):
                cells = row.find_all(['td', 'th'])
                if len(cells) == 2:
                    key = cells[0].text.strip().rstrip(':')
                    value = cells[1].text.strip()
                    if key and value and value != '-':
                        vessel_data[key] = value
        
        # Only return if we found meaningful data
        if len(vessel_data) > 2:  # More than just IMO and URL
            return vessel_data
        return None
    
    async def scrape_imo_batch(self, imos: List[int]) -> List[Dict]:
        """Scrape a batch of IMO numbers"""
        vessels = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            # Create multiple pages for concurrent scraping
            pages = []
            for _ in range(min(self.max_concurrent, len(imos))):
                context = await browser.new_context()
                page = await context.new_page()
                pages.append(page)
            
            # Process IMOs in batches
            for i in range(0, len(imos), self.max_concurrent):
                batch = imos[i:i+self.max_concurrent]
                tasks = []
                
                for j, imo in enumerate(batch):
                    page_idx = j % len(pages)
                    tasks.append(self.fetch_vessel(pages[page_idx], imo))
                
                results = await asyncio.gather(*tasks)
                
                for imo, vessel_data in results:
                    if vessel_data:
                        vessels.append(vessel_data)
                        console.print(f"[green]✅ IMO {imo}: {vessel_data.get('Vessel name', 'Found')}[/green]")
                    else:
                        console.print(f"[yellow]⚠️ IMO {imo}: Not found or no data[/yellow]")
            
            # Close all pages and browser
            for page in pages:
                await page.close()
            await browser.close()
        
        return vessels
    
    async def scrape_imo_range(self, start_imo: int, end_imo: int, batch_size: int = 50):
        """Scrape a range of IMO numbers using Playwright"""
        banner = Panel(
            Text("🚢 Baltic Shipping IMO Playwright Scraper v2.0 🚢\n⚓ JavaScript-Enabled Direct Access ⚓", 
                 justify="center", style="bold cyan"),
            border_style="blue",
            padding=(1, 2)
        )
        console.print(banner)
        
        total_imos = end_imo - start_imo + 1
        console.print(f"[green]Scanning IMO range: {start_imo:,} to {end_imo:,} ({total_imos:,} IMOs)[/green]")
        console.print(f"[cyan]Batch size: {batch_size}, Max concurrent: {self.max_concurrent}[/cyan]")
        
        all_vessels = []
        vessels_found = 0
        vessels_not_found = 0
        
        with Progress(
            SpinnerColumn("dots12", style="cyan"),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("•"),
            TextColumn("[cyan]{task.completed:,}/{task.total:,}"),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
            transient=False
        ) as progress:
            task = progress.add_task("🔍 Scanning IMOs", total=total_imos)
            
            for batch_start in range(start_imo, end_imo + 1, batch_size):
                batch_end = min(batch_start + batch_size - 1, end_imo)
                batch_imos = list(range(batch_start, batch_end + 1))
                
                progress.update(task, description=f"[cyan]Processing batch {batch_start}-{batch_end}[/cyan]")
                
                # Scrape this batch
                batch_vessels = await self.scrape_imo_batch(batch_imos)
                all_vessels.extend(batch_vessels)
                
                vessels_found = len(all_vessels)
                vessels_not_found = (batch_end - start_imo + 1) - vessels_found
                
                progress.update(task, 
                              description=f"[green]Found: {vessels_found:,} | Not found: {vessels_not_found:,}[/green]",
                              completed=batch_end - start_imo + 1)
                
                # Save checkpoint every 500 vessels
                if vessels_found > 0 and vessels_found % 500 == 0:
                    self.save_checkpoint(all_vessels, batch_end)
                
                # Small delay between batches
                await asyncio.sleep(1)
        
        # Save final results
        if all_vessels:
            self.save_results(all_vessels, start_imo, end_imo)
        
        # Summary
        console.print("\n" + "=" * 60)
        console.print(f"[green]✅ Scanning complete![/green]")
        console.print(f"[cyan]📊 Results:[/cyan]")
        console.print(f"  • Vessels found: {vessels_found:,}")
        console.print(f"  • IMOs checked: {total_imos:,}")
        console.print(f"  • Success rate: {vessels_found/total_imos*100:.1f}%")
        
        return all_vessels
    
    def save_checkpoint(self, vessels: List[Dict], last_imo: int):
        """Save checkpoint data"""
        checkpoint_file = self.output_dir / f"imo_playwright_checkpoint_{last_imo}.json"
        with open(checkpoint_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'last_imo': last_imo,
                'vessels_found': len(vessels)
            }, f, indent=2)
    
    def save_results(self, vessels: List[Dict], start_imo: int, end_imo: int):
        """Save results to CSV"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df = pd.DataFrame(vessels)
        
        csv_file = self.output_dir / f"vessels_imo_playwright_{start_imo}_{end_imo}_{timestamp}.csv"
        df.to_csv(csv_file, index=False)
        console.print(f"[green]💾 Saved {len(vessels)} vessels to {csv_file}[/green]")

async def test_missing_vessels():
    """Test with the known missing vessels"""
    scraper = IMOPlaywrightScraper(max_concurrent=3)
    
    missing_imos = [
        9872365, 9631814, 7129049, 7503166, 8721088,
        8400294, 8213744, 8129644, 7526259, 9012604
    ]
    
    console.print("[cyan]Testing with known missing vessels...[/cyan]")
    vessels = await scraper.scrape_imo_batch(missing_imos)
    
    if vessels:
        df = pd.DataFrame(vessels)
        df.to_csv('data/test_missing_vessels_playwright.csv', index=False)
        console.print(f"\n[green]✅ Found {len(vessels)}/{len(missing_imos)} vessels[/green]")
        console.print("[green]💾 Saved to test_missing_vessels_playwright.csv[/green]")
    else:
        console.print("[red]❌ No vessels found - check if site structure changed[/red]")
    
    return vessels

if __name__ == "__main__":
    asyncio.run(test_missing_vessels())