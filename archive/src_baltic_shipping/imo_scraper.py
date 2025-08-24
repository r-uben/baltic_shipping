"""
IMO-based vessel scraper for Baltic Shipping
This approach scrapes vessels directly by IMO number to ensure comprehensive coverage
"""
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import json
from typing import Dict, List, Optional, Tuple
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.text import Text
import time
from datetime import datetime

console = Console()

class IMOScraper:
    """Scraper that fetches vessels directly by IMO number"""
    
    def __init__(self, output_dir: str = "data", max_concurrent: int = 10):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.base_url = "https://www.balticshipping.com/vessel/imo/"
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
    async def fetch_vessel(self, session: aiohttp.ClientSession, imo: int) -> Tuple[int, Optional[Dict]]:
        """Fetch a single vessel by IMO number"""
        async with self.semaphore:
            url = f"{self.base_url}{imo}"
            try:
                async with session.get(url, timeout=30) as response:
                    if response.status == 200:
                        text = await response.text()
                        vessel_data = self.parse_vessel_page(text, imo, url)
                        return imo, vessel_data
                    elif response.status == 404:
                        return imo, None  # Vessel doesn't exist
                    else:
                        console.print(f"[yellow]Warning: IMO {imo} returned status {response.status}[/yellow]")
                        return imo, None
            except asyncio.TimeoutError:
                console.print(f"[yellow]Timeout for IMO {imo}[/yellow]")
                return imo, None
            except Exception as e:
                console.print(f"[red]Error fetching IMO {imo}: {str(e)}[/red]")
                return imo, None
    
    def parse_vessel_page(self, html: str, imo: int, url: str) -> Optional[Dict]:
        """Parse vessel data from HTML page"""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Check if this is a valid vessel page
        if "Vessel not found" in html or "404" in html:
            return None
            
        vessel_data = {
            'IMO number': str(imo),
            'source_url': url
        }
        
        # Extract vessel name from h1
        h1 = soup.find('h1')
        if h1:
            vessel_data['Vessel name'] = h1.text.strip()
        
        # Extract data from the info table
        info_table = soup.find('table', class_='table')
        if info_table:
            for row in info_table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) == 2:
                    key = cells[0].text.strip().rstrip(':')
                    value = cells[1].text.strip()
                    if key and value and value != '-':
                        vessel_data[key] = value
        
        # Only return if we found meaningful data
        if len(vessel_data) > 2:  # More than just IMO and URL
            return vessel_data
        return None
    
    async def scrape_imo_range(self, start_imo: int, end_imo: int, checkpoint_interval: int = 1000):
        """Scrape a range of IMO numbers"""
        banner = Panel(
            Text("🚢 Baltic Shipping IMO-Based Scraper v1.0 🚢\n⚓ Direct IMO Access Protocol ⚓", 
                 justify="center", style="bold cyan"),
            border_style="blue",
            padding=(1, 2)
        )
        console.print(banner)
        
        total_imos = end_imo - start_imo + 1
        console.print(f"[green]Scanning IMO range: {start_imo:,} to {end_imo:,} ({total_imos:,} IMOs)[/green]")
        console.print(f"[cyan]Max concurrent requests: {self.max_concurrent}[/cyan]")
        
        vessels_found = []
        vessels_not_found = []
        errors = []
        
        async with aiohttp.ClientSession() as session:
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
                
                # Process in batches for better progress tracking
                batch_size = 100
                for batch_start in range(start_imo, end_imo + 1, batch_size):
                    batch_end = min(batch_start + batch_size - 1, end_imo)
                    batch_imos = list(range(batch_start, batch_end + 1))
                    
                    # Create tasks for this batch
                    tasks = [self.fetch_vessel(session, imo) for imo in batch_imos]
                    results = await asyncio.gather(*tasks)
                    
                    # Process results
                    for imo, vessel_data in results:
                        if vessel_data:
                            vessels_found.append(vessel_data)
                            progress.update(task, description=f"[green]Found: {len(vessels_found):,} vessels[/green]")
                        else:
                            vessels_not_found.append(imo)
                        
                        progress.advance(task)
                    
                    # Checkpoint save
                    if len(vessels_found) % checkpoint_interval == 0 and vessels_found:
                        self.save_checkpoint(vessels_found, vessels_not_found, batch_end)
                        console.print(f"[blue]💾 Checkpoint saved at IMO {batch_end}[/blue]")
                    
                    # Small delay between batches to be respectful
                    await asyncio.sleep(0.5)
        
        # Final save
        self.save_final_results(vessels_found, vessels_not_found, start_imo, end_imo)
        
        # Summary
        console.print("\n" + "=" * 60)
        console.print(f"[green]✅ Scanning complete![/green]")
        console.print(f"[cyan]📊 Results:[/cyan]")
        console.print(f"  • Vessels found: {len(vessels_found):,}")
        console.print(f"  • IMOs not found: {len(vessels_not_found):,}")
        console.print(f"  • Total scanned: {total_imos:,}")
        console.print(f"  • Success rate: {len(vessels_found)/total_imos*100:.1f}%")
        
        return vessels_found
    
    def save_checkpoint(self, vessels: List[Dict], not_found: List[int], last_imo: int):
        """Save intermediate results"""
        checkpoint_file = self.output_dir / f"imo_checkpoint_{last_imo}.json"
        checkpoint_data = {
            'timestamp': datetime.now().isoformat(),
            'last_imo': last_imo,
            'vessels_found': len(vessels),
            'imos_not_found': len(not_found),
            'sample_not_found': not_found[-10:] if len(not_found) > 10 else not_found
        }
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)
    
    def save_final_results(self, vessels: List[Dict], not_found: List[int], start_imo: int, end_imo: int):
        """Save final results to CSV and JSON"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save vessels to CSV
        if vessels:
            df = pd.DataFrame(vessels)
            csv_file = self.output_dir / f"vessels_imo_{start_imo}_{end_imo}_{timestamp}.csv"
            df.to_csv(csv_file, index=False)
            console.print(f"[green]💾 Saved {len(vessels)} vessels to {csv_file}[/green]")
        
        # Save summary to JSON
        summary_file = self.output_dir / f"imo_scrape_summary_{timestamp}.json"
        summary = {
            'timestamp': datetime.now().isoformat(),
            'imo_range': {'start': start_imo, 'end': end_imo},
            'total_scanned': end_imo - start_imo + 1,
            'vessels_found': len(vessels),
            'imos_not_found': len(not_found),
            'success_rate': len(vessels) / (end_imo - start_imo + 1) * 100,
            'not_found_imos': not_found
        }
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        console.print(f"[green]📊 Saved summary to {summary_file}[/green]")

async def main():
    """Main function to run IMO-based scraping"""
    scraper = IMOScraper()
    
    # IMO numbers typically range from 5100000 to 9999999
    # Let's start with a smaller test range
    # For full scraping, we'd need to determine the actual IMO range used by Baltic Shipping
    
    # Test with a small range first
    console.print("[yellow]Starting with test range to validate approach...[/yellow]")
    test_vessels = await scraper.scrape_imo_range(9872360, 9872370)
    
    if test_vessels:
        console.print(f"\n[green]✅ Test successful! Found {len(test_vessels)} vessels[/green]")
        console.print("[cyan]Ready for full-scale IMO scraping.[/cyan]")
        console.print("[yellow]Note: Full scraping would require determining the complete IMO range.[/yellow]")
        console.print("[yellow]Recommendation: Scan IMO ranges 5100000-9999999 in segments.[/yellow]")

if __name__ == "__main__":
    asyncio.run(main())