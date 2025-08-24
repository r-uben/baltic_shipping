#!/usr/bin/env python3
"""
Main script to run IMO-based vessel scraping
This ensures we get ALL vessels by directly accessing them via IMO number
"""
import asyncio
import argparse
import aiohttp
from baltic_shipping.imo_scraper import IMOScraper
from baltic_shipping.imo_playwright_scraper import IMOPlaywrightScraper, test_missing_vessels
from rich.console import Console

console = Console()

async def run_full_scrape():
    """Run full IMO-based scraping"""
    scraper = IMOScraper(max_concurrent=20)  # Increase concurrency for production
    
    # IMO numbers are 7 digits, typically ranging from 5100000 to 9999999
    # But most modern vessels are in the 7000000-9999999 range
    # We'll scrape in segments to allow for interruption/resumption
    
    segments = [
        (7000000, 7499999),  # Older vessels
        (7500000, 7999999),  # 1970s-1980s vessels
        (8000000, 8499999),  # 1980s vessels
        (8500000, 8999999),  # 1990s vessels
        (9000000, 9499999),  # 2000s vessels
        (9500000, 9999999),  # Recent vessels
    ]
    
    all_vessels = []
    
    for start, end in segments:
        console.print(f"\n[cyan]📍 Processing segment: {start:,} - {end:,}[/cyan]")
        vessels = await scraper.scrape_imo_range(start, end, checkpoint_interval=5000)
        all_vessels.extend(vessels)
        console.print(f"[green]Segment complete. Total vessels so far: {len(all_vessels):,}[/green]")
        
        # Save intermediate full dataset
        if all_vessels:
            import pandas as pd
            df = pd.DataFrame(all_vessels)
            df.to_csv('data/vessels_imo_complete_intermediate.csv', index=False)
            console.print(f"[blue]💾 Saved intermediate full dataset[/blue]")
    
    # Save final complete dataset
    if all_vessels:
        import pandas as pd
        df = pd.DataFrame(all_vessels)
        df.to_csv('data/vessels_imo_complete.csv', index=False)
        console.print(f"\n[green]✅ Complete! Saved {len(all_vessels):,} vessels to vessels_imo_complete.csv[/green]")
    
    return all_vessels

async def run_test_scrape():
    """Run a test scrape with the missing vessels identified using Playwright"""
    # Use the Playwright version for JavaScript-rendered pages
    return await test_missing_vessels()

async def run_custom_range(start: int, end: int, concurrent: int = 5):
    """Run scraping for a custom IMO range using Playwright"""
    scraper = IMOPlaywrightScraper(max_concurrent=concurrent)
    vessels = await scraper.scrape_imo_range(start, end)
    return vessels

def main():
    parser = argparse.ArgumentParser(description='IMO-based vessel scraper for Baltic Shipping')
    parser.add_argument('--mode', choices=['test', 'full', 'custom'], default='test',
                        help='Scraping mode: test (missing vessels), full (all IMOs), custom (specify range)')
    parser.add_argument('--start', type=int, help='Start IMO for custom range')
    parser.add_argument('--end', type=int, help='End IMO for custom range')
    parser.add_argument('--concurrent', type=int, default=10, help='Max concurrent requests')
    
    args = parser.parse_args()
    
    if args.mode == 'test':
        asyncio.run(run_test_scrape())
    elif args.mode == 'full':
        console.print("[bold red]⚠️  WARNING: Full scraping will take several hours and make millions of requests![/bold red]")
        console.print("[yellow]Consider running in segments or using custom ranges first.[/yellow]")
        response = input("Continue with full scrape? (yes/no): ")
        if response.lower() == 'yes':
            asyncio.run(run_full_scrape())
        else:
            console.print("[cyan]Cancelled. Use --mode custom with --start and --end for specific ranges.[/cyan]")
    elif args.mode == 'custom':
        if not args.start or not args.end:
            console.print("[red]Error: Custom mode requires --start and --end arguments[/red]")
            return
        asyncio.run(run_custom_range(args.start, args.end, args.concurrent))

if __name__ == "__main__":
    main()