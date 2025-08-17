import asyncio
import time
import argparse
import os
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.table import Table
from baltic_shipping import config, file_handler
from baltic_shipping.fast_scraper import FastScraper

console = Console()

async def main(workers=None):
    """
    High-performance vessel scraper with parallel processing.
    """
    start_time = time.time()
    
    config.setup_directories()
    
    # Determine worker count
    if workers is None:
        cpu_count = os.cpu_count() or 4
        workers = min(cpu_count * 12, 100)  # Auto-scale: 12x CPU cores, max 100
    
    # Show performance banner
    banner = Panel(
        Text(f"🚀 FAST MODE ACTIVATED 🚀\n⚡ High-Performance Parallel Scraper ⚡\n🔧 Workers: {workers}", 
             justify="center", style="bold green"),
        border_style="green",
        padding=(1, 2)
    )
    console.print(banner)
    
    # Create scraper with custom worker count
    scraper = FastScraper(max_concurrent_pages=2, max_concurrent_vessels=workers)
    
    # Phase 1: Fast URL collection
    console.print("\n📡 [cyan]Phase 1: High-speed URL collection[/cyan]")
    urls = await scraper.get_all_vessel_urls_fast()
    
    if not urls:
        console.print("❌ [red]No URLs collected. Aborting.[/red]")
        return
    
    # Save URLs
    file_handler.save_urls(urls)
    console.print(f"💾 [green]Saved {len(urls):,} vessel URLs[/green]")
    
    # Phase 2: Parallel vessel scraping
    console.print("\n⚡ [cyan]Phase 2: Parallel vessel data collection[/cyan]")
    await scraper.scrape_vessels_parallel(urls)
    
    # Performance summary
    elapsed = time.time() - start_time
    
    completion_panel = Panel(
        Text("🚀 FAST MODE COMPLETE 🚀", justify="center", style="bold green"),
        border_style="green",
        padding=(1, 2)
    )
    console.print(completion_panel)
    
    # Performance stats
    stats_table = Table(show_header=True, header_style="bold green", border_style="green")
    stats_table.add_column("📊 Performance Metrics", style="bold")
    stats_table.add_column("Value", justify="right", style="bold cyan")
    
    stats_table.add_row("🔍 Total Vessels Found", f"{len(urls):,}")
    stats_table.add_row("⏱️ Total Time", f"{elapsed/60:.1f} minutes")
    stats_table.add_row("⚡ Speed", f"{len(urls)/(elapsed/60):.0f} vessels/minute")
    
    # Check completion
    existing_count = len(list(config.JSON_DIR.glob("*.json")))
    stats_table.add_row("📁 JSON Files Created", f"{existing_count:,}")
    stats_table.add_row("✅ Completion Rate", f"{(existing_count/len(urls)*100):.1f}%")
    
    console.print(stats_table)
    
    if existing_count >= len(urls) * 0.95:  # 95% success rate
        console.print("\n🎌 [green]High-performance mission successful![/green]")
    else:
        console.print(f"\n⚠️ [yellow]Completed with {len(urls) - existing_count:,} missing vessels[/yellow]")
    
    console.print("\n🔄 [blue]Next: Generate unified CSV → poetry run merge[/blue]")

def run_fast():
    """Synchronous wrapper for the async main function with CLI argument support."""
    parser = argparse.ArgumentParser(description='High-performance Baltic Shipping vessel scraper')
    parser.add_argument('--workers', '-w', type=int, default=None,
                       help='Number of concurrent workers for vessel scraping (default: auto-scale based on CPU cores)')
    parser.add_argument('--max-pages', '-p', type=int, default=25000,
                       help='Maximum pages to process (default: 25000)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.workers is not None:
        if args.workers < 1:
            console.print("❌ [red]Workers must be at least 1[/red]")
            return
        elif args.workers > 500:
            console.print("⚠️ [yellow]Warning: Very high worker count may cause issues[/yellow]")
    
    console.print(f"🔧 [cyan]Configuration:[/cyan]")
    if args.workers:
        console.print(f"   👥 Workers: {args.workers}")
    else:
        cpu_count = os.cpu_count() or 4
        auto_workers = min(cpu_count * 12, 100)
        console.print(f"   👥 Workers: {auto_workers} (auto-scaled from {cpu_count} CPU cores)")
    console.print(f"   📄 Max pages: {args.max_pages:,}")
    console.print()
    
    asyncio.run(main(workers=args.workers))

if __name__ == "__main__":
    run_fast()