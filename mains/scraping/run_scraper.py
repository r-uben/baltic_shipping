import time
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.text import Text
from rich.table import Table
from colorama import Fore, Back, Style, init
from baltic_shipping import config, file_handler, scraper

# Initialize colorama
init()

console = Console()

def main():
    """
    Main function to run the vessel scraper with beautiful output.
    """
    config.setup_directories()
    
    # The scraper now has its own beautiful header
    urls = scraper.get_all_vessel_urls()
    file_handler.save_urls(urls)
    
    console.print(f"\n⛩️  [green]Collected {len(urls):,} vessel URLs![/green]")
    console.print(f"🔍 [blue]Processing: {len(urls):,} vessels total[/blue]")
    
    # Count existing files
    existing_count = len(list(config.JSON_DIR.glob("*.json")))
    remaining_count = len(urls) - existing_count
    
    if remaining_count == 0:
        console.print("🎉 [green]All vessel data already collected![/green]")
        return
    
    console.print(f"🔍 [blue]Existing data: {existing_count:,} vessels | Remaining: {remaining_count:,} vessels[/blue]")
    
    # Create Japanese-style progress bar  
    with Progress(
        SpinnerColumn("dots12", style="cyan"),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=40, style="cyan", complete_style="green"),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("•"),
        TextColumn("[cyan]{task.completed:,}/{task.total:,} vessels[/cyan]"),
        TimeRemainingColumn(),
        console=console
    ) as progress:
        
        task = progress.add_task("⛩️ Deep Vessel Analysis", total=len(urls))
        
        success_count = 0
        skip_count = 0
        error_count = 0
        
        for i, url in enumerate(urls):
            try:
                # Check if data already exists
                imo = url.split('/')[-1]
                if (config.JSON_DIR / f"{imo}.json").exists():
                    skip_count += 1
                    progress.update(task, description="[yellow]🔄 Vessel data exists, skipping[/yellow]", advance=1)
                    continue

                vessel_data = scraper.scrape_vessel_page(url)
                if vessel_data:
                    file_handler.save_vessel_data(vessel_data)
                    success_count += 1
                    progress.update(task, description=f"[green]🚢 Analyzed vessel {imo}[/green]", advance=1)
                else:
                    error_count += 1
                    progress.update(task, description="[red]❌ No data extracted[/red]", advance=1)
                    
            except Exception as e:
                error_count += 1
                progress.update(task, description=f"[red]❌ Error: {str(e)[:30]}...[/red]", advance=1)
                
            time.sleep(0.1)  # Small delay for smooth progress
    
    # Final summary with Japanese aesthetics
    completion_panel = Panel(
        Text("⛩️ Deep Ocean Mission Complete ⛩️", justify="center", style="bold cyan"),
        border_style="cyan",
        padding=(1, 2)
    )
    console.print(completion_panel)
    
    summary_table = Table(show_header=True, header_style="bold magenta", border_style="cyan")
    summary_table.add_column("📊 Mission Results", style="bold")
    summary_table.add_column("Count", justify="right", style="bold green")
    
    summary_table.add_row("✅ Successfully Analyzed", f"{success_count:,}")
    summary_table.add_row("⏭️  Already Existing", f"{skip_count:,}")
    summary_table.add_row("❌ Failed Analysis", f"{error_count:,}")
    summary_table.add_row("🎯 Total Vessels", f"{len(urls):,}")
    
    console.print(summary_table)
    
    if error_count == 0:
        console.print("\n🎌 [green]Perfect Mission! All vessel data successfully collected![/green]")
    else:
        console.print(f"\n⚠️  [yellow]Mission completed with {error_count:,} analysis failures[/yellow]")
    
    console.print("\n🔄 [blue]Next: Generate unified CSV → poetry run merge[/blue]")

if __name__ == "__main__":
    main()
