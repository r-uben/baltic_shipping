#!/usr/bin/env python3
"""
Full IMO range scraper with progress tracking and resume capability
"""
import asyncio
import json
import os
from pathlib import Path
from datetime import datetime
import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn
from rich.table import Table

console = Console()

class FullScrapingManager:
    def __init__(self, checkpoint_file="data/scraping_checkpoint.json"):
        self.checkpoint_file = Path(checkpoint_file)
        self.checkpoint_file.parent.mkdir(exist_ok=True)
        self.checkpoint_data = self.load_checkpoint()
        
    def load_checkpoint(self):
        """Load checkpoint for resume capability"""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file) as f:
                return json.load(f)
        return {
            "last_imo": 0,
            "vessels_found": 0,
            "vessels_not_found": 0,
            "errors": 0,
            "start_time": datetime.now().isoformat()
        }
    
    def save_checkpoint(self, imo, found=False, error=False):
        """Save progress checkpoint"""
        self.checkpoint_data["last_imo"] = imo
        if found:
            self.checkpoint_data["vessels_found"] += 1
        elif error:
            self.checkpoint_data["errors"] += 1
        else:
            self.checkpoint_data["vessels_not_found"] += 1
            
        with open(self.checkpoint_file, 'w') as f:
            json.dump(self.checkpoint_data, f, indent=2)
    
    async def scrape_range(self, start_imo, end_imo, scraper, batch_size=100):
        """Scrape IMO range with progress tracking"""
        current = max(start_imo, self.checkpoint_data.get("last_imo", 0) + 1)
        
        if current > start_imo:
            console.print(f"[yellow]Resuming from IMO {current}[/yellow]")
        
        with Progress(
            SpinnerColumn(),
            *Progress.get_default_columns(),
            TimeElapsedColumn(),
            console=console
        ) as progress:
            task = progress.add_task(
                f"Scraping IMOs {current}-{end_imo}", 
                total=end_imo - current + 1
            )
            
            while current <= end_imo:
                batch_end = min(current + batch_size - 1, end_imo)
                batch_imos = list(range(current, batch_end + 1))
                
                # Scrape batch
                results = await scraper.scrape_vessels_batch(batch_imos)
                
                # Process results
                for result in results:
                    if result and result.get("combined_data"):
                        self.save_checkpoint(result["imo"], found=True)
                    else:
                        imo = result.get("imo") if result else current
                        self.save_checkpoint(imo, found=False)
                
                # Update progress
                progress.update(task, advance=len(batch_imos))
                current = batch_end + 1
                
                # Show stats
                self.show_stats()
                
                # Small delay between batches
                await asyncio.sleep(2)
    
    def show_stats(self):
        """Display current scraping statistics"""
        stats = Table(title="Scraping Statistics")
        stats.add_column("Metric", style="cyan")
        stats.add_column("Value", style="green")
        
        stats.add_row("Last IMO", str(self.checkpoint_data["last_imo"]))
        stats.add_row("Vessels Found", str(self.checkpoint_data["vessels_found"]))
        stats.add_row("Not Found", str(self.checkpoint_data["vessels_not_found"]))
        stats.add_row("Errors", str(self.checkpoint_data["errors"]))
        
        total = (self.checkpoint_data["vessels_found"] + 
                self.checkpoint_data["vessels_not_found"] + 
                self.checkpoint_data["errors"])
        if total > 0:
            success_rate = (self.checkpoint_data["vessels_found"] / total) * 100
            stats.add_row("Success Rate", f"{success_rate:.1f}%")
        
        console.print(stats)

@click.command()
@click.option('--start', default=1000000, help='Start IMO (default: 1000000)')
@click.option('--end', default=9999999, help='End IMO (default: 9999999)')
@click.option('--batch-size', default=100, help='Batch size for processing')
@click.option('--model', default='gpt-oss:20b', help='LLM model to use')
@click.option('--reset', is_flag=True, help='Reset checkpoint and start fresh')
def main(start, end, batch_size, model, reset):
    """
    Scrape all IMOs from Baltic Shipping with smart detection
    
    Features:
    - Validates IMO numbers
    - Skips non-existent vessels
    - Resume capability after interruption
    - Progress tracking
    - Statistics reporting
    """
    
    # Setup
    from src.baltic_shipping.llm_intelligent_scraper import LLMIntelligentScraper
    
    manager = FullScrapingManager()
    
    if reset:
        manager.checkpoint_data = {
            "last_imo": 0,
            "vessels_found": 0,
            "vessels_not_found": 0,
            "errors": 0,
            "start_time": datetime.now().isoformat()
        }
        manager.save_checkpoint(0)
        console.print("[yellow]Checkpoint reset[/yellow]")
    
    # Initialize scraper
    scraper = LLMIntelligentScraper(ollama_model=model)
    
    console.print(f"""
    [bold cyan]Full IMO Range Scraper[/bold cyan]
    Range: {start:,} - {end:,}
    Model: {model}
    Batch Size: {batch_size}
    Resume: {manager.checkpoint_data.get('last_imo', 0) > 0}
    """)
    
    # Run scraping
    asyncio.run(manager.scrape_range(start, end, scraper, batch_size))
    
    # Final stats
    console.print("\n[bold green]Scraping Complete![/bold green]")
    manager.show_stats()

if __name__ == "__main__":
    main()