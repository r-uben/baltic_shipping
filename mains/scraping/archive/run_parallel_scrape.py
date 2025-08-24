#!/usr/bin/env python3
"""
Parallel IMO scraper using multiple LLM instances
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

class ParallelScraper:
    def __init__(self, num_workers=3, model='llama3.2:latest'):
        self.num_workers = num_workers
        self.model = model
        self.semaphore = asyncio.Semaphore(num_workers)
        
    async def scrape_with_worker(self, imo: int, worker_id: int):
        """Individual worker to scrape one vessel"""
        async with self.semaphore:
            from src.baltic_shipping.llm_intelligent_scraper import LLMIntelligentScraper
            
            # Each worker gets its own scraper instance
            scraper = LLMIntelligentScraper(ollama_model=self.model)
            
            console.print(f"[dim]Worker {worker_id}: Scraping IMO {imo}[/dim]")
            
            try:
                result = await scraper.scrape_vessel_comprehensive(imo)
                if result:
                    console.print(f"[green]✓ Worker {worker_id}: IMO {imo} - {len(result.get('combined_data', {}))} fields[/green]")
                else:
                    console.print(f"[yellow]⚠ Worker {worker_id}: IMO {imo} - Not found[/yellow]")
                return result
            except Exception as e:
                console.print(f"[red]✗ Worker {worker_id}: IMO {imo} - Error: {str(e)[:50]}[/red]")
                return None
    
    async def scrape_parallel_batch(self, imos: list):
        """Scrape multiple IMOs in parallel"""
        tasks = []
        for i, imo in enumerate(imos):
            worker_id = i % self.num_workers + 1
            task = self.scrape_with_worker(imo, worker_id)
            tasks.append(task)
        
        # Run all tasks in parallel (limited by semaphore)
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

@click.command()
@click.option('--start', default=9000000, help='Start IMO')
@click.option('--end', default=9000010, help='End IMO')
@click.option('--workers', default=3, help='Number of parallel workers')
@click.option('--model', default='llama3.2:latest', help='LLM model (use fast model for parallel)')
def main(start, end, workers, model):
    """
    Parallel scraper using multiple workers
    
    IMPORTANT: Only use with fast models like llama3.2
    gpt-oss is too slow/heavy for parallel processing
    """
    
    console.print(f"""
    [bold cyan]Parallel IMO Scraper[/bold cyan]
    Range: {start:,} - {end:,}
    Workers: {workers}
    Model: {model}
    
    [yellow]Note: Each worker runs independently[/yellow]
    """)
    
    if model == 'gpt-oss:20b' and workers > 1:
        console.print("[red]Warning: gpt-oss is too slow for parallel processing![/red]")
        console.print("[yellow]Recommended: Use llama3.2:latest for parallel scraping[/yellow]")
        if not click.confirm("Continue anyway?"):
            return
    
    async def run():
        scraper = ParallelScraper(num_workers=workers, model=model)
        imos = list(range(start, end + 1))
        
        with Progress() as progress:
            task = progress.add_task(f"Scraping {len(imos)} vessels", total=len(imos))
            
            # Process in chunks equal to worker count
            batch_size = workers * 3  # Process 3x workers at a time
            all_results = []
            
            for i in range(0, len(imos), batch_size):
                batch = imos[i:i+batch_size]
                results = await scraper.scrape_parallel_batch(batch)
                all_results.extend(results)
                progress.update(task, advance=len(batch))
                
                # Save intermediate results
                if all_results:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_file = f"data/parallel_vessels_{timestamp}.json"
                    async with aiofiles.open(output_file, 'w') as f:
                        await f.write(json.dumps(all_results, indent=2))
            
            console.print(f"[green]✓ Completed: {len(all_results)} vessels found[/green]")
    
    asyncio.run(run())

if __name__ == "__main__":
    main()