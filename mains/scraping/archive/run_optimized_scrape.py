#!/usr/bin/env python3
"""
Optimized IMO scraper with checksum validation and smart sampling
"""
import asyncio
import json
from pathlib import Path
from datetime import datetime
import click
from rich.console import Console
from rich.progress import Progress
from rich.table import Table
import random

console = Console()

def is_valid_imo(imo: int) -> bool:
    """Validate IMO checksum - filters 90% of invalid numbers"""
    s = str(imo)
    if len(s) != 7: 
        return False
    checksum = sum(int(s[i]) * (7 - i) for i in range(6)) % 10
    return checksum == int(s[6])

class OptimizedScraper:
    def __init__(self, workers=10):
        self.workers = workers
        self.semaphore = asyncio.Semaphore(workers)
        self.found_vessels = []
        self.checked_imos = set()
        
    async def quick_check(self, imo: int) -> bool:
        """Ultra-fast vessel existence check (0.5-1s)"""
        # First validate checksum locally
        if not is_valid_imo(imo):
            return False
            
        async with self.semaphore:
            from playwright.async_api import async_playwright
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                url = f"https://www.balticshipping.com/vessel/imo/{imo}"
                try:
                    # Fast check with minimal wait
                    response = await page.goto(url, timeout=5000, wait_until='domcontentloaded')
                    
                    # Check 404 immediately
                    if response.status == 404:
                        await browser.close()
                        return False
                    
                    # Quick content check for soft 404s
                    content = await page.content()
                    exists = 'IMO number' in content or 'MMSI' in content
                    
                    await browser.close()
                    return exists
                    
                except Exception:
                    await browser.close()
                    return False
    
    async def sample_range(self, start: int, end: int, sample_size: int = 20):
        """Sample a range to estimate vessel density"""
        # Generate random sample of valid IMOs
        all_imos = [i for i in range(start, end) if is_valid_imo(i)]
        if len(all_imos) == 0:
            return 0.0
            
        sample = random.sample(all_imos, min(sample_size, len(all_imos)))
        
        # Check sample in parallel
        tasks = [self.quick_check(imo) for imo in sample]
        results = await asyncio.gather(*tasks)
        
        hit_rate = sum(results) / len(results) if results else 0
        return hit_rate
    
    async def smart_scan(self, start: int, end: int):
        """Smart scanning with sampling and targeting"""
        console.print("[bold cyan]Phase 1: Smart Sampling[/bold cyan]")
        
        # Divide range into buckets
        bucket_size = 10000
        buckets = []
        
        for bucket_start in range(start, end, bucket_size):
            bucket_end = min(bucket_start + bucket_size, end)
            buckets.append((bucket_start, bucket_end))
        
        # Sample each bucket
        console.print(f"Sampling {len(buckets)} buckets...")
        bucket_densities = []
        
        with Progress() as progress:
            task = progress.add_task("Sampling buckets", total=len(buckets))
            
            for bucket_start, bucket_end in buckets:
                density = await self.sample_range(bucket_start, bucket_end)
                bucket_densities.append((bucket_start, bucket_end, density))
                progress.advance(task)
                
                if density > 0:
                    console.print(f"  Bucket {bucket_start}-{bucket_end}: {density:.1%} hit rate")
        
        # Sort by density and scan high-priority buckets
        bucket_densities.sort(key=lambda x: x[2], reverse=True)
        
        console.print("\n[bold cyan]Phase 2: Targeted Deep Scan[/bold cyan]")
        
        # Focus on buckets with >3% hit rate
        hot_buckets = [(s, e) for s, e, d in bucket_densities if d > 0.03]
        
        if not hot_buckets:
            console.print("[yellow]No high-density buckets found[/yellow]")
            return []
        
        console.print(f"Found {len(hot_buckets)} promising buckets")
        
        # Deep scan hot buckets
        for bucket_start, bucket_end in hot_buckets:
            console.print(f"\nScanning bucket {bucket_start}-{bucket_end}")
            
            # Get all valid IMOs in bucket
            valid_imos = [i for i in range(bucket_start, bucket_end) if is_valid_imo(i)]
            
            # Check them in parallel batches
            batch_size = 50
            for i in range(0, len(valid_imos), batch_size):
                batch = valid_imos[i:i+batch_size]
                tasks = [self.quick_check(imo) for imo in batch]
                results = await asyncio.gather(*tasks)
                
                for imo, exists in zip(batch, results):
                    if exists:
                        self.found_vessels.append(imo)
                        console.print(f"[green]✓ Found: IMO {imo}[/green]")
            
            # Early stopping if we have enough
            if len(self.found_vessels) > 1000:  # Adjust threshold
                console.print(f"[yellow]Found {len(self.found_vessels)} vessels, stopping early[/yellow]")
                break
        
        return self.found_vessels
    
    async def extract_vessel_data(self, imos: list, model: str = 'gpt-oss:20b', parallel_extracts: int = 1):
        """Extract detailed data for found vessels using LLM
        
        Args:
            imos: List of IMO numbers to extract
            model: LLM model to use (gpt-oss:20b, llama3.2:latest, etc.)
            parallel_extracts: Number of parallel LLM extractions (1 for gpt-oss, 3-5 for llama)
        """
        from src.baltic_shipping.llm_intelligent_scraper import LLMIntelligentScraper
        
        console.print(f"\n[bold cyan]Phase 3: Quality Data Extraction[/bold cyan]")
        console.print(f"Extracting data for {len(imos)} vessels...")
        console.print(f"Model: [yellow]{model}[/yellow]")
        console.print(f"Parallel extractions: [yellow]{parallel_extracts}[/yellow]")
        
        # Semaphore for parallel LLM calls
        extract_sem = asyncio.Semaphore(parallel_extracts)
        results = []
        
        # Save as JSONL for efficiency
        output_file = f"data/vessels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        
        async def extract_one(imo):
            """Extract single vessel with semaphore control"""
            async with extract_sem:
                try:
                    scraper = LLMIntelligentScraper(ollama_model=model)
                    data = await scraper.scrape_vessel_comprehensive(imo)
                    if data:
                        console.print(f"[green]✓ Extracted IMO {imo}[/green]")
                        return data
                except Exception as e:
                    console.print(f"[red]Error extracting {imo}: {e}[/red]")
                    return None
        
        # Extract in parallel batches
        with open(output_file, 'w') as f:
            # Process in chunks to avoid overwhelming memory
            chunk_size = parallel_extracts * 10
            for i in range(0, len(imos), chunk_size):
                chunk = imos[i:i+chunk_size]
                
                # Extract chunk in parallel
                tasks = [extract_one(imo) for imo in chunk]
                chunk_results = await asyncio.gather(*tasks)
                
                # Write successful extractions
                for data in chunk_results:
                    if data:
                        f.write(json.dumps(data) + '\n')
                        f.flush()
                        results.append(data)
        
        console.print(f"[green]✓ Saved {len(results)} vessels to {output_file}[/green]")
        return results

@click.command()
@click.option('--start', default=9000000, help='Start IMO')
@click.option('--end', default=9100000, help='End IMO')
@click.option('--workers', default=10, help='Parallel workers for vessel detection')
@click.option('--sample-size', default=20, help='Sample size per bucket')
@click.option('--extract', is_flag=True, help='Extract full data for found vessels')
@click.option('--model', default='gpt-oss:20b', help='LLM model (gpt-oss:20b, llama3.2:latest, deepseek-r1:8b)')
@click.option('--parallel-llm', default=1, help='Parallel LLM extractions (1 for gpt-oss, 3-5 for lighter models)')
@click.option('--skip-sampling', is_flag=True, help='Skip sampling, scan entire range (slower)')
def main(start, end, workers, sample_size, extract, model, parallel_llm, skip_sampling):
    """
    Optimized scraper with smart sampling and checksum validation
    
    Features:
    - IMO checksum validation (90% reduction in checks)
    - Probabilistic sampling to find hot zones
    - Fast 404 detection (0.5s vs 45s)
    - JSONL batch storage
    - Early stopping when enough vessels found
    """
    
    # Auto-adjust parallel LLM based on model if not specified
    if parallel_llm == 1 and model != 'gpt-oss:20b':
        if 'llama' in model.lower():
            parallel_llm = 5
            console.print(f"[yellow]Auto-adjusted parallel LLM to {parallel_llm} for {model}[/yellow]")
        elif 'deepseek' in model.lower():
            parallel_llm = 3
            console.print(f"[yellow]Auto-adjusted parallel LLM to {parallel_llm} for {model}[/yellow]")
    
    console.print(f"""
    [bold cyan]Optimized IMO Scraper[/bold cyan]
    Range: {start:,} - {end:,}
    Detection Workers: {workers}
    LLM Model: {model}
    Parallel LLM: {parallel_llm}
    Sample Size: {sample_size} per bucket
    Skip Sampling: {skip_sampling}
    
    Strategy:
    1. Validate IMO checksums (filter 90%)
    2. {'Skip sampling, scan all valid IMOs' if skip_sampling else 'Sample buckets to find hot zones'}
    3. {'Scan entire range' if skip_sampling else 'Deep scan only promising ranges'}
    4. Extract data if requested (parallel: {parallel_llm})
    """)
    
    async def run():
        scraper = OptimizedScraper(workers=workers)
        
        if skip_sampling:
            # Direct scan of all valid IMOs
            console.print("[bold cyan]Direct Scan Mode (No Sampling)[/bold cyan]")
            valid_imos = [i for i in range(start, end) if is_valid_imo(i)]
            console.print(f"Found {len(valid_imos):,} valid IMOs to check")
            
            found = []
            batch_size = workers * 10
            with Progress() as progress:
                task = progress.add_task("Checking valid IMOs", total=len(valid_imos))
                
                for i in range(0, len(valid_imos), batch_size):
                    batch = valid_imos[i:i+batch_size]
                    tasks = [scraper.quick_check(imo) for imo in batch]
                    results = await asyncio.gather(*tasks)
                    
                    for imo, exists in zip(batch, results):
                        if exists:
                            found.append(imo)
                            console.print(f"[green]✓ Found: IMO {imo}[/green]")
                    
                    progress.advance(task, len(batch))
        else:
            # Smart scan with sampling
            found = await scraper.smart_scan(start, end)
        
        # Show statistics
        stats = Table(title="Scan Results")
        stats.add_column("Metric", style="cyan")
        stats.add_column("Value", style="green")
        stats.add_row("Range Scanned", f"{start:,} - {end:,}")
        stats.add_row("Valid IMOs (checksum)", f"~{(end-start)//10:,}")
        stats.add_row("Vessels Found", str(len(found)))
        stats.add_row("Efficiency", f"{len(scraper.checked_imos):,} checks vs {end-start:,} total")
        console.print(stats)
        
        # Extract full data if requested
        if extract and found:
            await scraper.extract_vessel_data(found, model=model, parallel_extracts=parallel_llm)
    
    asyncio.run(run())

if __name__ == "__main__":
    main()