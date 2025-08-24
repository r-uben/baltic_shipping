#!/usr/bin/env python3
"""
Simple consolidated scraper - direct LLM extraction without complex logic
"""
import asyncio
import aiohttp
import json
import os
from pathlib import Path
from datetime import datetime
import time
import click
from rich.console import Console
from rich.progress import Progress

console = Console()

# Global stats
stats = {
    'checked': 0,
    'valid': 0,
    'found': 0,
    'extracted': 0,
    'errors': 0,
    'start_time': time.time()
}

def is_valid_imo(imo: int) -> bool:
    """Validate IMO checksum"""
    s = str(imo)
    if len(s) != 7:
        return False
    checksum = sum(int(s[i]) * (7 - i) for i in range(6)) % 10
    return checksum == int(s[6])

async def check_and_extract(imo: int, session: aiohttp.ClientSession, model: str, data_dir: str):
    """Check if vessel exists and extract if it does"""
    url = f'https://www.balticshipping.com/vessel/imo/{imo}'
    
    # Step 1: Check if vessel exists
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
            if response.status == 404:
                return None
            html = await response.text()
            if 'vessel not found' in html.lower() or 'no vessel' in html.lower():
                return None
    except:
        return None
    
    # Step 2: Vessel exists! Extract with simple LLM call
    console.print(f"[green]✓ IMO {imo} exists[/green]")
    stats['found'] += 1
    
    try:
        # Simple direct prompt
        prompt = f"""Extract vessel data from this Baltic Shipping page. Return as JSON with fields: imo, mmsi, name, flag, type, length, breadth.

HTML (first 5000 chars):
{html[:5000]}

Return only JSON object, no other text."""

        # Direct Ollama call
        async with session.post(
            'http://localhost:11434/api/generate',
            json={
                'model': model,
                'prompt': prompt,
                'stream': False
            },
            timeout=aiohttp.ClientTimeout(total=30)  # 30 second timeout
        ) as response:
            if response.status == 200:
                result = await response.json()
                response_text = result.get('response', '')
                
                # Try to parse as JSON
                try:
                    # Find JSON in response (might have extra text)
                    import re
                    json_match = re.search(r'\{[^{}]*\}', response_text)
                    if json_match:
                        data = json.loads(json_match.group())
                    else:
                        # Fallback: try parsing whole response
                        data = json.loads(response_text)
                    
                    data['imo'] = str(imo)
                    data['scraped_at'] = datetime.now().isoformat()
                    
                    # Save to file
                    imo_str = str(imo)
                    dir_path = Path(data_dir) / imo_str[0] / imo_str[1] / imo_str[2]
                    dir_path.mkdir(parents=True, exist_ok=True)
                    
                    file_path = dir_path / f'{imo}.json'
                    with open(file_path, 'w') as f:
                        json.dump(data, f, indent=2)
                    
                    console.print(f"[cyan]✅ Saved IMO {imo}: {data.get('name', 'Unknown')}[/cyan]")
                    stats['extracted'] += 1
                    return data
                    
                except json.JSONDecodeError:
                    console.print(f"[yellow]⚠ IMO {imo}: LLM didn't return valid JSON[/yellow]")
                    stats['errors'] += 1
                    return None
    except asyncio.TimeoutError:
        console.print(f"[red]⏱ IMO {imo}: LLM timeout[/red]")
        stats['errors'] += 1
        return None
    except Exception as e:
        console.print(f"[red]❌ IMO {imo}: {str(e)[:50]}[/red]")
        stats['errors'] += 1
        return None

async def process_batch(imos: list, workers: int, model: str, data_dir: str):
    """Process a batch of IMOs"""
    semaphore = asyncio.Semaphore(workers)
    
    async def process_one(imo):
        async with semaphore:
            stats['checked'] += 1
            
            # Check IMO validity
            if not is_valid_imo(imo):
                return None
            
            stats['valid'] += 1
            
            # Check if already scraped
            imo_str = str(imo)
            file_path = Path(data_dir) / imo_str[0] / imo_str[1] / imo_str[2] / f'{imo}.json'
            if file_path.exists():
                stats['extracted'] += 1
                return None
            
            # Check and extract
            async with aiohttp.ClientSession() as session:
                return await check_and_extract(imo, session, model, data_dir)
    
    tasks = [process_one(imo) for imo in imos]
    await asyncio.gather(*tasks)

@click.command()
@click.option('--start', default=1000000, help='Start IMO')
@click.option('--end', default=1001000, help='End IMO')
@click.option('--workers', default=2, help='Parallel workers')
@click.option('--model', default='gpt-oss:20b', help='LLM model')
@click.option('--data-dir', default='data/vessels_simple', help='Output directory')
@click.option('--batch-size', default=100, help='Batch size')
def main(start, end, workers, model, data_dir, batch_size):
    """Simple consolidated scraper - no complex logic"""
    
    console.print(f"""
    [bold cyan]Simple Consolidated Scraper[/bold cyan]
    Range: {start:,} to {end:,}
    Workers: {workers}
    Model: {model}
    Output: {data_dir}
    
    Starting in 3 seconds...
    """)
    
    time.sleep(3)
    
    async def run():
        with Progress() as progress:
            task = progress.add_task(f"Processing {start:,}-{end:,}", total=end-start)
            
            for batch_start in range(start, end, batch_size):
                batch_end = min(batch_start + batch_size, end)
                batch = list(range(batch_start, batch_end))
                
                await process_batch(batch, workers, model, data_dir)
                
                progress.advance(task, len(batch))
                
                # Print stats every 500 IMOs
                if stats['checked'] % 500 == 0 and stats['checked'] > 0:
                    elapsed = time.time() - stats['start_time']
                    console.print(f"""
[dim]━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[/dim]
Checked: {stats['checked']:,} | Valid: {stats['valid']:,}
Found: {stats['found']:,} | Saved: {stats['extracted']:,}
Errors: {stats['errors']:,} | Speed: {stats['checked']/elapsed:.1f}/s
[dim]━━━━━━━━━━━━━━━━━━━━━━━━━━━━━[/dim]
                    """)
    
    asyncio.run(run())
    
    # Final stats
    elapsed = time.time() - stats['start_time']
    console.print(f"""
    
    [bold green]✓ Complete![/bold green]
    
    Total time: {elapsed/60:.1f} minutes
    Checked: {stats['checked']:,}
    Valid IMOs: {stats['valid']:,}
    Found vessels: {stats['found']:,}
    Extracted: {stats['extracted']:,}
    Errors: {stats['errors']:,}
    
    Speed: {stats['checked']/elapsed:.1f} IMOs/sec
    Hit rate: {stats['found']/stats['valid']*100 if stats['valid'] > 0 else 0:.1f}%
    """)

if __name__ == '__main__':
    main()