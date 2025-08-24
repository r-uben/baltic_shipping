#!/usr/bin/env python3
"""
Main script for LLM-powered intelligent vessel scraping
Uses local LLM to comprehensively extract ALL available data
"""
import asyncio
import argparse
from typing import List
from baltic_shipping.llm_intelligent_scraper import LLMIntelligentScraper
from rich.console import Console
import aiohttp

console = Console()

async def check_ollama_status():
    """Check if Ollama is running and list available models"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("http://localhost:11434/api/tags", timeout=5) as response:
                if response.status == 200:
                    data = await response.json()
                    models = [m["name"] for m in data.get("models", [])]
                    return True, models
                return False, []
    except:
        return False, []

async def run_test_scrape(model: str = None):
    """Test LLM scraper with missing vessels"""
    console.print("[cyan]🧠 Testing LLM-powered intelligent scraper...[/cyan]")
    
    # Check Ollama
    is_running, models = await check_ollama_status()
    
    if not is_running:
        console.print("[red]❌ Ollama is not running![/red]")
        console.print("[yellow]Start Ollama with: ollama serve[/yellow]")
        console.print("[yellow]Then pull a model: ollama pull llama3.2[/yellow]")
        return
    
    if not models:
        console.print("[red]❌ No models found in Ollama![/red]")
        console.print("[yellow]Pull a model first: ollama pull llama3.2[/yellow]")
        return
    
    console.print(f"[green]✅ Ollama running with models: {models}[/green]")
    
    # Use specified model or prefer llama3.2 for now (gpt-oss having issues)
    if model and model in models:
        selected_model = model
    elif "llama3.2:latest" in models:
        selected_model = "llama3.2:latest"
    elif "gpt-oss:20b" in models:
        selected_model = "gpt-oss:20b"
    else:
        selected_model = models[0]
        if model and model not in models:
            console.print(f"[yellow]Model '{model}' not found, using '{selected_model}'[/yellow]")
    
    # Test with vessels that were missing
    test_imos = [
        9872365,  # GALILEO GALILEI
        9631814,  # GALILEO G
        7129049,  # SOLITAIRE
    ]
    
    scraper = LLMIntelligentScraper(ollama_model=selected_model)
    await scraper.scrape_vessels_batch(test_imos)

async def run_custom_scrape(imos: List[int], model: str = None):
    """Scrape specific IMOs with LLM"""
    is_running, models = await check_ollama_status()
    
    if not is_running:
        console.print("[red]❌ Ollama is not running![/red]")
        return
    
    # Use specified model or prefer llama3.2 for now (gpt-oss having issues)
    if model and model in models:
        selected_model = model
    elif "llama3.2:latest" in models:
        selected_model = "llama3.2:latest"
    elif "gpt-oss:20b" in models:
        selected_model = "gpt-oss:20b"
    else:
        selected_model = models[0]
    
    scraper = LLMIntelligentScraper(ollama_model=selected_model)
    await scraper.scrape_vessels_batch(imos)

async def run_comprehensive_scrape(start_imo: int, end_imo: int, model: str = None):
    """Run comprehensive LLM scraping for IMO range"""
    console.print("[bold magenta]🧠 AI-POWERED COMPREHENSIVE VESSEL SCRAPING 🧠[/bold magenta]")
    console.print("[yellow]This will extract ALL available data including:[/yellow]")
    console.print("  • Main vessel specifications")
    console.print("  • Current position and voyage data")
    console.print("  • Crew and seafarer information")
    console.print("  • Photos and visual assets")
    console.print("  • Comments and reviews")
    console.print("  • Any other discovered data")
    
    is_running, models = await check_ollama_status()
    
    if not is_running:
        console.print("[red]❌ Ollama is not running![/red]")
        return
    
    # Use specified model or prefer llama3.2 for now (gpt-oss having issues)
    if model and model in models:
        selected_model = model
    elif "llama3.2:latest" in models:
        selected_model = "llama3.2:latest"
    elif "gpt-oss:20b" in models:
        selected_model = "gpt-oss:20b"
    else:
        selected_model = models[0]
    console.print(f"[cyan]Using model: {selected_model}[/cyan]")
    
    # Process in batches
    batch_size = 10
    all_imos = list(range(start_imo, end_imo + 1))
    
    scraper = LLMIntelligentScraper(ollama_model=selected_model)
    
    for i in range(0, len(all_imos), batch_size):
        batch = all_imos[i:i+batch_size]
        console.print(f"\n[cyan]Processing batch: IMO {batch[0]} - {batch[-1]}[/cyan]")
        await scraper.scrape_vessels_batch(batch)
        
        # Pause between batches
        if i + batch_size < len(all_imos):
            console.print("[yellow]Pausing between batches...[/yellow]")
            await asyncio.sleep(5)

def main():
    parser = argparse.ArgumentParser(description='LLM-powered intelligent vessel scraper')
    parser.add_argument('--mode', choices=['test', 'custom', 'comprehensive'], default='test',
                        help='Scraping mode')
    parser.add_argument('--model', type=str, help='Ollama model to use (e.g., llama3.2, mistral)')
    parser.add_argument('--imos', type=int, nargs='+', help='Specific IMOs for custom mode')
    parser.add_argument('--start', type=int, help='Start IMO for comprehensive mode')
    parser.add_argument('--end', type=int, help='End IMO for comprehensive mode')
    
    args = parser.parse_args()
    
    if args.mode == 'test':
        asyncio.run(run_test_scrape(args.model))
    
    elif args.mode == 'custom':
        if not args.imos:
            console.print("[red]Error: Custom mode requires --imos[/red]")
            console.print("Example: --mode custom --imos 9872365 9631814")
            return
        asyncio.run(run_custom_scrape(args.imos, args.model))
    
    elif args.mode == 'comprehensive':
        if not args.start or not args.end:
            console.print("[red]Error: Comprehensive mode requires --start and --end[/red]")
            console.print("Example: --mode comprehensive --start 9872360 --end 9872370")
            return
        
        console.print(f"[bold red]⚠️  WARNING: LLM scraping is slower but more thorough[/bold red]")
        console.print(f"Will analyze {args.end - args.start + 1} vessels with AI")
        response = input("Continue? (yes/no): ")
        
        if response.lower() == 'yes':
            asyncio.run(run_comprehensive_scrape(args.start, args.end, args.model))
        else:
            console.print("[cyan]Cancelled[/cyan]")

if __name__ == "__main__":
    main()