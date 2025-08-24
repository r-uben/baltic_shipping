import asyncio
import aiohttp
import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from . import config
from .logger import get_logger
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.text import Text

logger = get_logger(__name__)
console = Console()

class FastScraper:
    """
    High-performance vessel scraper with parallel processing and optimized pagination.
    """
    
    def __init__(self, max_concurrent_pages=2, max_concurrent_vessels=10):
        self.max_concurrent_pages = max_concurrent_pages
        self.max_concurrent_vessels = max_concurrent_vessels
        self.session = None
        
    async def get_all_vessel_urls_fast(self) -> list[str]:
        """
        Fast URL collection using direct page requests instead of clicking pagination.
        """
        vessel_urls = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                # Get total vessel count and pages
                await page.goto(config.VESSELS_URL, timeout=config.TIMEOUT)
                await page.wait_for_load_state("networkidle")
                
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                banner = Panel(
                    Text("🚀 Fast Baltic Shipping Scanner v3.0 🚀\n⚡ Parallel Deep Ocean Collection System ⚡", 
                         justify="center", style="bold green"),
                    border_style="green",
                    padding=(1, 2)
                )
                console.print(banner)
                
                # Find total vessels
                total_found_node = soup.find(text=re.compile(r'Total found:'))
                if not total_found_node:
                    console.print("❌ [red]Could not locate vessel database.[/red]")
                    return []

                container_text = total_found_node.parent.get_text()
                match = re.search(r'([\d,]+)', container_text)
                if not match:
                    console.print("❌ [red]Unable to parse vessel count.[/red]")
                    return []
                    
                total_vessels = int(match.group(1).replace(',', ''))
                vessels_per_page = 9
                total_pages = math.ceil(total_vessels / vessels_per_page)
                
                console.print(f"🚀 [green]Fast mode: {total_vessels:,} vessels across {total_pages:,} pages[/green]")
                console.print(f"⚡ [cyan]Using {self.max_concurrent_pages} concurrent page workers (rate-limit friendly)[/cyan]")
                
                await browser.close()
                
                # Now use sequential interactive navigation (the ONLY method that works)
                vessel_urls = await self._fetch_pages_sequential(total_pages)
                
            except Exception as e:
                console.print(f"\n❌ [red]Critical error in fast scan: {e}[/red]")
                await browser.close()
                raise
        
        return vessel_urls
    
    async def _fetch_pages_sequential(self, total_pages: int) -> list[str]:
        """
        Fetch pages sequentially using interactive navigation (like slow scraper).
        This is the ONLY method that works - URL parameters don't work on this site.
        """
        vessel_urls = []
        seen_vessels = set()
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                # Start from the first page
                await page.goto(config.VESSELS_URL, timeout=config.TIMEOUT)
                await page.wait_for_load_state("networkidle")
                
                current_page = 1
                max_pages = min(total_pages, 25000)  # Safety limit
                failed_pages = []
                
                with Progress(
                    SpinnerColumn("dots12", style="green"),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(bar_width=40),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TextColumn("•"),
                    TextColumn("[green]{task.completed:,}/{task.total:,}[/green]"),
                    TimeRemainingColumn(),
                    console=console,
                    transient=False
                ) as progress:
                    task = progress.add_task("🚀 Sequential Interactive Navigation", total=max_pages)
                    
                    while current_page <= max_pages:
                        page_success = False
                        retry_count = 0
                        max_retries = 3
                        
                        while not page_success and retry_count < max_retries:
                            try:
                                # Wait for page to fully load
                                await page.wait_for_load_state("networkidle")
                                
                                # Wait for vessel links to appear
                                try:
                                    await page.wait_for_selector("a[href*='/vessel/imo/']", timeout=10000)
                                except:
                                    # No vessel links found - might be end of results
                                    pass
                                
                                # Extract vessel links from current page
                                vessel_links = await page.evaluate("""
                                    Array.from(document.querySelectorAll('a[href*="/vessel/imo/"]'))
                                        .map(a => a.href)
                                        .filter((href, index, self) => self.indexOf(href) === index)
                                """)
                                
                                # Process vessels on this page
                                page_vessels = []
                                for link in vessel_links:
                                    if link.startswith('/'):
                                        full_url = config.BASE_URL + link
                                    else:
                                        full_url = link
                                    page_vessels.append(full_url)
                                
                                page_vessels = list(dict.fromkeys(page_vessels))
                                
                                # Filter out vessels we've already seen
                                new_vessels = []
                                duplicate_count = 0
                                for url in page_vessels:
                                    if url not in seen_vessels:
                                        new_vessels.append(url)
                                        seen_vessels.add(url)
                                        vessel_urls.append(url)
                                    else:
                                        duplicate_count += 1
                                
                                if new_vessels:
                                    progress.update(task, description=f"[green]⚡ Page {current_page}: +{len(new_vessels)} vessels[/green]")
                                elif duplicate_count > 0:
                                    progress.update(task, description=f"[yellow]🔄 Page {current_page}: {duplicate_count} duplicates[/yellow]")
                                elif not vessel_links:
                                    progress.update(task, description=f"[yellow]⚠️ Page {current_page}: No vessels found[/yellow]")
                                
                                page_success = True
                                
                            except Exception as e:
                                retry_count += 1
                                if retry_count < max_retries:
                                    progress.update(task, description=f"[yellow]⚠️ Page {current_page} error (retry {retry_count}/{max_retries})[/yellow]")
                                    await asyncio.sleep(2 * retry_count)
                                else:
                                    progress.update(task, description=f"[red]❌ Page {current_page} failed after {max_retries} retries[/red]")
                                    failed_pages.append(current_page)
                                    page_success = True
                        
                        # Navigate to next page (CRITICAL: Use interactive clicking, not URLs)
                        if current_page < max_pages:
                            navigation_success = False
                            
                            try:
                                # Method 1: Try clicking "Next" button
                                next_selectors = [
                                    ".next a",
                                    "li.next a", 
                                    ".pagination .next a",
                                    "a:has-text('Next')",
                                    ".pagination li:last-child a"
                                ]
                                
                                next_button = None
                                for selector in next_selectors:
                                    try:
                                        button = page.locator(selector).first
                                        if await button.is_visible():
                                            next_button = button
                                            break
                                    except:
                                        continue
                                
                                if next_button:
                                    await next_button.click()
                                    
                                    # Wait for navigation to complete
                                    try:
                                        await page.wait_for_function(
                                            f"document.querySelector('.pagination .active') && document.querySelector('.pagination .active').textContent.trim() == '{current_page + 1}'",
                                            timeout=10000
                                        )
                                        navigation_success = True
                                    except:
                                        # Fallback: wait for URL change or content change
                                        await asyncio.sleep(2)
                                        navigation_success = True
                                
                                # Method 2: If button click failed, try direct URL as last resort
                                if not navigation_success:
                                    progress.update(task, description=f"[yellow]⚠️ Button navigation failed, trying URL method[/yellow]")
                                    next_url = f"{config.VESSELS_URL}?page={current_page + 1}"
                                    await page.goto(next_url, timeout=config.TIMEOUT)
                                    await page.wait_for_load_state("networkidle")
                                    navigation_success = True
                                    
                            except Exception as nav_e:
                                progress.update(task, description=f"[red]❌ Navigation failed for page {current_page}[/red]")
                                failed_pages.append(f"nav_{current_page}")
                                # Try to continue anyway
                        
                        current_page += 1
                        progress.update(task, advance=1)
                        
                        # Small delay to be respectful
                        await asyncio.sleep(0.5)
                
                # Show summary
                if failed_pages:
                    console.print(f"\n⚠️ [yellow]Failed pages: {len(failed_pages)} ({failed_pages[:10]}{'...' if len(failed_pages) > 10 else ''})[/yellow]")
                
            except Exception as e:
                console.print(f"\n❌ [red]Critical error in sequential navigation: {e}[/red]")
                raise
            finally:
                await browser.close()
        
        console.print(f"\n🚀 [green]Sequential navigation complete: {len(vessel_urls):,} unique vessels![/green]")
        return vessel_urls
    
    async def scrape_vessels_parallel(self, urls: list[str]) -> None:
        """
        Scrape individual vessel pages in parallel using HTTP requests.
        """
        if not urls:
            return
            
        console.print(f"\n⚡ [cyan]Starting parallel vessel scraping with {self.max_concurrent_vessels} workers[/cyan]")
        
        # Filter out existing files
        remaining_urls = []
        for url in urls:
            imo = url.split('/')[-1]
            if not (config.JSON_DIR / f"{imo}.json").exists():
                remaining_urls.append(url)
        
        if not remaining_urls:
            console.print("🎉 [green]All vessel data already exists![/green]")
            return
            
        console.print(f"📊 [blue]Processing {len(remaining_urls):,} new vessels[/blue]")
        
        # Use HTTP session for speed
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            
            # Create semaphore for concurrent vessel scraping
            semaphore = asyncio.Semaphore(self.max_concurrent_vessels)
            
            async def scrape_vessel(url):
                async with semaphore:
                    try:
                        imo = url.split('/')[-1]
                        
                        # Try HTTP first (faster)
                        vessel_data = await self._scrape_vessel_http(url)
                        
                        # Fallback to browser if needed
                        if not vessel_data:
                            vessel_data = await self._scrape_vessel_browser(url)
                        
                        if vessel_data:
                            # Save immediately
                            import json
                            file_path = config.JSON_DIR / f"{imo}.json"
                            with open(file_path, 'w') as f:
                                json.dump(vessel_data, f, indent=4)
                            return imo, True
                        else:
                            return imo, False
                            
                    except Exception as e:
                        logger.error(f"Error scraping {url}: {e}")
                        return imo, False
            
            # Create tasks
            tasks = [scrape_vessel(url) for url in remaining_urls]
            
            with Progress(
                SpinnerColumn("dots12", style="green"),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(bar_width=40),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("•"),
                TextColumn("[green]{task.completed:,}/{task.total:,}[/green]"),
                TimeRemainingColumn(),
                console=console,
                transient=False
            ) as progress:
                task = progress.add_task("⚡ Parallel Vessel Scraping", total=len(remaining_urls))
                
                success_count = 0
                error_count = 0
                
                # Process as they complete
                for coro in asyncio.as_completed(tasks):
                    imo, success = await coro
                    
                    if success:
                        success_count += 1
                        progress.update(task, 
                            description=f"[green]⚡ Scraped {imo}[/green]",
                            advance=1
                        )
                    else:
                        error_count += 1
                        progress.update(task, 
                            description=f"[red]❌ Failed {imo}[/red]",
                            advance=1
                        )
                
                console.print(f"\n🚀 [green]Parallel scraping complete![/green]")
                console.print(f"✅ [green]Success: {success_count:,}[/green]")
                console.print(f"❌ [red]Errors: {error_count:,}[/red]")
    
    async def _scrape_vessel_http(self, url: str) -> dict:
        """
        Fast HTTP-based vessel scraping (no browser overhead).
        """
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    vessel_data = {}
                    
                    # Extract data using same logic as browser version
                    dl_elements = soup.find_all('dl')
                    for dl in dl_elements:
                        dt_elements = dl.find_all('dt')
                        dd_elements = dl.find_all('dd')
                        
                        for dt, dd in zip(dt_elements, dd_elements):
                            key = dt.get_text(strip=True)
                            value = dd.get_text(strip=True)
                            if key and value:
                                vessel_data[key] = value
                    
                    # Extract IMO from URL
                    if 'IMO number' not in vessel_data:
                        imo = url.split('/imo/')[-1]
                        if imo.isdigit():
                            vessel_data['IMO number'] = imo
                    
                    vessel_data['source_url'] = url
                    
                    # Clean data
                    return self._clean_vessel_data(vessel_data)
                    
        except Exception as e:
            logger.debug(f"HTTP scraping failed for {url}: {e}")
            return {}
        
        return {}
    
    async def _scrape_vessel_browser(self, url: str) -> dict:
        """
        Fallback browser-based scraping for complex pages.
        """
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                await page.goto(url, timeout=config.TIMEOUT)
                await page.wait_for_load_state("networkidle")
                
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                vessel_data = {}
                
                # Same extraction logic as original
                dl_elements = soup.find_all('dl')
                for dl in dl_elements:
                    dt_elements = dl.find_all('dt')
                    dd_elements = dl.find_all('dd')
                    
                    for dt, dd in zip(dt_elements, dd_elements):
                        key = dt.get_text(strip=True)
                        value = dd.get_text(strip=True)
                        if key and value:
                            vessel_data[key] = value
                
                # Extract IMO from URL
                if 'IMO number' not in vessel_data:
                    imo = url.split('/imo/')[-1]
                    if imo.isdigit():
                        vessel_data['IMO number'] = imo
                
                vessel_data['source_url'] = url
                
                await browser.close()
                
                return self._clean_vessel_data(vessel_data)
                
        except Exception as e:
            logger.error(f"Browser scraping failed for {url}: {e}")
            return {}
    
    def _clean_vessel_data(self, data: dict) -> dict:
        """
        Clean and standardize vessel data.
        """
        remove_keys = {
            'page_title', 'vessel_name', 'Clear all', 'Vessel MLC insurance', 
            'Search', 'Close', 'Seafarers worked on', 'Open vacancies on'
        }
        
        cleaned_data = {k: v for k, v in data.items() if k not in remove_keys}
        
        key_mapping = {
            'Name of the ship': 'Vessel name',
            'Gross tonnage': 'Gross tonnage (tons)',
            'Deadweight': 'Deadweight (tons)'
        }
        
        for old_key, new_key in key_mapping.items():
            if old_key in cleaned_data:
                cleaned_data[new_key] = cleaned_data.pop(old_key)
        
        return cleaned_data


# Async wrapper functions for compatibility
async def get_all_vessel_urls_fast() -> list[str]:
    """Fast vessel URL collection with high-performance parallel processing."""
    # Determine optimal concurrency based on system
    import os
    cpu_count = os.cpu_count() or 4
    optimal_concurrency = min(cpu_count * 12, 100)  # 12x CPU cores, max 100
    
    scraper = FastScraper(max_concurrent_pages=2, max_concurrent_vessels=optimal_concurrency)
    return await scraper.get_all_vessel_urls_fast()

async def scrape_vessels_parallel(urls: list[str]) -> None:
    """Fast parallel vessel scraping with auto-scaling based on CPU cores."""
    # Determine optimal concurrency based on system
    import os
    cpu_count = os.cpu_count() or 4
    optimal_concurrency = min(cpu_count * 12, 100)  # 12x CPU cores, max 100
    
    scraper = FastScraper(max_concurrent_pages=2, max_concurrent_vessels=optimal_concurrency)
    await scraper.scrape_vessels_parallel(urls)