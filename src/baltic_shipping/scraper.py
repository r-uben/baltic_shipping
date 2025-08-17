import math
import re
import time
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from . import config
from .logger import get_logger
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn
from rich.panel import Panel
from rich.text import Text

logger = get_logger(__name__)
console = Console()

def get_all_vessel_urls() -> list[str]:
    """
    Scrapes all vessel URLs from the main vessel listing page, handling pagination.
    Fixed version with retry logic and proper navigation verification.
    """
    vessel_urls = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            # First, navigate to the initial page to find the total number of vessels
            page.goto(config.VESSELS_URL, timeout=config.TIMEOUT)
            page.wait_for_load_state("networkidle")

            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Show Japanese-style banner
            banner = Panel(
                Text("⛩️  Baltic Shipping Vessel Scanner v2.0  ⛩️\n🌊 Enhanced Deep Ocean Data Collection System 🌊", 
                     justify="center", style="bold cyan"),
                border_style="blue",
                padding=(1, 2)
            )
            console.print(banner)
            
            # Find the text node containing "Total found:"
            total_found_node = soup.find(text=re.compile(r'Total found:'))
            if not total_found_node:
                console.print("❌ [red]Could not locate vessel database. Aborting mission.[/red]")
                return []

            # Get the text from the parent element to capture the number
            container_text = total_found_node.parent.get_text()
            
            # Use regex to extract the number
            match = re.search(r'([\d,]+)', container_text)
            if not match:
                console.print("❌ [red]Unable to parse vessel count. Mission failed.[/red]")
                return []
                
            total_vessels = int(match.group(1).replace(',', ''))
            vessels_per_page = 9  # As observed from the website
            total_pages = math.ceil(total_vessels / vessels_per_page)
            
            console.print(f"🔍 [green]Discovered {total_vessels:,} vessels across {total_pages:,} pages[/green]")
            console.print("🚀 [cyan]Initiating enhanced deep scan protocol with retry logic...[/cyan]")

            current_page = 1
            max_pages = total_pages  # Get ALL pages - no limit
            seen_vessels = set()  # Track all vessels we've seen to avoid duplicates
            start_time = time.time()
            failed_pages = []  # Track failed pages for summary
            
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
                task = progress.add_task("⛩️ Enhanced Deep Ocean Scan", total=max_pages)
                
                while current_page <= max_pages:
                    # FIXED: Implement retry logic for each page
                    page_success = False
                    retry_count = 0
                    max_retries = 3
                    
                    while not page_success and retry_count < max_retries:
                        try:
                            # Wait for page to load and get vessel links
                            page.wait_for_load_state("networkidle")
                            
                            # FIXED: Wait for specific page elements to ensure full load
                            try:
                                page.wait_for_selector("a[href*='/vessel/imo/']", timeout=10000)
                            except:
                                # If no vessel links found, might be end of results or error page
                                pass
                            
                            content = page.content()
                            soup = BeautifulSoup(content, 'html.parser')
                            
                            vessel_links = soup.find_all('a', href=lambda x: x and '/vessel/imo/' in x)
                            
                            page_vessels = []
                            for link in vessel_links:
                                href = link.get('href')
                                if href:
                                    if href.startswith('/'):
                                        full_url = config.BASE_URL + href
                                    else:
                                        full_url = href
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
                                progress.update(task, description=f"[green]🚢 Page {current_page}: Found {len(new_vessels)} new vessels[/green]")
                            elif duplicate_count > 0:
                                progress.update(task, description=f"[yellow]🔄 Page {current_page}: {duplicate_count} duplicates - OK[/yellow]")
                            elif not vessel_links:
                                progress.update(task, description=f"[yellow]⚠️ Page {current_page}: No vessels - might be end[/yellow]")
                            
                            page_success = True  # Page processed successfully
                            
                        except Exception as e:
                            retry_count += 1
                            if retry_count < max_retries:
                                progress.update(task, description=f"[yellow]⚠️ Page {current_page} error (retry {retry_count}/{max_retries}): {str(e)[:30]}[/yellow]")
                                time.sleep(2 * retry_count)  # Exponential backoff
                            else:
                                progress.update(task, description=f"[red]❌ Page {current_page} failed after {max_retries} retries[/red]")
                                failed_pages.append(current_page)
                                page_success = True  # Give up and move on
                    
                    # FIXED: Navigation logic with proper verification
                    if current_page < max_pages:
                        navigation_success = False
                        
                        # Store current page number to verify navigation worked
                        old_page_num = current_page
                        
                        # Try to navigate to next page
                        try:
                            # Method 1: Try clicking "Next" button
                            next_selectors = [
                                ".next a",
                                "li.next a", 
                                ".pagination .next a",
                                "a:has-text('Next')"
                            ]
                            
                            next_button = None
                            for selector in next_selectors:
                                try:
                                    button = page.locator(selector).first
                                    if button.is_visible():
                                        next_button = button
                                        break
                                except:
                                    continue
                            
                            if next_button:
                                next_button.click()
                                # FIXED: Wait for navigation to complete with verification
                                try:
                                    # Wait for URL to change or page content to update
                                    page.wait_for_function(
                                        f"document.querySelector('.pagination .active') && document.querySelector('.pagination .active').textContent.trim() == '{current_page + 1}'",
                                        timeout=10000
                                    )
                                    navigation_success = True
                                except:
                                    # Fallback verification - check if vessel links changed
                                    new_content = page.content()
                                    if new_content != content:
                                        navigation_success = True
                            
                            # Method 2: If button click failed, try direct URL navigation
                            if not navigation_success:
                                progress.update(task, description=f"[yellow]⚠️ Click navigation failed, trying URL method[/yellow]")
                                next_url = f"{config.VESSELS_URL}?page={current_page + 1}"
                                page.goto(next_url, timeout=config.TIMEOUT)
                                page.wait_for_load_state("networkidle")
                                navigation_success = True
                                
                        except Exception as nav_e:
                            progress.update(task, description=f"[red]❌ Navigation failed for page {current_page}: {str(nav_e)[:30]}[/red]")
                            failed_pages.append(f"nav_{current_page}")
                    
                    # FIXED: Only increment page counter once, regardless of navigation method
                    current_page += 1
                    progress.update(task, advance=1)
            
                # Final completion
                progress.update(task, description="[green]⛩️ Enhanced deep scan complete - analyzing discoveries[/green]")
            
            # Show completion summary
            elapsed = time.time() - start_time
            console.print(f"\n🎌 [green]Enhanced mission accomplished![/green]")
            console.print(f"⛩️  [cyan]Discovered {len(vessel_urls):,} unique vessels[/cyan]")
            console.print(f"🌊 [blue]Scan duration: {elapsed/60:.1f} minutes[/blue]")
            if failed_pages:
                console.print(f"⚠️  [yellow]Failed pages: {len(failed_pages)} ({failed_pages[:10]}{'...' if len(failed_pages) > 10 else ''})[/yellow]")
            
        except Exception as e:
            console.print(f"\n❌ [red]Critical error in enhanced deep scan: {e}[/red]")
            raise
        finally:
            browser.close()
    
    return vessel_urls

def scrape_vessel_page(url: str) -> dict:
    """
    Scrapes the data for a single vessel from its dedicated page.
    """
    # Silent scraping - no logs during progress
    vessel_data = {}
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            page.goto(url, timeout=config.TIMEOUT)
            page.wait_for_load_state("networkidle")
            
            # Get the page content and parse with BeautifulSoup
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Look for definition lists (dl) which commonly contain vessel specifications
            dl_elements = soup.find_all('dl')
            for dl in dl_elements:
                dt_elements = dl.find_all('dt')
                dd_elements = dl.find_all('dd')
                
                # Match dt (definition term) with dd (definition description)
                for dt, dd in zip(dt_elements, dd_elements):
                    key = dt.get_text(strip=True)
                    value = dd.get_text(strip=True)
                    if key and value:
                        vessel_data[key] = value
            
            # Look for tables with vessel data
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True)
                        value = cells[1].get_text(strip=True)
                        if key and value:
                            vessel_data[key] = value
            
            # Look for div or span elements that might contain vessel data
            # Common patterns: class containing 'spec', 'detail', 'info', etc.
            info_divs = soup.find_all('div', class_=lambda x: x and any(word in x.lower() for word in ['spec', 'detail', 'info', 'data']))
            for div in info_divs:
                # Look for label-value pairs within these divs
                labels = div.find_all(['label', 'span', 'div'], class_=lambda x: x and 'label' in x.lower())
                for label in labels:
                    key = label.get_text(strip=True)
                    # Try to find the corresponding value
                    next_sibling = label.find_next_sibling()
                    if next_sibling:
                        value = next_sibling.get_text(strip=True)
                        if key and value:
                            vessel_data[key] = value
            
            # Extract IMO number from URL if not found in data
            if 'IMO number' not in vessel_data and 'imo' in url.lower():
                try:
                    imo_part = url.split('/imo/')[-1]
                    if imo_part.isdigit():
                        vessel_data['IMO number'] = imo_part
                except:
                    pass
            
            # Clean up the data - remove non-vessel attributes
            vessel_data = _clean_vessel_data(vessel_data)
            
            # Add source URL
            vessel_data['source_url'] = url
            
            # Silent success - no logs during progress
            
        except Exception as e:
            logger.error(f"Error scraping vessel page {url}: {e}")
            raise
        finally:
            browser.close()
    
    return vessel_data


def _clean_vessel_data(data: dict) -> dict:
    """
    Clean and standardize vessel data, removing non-vessel attributes.
    """
    # Define non-vessel attributes to remove
    remove_keys = {
        'page_title', 'vessel_name', 'Clear all', 'Vessel MLC insurance', 
        'Search', 'Close', 'Seafarers worked on', 'Open vacancies on'
    }
    
    # Remove unwanted keys
    cleaned_data = {k: v for k, v in data.items() if k not in remove_keys}
    
    # Standardize key names if needed
    key_mapping = {
        'Name of the ship': 'Vessel name',
        'Gross tonnage': 'Gross tonnage (tons)',
        'Deadweight': 'Deadweight (tons)'
    }
    
    for old_key, new_key in key_mapping.items():
        if old_key in cleaned_data:
            cleaned_data[new_key] = cleaned_data.pop(old_key)
    
    return cleaned_data
