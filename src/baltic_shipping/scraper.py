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
    """
    vessel_urls = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            # First, navigate to the initial page to find the total number of vessels
            page.goto(config.VESSELS_URL, timeout=config.TIMEOUT)
            page.wait_for_load_state("networkidle")
            
            # Remove debug code - pagination is now working

            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Show Japanese-style banner
            banner = Panel(
                Text("⛩️  Baltic Shipping Vessel Scanner  ⛩️\n🌊 Deep Ocean Data Collection System 🌊", 
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
            console.print("🚀 [cyan]Initiating deep scan protocol...[/cyan]")

            current_page = 1
            max_pages = total_pages  # Get ALL pages - no limit
            consecutive_duplicate_pages = 0
            max_consecutive_duplicates = 10  # Increase tolerance for duplicate pages
            recent_pages_vessels = []  # Track vessels from recent pages only
            start_time = time.time()
            
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
                task = progress.add_task("⛩️ Deep Ocean Scan", total=max_pages)
                
                while current_page <= max_pages:
                    try:
                        # Wait for page to load and get vessel links
                        page.wait_for_load_state("networkidle")
                        content = page.content()
                        soup = BeautifulSoup(content, 'html.parser')
                        
                        vessel_links = soup.find_all('a', href=lambda x: x and '/vessel/imo/' in x)
                        
                        if not vessel_links:
                            progress.update(task, description="[yellow]⚠️  No vessels detected[/yellow]")
                            break
                    
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
                        
                        if not page_vessels:
                            progress.update(task, description="[yellow]⚠️  Empty ocean detected[/yellow]")
                            break
                        
                        new_vessels = [url for url in page_vessels if url not in vessel_urls]
                        
                        # Check if current page vessels are duplicates of recent pages (not all previous pages)
                        is_duplicate_of_recent = False
                        if current_page > 1 and len(recent_pages_vessels) > 0:
                            # Check if all vessels on this page are in the last few pages
                            recent_vessels_set = set(recent_pages_vessels)
                            current_vessels_set = set(page_vessels)
                            if current_vessels_set.issubset(recent_vessels_set):
                                is_duplicate_of_recent = True
                        
                        if is_duplicate_of_recent and current_page > 1:
                            consecutive_duplicate_pages += 1
                            progress.update(task, description=f"[yellow]🔄 Echo waves detected ({consecutive_duplicate_pages}/{max_consecutive_duplicates})[/yellow]")
                            
                            if consecutive_duplicate_pages >= max_consecutive_duplicates:
                                if current_page > max_pages * 0.9:  # If we're in the last 10% of pages
                                    progress.update(task, description="[green]🏁 Ocean depths fully explored[/green]")
                                    break
                                else:
                                    progress.update(task, description="[cyan]🌊 Navigating through echo zones[/cyan]")
                                    consecutive_duplicate_pages = max_consecutive_duplicates - 5  # Reset but keep some count
                        else:
                            consecutive_duplicate_pages = 0  # Reset counter when new vessels are found
                            progress.update(task, description=f"[green]🚢 Found {len(new_vessels)} vessels - scanning deeper[/green]")
                        
                        vessel_urls.extend(new_vessels)
                        
                        # Keep track of vessels from recent pages only (last 10 pages)
                        recent_pages_vessels.extend(page_vessels)
                        if len(recent_pages_vessels) > 90:  # Keep last ~10 pages worth (9 vessels per page)
                            recent_pages_vessels = recent_pages_vessels[-90:]
                        
                        # Try to click the "Next" button to go to the next page
                        try:
                            # The Next button is likely inside a li.next element
                            next_selectors = [
                                ".next a",  # Anchor inside .next li
                                "li.next a",  # More specific
                                ".pagination .next a",  # Even more specific
                                ".page:has-text('2')",  # Try clicking page 2 directly
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
                            
                            # If no Next button, try clicking page number "2"
                            if not next_button and current_page == 1:
                                try:
                                    page_2 = page.locator("a.page:has-text('2')").first
                                    if page_2.is_visible():
                                        next_button = page_2
                                except:
                                    pass
                            
                            if next_button:
                                next_button.click()
                                current_page += 1
                                progress.update(task, advance=1)
                                page.wait_for_timeout(3000)  # Wait for page transition
                            else:
                                progress.update(task, description="[red]⛩️ Navigation path exhausted[/red]")
                                break
                        except Exception as e:
                            progress.update(task, description=f"[red]❌ Navigation error: {str(e)[:30]}[/red]")
                            break

                    except Exception as e:
                        progress.update(task, description=f"[red]❌ Scanning error: {str(e)[:30]}[/red]")
                        break
            
                # Final completion
                progress.update(task, description="[green]⛩️ Deep scan complete - analyzing discoveries[/green]")
                
            vessel_urls = list(dict.fromkeys(vessel_urls))
            
            # Show completion summary
            elapsed = time.time() - start_time
            console.print(f"\n🎌 [green]Mission accomplished![/green]")
            console.print(f"⛩️  [cyan]Discovered {len(vessel_urls):,} unique vessels[/cyan]")
            console.print(f"🌊 [blue]Scan duration: {elapsed/60:.1f} minutes[/blue]")
            
        except Exception as e:
            console.print(f"\n❌ [red]Critical error in deep scan: {e}[/red]")
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
