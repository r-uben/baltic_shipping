#!/usr/bin/env python3
"""
Playwright-based Baltic Shipping Scraper
Uses browser automation to handle JavaScript-rendered content
"""
import asyncio
import json
import os
from pathlib import Path
from datetime import datetime
import time
import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from playwright.async_api import async_playwright

console = Console()

# Global statistics
stats = {
    'total_checked': 0,
    'valid_imos': 0,
    'vessels_found': 0,
    'successfully_scraped': 0,
    'errors': 0,
    'not_found_404': 0,
    'start_time': time.time()
}

def validate_imo_checksum(imo: int) -> bool:
    """
    Validate IMO number using mod-10 checksum algorithm
    This filters out ~90% of invalid numbers locally before making HTTP requests
    """
    imo_str = str(imo)
    if len(imo_str) != 7:
        return False
    
    # Calculate checksum: multiply each digit by (7-position) and sum
    checksum = sum(int(imo_str[i]) * (7 - i) for i in range(6)) % 10
    return checksum == int(imo_str[6])

def get_output_path(imo: int, data_dir: str) -> Path:
    """Simple flat file structure with IMO as filename"""
    return Path(data_dir) / f"vessel_{imo}.json"

def already_scraped(imo: int, data_dir: str) -> bool:
    """Check if we already have this vessel's data"""
    return get_output_path(imo, data_dir).exists()

async def scrape_vessel_with_playwright(browser, imo: int, timeout: int = 15) -> tuple[bool, str]:
    """
    Use Playwright to get vessel page content with JavaScript rendering
    Returns (success, html_content)
    """
    url = f'https://www.balticshipping.com/vessel/imo/{imo}'
    page = None
    
    try:
        if browser is None:
            console.print(f"[red]❌ IMO {imo}: Browser is None[/red]")
            return False, ""
        
        # Create new page from shared browser
        page = await browser.new_page()
        if page is None:
            return False, ""
        
        # Set shorter timeouts and more realistic headers  
        page.set_default_timeout(timeout * 1000)  # Synchronous method
        await page.set_extra_http_headers({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
        # Try fast navigation first - don't wait for everything
        try:
            response = await page.goto(url, timeout=timeout * 1000, wait_until='domcontentloaded')
            
            if response is None:
                console.print(f"[yellow]⚠ IMO {imo}: page.goto returned None[/yellow]")
                return False, ""
            
            if response.status == 404:
                return False, ""
            
            # Quick check for immediate 404 indicators
            try:
                # Wait shorter time for critical elements
                timeout_result = await page.wait_for_timeout(2000)  # 2 seconds instead of 3
                if timeout_result is not None:
                    console.print(f"[yellow]⚠ IMO {imo}: wait_for_timeout returned {timeout_result}[/yellow]")
                
                # Get HTML early
                html = await page.content()
                if html is None:
                    console.print(f"[yellow]⚠ IMO {imo}: page.content() returned None[/yellow]")
                    return False, ""
                
                # Quick validation - check for 404 pages
                html_lower = html.lower()
                if (len(html) < 1500 or
                    'page not found' in html_lower or
                    'error 404' in html_lower or
                    'vessel not found' in html_lower or 
                    'no vessel' in html_lower or
                    'vessel details not available' in html_lower):
                    return False, ""
                
                return True, html
                
            except Exception as content_error:
                console.print(f"[yellow]⚠ IMO {imo}: Content error: {str(content_error)[:50]}[/yellow]")
                # If waiting fails, try to get content anyway
                try:
                    html = await page.content()
                    if html is None:
                        return False, ""
                    if len(html) > 1500:
                        return True, html
                    return False, ""
                except Exception as final_error:
                    console.print(f"[red]❌ IMO {imo}: Final content error: {str(final_error)[:50]}[/red]")
                    return False, ""
                
        except Exception as nav_error:
            # Navigation failed completely
            return False, ""
        
    except Exception as e:
        # More detailed error logging to identify the exact issue
        error_msg = str(e)
        if 'NoneType' in error_msg:
            console.print(f"[red]❌ IMO {imo}: NoneType error - {error_msg}[/red]")
        elif 'Timeout' not in error_msg:
            console.print(f"[red]❌ Playwright error for IMO {imo}: {error_msg[:50]}[/red]")
        return False, ""
        
    finally:
        if page:
            try:
                await page.close()
            except:
                pass

def extract_json_from_reasoning(text: str) -> str:
    """
    Extract JSON from reasoning model output that includes thinking process.
    Reasoning models often output their thought process before the actual answer.
    """
    import re
    
    # Look for JSON that appears after common reasoning markers
    patterns = [
        # JSON between code blocks (most common)
        r'```json?\s*(\{.*?\})\s*```',
        # After "Final answer:" or similar
        r'(?:final answer|answer|output|result|json response|OUTPUT JSON):\s*(\{.*?\})',
        # JSON after thinking tags (for models that use XML-like tags)
        r'</thinking>\s*(\{.*?\})',
        # After "The JSON is:" or similar phrases
        r'(?:The JSON is|Here is the JSON|JSON output):\s*(\{.*?\})',
        # Last JSON object in the text (greedy match)
        r'(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})\s*$',
        # Any complete JSON object with vessel-related keys
        r'(\{\s*"(?:name|mmsi|flag|vessel_type|imo)"[^{}]*\})',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
        if matches:
            # Return the last match (most likely to be the final answer)
            return matches[-1]
    
    # If no pattern matches, try to find any JSON-like structure
    json_start = text.rfind('{')
    json_end = text.rfind('}')
    if json_start != -1 and json_end != -1 and json_end > json_start:
        return text[json_start:json_end+1]
    
    return text

async def extract_with_local_llm(imo: int, html: str, model: str, retry_count: int = 2) -> dict:
    """Extract vessel data using local LLM via Ollama with retries and fallback"""
    
    # Extract just the main content area to reduce noise
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        # Find vessel data tables or main content
        tables = soup.find_all('table', class_='ship-info') or soup.find_all('table')
        if tables:
            html_snippet = str(tables[0])[:5000]  # Just the first table
        else:
            # Get the main content area
            main_content = soup.find('main') or soup.find('div', class_='content')
            if main_content:
                html_snippet = str(main_content)[:5000]
            else:
                html_snippet = html[2000:7000]  # Skip headers, get middle content
    except:
        html_snippet = html[:5000]
    
    # Debug: Check if we have meaningful content
    if 'vessel' not in html.lower() and 'ship' not in html.lower():
        console.print(f"[yellow]⚠ IMO {imo}: HTML doesn't seem to contain vessel data[/yellow]")
        return None
    
    # Debug: Save the snippet being sent to LLM
    import os
    from pathlib import Path
    if os.getenv('DEBUG_LLM', ''):
        debug_dir = Path('data/vessels/debug_llm')
        debug_dir.mkdir(parents=True, exist_ok=True)
        with open(debug_dir / f"imo_{imo}_snippet.html", 'w', encoding='utf-8') as f:
            f.write(html_snippet)
        console.print(f"[dim]Debug: Saved LLM snippet for IMO {imo}[/dim]")
    
    # Comprehensive extraction prompt
    prompt = f"""Extract ALL vessel information from the HTML table below and return as JSON.

IMPORTANT: Extract EVERY field you can find in the HTML, including:
- Basic info: IMO, MMSI, name, former names
- Type and status: vessel type, operating status
- Flag and registration: flag, home port
- Dimensions: length, breadth, depth, draft
- Tonnage: gross tonnage, deadweight (DWT), net tonnage
- Engine: type, model, power, speed
- Build info: year built, builder, yard number
- Classification: classification society, class notation
- Ownership: owner, manager, operator, technical manager
- Call sign, ENI number
- ANY other fields present in the HTML

Return complete JSON with ALL available data. Use exact field names from the HTML where possible:

HTML:
{html_snippet}

Complete JSON:"""

    for attempt in range(retry_count):
        try:
            import aiohttp
            async with aiohttp.ClientSession() as llm_session:
                async with llm_session.post(
                    'http://localhost:11434/api/generate',
                    json={
                        'model': model,
                        'prompt': prompt,
                        'stream': False,
                        # Removed 'format': 'json' as it may cause issues
                        'options': {
                            'temperature': 0.3,  # Slightly higher for better completion
                            'num_predict': 1000,  # Increased for fuller outputs
                            'top_k': 40,  # More tokens to consider
                            'top_p': 0.9,  # Wider sampling
                            'seed': 42  # Consistent seed for reproducibility
                        },
                        'keep_alive': '5m'  # Keep model loaded for 5 minutes
                    },
                    timeout=aiohttp.ClientTimeout(total=90)  # Increased timeout
                ) as response:
                    
                    if response.status != 200:
                        error_text = await response.text()
                        console.print(f"[red]❌ IMO {imo}: Ollama API error {response.status}: {error_text[:100]}[/red]")
                        if attempt < retry_count - 1:
                            await asyncio.sleep(2)
                            continue
                        return await extract_fallback(imo, html)
                        
                    result = await response.json()
                    
                    # Check for various response issues
                    if result.get('done_reason') == 'unload':
                        console.print(f"[yellow]⚠ IMO {imo}: Model was unloaded[/yellow]")
                        return await extract_fallback(imo, html)
                    
                    if result.get('done_reason') == 'load':
                        console.print(f"[yellow]⚠ IMO {imo}: Model is loading[/yellow]")
                        if attempt < retry_count - 1:
                            await asyncio.sleep(5)  # Wait for model to load
                            continue
                        return await extract_fallback(imo, html)
                    
                    llm_response = result.get('response', '').strip()
                    
                    # Debug: Always save full LLM response when debugging
                    import os
                    from pathlib import Path
                    
                    # Always show response info
                    if llm_response:
                        console.print(f"[cyan]Debug IMO {imo}: Raw LLM response length: {len(llm_response)} chars[/cyan]")
                        console.print(f"[cyan]First 300 chars: {llm_response[:300]}...[/cyan]")
                    else:
                        console.print(f"[red]Debug IMO {imo}: LLM response is empty/None[/red]")
                    
                    # Save full response to file when debugging
                    if os.getenv('DEBUG_LLM', ''):
                        debug_dir = Path('data/vessels/debug_llm')
                        debug_dir.mkdir(parents=True, exist_ok=True)
                        with open(debug_dir / f"imo_{imo}_llm_response.txt", 'w', encoding='utf-8') as f:
                            f.write(f"Model: {model}\n")
                            f.write(f"Response: '{llm_response}'\n")
                            f.write(f"Response length: {len(llm_response) if llm_response else 0} chars\n")
                            f.write(f"="*50 + "\n")
                            f.write(llm_response if llm_response else "EMPTY RESPONSE")
                        console.print(f"[green]✓ Saved full LLM response to debug_llm/imo_{imo}_llm_response.txt[/green]")
                    
                    # Extract JSON from reasoning models' output
                    if model in ['deepseek-r1:8b', 'gpt-oss:20b', 'qwen2.5-coder:32b']:
                        original_response = llm_response
                        llm_response = extract_json_from_reasoning(llm_response)
                        if llm_response != original_response:
                            console.print(f"[dim]Debug IMO {imo}: Extracted JSON: {llm_response[:100]}...[/dim]")
                    
                    # Check for empty or minimal response
                    if not llm_response or llm_response in ['{}', '{"error": "No extractable data"}']:
                        console.print(f"[yellow]⚠ IMO {imo}: LLM returned empty/minimal JSON[/yellow]")
                        return await extract_fallback(imo, html)
                    
                    # Try to extract JSON from response
                    try:
                        import re
                        
                        # Remove any markdown code blocks
                        llm_response = re.sub(r'```json?\s*', '', llm_response)
                        llm_response = re.sub(r'```\s*', '', llm_response)
                        
                        # Find the JSON object
                        json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', llm_response, re.DOTALL)
                        
                        if json_match:
                            json_str = json_match.group(0)
                            # Clean up common issues
                            json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas
                            json_str = re.sub(r',\s*]', ']', json_str)  # Remove trailing commas in arrays
                            
                            vessel_data = json.loads(json_str)
                        else:
                            # Try parsing the whole response
                            vessel_data = json.loads(llm_response)
                        
                        # Validate the data structure
                        if not isinstance(vessel_data, dict):
                            raise ValueError("Response is not a valid dictionary")
                        
                        # Clean up the data - convert empty strings to None
                        for key, value in vessel_data.items():
                            if value == "" or value == "N/A":
                                vessel_data[key] = None
                        
                        # Add metadata
                        vessel_data['imo'] = str(imo)
                        vessel_data['scraped_at'] = datetime.now().isoformat()
                        vessel_data['source_url'] = f'https://www.balticshipping.com/vessel/imo/{imo}'
                        
                        return vessel_data
                        
                    except json.JSONDecodeError as e:
                        if attempt < retry_count - 1:
                            console.print(f"[yellow]⚠ IMO {imo}: Attempt {attempt + 1} - Invalid JSON, retrying...[/yellow]")
                            await asyncio.sleep(0.5)
                            continue
                        else:
                            console.print(f"[yellow]⚠ IMO {imo}: Failed to extract valid JSON after {retry_count} attempts[/yellow]")
                            # Fallback to basic extraction
                            return await extract_fallback(imo, html)
                        
        except asyncio.TimeoutError:
            if attempt < retry_count - 1:
                console.print(f"[yellow]⏱ IMO {imo}: Attempt {attempt + 1} - Timeout, retrying...[/yellow]")
                await asyncio.sleep(0.5)
                continue
            else:
                console.print(f"[red]⏱ IMO {imo}: LLM timeout after {retry_count} attempts[/red]")
                # Fallback to basic extraction
                return await extract_fallback(imo, html)
        except Exception as e:
            if attempt < retry_count - 1:
                await asyncio.sleep(0.5)
                continue
            else:
                console.print(f"[red]❌ IMO {imo}: LLM error: {str(e)[:50]}[/red]")
                # Fallback to basic extraction
                return await extract_fallback(imo, html)
    
    return None

async def extract_fallback(imo: int, html: str) -> dict:
    """Comprehensive fallback extraction using BeautifulSoup when LLM fails"""
    import re
    from bs4 import BeautifulSoup
    
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Initialize vessel data
        vessel_data = {}
        
        # FIRST: Try to extract ALL fields from the HTML table
        table = soup.find('table', class_='ship-info') or soup.find('table')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    # Get field name and value
                    field_name = th.text.strip()
                    field_value = td.text.strip()
                    
                    # Clean up the value
                    if field_value and field_value not in ['', 'N/A', '-']:
                        # Create snake_case key
                        field_key = field_name.lower().replace(' ', '_').replace('/', '_')
                        field_key = re.sub(r'[^\w_]', '', field_key)
                        
                        # Store the value
                        vessel_data[field_key] = field_value
        
        # FALLBACK: Extract from title if no table data
        if not vessel_data.get('name_of_the_ship') and not vessel_data.get('name'):
            title = soup.find('title')
            if title:
                title_text = title.text.strip()
                title_match = re.search(r'^([^,]+),\s*([^,]+),\s*IMO', title_text)
                if title_match:
                    vessel_data['name'] = title_match.group(1).strip()
                    vessel_data['vessel_type'] = title_match.group(2).strip()
        
        # Extract from meta description
        meta_desc = soup.find('meta', {'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            desc_content = meta_desc['content']
            vessel_data['description'] = desc_content
            
            # Extract vessel type if not already found
            if not vessel_data['vessel_type']:
                type_match = re.search(r'is a\s+([^\\s]+)', desc_content, re.IGNORECASE)
                if type_match:
                    vessel_data['vessel_type'] = type_match.group(1)
            
            # Extract build year - "built in YYYY"
            year_match = re.search(r'built in\s+(\d{4})', desc_content, re.IGNORECASE)
            if year_match:
                vessel_data['built_year'] = year_match.group(1)
            
            # Extract flag - "sailing under the flag of COUNTRY"
            flag_match = re.search(r'flag of\s+([^.]+)', desc_content, re.IGNORECASE)
            if flag_match:
                vessel_data['flag'] = flag_match.group(1).strip()
            
            # Extract tonnage
            tonnage_match = re.search(r'gross tonnage is\s+([\d,]+)', desc_content, re.IGNORECASE)
            if tonnage_match:
                vessel_data['dwt'] = tonnage_match.group(1).replace(',', '')
        
        # Look for data in the main content
        text = soup.get_text()
        
        # Additional patterns for vessel data that might be in tables
        patterns = {
            'mmsi': r'MMSI[:\s]+(\d{9})',
            'length': r'Length[:\s]+([\d.]+)\s*(?:m|meters)?',
            'breadth': r'(?:Breadth|Beam)[:\s]+([\d.]+)\s*(?:m|meters)?',
        }
        
        for key, pattern in patterns.items():
            if not vessel_data.get(key):  # Use .get() to avoid KeyError
                match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if match:
                    vessel_data[key] = match.group(1).strip()
        
        # Clean up the data - remove empty values
        vessel_data = {k: v if v else None for k, v in vessel_data.items()}
        
        # Ensure we have a name field (might be under 'name_of_the_ship')
        if vessel_data.get('name_of_the_ship') and not vessel_data.get('name'):
            vessel_data['name'] = vessel_data['name_of_the_ship']
        
        # Only return if we found at least the vessel name
        if vessel_data.get('name') or vessel_data.get('name_of_the_ship'):
            vessel_data['imo'] = str(imo)
            vessel_data['scraped_at'] = datetime.now().isoformat()
            vessel_data['source_url'] = f'https://www.balticshipping.com/vessel/imo/{imo}'
            vessel_data['extraction_method'] = 'fallback'
            
            vessel_name = vessel_data.get('name') or vessel_data.get('name_of_the_ship', 'Unknown')
            console.print(f"[blue]🔄 IMO {imo}: Extracted {vessel_name} via fallback with {len(vessel_data)} fields[/blue]")
            return vessel_data
            
    except Exception as e:
        console.print(f"[red]❌ IMO {imo}: Fallback extraction failed: {str(e)[:50]}[/red]")
    
    return None

async def process_imo(
    semaphore: asyncio.Semaphore, 
    browser,
    imo: int, 
    model: str, 
    data_dir: str,
    debug_html: bool = False,
    page_timeout: int = 15,
    use_llm: bool = True
):
    """Process a single IMO: validate -> check exists -> extract -> save"""
    
    async with semaphore:
        stats['total_checked'] += 1
        
        # Step 1: Validate IMO checksum locally (instant)
        if not validate_imo_checksum(imo):
            return  # Skip invalid IMOs
        
        stats['valid_imos'] += 1
        
        # Step 2: Skip if already scraped
        if already_scraped(imo, data_dir):
            stats['successfully_scraped'] += 1
            return
        
        # Step 3: Check if vessel exists and get rendered HTML
        exists, html = await scrape_vessel_with_playwright(browser, imo, page_timeout)
        if not exists:
            stats['not_found_404'] += 1
            # Don't save anything for 404 pages
            return
        
        # Add small delay to be respectful to the server
        await asyncio.sleep(0.1)
        
        # Step 4: Vessel found! Extract data
        stats['vessels_found'] += 1
        console.print(f"[green]🚢 IMO {imo} found - extracting data...[/green]")
        
        # Debug: Save HTML if requested
        if debug_html:
            debug_dir = Path(data_dir) / "debug_html"
            debug_dir.mkdir(parents=True, exist_ok=True)
            with open(debug_dir / f"imo_{imo}_playwright.html", 'w', encoding='utf-8') as f:
                f.write(html)
        
        # Extract data using LLM or fallback directly
        if use_llm:
            vessel_data = await extract_with_local_llm(imo, html, model)
        else:
            vessel_data = await extract_fallback(imo, html)
        
        if vessel_data:
            # Check if we actually got meaningful data (not all nulls)
            has_data = any(vessel_data.get(key) for key in ['name', 'mmsi', 'flag', 'vessel_type', 'length', 'breadth', 'dwt', 'built_year'])
            
            if has_data:
                # Step 5: Save to file only if we have actual data
                output_path = get_output_path(imo, data_dir)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(vessel_data, f, indent=2, ensure_ascii=False)
                
                stats['successfully_scraped'] += 1
                vessel_name = vessel_data.get('name', 'Unknown')
                console.print(f"[cyan]✅ IMO {imo}: {vessel_name} - SAVED[/cyan]")
            else:
                # Data extraction returned all nulls - likely a parsing error
                stats['errors'] += 1
                console.print(f"[yellow]⚠ IMO {imo}: No meaningful data extracted - skipping save[/yellow]")
        else:
            stats['errors'] += 1

def print_progress_stats():
    """Print current progress statistics"""
    elapsed = time.time() - stats['start_time']
    rate = stats['total_checked'] / elapsed if elapsed > 0 else 0
    
    valid_rate = stats['valid_imos'] / stats['total_checked'] * 100 if stats['total_checked'] > 0 else 0
    hit_rate = stats['vessels_found'] / stats['valid_imos'] * 100 if stats['valid_imos'] > 0 else 0
    
    console.print(f"""
[bold cyan]Progress Update[/bold cyan]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Checked: {stats['total_checked']:,} IMOs ({rate:.1f}/sec)
Valid: {stats['valid_imos']:,} ({valid_rate:.1f}% of checked)
Found: {stats['vessels_found']:,} vessels ({hit_rate:.2f}% of valid)
Scraped: {stats['successfully_scraped']:,}
Errors: {stats['errors']:,}
Not Found: {stats['not_found_404']:,}
Runtime: {elapsed/60:.1f} minutes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    """)

@click.command()
@click.option('--start-imo', default=1000000, help='Starting IMO number')
@click.option('--end-imo', default=9999999, help='Ending IMO number') 
@click.option('--workers', default=4, help='Number of parallel browser contexts')
@click.option('--model', default='llama3.2:latest', help='Local LLM model name')
@click.option('--data-dir', default='data/vessels', help='Output directory')
@click.option('--batch-size', default=200, help='Process in batches of this size')
@click.option('--debug-html', is_flag=True, help='Save HTML files for debugging')
@click.option('--headless/--headed', default=True, help='Run browser in headless mode')
@click.option('--page-timeout', default=15, help='Page load timeout in seconds')
@click.option('--no-llm', is_flag=True, help='Skip LLM extraction and use fallback only')
def main(start_imo, end_imo, workers, model, data_dir, batch_size, debug_html, headless, page_timeout, no_llm):
    """
    Playwright-based Baltic Shipping Scraper
    
    Uses browser automation to handle JavaScript-rendered content:
    1. Iterate through IMO number range
    2. Validate IMO checksum (filters ~90% invalid locally)
    3. Use Playwright to load vessel page with JavaScript
    4. Extract vessel data using local LLM
    5. Save as individual JSON files
    
    Features:
    - JavaScript rendering with Playwright
    - Progress tracking
    - Error handling
    - Rate limiting
    - Debug HTML saving
    """
    
    # Ensure output directory exists
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    
    console.print(f"""
[bold cyan]Playwright Baltic Shipping Scraper[/bold cyan]
════════════════════════════════════════════════
Configuration:
• IMO Range: {start_imo:,} → {end_imo:,} ({end_imo - start_imo:,} numbers)
• Parallel Workers: {workers}
• Extraction: {'Fallback only (no LLM)' if no_llm else f'LLM ({model}) with fallback'}
• Output Directory: {data_dir}
• Batch Size: {batch_size:,}
• Browser Mode: {'Headless' if headless else 'Headed'}
• Page Timeout: {page_timeout}s

Process:
1. Validate IMO checksum (instant, ~10% pass)
2. Use Playwright to load vessel page (~5-10 sec per page)
3. Extract data {'using regex patterns' if no_llm else 'with LLM or fallback'}
4. Save to JSON file

[yellow]Starting in 3 seconds... Press Ctrl+C to stop gracefully[/yellow]
    """)
    
    time.sleep(3)
    
    async def run_scraper():
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=headless,
                args=['--no-sandbox', '--disable-dev-shm-usage']
            )
            
            try:
                # Set up concurrency control
                semaphore = asyncio.Semaphore(workers)
                
                # Progress bar setup
                progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TaskProgressColumn(),
                    TextColumn("[cyan]{task.completed}/{task.total} IMOs"),
                    console=console
                )
                
                with progress:
                    task = progress.add_task(
                        f"Processing IMOs {start_imo:,} to {end_imo:,}", 
                        total=end_imo - start_imo + 1
                    )
                    
                    # Process in batches
                    current_imo = start_imo
                    
                    while current_imo <= end_imo:
                        batch_end = min(current_imo + batch_size, end_imo + 1)
                        
                        # Create tasks for this batch
                        tasks = []
                        for imo in range(current_imo, batch_end):
                            task_coro = process_imo(
                                semaphore, browser, imo, model, data_dir, 
                                debug_html, page_timeout, use_llm=(not no_llm)
                            )
                            tasks.append(task_coro)
                        
                        # Execute batch
                        await asyncio.gather(*tasks, return_exceptions=True)
                        
                        # Update progress
                        progress.advance(task, batch_end - current_imo)
                        current_imo = batch_end
                        
                        # Print stats every 1000 IMOs
                        if stats['total_checked'] % 1000 == 0 and stats['total_checked'] > 0:
                            print_progress_stats()
                
                # Final results
                console.print("\n[bold green]✓ SCRAPING COMPLETE![/bold green]")
                print_progress_stats()
                
            except Exception as e:
                console.print(f"[red]Error in scraper: {e}[/red]")
                print_progress_stats()
            finally:
                await browser.close()
    
    try:
        asyncio.run(run_scraper())
    except KeyboardInterrupt:
        console.print("\n[yellow]⏸ STOPPED - Current progress saved[/yellow]")
        print_progress_stats()

if __name__ == '__main__':
    main()