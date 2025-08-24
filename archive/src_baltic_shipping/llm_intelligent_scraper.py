"""
LLM-Powered Intelligent Vessel Scraper
Uses local LLM via Ollama to dynamically analyze pages and extract ALL available data
"""
import asyncio
import json
import re
from typing import Dict, List, Optional, Any
from pathlib import Path
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import aiohttp
from datetime import datetime
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from rich.text import Text
import pandas as pd

console = Console()

class LLMIntelligentScraper:
    """
    Intelligent scraper that uses LLM to:
    1. Analyze page content
    2. Identify all data fields
    3. Discover relevant links
    4. Extract comprehensive information
    """
    
    def __init__(self, 
                 ollama_model: str = "gpt-oss:20b",  # Using GPT-OSS for better extraction
                 ollama_host: str = "http://localhost:11434",
                 output_dir: str = "data"):
        self.ollama_model = ollama_model
        self.ollama_host = ollama_host
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.base_url = "https://www.balticshipping.com"
        
    async def query_llm(self, prompt: str, context: str = "", max_retries: int = 3) -> str:
        """Query the local LLM via Ollama API with retry logic"""
        
        for attempt in range(max_retries):
            try:
                timeout = aiohttp.ClientTimeout(total=300, connect=10, sock_read=300)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    # Ollama uses 'options' differently than expected
                    payload = {
                        "model": self.ollama_model,
                        "prompt": f"{context}\n\n{prompt}",
                        "stream": False,
                        "options": {
                            "temperature": 0.1,
                            "num_predict": 8192,  # Increase for complete JSON responses
                        }
                    }
                    
                    async with session.post(
                        f"{self.ollama_host}/api/generate",
                        json=payload
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            return result.get("response", "")
                        else:
                            error_text = await response.text()
                            console.print(f"[yellow]LLM error (attempt {attempt+1}/{max_retries}): {response.status} - {error_text[:100]}[/yellow]")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                            
            except asyncio.TimeoutError:
                console.print(f"[yellow]LLM timeout (attempt {attempt+1}/{max_retries})[/yellow]")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    
            except aiohttp.ClientError as e:
                console.print(f"[yellow]LLM connection error (attempt {attempt+1}/{max_retries}): {str(e)[:100]}[/yellow]")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                    
            except Exception as e:
                console.print(f"[red]Unexpected LLM error: {str(e)[:100]}[/red]")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
        
        console.print(f"[red]Failed to get LLM response after {max_retries} attempts[/red]")
        return ""
    
    def convert_flat_to_nested(self, flat_data: Dict) -> Dict[str, Any]:
        """Convert flat JSON response to expected nested structure"""
        nested = {
            "vessel_data": {
                "identification": {},
                "specifications": {},
                "dimensions": {},
                "engine": {},
                "ownership": {},
                "position": {},
                "other": {}
            },
            "links": [],
            "images": [],
            "tables": []
        }
        
        # Field to category mappings
        id_fields = ['imo', 'mmsi', 'name', 'call_sign', 'flag']
        spec_fields = ['type', 'vessel_type', 'year_built', 'gross_tonnage', 'deadweight', 'net_tonnage']
        dim_fields = ['length', 'breadth', 'beam', 'draft', 'draught']
        engine_fields = ['engine', 'main_engine', 'power', 'speed']
        owner_fields = ['owner', 'manager', 'operator', 'builder']
        pos_fields = ['latitude', 'longitude', 'course', 'destination', 'eta']
        
        for key, value in flat_data.items():
            key_lower = key.lower()
            
            if key_lower in id_fields:
                nested["vessel_data"]["identification"][key_lower] = value
            elif key_lower in spec_fields:
                nested["vessel_data"]["specifications"][key_lower] = value
            elif key_lower in dim_fields:
                nested["vessel_data"]["dimensions"][key_lower] = value
            elif key_lower in engine_fields:
                nested["vessel_data"]["engine"][key_lower] = value
            elif key_lower in owner_fields:
                nested["vessel_data"]["ownership"][key_lower] = value
            elif key_lower in pos_fields:
                nested["vessel_data"]["position"][key_lower] = value
            elif key == "links" and isinstance(value, list):
                nested["links"] = value
            elif key == "images" and isinstance(value, list):
                nested["images"] = value
            else:
                nested["vessel_data"]["other"][key] = value
        
        return nested
    
    def fallback_extraction(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Fallback extraction when LLM fails - comprehensive field mapping"""
        data = {
            "vessel_data": {
                "identification": {},
                "specifications": {},
                "dimensions": {},
                "engine": {},
                "ownership": {},
                "position": {},
                "other": {}
            },
            "links": [],
            "images": [],
            "tables": []
        }
        
        # Enhanced field mappings for Baltic Shipping - matches exact page structure
        field_mappings = {
            # Identification
            'IMO number': ('identification', 'imo'),
            'IMO': ('identification', 'imo'),
            'MMSI': ('identification', 'mmsi'),
            'Name of the ship': ('identification', 'name'),
            'Vessel Name': ('identification', 'name'),
            'Ship Name': ('identification', 'name'),
            'Call Sign': ('identification', 'call_sign'),
            'Flag': ('identification', 'flag'),
            
            # Specifications
            'Vessel type': ('specifications', 'type'),
            'Type': ('specifications', 'type'),
            'Year Built': ('specifications', 'year_built'),
            'Build': ('specifications', 'year_built'),
            'Built': ('specifications', 'year_built'),
            'Gross Tonnage': ('specifications', 'gross_tonnage'),
            'GT': ('specifications', 'gross_tonnage'),
            'Deadweight': ('specifications', 'deadweight'),
            'DWT': ('specifications', 'deadweight'),
            'Net Tonnage': ('specifications', 'net_tonnage'),
            
            # Dimensions
            'Length': ('dimensions', 'length'),
            'LOA': ('dimensions', 'length'),
            'Breadth': ('dimensions', 'breadth'),
            'Beam': ('dimensions', 'breadth'),
            'Draft': ('dimensions', 'draft'),
            'Draught': ('dimensions', 'draft'),
            
            # Engine
            'Engine': ('engine', 'type'),
            'Main Engine': ('engine', 'type'),
            'Engine Model': ('engine', 'model'),
            'Power': ('engine', 'power'),
            'Speed': ('engine', 'speed'),
            
            # Ownership
            'Owner': ('ownership', 'owner'),
            'Manager': ('ownership', 'manager'),
            'Operator': ('ownership', 'operator'),
            'Builder': ('ownership', 'builder'),
            'Yard': ('ownership', 'builder'),
            
            # Position
            'Latitude': ('position', 'latitude'),
            'Longitude': ('position', 'longitude'),
            'Course': ('position', 'course'),
            'Speed': ('position', 'speed'),
            'Destination': ('position', 'destination'),
            'ETA': ('position', 'eta'),
            
            # Other
            'Description': ('other', 'description'),
            'Seafarers worked on': ('other', 'seafarers_worked_on'),
        }
        
        try:
            # Extract from table rows
            for tr in soup.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) >= 2:
                    label = tds[0].get_text(strip=True).replace(':', '')
                    value = tds[1].get_text(strip=True)
                    
                    # Check each mapping
                    for field_name, (category, key) in field_mappings.items():
                        if field_name.lower() in label.lower():
                            if value and value != '-' and value != 'N/A':
                                data["vessel_data"][category][key] = value
                                break
            
            # Also check div/span patterns
            for elem in soup.find_all(['div', 'span']):
                text = elem.get_text(strip=True)
                for field_name, (category, key) in field_mappings.items():
                    if field_name.lower() in text.lower():
                        # Try to extract value after colon or from next element
                        if ':' in text:
                            parts = text.split(':', 1)
                            if len(parts) == 2:
                                value = parts[1].strip()
                                if value and value != '-' and value != 'N/A':
                                    data["vessel_data"][category][key] = value
            
            # Extract vessel name from h1/h2/h3 tags
            for header in soup.find_all(['h1', 'h2', 'h3']):
                text = header.get_text(strip=True)
                if text and len(text) < 100:  # Reasonable vessel name length
                    if not data["vessel_data"]["identification"].get("name"):
                        # Check if it looks like a vessel name (capitals, not a sentence)
                        if text.isupper() or (text[0].isupper() and ' ' not in text[:5]):
                            data["vessel_data"]["identification"]["name"] = text
            
            # Extract links
            for link in soup.find_all('a', href=True)[:20]:
                href = link.get('href', '')
                text = link.get_text(strip=True)
                if text and href and len(text) > 2:
                    data["links"].append({
                        "text": text,
                        "url": href,
                        "relevance": "vessel data"
                    })
            
            # Extract images
            for img in soup.find_all('img', src=True)[:5]:
                src = img.get('src', '')
                if src and ('vessel' in src.lower() or 'ship' in src.lower() or '.jpg' in src.lower() or '.png' in src.lower()):
                    data["images"].append({
                        "url": src,
                        "description": img.get('alt', 'vessel image')
                    })
                    
        except Exception as e:
            console.print(f"[red]Fallback extraction error: {str(e)[:100]}[/red]")
        
        return data
    
    async def analyze_page_with_llm(self, html_content: str) -> Dict[str, Any]:
        """Use LLM to analyze page and extract structured data"""
        
        # Clean HTML for LLM (remove scripts, styles, etc.)
        soup = BeautifulSoup(html_content, 'html.parser')
        for script in soup(["script", "style", "meta", "link"]):
            script.decompose()
        
        # Get text representation
        text_content = soup.get_text(separator='\n', strip=True)
        
        # Limit content size for LLM context - reduced for faster processing
        if len(text_content) > 3000:
            text_content = text_content[:3000]
        
        # Optimized prompt for gpt-oss - very specific format request
        extraction_prompt = """Extract vessel information from this page and return as JSON:

{
  "imo": "IMO number",
  "mmsi": "MMSI number", 
  "name": "vessel name",
  "flag": "flag country",
  "type": "vessel type",
  "length": "length",
  "breadth": "breadth/beam", 
  "description": "description text"
}

Return ONLY the JSON object. No explanation."""
        
        llm_response = await self.query_llm(extraction_prompt, f"Page content:\n{text_content}")
        
        # Parse LLM response
        if not llm_response:
            console.print("[yellow]Empty LLM response, using fallback extraction[/yellow]")
            return self.fallback_extraction(soup)
        
        try:
            # Try to extract JSON from response (LLM might add explanation)
            # Look for JSON object or array
            json_patterns = [
                r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}',  # Nested JSON object
                r'\{.*?\}(?=\s*$)',  # JSON at end
                r'```json\s*(.*?)\s*```',  # JSON in code block
                r'\{.*\}',  # Simple JSON object
            ]
            
            for pattern in json_patterns:
                json_match = re.search(pattern, llm_response, re.DOTALL | re.MULTILINE)
                if json_match:
                    json_str = json_match.group(1) if '```' in pattern else json_match.group()
                    try:
                        parsed = json.loads(json_str)
                        if parsed:  # Check if we got valid data
                            # If flat structure, convert to expected nested format
                            if isinstance(parsed, dict) and "vessel_data" not in parsed:
                                return self.convert_flat_to_nested(parsed)
                            return parsed
                    except json.JSONDecodeError:
                        continue
            
            console.print("[yellow]Could not extract valid JSON from LLM response, using fallback[/yellow]")
            return self.fallback_extraction(soup)
            
        except Exception as e:
            console.print(f"[yellow]Error parsing LLM response: {str(e)[:100]}, using fallback[/yellow]")
            return self.fallback_extraction(soup)
    
    async def discover_relevant_links(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """Use LLM to identify which links should be followed"""
        
        # Extract all links
        all_links = []
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True)
            if href and text:
                full_url = href if href.startswith('http') else base_url + href
                all_links.append({"text": text, "url": full_url})
        
        if not all_links:
            return []
        
        # Ask LLM which links are relevant
        links_text = "\n".join([f"- {l['text']}: {l['url']}" for l in all_links[:50]])
        
        relevance_prompt = f"""
        These links were found on a vessel information page.
        Identify which ones likely contain additional vessel data:
        
        {links_text}
        
        Return a JSON array of relevant links that should be scraped:
        [{{"text": "link text", "url": "url", "reason": "why relevant"}}]
        
        Focus on: position data, crew info, photos, specifications, history, certificates
        """
        
        llm_response = await self.query_llm(relevance_prompt)
        
        try:
            json_match = re.search(r'\[.*\]', llm_response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            return []
        except:
            return []
    
    async def scrape_vessel_comprehensive(self, imo: int) -> Dict[str, Any]:
        """Comprehensively scrape a vessel using LLM guidance"""
        
        # Validate IMO number
        if not (1000000 <= imo <= 9999999):
            console.print(f"[red]  ❌ Invalid IMO range: {imo}[/red]")
            return None
            
        console.print(f"\n[cyan]🤖 LLM-Powered Scraping: IMO {imo}[/cyan]")
        
        all_data = {
            "imo": imo,
            "timestamp": datetime.now().isoformat(),
            "pages_scraped": {},
            "combined_data": {}
        }
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Start with main vessel page
            main_url = f"{self.base_url}/vessel/imo/{imo}"
            
            try:
                # Load main page
                await page.goto(main_url, timeout=30000)
                await page.wait_for_load_state("networkidle")
                
                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                # Check if vessel exists
                page_text = soup.get_text()
                if 'not found' in page_text.lower() or 'no vessel' in page_text.lower():
                    console.print(f"[yellow]  ⚠️ No vessel found for IMO {imo}[/yellow]")
                    await browser.close()
                    return None
                
                # Step 1: Analyze main page with LLM
                console.print("[yellow]  📝 LLM analyzing main page...[/yellow]")
                main_data = await self.analyze_page_with_llm(content)
                all_data["pages_scraped"]["main"] = main_data
                
                # Step 2: Discover relevant links
                console.print("[yellow]  🔍 LLM discovering relevant links...[/yellow]")
                relevant_links = await self.discover_relevant_links(soup, self.base_url)
                
                # Step 3: Scrape each relevant link
                for link_info in relevant_links[:5]:  # Limit to avoid too many requests
                    link_url = link_info.get("url", "")
                    link_text = link_info.get("text", "")
                    
                    if not link_url or "vessel/imo" not in link_url:
                        continue
                    
                    console.print(f"[yellow]  📄 Scraping: {link_text}[/yellow]")
                    
                    try:
                        await page.goto(link_url, timeout=20000)
                        await page.wait_for_load_state("networkidle")
                        
                        sub_content = await page.content()
                        sub_data = await self.analyze_page_with_llm(sub_content)
                        
                        page_key = link_text.lower().replace(" ", "_")
                        all_data["pages_scraped"][page_key] = sub_data
                        
                    except Exception as e:
                        console.print(f"[red]    Failed to scrape {link_text}: {e}[/red]")
                
                # Step 4: Combine all extracted data
                all_data["combined_data"] = self.combine_extracted_data(all_data["pages_scraped"])
                
                console.print(f"[green]  ✅ Completed: Found {len(all_data['combined_data'])} data fields[/green]")
                
            except Exception as e:
                console.print(f"[red]Error scraping IMO {imo}: {e}[/red]")
            
            finally:
                await browser.close()
        
        return all_data
    
    def combine_extracted_data(self, pages_data: Dict) -> Dict:
        """Combine data from all scraped pages into unified structure"""
        combined = {}
        
        for page_name, page_data in pages_data.items():
            if isinstance(page_data, dict):
                # Handle both nested vessel_data structure and flat structure
                vessel_data = page_data.get("vessel_data", {})
                
                # If we have vessel_data, flatten it
                if vessel_data:
                    for category, fields in vessel_data.items():
                        if isinstance(fields, dict):
                            for key, value in fields.items():
                                if value and str(value).strip() and str(value) != 'None':
                                    # Prefer structured data over generic/wrong values
                                    current_value = str(value).lower()
                                    if key not in combined:
                                        combined[key] = value
                                    elif 'not specified' in current_value or 'not found' in current_value:
                                        # Don't overwrite good data with "not specified"
                                        continue
                                    elif len(str(value)) > len(str(combined.get(key, ""))):
                                        combined[key] = value
                
                # Also check for flat fields at top level
                for key, value in page_data.items():
                    if key not in ["vessel_data", "links", "images", "tables"]:
                        if value and str(value).strip() and str(value) != 'None':
                            if key not in combined or len(str(value)) > len(str(combined.get(key, ""))):
                                combined[key] = value
                
                # Add images
                images = page_data.get("images", [])
                if images and any(img.get("url") for img in images if isinstance(img, dict)):
                    if "images" not in combined:
                        combined["images"] = []
                    for img in images:
                        if isinstance(img, dict) and img.get("url"):
                            combined["images"].append(img)
                
                # Add links
                links = page_data.get("links", [])
                if links:
                    if "links" not in combined:
                        combined["links"] = []
                    combined["links"].extend(links)
                
                # Add tables data
                tables = page_data.get("tables", [])
                for table in tables:
                    if isinstance(table, dict) and "data" in table:
                        table_data = table["data"]
                        if isinstance(table_data, dict):
                            for key, value in table_data.items():
                                if value and key not in combined:
                                    combined[key] = value
        
        return combined
    
    async def scrape_vessels_batch(self, imos: List[int]) -> List[Dict]:
        """Scrape multiple vessels with LLM assistance"""
        
        banner = Panel(
            Text("🤖 AI-Powered Vessel Intelligence System 🤖\n🧠 LLM-Guided Comprehensive Data Extraction 🧠", 
                 justify="center", style="bold magenta"),
            border_style="magenta",
            padding=(1, 2)
        )
        console.print(banner)
        
        console.print(f"[cyan]Model: {self.ollama_model}[/cyan]")
        console.print(f"[cyan]Vessels to analyze: {len(imos)}[/cyan]\n")
        
        all_vessels = []
        
        with Progress(
            SpinnerColumn("dots12", style="magenta"),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("🤖 AI Analysis", total=len(imos))
            
            for imo in imos:
                vessel_data = await self.scrape_vessel_comprehensive(imo)
                all_vessels.append(vessel_data)
                
                progress.update(task, advance=1, 
                              description=f"🤖 Analyzed {len(all_vessels)}/{len(imos)} vessels")
                
                # Save checkpoint
                if len(all_vessels) % 5 == 0:
                    self.save_checkpoint(all_vessels)
        
        # Save final results
        self.save_results(all_vessels)
        
        return all_vessels
    
    def save_checkpoint(self, vessels: List[Dict]):
        """Save intermediate results"""
        checkpoint_file = self.output_dir / f"llm_checkpoint_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(checkpoint_file, 'w') as f:
            json.dump(vessels, f, indent=2)
    
    def save_results(self, vessels: List[Dict]):
        """Save final comprehensive results"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save full JSON with all details
        json_file = self.output_dir / f"vessels_llm_complete_{timestamp}.json"
        with open(json_file, 'w') as f:
            json.dump(vessels, f, indent=2)
        console.print(f"[green]💾 Saved complete data to {json_file}[/green]")
        
        # Create flattened CSV
        csv_data = []
        for vessel in vessels:
            combined = vessel.get("combined_data", {})
            combined["imo"] = vessel.get("imo")
            csv_data.append(combined)
        
        if csv_data:
            df = pd.DataFrame(csv_data)
            csv_file = self.output_dir / f"vessels_llm_flat_{timestamp}.csv"
            df.to_csv(csv_file, index=False)
            console.print(f"[green]💾 Saved CSV to {csv_file}[/green]")
            console.print(f"[cyan]📊 Total fields captured: {len(df.columns)}[/cyan]")

async def test_llm_scraper():
    """Test the LLM-powered scraper"""
    
    # Check if Ollama is running
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("http://localhost:11434/api/tags") as response:
                if response.status == 200:
                    data = await response.json()
                    models = [m["name"] for m in data.get("models", [])]
                    console.print(f"[green]✅ Ollama is running with models: {models}[/green]")
                    
                    # Use gpt-oss if available, otherwise default
                    if "gpt-oss:20b" in models:
                        model = "gpt-oss:20b"
                    elif models:
                        model = models[0]
                    else:
                        model = "gpt-oss:20b"
                else:
                    console.print("[red]❌ Ollama not responding properly[/red]")
                    return
        except:
            console.print("[red]❌ Ollama not running. Start with: ollama serve[/red]")
            return
    
    # Test with a few vessels
    scraper = LLMIntelligentScraper(ollama_model=model)
    
    test_imos = [9872365, 9631814]  # Test with 2 vessels
    
    await scraper.scrape_vessels_batch(test_imos)

if __name__ == "__main__":
    asyncio.run(test_llm_scraper())