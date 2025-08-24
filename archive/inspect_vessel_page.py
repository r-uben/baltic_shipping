#!/usr/bin/env python3
"""
Script to thoroughly inspect a vessel page and identify ALL available data fields
"""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import json
import re

def inspect_vessel_page(imo: int = 9872365):
    """Thoroughly inspect a vessel page to find all available data"""
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        url = f"https://www.balticshipping.com/vessel/imo/{imo}"
        print(f"🔍 Inspecting vessel page: {url}")
        print("=" * 80)
        
        try:
            page.goto(url, timeout=30000)
            page.wait_for_load_state("networkidle")
            
            content = page.content()
            soup = BeautifulSoup(content, 'html.parser')
            
            # 1. Check for tabs or sections
            print("\n📑 TABS/SECTIONS FOUND:")
            print("-" * 40)
            tabs = soup.find_all(['ul', 'nav'], class_=re.compile('tab|nav'))
            for tab in tabs:
                links = tab.find_all('a')
                if links:
                    print(f"Navigation found with {len(links)} links:")
                    for link in links:
                        print(f"  • {link.text.strip()}: {link.get('href', 'no-href')}")
            
            # 2. Find all data tables
            print("\n📊 TABLES FOUND:")
            print("-" * 40)
            tables = soup.find_all('table')
            for i, table in enumerate(tables, 1):
                print(f"\nTable {i}:")
                rows = table.find_all('tr')
                for row in rows[:5]:  # First 5 rows
                    cells = row.find_all(['td', 'th'])
                    if cells:
                        row_text = ' | '.join([c.text.strip()[:30] for c in cells])
                        print(f"  {row_text}")
                if len(rows) > 5:
                    print(f"  ... and {len(rows)-5} more rows")
            
            # 3. Find all divs with specific classes
            print("\n📦 DATA SECTIONS:")
            print("-" * 40)
            data_divs = soup.find_all('div', class_=re.compile('info|data|detail|spec|characteristic'))
            seen_classes = set()
            for div in data_divs:
                div_class = ' '.join(div.get('class', []))
                if div_class not in seen_classes:
                    seen_classes.add(div_class)
                    text_preview = div.text.strip()[:100]
                    if text_preview:
                        print(f"Class: {div_class}")
                        print(f"  Content: {text_preview}...")
            
            # 4. Check for images/photos
            print("\n🖼️ IMAGES/MEDIA:")
            print("-" * 40)
            images = soup.find_all('img')
            print(f"Found {len(images)} images")
            for img in images[:5]:
                src = img.get('src', 'no-src')
                alt = img.get('alt', 'no-alt')
                if 'vessel' in src.lower() or 'ship' in src.lower() or 'photo' in src.lower():
                    print(f"  • {alt}: {src[:50]}...")
            
            # 5. Check for links to other pages/data
            print("\n🔗 LINKS TO ADDITIONAL DATA:")
            print("-" * 40)
            links = soup.find_all('a', href=True)
            relevant_links = {}
            for link in links:
                href = link.get('href', '')
                text = link.text.strip()
                if text and ('history' in href.lower() or 'track' in href.lower() or 
                           'position' in href.lower() or 'photo' in href.lower() or
                           'document' in href.lower() or 'certificate' in href.lower() or
                           'crew' in text.lower() or 'voyage' in text.lower()):
                    relevant_links[text] = href
            
            for text, href in list(relevant_links.items())[:10]:
                print(f"  • {text}: {href}")
            
            # 6. Check for JavaScript data
            print("\n💻 JAVASCRIPT DATA:")
            print("-" * 40)
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and ('vessel' in script.string.lower() or 'ship' in script.string.lower()):
                    # Look for JSON data
                    json_matches = re.findall(r'\{[^{}]*"[^"]*"[^{}]*\}', script.string)
                    if json_matches:
                        print(f"Found potential JSON data in script")
                        for match in json_matches[:2]:
                            print(f"  {match[:100]}...")
            
            # 7. Extract ALL text fields with labels
            print("\n📝 ALL DATA FIELDS FOUND:")
            print("-" * 40)
            all_fields = {}
            
            # Method 1: Look for label-value pairs
            for element in soup.find_all(['div', 'span', 'td', 'p']):
                text = element.text.strip()
                if ':' in text and len(text) < 200:
                    parts = text.split(':', 1)
                    if len(parts) == 2:
                        label = parts[0].strip()
                        value = parts[1].strip()
                        if label and value and value != '-':
                            all_fields[label] = value
            
            # Sort and display
            for label, value in sorted(all_fields.items()):
                print(f"  • {label}: {value[:50]}{'...' if len(value) > 50 else ''}")
            
            print(f"\n📊 Total unique fields found: {len(all_fields)}")
            
            # 8. Check page source for hidden data
            print("\n🔍 CHECKING FOR HIDDEN/API DATA:")
            print("-" * 40)
            
            # Check for API calls in network
            # This would require intercepting network requests
            print("Note: Full API inspection would require network interception")
            
            # Save full HTML for manual inspection
            with open('data/vessel_page_full.html', 'w', encoding='utf-8') as f:
                f.write(content)
            print("✅ Full HTML saved to data/vessel_page_full.html for manual inspection")
            
        except Exception as e:
            print(f"Error: {e}")
        finally:
            # input("\n🔴 Press Enter to close browser...")
            browser.close()

if __name__ == "__main__":
    # Test with a vessel that should have complete data
    inspect_vessel_page(9872365)  # GALILEO GALILEI