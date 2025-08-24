#!/usr/bin/env python3
"""
Check all tabs/sections available for a vessel to ensure we capture everything
"""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import json

def check_all_vessel_sections(imo: int = 9872365):
    """Check all available sections for a vessel"""
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        base_url = f"https://www.balticshipping.com/vessel/imo/{imo}"
        
        # Sections we found from the inspection
        sections = {
            'Summary': '',
            'Current position': '/position',
            'Worked on': '/seafarers', 
            'Comments': '/comments'
        }
        
        all_data = {}
        
        for section_name, section_path in sections.items():
            url = base_url + section_path
            print(f"\n{'='*60}")
            print(f"📍 Checking: {section_name}")
            print(f"URL: {url}")
            print('-'*60)
            
            try:
                page.goto(url, timeout=30000)
                page.wait_for_load_state("networkidle")
                
                content = page.content()
                soup = BeautifulSoup(content, 'html.parser')
                
                section_data = {}
                
                # Extract all tables
                tables = soup.find_all('table')
                if tables:
                    print(f"Found {len(tables)} table(s):")
                    for table in tables:
                        for row in table.find_all('tr'):
                            cells = row.find_all(['td', 'th'])
                            if len(cells) == 2:
                                key = cells[0].text.strip().rstrip(':')
                                value = cells[1].text.strip()
                                if key and value and value != '-':
                                    section_data[key] = value
                                    print(f"  • {key}: {value[:50]}{'...' if len(value) > 50 else ''}")
                
                # For position page, look for coordinates
                if 'position' in section_path:
                    # Look for lat/lon data
                    text = soup.get_text()
                    import re
                    
                    # Common patterns for coordinates
                    lat_pattern = r'(?:Latitude|Lat|LAT)[:\s]*([+-]?\d+\.?\d*)'
                    lon_pattern = r'(?:Longitude|Lon|LON)[:\s]*([+-]?\d+\.?\d*)'
                    
                    lat_match = re.search(lat_pattern, text, re.IGNORECASE)
                    lon_match = re.search(lon_pattern, text, re.IGNORECASE)
                    
                    if lat_match:
                        section_data['Latitude'] = lat_match.group(1)
                        print(f"  • Latitude: {lat_match.group(1)}")
                    if lon_match:
                        section_data['Longitude'] = lon_match.group(1)
                        print(f"  • Longitude: {lon_match.group(1)}")
                    
                    # Look for position timestamp
                    time_pattern = r'(?:Last\s+)?(?:Position|Updated|Reported)[:\s]*([^,\n]+(?:UTC|GMT)?)'
                    time_match = re.search(time_pattern, text, re.IGNORECASE)
                    if time_match:
                        section_data['Position Time'] = time_match.group(1).strip()
                        print(f"  • Position Time: {time_match.group(1).strip()}")
                    
                    # Look for speed/course
                    speed_pattern = r'Speed[:\s]*(\d+\.?\d*)\s*(?:knots|kn)?'
                    course_pattern = r'Course[:\s]*(\d+\.?\d*)°?'
                    
                    speed_match = re.search(speed_pattern, text, re.IGNORECASE)
                    course_match = re.search(course_pattern, text, re.IGNORECASE)
                    
                    if speed_match:
                        section_data['Speed'] = speed_match.group(1)
                        print(f"  • Speed: {speed_match.group(1)} knots")
                    if course_match:
                        section_data['Course'] = course_match.group(1)
                        print(f"  • Course: {course_match.group(1)}°")
                
                # For seafarers page
                if 'seafarers' in section_path:
                    # Look for crew list or statistics
                    crew_info = soup.find_all(['div', 'table'], class_=re.compile('crew|seafarer|worker'))
                    if crew_info:
                        print(f"  Found crew/seafarer information sections")
                
                # For comments page
                if 'comments' in section_path:
                    comments = soup.find_all(['div', 'article'], class_=re.compile('comment|review|post'))
                    if comments:
                        section_data['Number of comments'] = str(len(comments))
                        print(f"  • Number of comments: {len(comments)}")
                
                if not section_data:
                    print("  ⚠️ No additional data found in this section")
                
                all_data[section_name] = section_data
                
            except Exception as e:
                print(f"  ❌ Error accessing section: {e}")
        
        browser.close()
        
        # Summary
        print(f"\n{'='*60}")
        print("📊 SUMMARY OF ALL AVAILABLE DATA")
        print('-'*60)
        
        unique_fields = set()
        for section, data in all_data.items():
            unique_fields.update(data.keys())
        
        print(f"Total unique fields across all sections: {len(unique_fields)}")
        print("\nFields by section:")
        for section, data in all_data.items():
            if data:
                print(f"\n{section}:")
                for key in sorted(data.keys()):
                    print(f"  • {key}")
        
        # Check what we might be missing
        print(f"\n🔍 POTENTIAL MISSING DATA:")
        print('-'*40)
        
        # Fields that might exist but we're not capturing
        potential_fields = [
            'Draft', 'Displacement', 'Net tonnage', 'TEU capacity',
            'Passenger capacity', 'Crew size', 'Bollard pull',
            'Dynamic positioning', 'Ice class', 'Call sign',
            'Port of registry', 'Keel laid', 'Delivered',
            'Speed (max)', 'Speed (service)', 'Fuel consumption',
            'Cargo capacity', 'Tank capacity', 'Sister ships',
            'Insurance company', 'P&I Club', 'Last inspection',
            'Next dry dock', 'Current voyage', 'Destination',
            'ETA', 'Previous port', 'Route', 'Charter rate'
        ]
        
        captured_lower = [f.lower() for f in unique_fields]
        missing = []
        for field in potential_fields:
            if field.lower() not in captured_lower and not any(field.lower() in c for c in captured_lower):
                missing.append(field)
        
        if missing:
            print("Fields that might exist but aren't captured:")
            for field in missing:
                print(f"  • {field}")
        else:
            print("✅ Appears we're capturing most available fields")
        
        return all_data

if __name__ == "__main__":
    # Test with a vessel
    data = check_all_vessel_sections(9872365)
    
    # Save for analysis
    with open('data/vessel_all_sections_data.json', 'w') as f:
        json.dump(data, f, indent=2)
    print(f"\n💾 Full data saved to data/vessel_all_sections_data.json")