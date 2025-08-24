#!/usr/bin/env python3
"""
Test script to verify IMO-based URL scraping approach
"""
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import time

async def test_imo_url(session, imo):
    """Test if we can access vessel by IMO directly"""
    url = f"https://www.balticshipping.com/vessel/imo/{imo}"
    
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                text = await response.text()
                soup = BeautifulSoup(text, 'html.parser')
                # Check if we got a valid vessel page
                vessel_name = soup.find('h1')
                if vessel_name:
                    return True, vessel_name.text.strip()
                return True, "Page exists but no vessel name found"
            elif response.status == 404:
                return False, "404 - Vessel not found"
            else:
                return False, f"Status code: {response.status}"
    except Exception as e:
        return False, str(e)

async def main():
    # Test with the missing IMOs your coauthor provided
    missing_imos = [
        9872365,  # First example
        9631814,  # Second example
        7129049,
        7503166,
        8721088,
        8400294,
        8213744,
        8129644,
        7526259,
        9012604
    ]
    
    print("Testing IMO-based URL approach for missing vessels:\n")
    print("-" * 60)
    
    success_count = 0
    async with aiohttp.ClientSession() as session:
        for imo in missing_imos:
            success, result = await test_imo_url(session, imo)
            status = "✅" if success else "❌"
            print(f"{status} IMO {imo}: {result}")
            if success:
                success_count += 1
            await asyncio.sleep(0.5)  # Be respectful with requests
    
    print("-" * 60)
    print(f"\nResults: {success_count}/{len(missing_imos)} vessels accessible via IMO URL")
    
    if success_count == len(missing_imos):
        print("\n🎯 IMO-based approach works! We can scrape vessels directly by IMO.")
        print("Next step: Generate list of valid IMO numbers and scrape comprehensively.")

if __name__ == "__main__":
    asyncio.run(main())