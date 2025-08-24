# Baltic Shipping IMO-Based Scraping Solution

## Problem Identified

The pagination-based scraping approach (`poetry run scrape`) was missing vessels due to:
1. **Duplicate vessels** appearing on multiple pages in the pagination system
2. **Missing vessels** that don't appear in pagination but exist in the database
3. The discrepancy between advertised ~200K vessels and captured ~140K vessels

## Solution: IMO-Based Direct Access

### Discovery
- Baltic Shipping vessels are accessible via direct IMO URLs: `https://www.balticshipping.com/vessel/imo/{IMO_NUMBER}`
- This bypasses pagination issues entirely
- Successfully tested with 10 missing vessels provided by Jean-Baptiste

### Implementation

Created two IMO scrapers:
1. **`imo_scraper.py`** - Async scraper using aiohttp (for simple HTML)
2. **`imo_playwright_scraper.py`** - Playwright-based for JavaScript-rendered pages (WORKING)

### Test Results
```
✅ 9/10 missing vessels successfully retrieved:
- GALILEO GALILEI (IMO 9872365)
- GALILEO G (IMO 9631814)
- SOLITAIRE (IMO 7129049)
- CASTORO 10 (IMO 7503166)
- GALILEO (IMO 8721088)
- SUNRISE 2000 (IMO 8400294)
- TRUONG SA (IMO 8129644)
- GOLDEN DOLPHIN (IMO 7526259)
- CERES (IMO 9012604)
```

## Usage

### Test Mode (Missing Vessels)
```bash
poetry run scrape-imo --mode test
```

### Custom Range
```bash
poetry run scrape-imo --mode custom --start 9000000 --end 9001000 --concurrent 5
```

### Full Scraping Strategy

IMO numbers typically range from 5100000 to 9999999. Recommended segments:
- **7000000-7499999**: Older vessels
- **7500000-7999999**: 1970s-1980s vessels  
- **8000000-8499999**: 1980s vessels
- **8500000-8999999**: 1990s vessels
- **9000000-9499999**: 2000s vessels
- **9500000-9999999**: Recent vessels

## Key Advantages

1. **Comprehensive Coverage**: Direct IMO access ensures no vessels are missed
2. **No Duplicates**: Each IMO is unique, eliminating duplicate issues
3. **Reliable**: Not affected by pagination bugs or display issues
4. **Verifiable**: Can systematically check all IMO ranges

## Next Steps

1. **Determine Active IMO Range**: Not all IMO numbers have vessels
2. **Optimize Scraping Speed**: Balance between speed and server respect
3. **Incremental Updates**: Track which IMOs have been checked to allow resumption
4. **Merge with Existing Data**: Combine with previously scraped data

## Technical Notes

- Uses Playwright for JavaScript rendering
- Implements concurrent scraping with semaphore control
- Saves checkpoints for interruption recovery
- Respects server with delays between batches
- Outputs to CSV format matching existing structure