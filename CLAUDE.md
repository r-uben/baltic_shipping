# Baltic Shipping Vessel Data Scraping

## Overview

This document outlines the approach for scraping vessel data from the Baltic Shipping website.

## Target URL

https://www.balticshipping.com/vessels

## Data Structure Analysis

The Baltic Shipping vessels page contains information about their fleet, including:

- Vessel names and types
- Technical specifications
- Operational details
- Fleet composition

## Scraping Strategy

### 1. Initial Investigation

- Inspect the page structure to identify data containers
- Check for dynamic content loading (JavaScript-rendered data)
- Analyze pagination or filtering mechanisms
- Identify rate limiting or anti-bot measures

### 2. Data Extraction Approach

- Use `requests` and `BeautifulSoup` for static content
- Consider `selenium` if dynamic content loading is detected
- Implement proper headers and user-agent rotation
- Add delays between requests to respect server resources

### 3. Data Points to Extract

- Vessel identification (name, IMO number, flag)
- Vessel specifications (DWT, length, beam, draft)
- Vessel type and classification
- Build year and shipyard information
- Current operational status

### 4. Implementation Considerations

- Store scraped data in structured format (CSV/JSON)
- Implement error handling for network issues
- Add logging for monitoring scraping progress
- Respect robots.txt and terms of service
- Consider data freshness and update frequency

### 5. Technical Setup

- Create scraper class in `src/balticshipping/data/`
- Implement main script in `mains/data/`
- Add configuration for request intervals and retry logic
- Include data validation and cleaning pipeline

### 6. Compliance Notes

- Review website terms of service before implementation
- Implement respectful scraping practices
- Consider reaching out for API access if available
- Monitor for any changes in website structure
