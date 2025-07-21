# Baltic Shipping Scraper

This document outlines the technical specifications for creating a web scraper to gather vessel data from [balticshipping.com](https://www.balticshipping.com/vessels).

## 1. Project Overview

The primary goal is to build a robust scraper that systematically collects information for all vessels listed on `balticshipping.com`. The collected data for each vessel will be stored as an individual JSON file, which can then be merged into a single CSV file for analysis.

**Note:** The example image provided in the request was from `marinetraffic.com`. This project will target `balticshipping.com` as per the explicit instructions. The data fields to be scraped will be those available on the `balticshipping.com` vessel pages.

## 2. Project Setup

### 2.1. Dependencies

This project uses Poetry for dependency management. The following libraries are required and will be added to `pyproject.toml`:

-   **`playwright`**: To control a headless browser, handling JavaScript-heavy pages and complex user interactions.
-   **`beautifulsoup4`**: For efficient HTML parsing.
-   **`pandas`**: For data manipulation and creating the final CSV file.
-   **`tqdm`**: To display progress bars for long-running scraping tasks.
-   **`colorama`**: For producing colored terminal output for logging, aiding in debugging.

To install the dependencies, run:

```bash
poetry install
```

After installation, the browser binaries for Playwright must be installed:

```bash
poetry run playwright install
```

### 2.2. Project Structure

The project will follow this structure to maintain a clean and scalable codebase:

```
.
├── data/
│   ├── json/                 # Directory for individual vessel JSON files
│   └── vessel_urls.txt       # File to store scraped vessel URLs
├── mains/
│   ├── scraping/
│   │   └── run_scraper.py      # Main script to execute the scraping process
│   └── processing/
│       └── merge_jsons.py      # Script to merge JSON files into a single CSV
├── pyproject.toml
├── README.md
└── src/
    └── baltic_shipping/
        ├── __init__.py
        ├── scraper.py          # Core scraping logic
        ├── file_handler.py     # Functions for saving and loading files
        ├── config.py           # Configuration variables (URLs, file paths)
        └── logger.py           # Logger setup with colorama
```

## 3. Implementation Guide

### Step 1: Configuration (`src/baltic_shipping/config.py`)

Centralize all constants and configuration variables in this file to improve maintainability.

```python
# src/baltic_shipping/config.py
from pathlib import Path

# --- URLs ---
BASE_URL = "https://www.balticshipping.com"
VESSELS_URL = f"{BASE_URL}/vessels"

# --- File Paths ---
DATA_DIR = Path("data")
JSON_DIR = DATA_DIR / "json"
VESSEL_URLS_FILE = DATA_DIR / "vessel_urls.txt"
FINAL_CSV_FILE = DATA_DIR / "vessels.csv"

# --- Scraping Parameters ---
TIMEOUT = 30000  # 30 seconds for Playwright operations

# --- Initial Setup ---
def setup_directories():
    DATA_DIR.mkdir(exist_ok=True)
    JSON_DIR.mkdir(exist_ok=True)
```

### Step 2: Logger (`src/baltic_shipping/logger.py`)

Set up a reusable logger that uses `colorama` for visually distinct log levels.

### Step 3: Scraping Logic (`src/baltic_shipping/scraper.py`)

This module will contain the core functions for scraping.

#### 3.1. Get All Vessel URLs

-   **Function**: `get_all_vessel_urls() -> list[str]`

-   **Logic**:
    1.  Initialize Playwright and navigate to `config.VESSELS_URL`.
    2.  Implement a loop to handle pagination. Inspect the website's pagination mechanism (e.g., a "Next" button or page number links) to navigate through all pages of the vessel list.
    3.  On each page, extract the relative URL for each vessel (e.g., `/vessel/imo/9265378`).
    4.  Construct the full, absolute URL and add it to a list.
    5.  Return the complete list of vessel URLs.

#### 3.2. Scrape a Single Vessel Page

-   **Function**: `scrape_vessel_page(url: str) -> dict`

-   **Logic**:
    1.  Accept a vessel URL as input.
    2.  Use Playwright to navigate to the URL and retrieve the page's HTML content.
    3.  Parse the HTML with `BeautifulSoup`.
    4.  The vessel data is organized in a definition list (`<dl>`) or a table (`<table>`). Inspect the page structure to identify the correct HTML tags and classes.
    5.  Iterate through the data elements, extracting the label and its corresponding value for each vessel attribute.
    6.  Clean the extracted text to remove extra whitespace or unwanted characters.
    7.  Store the data in a dictionary with the following vessel attributes:
        - IMO number
        - MMSI
        - Name of the ship
        - Former names
        - Vessel type
        - Operating status
        - Flag
        - Gross tonnage
        - Deadweight
        - Length
        - Additional technical specifications as available
    8.  Return the dictionary. Ensure the function is robust enough to handle missing fields gracefully.

### Step 4: File I/O (`src/baltic_shipping/file_handler.py`)

Create helper functions to manage file operations.

-   `save_urls(urls: list[str])`
-   `load_urls() -> list[str]`
-   `save_vessel_data(data: dict)`

### Step 5: Main Scraper Script (`mains/scraping/run_scraper.py`)

This is the entrypoint for the scraping process.

-   **Function**: `main()`

-   **Logic**:
    1.  Call `config.setup_directories()` to ensure output folders exist.
    2.  Check if `vessel_urls.txt` exists. If not, call `get_all_vessel_urls()` and save the result using `file_handler.save_urls()`. Otherwise, load the URLs from the file.
    3.  Iterate through the list of vessel URLs, using `tqdm` for a progress bar.
    4.  For each URL, derive a unique identifier (like the IMO number) to use as the JSON filename.
    5.  Implement a check to see if the JSON file already exists. If it does, skip that URL to allow for easy resumption of an interrupted process.
    6.  Call `scrape_vessel_page(url)`.
    7.  Use a `try...except` block to catch and log any errors during the scraping of a single page, preventing the entire script from failing.
    8.  On success, save the returned dictionary as a JSON file using `file_handler.save_vessel_data()`.

### Step 6: JSON to CSV Merger (`mains/processing/merge_jsons.py`)

This script will consolidate the scraped data.

-   **Function**: `main()`

-   **Logic**:
    1.  Get a list of all `.json` files in `config.JSON_DIR`.
    2.  Load each JSON file into a list of dictionaries.
    3.  Convert the list of dictionaries into a Pandas DataFrame.
    4.  Save the DataFrame to a CSV file as specified in `config.FINAL_CSV_FILE`, with `index=False`.

## 4. How to Run the Scraper

The project will be configured with Poetry scripts for easy execution.

1.  **Run the main scraper**:

    ```bash
    poetry run scrape
    ```
    This command will execute the `run_scraper.py` script, populating the `data/json/` directory with vessel data.

2.  **Merge JSON files into a CSV**:

    ```bash
    poetry run merge
    ```
    This will run `merge_jsons.py` and generate the final `vessels.csv` file.

## 5. Future Enhancements

-   **Asynchronous Scraping**: Refactor the scraper to use `asyncio` with Playwright to fetch multiple pages concurrently, significantly speeding up the process.
-   **Testing**: Implement unit tests for the parsing logic in `scraper.py` to ensure its correctness and guard against regressions.
-   **Cloud Storage**: For larger-scale operations, consider uploading the JSON files and the final CSV to a cloud storage solution like Amazon S3.
