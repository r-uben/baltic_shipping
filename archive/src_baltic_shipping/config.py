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
    """Create necessary directories if they don't exist."""
    DATA_DIR.mkdir(exist_ok=True)
    JSON_DIR.mkdir(exist_ok=True)
