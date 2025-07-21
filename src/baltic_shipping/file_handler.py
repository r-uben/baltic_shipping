import json
from . import config
from .logger import get_logger

logger = get_logger(__name__)

def save_urls(urls: list[str]):
    """Saves a list of URLs to a text file."""
    logger.info(f"Saving {len(urls)} URLs to {config.VESSEL_URLS_FILE}")
    with open(config.VESSEL_URLS_FILE, "w") as f:
        for url in urls:
            f.write(f"{url}\n")

def load_urls() -> list[str]:
    """Loads URLs from a text file."""
    if not config.VESSEL_URLS_FILE.exists():
        return []
    # Silent load - no logs during progress
    with open(config.VESSEL_URLS_FILE, "r") as f:
        return [line.strip() for line in f.readlines()]

def save_vessel_data(data: dict):
    """Saves vessel data to a JSON file."""
    imo = data.get("IMO number", "unknown_imo")
    filename = config.JSON_DIR / f"{imo}.json"
    # Silent save - no logs during progress
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
