import json
import pandas as pd
from baltic_shipping import config
from baltic_shipping.logger import get_logger

logger = get_logger(__name__)

def main():
    """
    Merges all vessel JSON files into a single CSV.
    """
    logger.info("Starting to merge JSON files into a single CSV.")
    
    json_files = list(config.JSON_DIR.glob("*.json"))
    if not json_files:
        logger.warning("No JSON files found to merge.")
        return
        
    all_data = []
    for file in json_files:
        with open(file, 'r') as f:
            all_data.append(json.load(f))
            
    df = pd.DataFrame(all_data)
    df.to_csv(config.FINAL_CSV_FILE, index=False)
    
    logger.info(f"Successfully merged {len(df)} records into {config.FINAL_CSV_FILE}")

if __name__ == "__main__":
    main()
