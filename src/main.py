"""
Main Pipeline - Orchestrates the ETL process.
"""

import sys
import logging
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config import load_config
from readers import fetch_data
from clean import clean_data
from validate import validate_data
from load import create_loader, load_data, load_rejects, init_database

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log"),
    ]
)
logger = logging.getLogger(__name__)


'''
Runs the full ingestion pipeline. It loads configuration,
initializes database tables, iterates through each source to extract, clean,
validate, and load data into PostgreSQL. It also keeps track of and logs 
statistics for each source and produces a final summary.

Args:
    config_path: Path to the YAML configuration file.
    init_db: If True, drop and recreate database tables before running.
'''
def run_pipeline(config_path, init_db=False):
    pipeline_start = time.time()
    config = load_config(config_path)
    engine = create_loader(config["db_url"])
    
    if init_db:
        init_database(engine)
    
    logger.info(f"Pipeline started - Processing {len(config['sources'])} sources")
    logger.info("=" * 60)
    
    # Pipeline totals
    total_input = 0
    total_valid = 0
    total_rejected = 0
    total_inserted = 0
    sources_succeeded = 0
    sources_failed = 0
    
    for source in config["sources"]:
        name = source["name"]
        source_start = time.time()
        status = "SUCCESS"
        
        try:
            # Extract
            df = fetch_data(source["path"], pages=10)
            
            if df.empty:
                logger.warning(f"[{name}] No data fetched, skipping...")
                sources_failed += 1
                continue
            
            input_rows = len(df)
            total_input += input_rows
            
            # Clean
            df = clean_data(df, source["schema"], source["pk"])
            
            # Validate
            valid_df, rejects = validate_data(df, name, source["schema"], source["rules"])
            valid_count = len(valid_df)
            rejected_count = len(rejects)
            total_valid += valid_count
            total_rejected += rejected_count
            
            # Load
            inserted = load_data(valid_df, engine, source["target_table"], source["pk"])
            load_rejects(rejects, engine)
            total_inserted += inserted
            
            source_duration = time.time() - source_start
            sources_succeeded += 1
            
            # Source summary
            logger.info(f"[{name}] Input: {input_rows} | Valid: {valid_count} | Rejected: {rejected_count} | Inserted: {inserted} | Duration: {source_duration:.2f}s | Status: {status}")
            
        except Exception as e:
            status = "FAILED"
            sources_failed += 1
            source_duration = time.time() - source_start
            logger.error(f"[{name}] Error: {e} | Duration: {source_duration:.2f}s | Status: {status}")
    
    # Pipeline summary
    pipeline_duration = time.time() - pipeline_start
    pipeline_status = "SUCCESS" if sources_failed == 0 else "PARTIAL" if sources_succeeded > 0 else "FAILED"
    
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total Input Rows:    {total_input}")
    logger.info(f"Valid Records:       {total_valid}")
    logger.info(f"Rejected Records:    {total_rejected}")
    logger.info(f"Inserted Records:    {total_inserted}")
    logger.info(f"Sources Succeeded:   {sources_succeeded}")
    logger.info(f"Sources Failed:      {sources_failed}")
    logger.info(f"Total Duration:      {pipeline_duration:.2f}s")
    logger.info(f"Pipeline Status:     {pipeline_status}")
    logger.info("=" * 60)
    
    engine.dispose()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Data Ingestion Pipeline")
    parser.add_argument("--config", default="../config/sources.yml", help="Config file path")
    parser.add_argument("--init-db", action="store_true", help="Initialize database tables")
    
    args = parser.parse_args()
    run_pipeline(args.config, args.init_db)
