"""
Main ingestion pipeline.
Orchestrates the complete data ingestion workflow.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config import AppConfig, SourceConfig
from readers import get_reader
from validate import Validator
from clean import Cleaner
from load import DatabaseLoader
from logger import logger


class IngestionPipeline:
    """Main pipeline for ingesting data from sources to PostgreSQL."""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.loader = DatabaseLoader(
            db_url=config.db_url,
            batch_size=config.batch_size,
        )
    
    def run_source(self, source_config: SourceConfig) -> dict:
        """Run the ingestion pipeline for a single source."""
        source_name = source_config.name
        results = {
            "source": source_name,
            "status": "success",
            "input_rows": 0,
            "valid_rows": 0,
            "rejected_rows": 0,
            "inserted": 0,
            "errors": [],
        }
        
        try:
            # Step 1: Read data
            reader = get_reader(source_config)
            df = reader.read()
            results["input_rows"] = len(df)
            logger.start_run(source_name, len(df), source_config.path)
            
            # Step 2: Clean data
            cleaner = Cleaner(source_config)
            df = cleaner.clean(df)
            
            # Step 3: Validate data
            validator = Validator(source_config)
            validation_result = validator.validate(df)
            df = validation_result.valid_df
            rejects = validation_result.rejects
            
            results["valid_rows"] = len(df)
            results["rejected_rows"] = len(rejects)
            logger.log_cleaned(source_name, len(df), len(rejects))
            
            # Step 4: Load to database
            start_time = time.time()
            load_result = self.loader.load_upsert(
                df=df,
                table=source_config.target_table,
                pk=source_config.pk,
            )
            load_duration = time.time() - start_time
            
            results["inserted"] = load_result["inserted"]
            logger.log_load(
                source_name,
                load_result["inserted"],
                load_result["updated"],
                load_duration,
            )
            
            # Step 5: Load rejects
            if rejects:
                self.loader.load_rejects(rejects)
            
            logger.end_run(source_name, "success")
            
        except Exception as e:
            results["status"] = "error"
            results["errors"].append(str(e))
            logger.log_error(source_name, str(e))
            logger.end_run(source_name, "error")
        
        return results
    
    def run_all(self) -> list[dict]:
        """Run ingestion for all configured sources."""
        results = []
        for source_config in self.config.sources:
            result = self.run_source(source_config)
            results.append(result)
        return results
    
    def run_by_name(self, source_name: str) -> dict | None:
        """Run ingestion for a specific source by name."""
        source_config = self.config.get_source(source_name)
        if source_config is None:
            logger.log_error(source_name, f"Source not found: {source_name}")
            return None
        return self.run_source(source_config)
    
    def close(self):
        """Close database connections."""
        self.loader.close()


def init_database(loader: DatabaseLoader):
    """Initialize database tables."""
    schema_sql = """
        DROP TABLE IF EXISTS stg_books CASCADE;
        DROP TABLE IF EXISTS stg_rejects CASCADE;
        
        CREATE TABLE IF NOT EXISTS stg_rejects (
            id            SERIAL PRIMARY KEY,
            source_name   TEXT NOT NULL,
            raw_payload   JSONB NOT NULL,
            reason        TEXT NOT NULL,
            rejected_at   TIMESTAMP NOT NULL DEFAULT NOW()
        );
        
        CREATE TABLE IF NOT EXISTS stg_books (
            key                     TEXT PRIMARY KEY,
            title                   TEXT NOT NULL,
            subtitle                TEXT,
            author_name             TEXT,
            first_publish_year      INTEGER,
            edition_count           INTEGER,
            language                TEXT,
            publisher               TEXT,
            publish_date            TEXT,
            isbn                    TEXT,
            number_of_pages_median  INTEGER,
            ratings_average         FLOAT,
            ratings_count           INTEGER,
            already_read_count      INTEGER,
            subject                 TEXT,
            has_fulltext            BOOLEAN,
            _loaded_at              TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """
    loader.execute_sql(schema_sql)
    print("Database tables initialized.")


def main():
    """Main entry point for the ingestion pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Data Ingestion Pipeline")
    parser.add_argument(
        "--config",
        type=str,
        default="config/sources.yml",
        help="Path to configuration file",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Specific source to run (runs all if not specified)",
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize database tables before running",
    )
    
    args = parser.parse_args()
    
    config = AppConfig.from_yaml(args.config)
    pipeline = IngestionPipeline(config)
    
    try:
        if args.init_db:
            init_database(pipeline.loader)
        
        if args.source:
            result = pipeline.run_by_name(args.source)
            if result:
                print(f"\n{logger.get_summary(args.source)}")
        else:
            results = pipeline.run_all()
            print("\n=== Ingestion Summary ===")
            for result in results:
                print(logger.get_summary(result["source"]))
    
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
