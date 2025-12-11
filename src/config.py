"""
Configuration loader for the Data Ingestion Pipeline.
"""

import os
import yaml
from dotenv import load_dotenv

'''
Load configuration from YAML file.
Reads YAML configuration file, extracts default settings and source definitions.
The database URL is overriden by DATABASE_URL environment variable from the .env file.

Args:
    config_path: The path to the YAML configuration file.

Returns:
    A dictionary containing the database URL, batch size and a list of sources.
'''
def load_config(config_path):
    """Load configuration from YAML file."""
    load_dotenv()
    
    with open(config_path, "r") as f:
        data = yaml.safe_load(f)
    
    defaults = data.get("defaults", {})
    
    return {
        "db_url": os.getenv("DATABASE_URL", defaults.get("db_url", "")),
        "batch_size": defaults.get("batch_size", 5000),
        "sources": data.get("sources", []),
    }
