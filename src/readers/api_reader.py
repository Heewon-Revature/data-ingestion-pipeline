"""
API data reader implementation.
"""

import requests
import pandas as pd

from config import SourceConfig


class APIReader:
    """Reader for REST API endpoints."""
    
    def __init__(self, source_config: SourceConfig, headers: dict | None = None):
        self.config = source_config
        self.headers = headers or {
            "User-Agent": "DataIngestionPipeline/1.0 (educational project)"
        }
    
    def read(self) -> pd.DataFrame:
        """Fetch data from API endpoint and return normalized DataFrame."""
        response = requests.get(self.config.path, headers=self.headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Handle both list and dict responses
        if isinstance(data, dict):
            if "docs" in data:
                data = data["docs"]
            else:
                data = [data]
        
        df = pd.DataFrame(data)
        return self._normalize_columns(df)
    
    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names: lowercase, strip whitespace, replace spaces with underscores."""
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_", regex=False)
            .str.replace("-", "_", regex=False)
        )
        return df
