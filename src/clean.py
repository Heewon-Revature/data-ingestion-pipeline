"""
Data cleaning module.
Handles data normalization, standardization, and deduplication.
"""

import pandas as pd
import numpy as np

from config import SourceConfig


class Cleaner:
    """Cleans and standardizes DataFrame data."""
    
    def __init__(self, source_config: SourceConfig):
        self.config = source_config
    
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all cleaning steps to the DataFrame."""
        df = self._flatten_lists(df)
        df = self._filter_columns(df)
        df = self._strip_strings(df)
        df = self._normalize_nulls(df)
        df = self._remove_duplicates(df)
        return df
    
    def _flatten_lists(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert list columns to comma-separated strings."""
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(
                    lambda x: ", ".join(str(i) for i in x) if isinstance(x, list) else x
                )
        return df
    
    def _filter_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep only columns defined in the schema."""
        schema_cols = list(self.config.schema.keys())
        cols_to_keep = [col for col in df.columns if col in schema_cols]
        return df[cols_to_keep].copy()
    
    def _strip_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Strip whitespace from string columns."""
        for col in df.columns:
            if df[col].dtype == "object" or df[col].dtype == "string":
                df[col] = df[col].astype(str).str.strip()
        return df
    
    def _normalize_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize various null representations to NaN."""
        null_values = ["", "nan", "None", "none", "null", "NULL", "N/A", "n/a", "NA"]
        
        for col in df.columns:
            if df[col].dtype == "object" or df[col].dtype == "string":
                df[col] = df[col].replace(null_values, np.nan)
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows based on primary key columns."""
        pk_cols = self.config.pk
        existing_pk_cols = [col for col in pk_cols if col in df.columns]
        
        if existing_pk_cols:
            df = df.drop_duplicates(subset=existing_pk_cols, keep="last")
        
        return df.reset_index(drop=True)
