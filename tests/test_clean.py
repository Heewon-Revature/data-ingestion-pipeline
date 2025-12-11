"""
Unit tests for clean.py
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from clean import (
    flatten_lists,
    filter_columns,
    strip_strings,
    normalize_nulls,
    remove_duplicates,
    clean_data,
)


class TestFlattenLists:
    
    def test_flattens_list_to_string(self):
        df = pd.DataFrame({"author": [["Mark", "David"]]})
        result = flatten_lists(df)
        assert result.loc[0, "author"] == "Mark, David"
    
    def test_leaves_non_list_unchanged(self):
        df = pd.DataFrame({"author": ["Mark"]})
        result = flatten_lists(df)
        assert result.loc[0, "author"] == "Mark"


class TestFilterColumns:
    
    def test_keeps_only_schema_columns(self):
        df = pd.DataFrame({"id": [1], "title": ["Python"], "extra": ["remove"]})
        schema = {"id": "int", "title": "str"}
        result = filter_columns(df, schema)
        assert "extra" not in result.columns
        assert "id" in result.columns


class TestStripStrings:
    
    def test_strips_whitespace(self):
        df = pd.DataFrame({"title": ["  Python  "]})
        result = strip_strings(df)
        assert result.loc[0, "title"] == "Python"


class TestNormalizeNulls:
    
    def test_converts_empty_string_to_nan(self):
        df = pd.DataFrame({"title": [""]})
        result = normalize_nulls(df)
        assert pd.isna(result.loc[0, "title"])
    
    def test_converts_na_string_to_nan(self):
        df = pd.DataFrame({"title": ["N/A"]})
        result = normalize_nulls(df)
        assert pd.isna(result.loc[0, "title"])


class TestRemoveDuplicates:
    
    def test_removes_duplicates_keeps_last(self):
        df = pd.DataFrame({"id": [1, 1], "title": ["First", "Last"]})
        result = remove_duplicates(df, ["id"])
        assert len(result) == 1
        assert result.iloc[0]["title"] == "Last"


class TestCleanDataIntegration:
    
    def test_full_pipeline(self):
        df = pd.DataFrame({
            "id": [1, 1],
            "title": ["  Python  ", "  Updated  "],
            "author": [["Mark"], ["Mark"]],
            "extra": ["remove", "me"],
        })
        schema = {"id": "int", "title": "str", "author": "str"}
        result = clean_data(df, schema, ["id"])
        
        assert "extra" not in result.columns
        assert len(result) == 1
        assert result.iloc[0]["title"] == "Updated"