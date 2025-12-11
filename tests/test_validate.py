"""
Unit tests for validate.py
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from validate import (
    cast_types,
    apply_rules,
    evaluate_rule,
    validate_data,
)


class TestCastTypes:
    
    def test_casts_string_to_int(self):
        df = pd.DataFrame({"year": ["2020", "2021"]})
        schema = {"year": "int"}
        result, rejects = cast_types(df, "test", schema)
        assert result.iloc[0]["year"] == 2020
    
    def test_rejects_invalid_int(self):
        df = pd.DataFrame({"year": ["2020", "invalid"]})
        schema = {"year": "int"}
        result, rejects = cast_types(df, "test", schema)
        assert len(result) == 1
        assert len(rejects) == 1


class TestEvaluateRule:
    
    def test_not_null_rule(self):
        df = pd.DataFrame({"title": ["Python", None]})
        mask = evaluate_rule(df, "title NOT NULL")
        assert mask.iloc[0] == True
        assert mask.iloc[1] == False
    
    def test_len_greater_than_zero(self):
        df = pd.DataFrame({"title": ["Python", ""]})
        mask = evaluate_rule(df, "len(title) > 0")
        assert mask.iloc[0] == True
        assert mask.iloc[1] == False


class TestApplyRules:
    
    def test_applies_rules_and_rejects(self):
        df = pd.DataFrame({"title": ["Python", ""]})
        result, rejects = apply_rules(df, "test", ["len(title) > 0"])
        assert len(result) == 1
        assert len(rejects) == 1


class TestValidateDataIntegration:
    
    def test_full_validation(self):
        df = pd.DataFrame({
            "title": ["Python", ""],
            "year": ["2020", "invalid"],
        })
        schema = {"title": "str", "year": "int"}
        rules = ["len(title) > 0"]
        
        valid_df, rejects = validate_data(df, "test", schema, rules)
        
        # Row 0: passes (title has length, year casts fine)
        # Row 1: fails cast (invalid year) - removed before rules
        assert len(valid_df) == 1
        assert len(rejects) == 1