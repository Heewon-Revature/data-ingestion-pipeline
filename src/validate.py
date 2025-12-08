"""
Data validation module.
Handles schema validation, type casting, and rule enforcement.
"""

import re

import pandas as pd
import numpy as np

from config import SourceConfig


class ValidationResult:
    """Container for validation results."""
    
    def __init__(self, valid_df: pd.DataFrame, rejects: list[dict]):
        self.valid_df = valid_df
        self.rejects = rejects


class Validator:
    """Validates DataFrame against schema and rules."""
    
    TYPE_MAP = {
        "int": "Int64",
        "float": "float64",
        "str": "string",
        "datetime": "datetime64[ns]",
        "bool": "boolean",
    }
    
    def __init__(self, source_config: SourceConfig):
        self.config = source_config
        self.schema = source_config.schema
        self.rules = source_config.rules
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """Run all validation steps and return results."""
        rejects = []
        
        # Step 1: Apply schema casts
        df, cast_rejects = self._apply_schema_casts(df)
        rejects.extend(cast_rejects)
        
        # Step 2: Enforce required fields (primary keys)
        df, required_rejects = self._enforce_required(df)
        rejects.extend(required_rejects)
        
        # Step 3: Apply custom rules
        df, rule_rejects = self._apply_rules(df)
        rejects.extend(rule_rejects)
        
        return ValidationResult(df, rejects)
    
    def _apply_schema_casts(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
        """Cast columns to schema types, rejecting rows that can't be cast."""
        rejects = []
        valid_mask = pd.Series(True, index=df.index)
        
        for col, dtype in self.schema.items():
            if col not in df.columns:
                continue
            
            target_dtype = self.TYPE_MAP.get(dtype, dtype)
            
            try:
                if dtype == "datetime":
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif dtype == "int":
                    numeric = pd.to_numeric(df[col], errors="coerce")
                    invalid = numeric.isna() & df[col].notna()
                    df[col] = numeric.astype("Int64")
                    
                    if invalid.any():
                        for idx in df[invalid].index:
                            rejects.append({
                                "source_name": self.config.name,
                                "raw_payload": df.loc[idx].to_dict(),
                                "reason": f"Failed to cast column '{col}' to {dtype}",
                            })
                        valid_mask &= ~invalid
                        
                elif dtype == "float":
                    numeric = pd.to_numeric(df[col], errors="coerce")
                    invalid = numeric.isna() & df[col].notna()
                    df[col] = numeric
                    
                    if invalid.any():
                        for idx in df[invalid].index:
                            rejects.append({
                                "source_name": self.config.name,
                                "raw_payload": df.loc[idx].to_dict(),
                                "reason": f"Failed to cast column '{col}' to {dtype}",
                            })
                        valid_mask &= ~invalid
                else:
                    df[col] = df[col].astype(target_dtype)
                    
            except Exception as e:
                pass
        
        return df[valid_mask].copy(), rejects
    
    def _enforce_required(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
        """Drop rows missing required values (primary keys)."""
        rejects = []
        valid_mask = pd.Series(True, index=df.index)
        
        for pk_col in self.config.pk:
            if pk_col in df.columns:
                null_mask = df[pk_col].isna()
                if null_mask.any():
                    for idx in df[null_mask].index:
                        rejects.append({
                            "source_name": self.config.name,
                            "raw_payload": df.loc[idx].to_dict(),
                            "reason": f"Missing required primary key: {pk_col}",
                        })
                    valid_mask &= ~null_mask
        
        return df[valid_mask].copy(), rejects
    
    def _apply_rules(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
        """Apply custom validation rules from configuration."""
        rejects = []
        valid_mask = pd.Series(True, index=df.index)
        
        for rule_def in self.rules:
            rule = rule_def.get("rule", "")
            if not rule:
                continue
            
            try:
                rule_mask = self._evaluate_rule(df, rule)
                invalid = ~rule_mask
                
                if invalid.any():
                    for idx in df[invalid].index:
                        rejects.append({
                            "source_name": self.config.name,
                            "raw_payload": df.loc[idx].to_dict(),
                            "reason": f"Failed rule: {rule}",
                        })
                    valid_mask &= rule_mask
                    
            except Exception as e:
                pass
        
        return df[valid_mask].copy(), rejects
    
    def _evaluate_rule(self, df: pd.DataFrame, rule: str) -> pd.Series:
        """Evaluate a single rule and return a boolean mask."""
        
        # Pattern: "column NOT NULL"
        not_null_match = re.match(r"(\w+)\s+NOT\s+NULL", rule, re.IGNORECASE)
        if not_null_match:
            col = not_null_match.group(1)
            return df[col].notna()
        
        # Pattern: "column >= value" or similar comparisons
        comparison_match = re.match(r"(\w+)\s*(>=|<=|>|<|==|!=)\s*(\d+\.?\d*)", rule)
        if comparison_match:
            col, op, value = comparison_match.groups()
            value = float(value)
            if op == ">=":
                return df[col] >= value
            elif op == "<=":
                return df[col] <= value
            elif op == ">":
                return df[col] > value
            elif op == "<":
                return df[col] < value
            elif op == "==":
                return df[col] == value
            elif op == "!=":
                return df[col] != value
        
        # Pattern: "column IN ('val1','val2')"
        in_match = re.match(r"(\w+)\s+IN\s*\(([^)]+)\)", rule, re.IGNORECASE)
        if in_match:
            col = in_match.group(1)
            values_str = in_match.group(2)
            values = [v.strip().strip("'\"") for v in values_str.split(",")]
            return df[col].isin(values)
        
        # Pattern: "column LIKE '%@%'" (simple contains check)
        like_match = re.match(r"(\w+)\s+LIKE\s+'%([^%]+)%'", rule, re.IGNORECASE)
        if like_match:
            col, pattern = like_match.groups()
            return df[col].astype(str).str.contains(pattern, na=False)
        
        # Pattern: "len(column) > value"
        len_match = re.match(r"len\((\w+)\)\s*(>=|<=|>|<|==|!=)\s*(\d+)", rule)
        if len_match:
            col, op, value = len_match.groups()
            value = int(value)
            lengths = df[col].astype(str).str.len()
            if op == ">=":
                return lengths >= value
            elif op == "<=":
                return lengths <= value
            elif op == ">":
                return lengths > value
            elif op == "<":
                return lengths < value
            elif op == "==":
                return lengths == value
            elif op == "!=":
                return lengths != value
        
        # Default: return all True if rule not recognized
        return pd.Series(True, index=df.index)
