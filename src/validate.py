"""
Data Validator - Validates data against schema and rules.
"""

import re
import pandas as pd
import numpy as np

'''
This method casts columns to the schema-defined types since all of the fields
returned by the API are Strings. Records that fail type conversion are 
rejected.

Args:
    df: Input DataFrame
    source_name: Name of the data source for reject tracking
    schema: Dictionary mapping column names to data types
Returns:
    Tuple of (valid_df, rejects) where rejects is a list of
    dictionaries containing source_name, raw_payload and reason
'''
def cast_types(df, source_name, schema):
    rejects = []
    
    for col, dtype in schema.items():
        if col not in df.columns:
            continue
            
        if dtype == "int":
            original = df[col].copy()
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

            failed_mask = df[col].isna() & original.notna()
            for idx in df[failed_mask].index:
                rejects.append({
                    "source_name": source_name,
                    "raw_payload": df.loc[idx].to_dict(),
                    "reason": f"Failed to cast '{col}' to {dtype}"
                })
            df = df[~failed_mask]
    
    return df.copy(), rejects

'''
Apply validation rules to the DataFrame. Evaluates each
rule against all rows. Collects all rows that fail a rule as rejects.

Args:
    df: Input DataFrame
    source_name: Name of the data source for reject tracking
    rules: List of rule strings to evaluate

Returns:
    Tuple of (valid_df, rejects) where rejects contains failed rows.
'''
def apply_rules(df, source_name, rules):
    rejects = []
    valid_mask = pd.Series(True, index=df.index)
    
    for rule in rules:
        mask = evaluate_rule(df, rule)
        invalid = ~mask
        
        for idx in df[invalid].index:
            rejects.append({
                "source_name": source_name,
                "raw_payload": df.loc[idx].to_dict(),
                "reason": f"Failed rule: {rule}",
            })
        valid_mask &= mask
    
    return df[valid_mask].copy(), rejects

'''
Evaluates a single validation rule and returns a boolean mask.
Checks if a field is NOT NULL and checks if the value has a length
greater than 0. If it observes an unknown rule then it just returns True.

Args:
    df: Input DataFrame
    rule: Rule string to be evaluated

Returns:
    Boolean Series where True indicates the row passes the rule.
'''
def evaluate_rule(df, rule):
    """Evaluate a validation rule and return a boolean mask."""
    rule_upper = rule.upper()
    
    if "NOT NULL" in rule_upper:
        col = rule.split()[0]
        return df[col].notna()
    
    if rule.startswith("len("):
        col = rule.split("(")[1].split(")")[0]
        return df[col].fillna("").astype(str).str.len() > 0
    
    # Unknown rule - pass all rows
    return pd.Series([True] * len(df), index=df.index)
    

'''
Validate data against schema types and rules. Runs the full validation
pipeline. Returns all valid and rejected data.

Args:
    df: Input DataFrame
    source_name: Name of the data source for reject tracking
    schema: Dictionary mapping column names to data types.
    rules: List of validation rule strings

Returns:
    Tuple of (valid_df, all_rejects) which contains valid records
    and all rejected records
'''
def validate_data(df, source_name, schema, rules):
    all_rejects = []
    
    # Cast types
    df, cast_rejects = cast_types(df, source_name, schema)
    all_rejects.extend(cast_rejects)
    
    # Apply rules
    df, rule_rejects = apply_rules(df, source_name, rules)
    all_rejects.extend(rule_rejects)
    
    return df, all_rejects
