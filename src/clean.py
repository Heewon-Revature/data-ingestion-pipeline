"""
Data Cleaner - Transforms and standardizes data.
"""

import pandas as pd
import numpy as np


'''
Convert list columns to comma-separated strings. This method iterates through
all columsn and converts a list into a comma-separated string. Doesn't change 
values that aren't lists.

Args:
    df: Input DataFrame with list columns

Returns:
    A DataFrame wiht all list values converted into comma-separated strings.
'''
def flatten_lists(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(
                lambda x: ", ".join(str(i) for i in x) if isinstance(x, list) else x
            )
    return df

'''
Only keeps columns that are defined in the schema and the DataFrame. Any extra
columns from the API response are discarded.

Args:
    df: Dataframe
    schema: A dictionary that maps column names to data types
'''
def filter_columns(df, schema):
    schema_cols = list(schema.keys())
    cols_to_keep = [col for col in df.columns if col in schema_cols]
    return df[cols_to_keep].copy()

'''
Strip leading and trailing whitespace from string columns. Non-string
columns aren't affected.

Args:  
    df: DataFrame

Returns:
    A DataFrame with whitespace stripped from string values.
'''
def strip_strings(df):
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()
    return df

'''
Normalize null values to NaN. We convert all possible null representations
to pandas NaN values for consistent null handling.

Args:
    df: Input DataFrame

Returns:
    A DataFrame with normalized null values.
'''
def normalize_nulls(df):
    """Normalize various null representations to NaN."""
    null_values = ["", "nan", "None", "none", "null", "NULL", "N/A", "n/a", "NA"]
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].replace(null_values, np.nan)
    return df

'''
Remove duplicate records based on the primary key. Resets indexes at 
the end since when you remove rows, pandas keeps the original index 
numbers which leaves gaps.

Args:
    df: Input DataFrame
    pk: List of primary keys
Returns:
    Deduplicated DataFrame
'''
def remove_duplicates(df, pk):
    if pk:
        existing_pk = [col for col in pk if col in df.columns]
        if existing_pk:
            df = df.drop_duplicates(subset=existing_pk, keep="last")
    return df.reset_index(drop=True)

'''
Apply all cleaning steps to the DataFrame.

Args:
    df: Input DataFrame
    schema: Dictionary that maps column names to data types
    pk: List of primary keys needed to remove duplicates

Returns:
    Cleaned DataFrame that is ready for validation.
'''
def clean_data(df, schema, pk):
    df = flatten_lists(df)
    df = filter_columns(df, schema)
    df = strip_strings(df)
    df = normalize_nulls(df)
    df = remove_duplicates(df, pk)
    return df
