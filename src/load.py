"""
Database Loader - Loads data into PostgreSQL.
"""

import json
import pandas as pd
from sqlalchemy import create_engine, text

'''
Creates SQLAlchemy database engine

Args:
    db_url: PostgreSQL connection string

Returns:
    SQLAlchemy Engine Instance
'''
def create_loader(db_url):
    return create_engine(db_url)

'''
Load valid records into the database using UPSERT. Inserts new records or 
updates existing ones based on primary key conflict. Uses PostgreSQL's ON 
CONFLICT DO UPDATE syntax foratomic upsert operations. 

Args:
    df: DataFrame containing records to load
    engine: SQLAlchemy database engine
    table: Target table name
    pk: List of primary key column names
Returns:
    Number of records inserted/updated
'''
def load_data(df, engine, table, pk):
    if df.empty:
        return 0
    
    columns = list(df.columns)
    non_pk = [col for col in columns if col not in pk]
    
    placeholders = ", ".join([f":{col}" for col in columns])
    column_list = ", ".join(columns)
    pk_list = ", ".join(pk)
    update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in non_pk])
    
    query = f"""
        INSERT INTO {table} ({column_list})
        VALUES ({placeholders})
        ON CONFLICT ({pk_list}) DO UPDATE SET {update_set}
    """
    
    inserted = 0
    with engine.connect() as conn:
        for _, row in df.iterrows():
            params = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
            conn.execute(text(query), params)
            inserted += 1
        conn.commit()
    
    return inserted

'''
Load rejected records inot the stg_rejects table. Stores rejected records
with their source name, raw payload as JSON, and rejection reason. Converts
NaN values to None for valid JSON serialization.

Args:
    rejects: List of reject dictionaries with source_name, raw_payload and
    reason for rejection.
    engine: SQLAlchemy database engine

Returns:
    Number of rejected records inserted
'''
def load_rejects(rejects, engine):
    if not rejects:
        return 0
    
    query = """
        INSERT INTO stg_rejects (source_name, raw_payload, reason)
        VALUES (:source_name, :raw_payload, :reason)
    """
    
    with engine.connect() as conn:
        for reject in rejects:
            payload = {}
            for k, v in reject["raw_payload"].items():
                if pd.isna(v):
                    payload[k] = None
                else:
                    payload[k] = v
            
            params = {
                "source_name": reject["source_name"],
                "raw_payload": json.dumps(payload, default=str),
                "reason": reject["reason"],
            }
            conn.execute(text(query), params)
        conn.commit()
    
    return len(rejects)

'''
Creates the database tables for the pipeline. It drops existing
tables and creates fresh stg_books and stg_rejects tables. stg_books stores
valid records and stg_records stores failed records

Args:
    engine: SQLAlchemy database engine
'''
def init_database(engine):
    schema_sql = """
        DROP TABLE IF EXISTS stg_books CASCADE;
        DROP TABLE IF EXISTS stg_rejects CASCADE;
        
        CREATE TABLE stg_rejects (
            id            SERIAL PRIMARY KEY,
            source_name   TEXT NOT NULL,
            raw_payload   JSONB NOT NULL,
            reason        TEXT NOT NULL,
            rejected_at   TIMESTAMP NOT NULL DEFAULT NOW()
        );
        
        CREATE TABLE stg_books (
            key                TEXT PRIMARY KEY,
            title              TEXT NOT NULL,
            author_name        TEXT,
            first_publish_year INTEGER,
            edition_count      INTEGER,
            language           TEXT,
            has_fulltext       BOOLEAN,
            _loaded_at         TIMESTAMP NOT NULL DEFAULT NOW()
        )
    """
    with engine.connect() as conn:
        for statement in schema_sql.split(";"):
            if statement.strip():
                conn.execute(text(statement))
        conn.commit()
    print("Database tables initialized.")
