"""
Database loader module.
Handles loading data into PostgreSQL using UPSERT pattern.
"""

import json
from contextlib import contextmanager
from typing import Generator

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection


class DatabaseLoader:
    """Loads data into PostgreSQL staging tables."""
    
    def __init__(self, db_url: str, batch_size: int = 5000):
        self.db_url = db_url
        self.batch_size = batch_size
        self._engine: Engine | None = None
    
    @property
    def engine(self) -> Engine:
        """Lazy-load the database engine."""
        if self._engine is None:
            self._engine = create_engine(self.db_url)
        return self._engine
    
    @contextmanager
    def get_connection(self) -> Generator[Connection, None, None]:
        """Get a database connection with proper cleanup."""
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()
    
    def close(self):
        """Close the database engine."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
    
    def load_upsert(
        self,
        df: pd.DataFrame,
        table: str,
        pk: list[str],
    ) -> dict[str, int]:
        """Load DataFrame into table using UPSERT pattern."""
        if df.empty:
            return {"inserted": 0, "updated": 0}
        
        columns = list(df.columns)
        non_pk_columns = [col for col in columns if col not in pk]
        
        # Build the UPSERT query
        placeholders = ", ".join([f":{col}" for col in columns])
        column_list = ", ".join(columns)
        
        update_set = ", ".join([
            f"{col} = EXCLUDED.{col}" for col in non_pk_columns
        ])
        
        pk_list = ", ".join(pk)
        
        if update_set:
            query = f"""
                INSERT INTO {table} ({column_list})
                VALUES ({placeholders})
                ON CONFLICT ({pk_list}) DO UPDATE
                SET {update_set}
            """
        else:
            query = f"""
                INSERT INTO {table} ({column_list})
                VALUES ({placeholders})
                ON CONFLICT ({pk_list}) DO NOTHING
            """
        
        inserted = 0
        
        with self.get_connection() as conn:
            for start in range(0, len(df), self.batch_size):
                batch = df.iloc[start:start + self.batch_size]
                
                for _, row in batch.iterrows():
                    params = row.to_dict()
                    params = {
                        k: (None if pd.isna(v) else v) 
                        for k, v in params.items()
                    }
                    
                    result = conn.execute(text(query), params)
                    inserted += result.rowcount
                
                conn.commit()
        
        return {"inserted": inserted, "updated": 0}
    
    def load_rejects(
        self,
        rejects: list[dict],
        table: str = "stg_rejects",
    ) -> int:
        """Load rejected records into the rejects table."""
        if not rejects:
            return 0
        
        query = f"""
            INSERT INTO {table} (source_name, raw_payload, reason)
            VALUES (:source_name, :raw_payload, :reason)
        """
        
        with self.get_connection() as conn:
            for reject in rejects:
                params = {
                    "source_name": reject["source_name"],
                    "raw_payload": json.dumps(reject["raw_payload"], default=str),
                    "reason": reject["reason"],
                }
                conn.execute(text(query), params)
            conn.commit()
        
        return len(rejects)
    
    def execute_sql(self, sql: str) -> None:
        """Execute arbitrary SQL (for schema setup, etc.)."""
        with self.get_connection() as conn:
            for statement in sql.split(";"):
                statement = statement.strip()
                if statement:
                    conn.execute(text(statement))
            conn.commit()
