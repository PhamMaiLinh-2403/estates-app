import sqlite3
import time
import pandas as pd
from pathlib import Path
from contextlib import contextmanager
from typing import List, Dict, Any, Optional

from .schema import * 

class DatabaseManager:
    def __init__(self, db_path: str = "output/real_estate.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row 
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def _ensure_schema(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(BDS_RAW_LISTINGS_TABLE) 
            cursor.execute(ONEHOUSING_RAW_LISTINGS_TABLE)
            cursor.execute(CLEANED_LISTINGS_TABLE)

    def _generate_key(self, index: int) -> str:
        """Generates key: timestamp_index."""
        return f"{int(time.time())}_{index}"

    def _is_duplicate(self, cursor, table_name: str, data: Dict[str, Any], exclude_cols: List[str]) -> bool:
        """
        Checks if a record exists where all columns (except excluded ones) match.
        """
        # Filter out the key/ID from the check
        check_data = {k: v for k, v in data.items() if k not in exclude_cols}
        
        columns = list(check_data.keys())
        # Handle NULLs correctly in SQL with 'IS' instead of '='
        where_clause = " AND ".join([f'"{col}" {"IS ?" if check_data[col] is None else "= ?"}' for col in columns])
        values = list(check_data.values())
        
        query = f'SELECT 1 FROM "{table_name}" WHERE {where_clause} LIMIT 1'
        cursor.execute(query, values)
        return cursor.fetchone() is not None

    def insert_raw_data(self, table_name: str, df: pd.DataFrame):
        """
        Inserts raw data into bds_raw or onehousing_raw with duplicate checking.
        """
        if df.empty: return
        
        # Ensure data is converted to native Python types for SQLite
        records = df.where(pd.notnull(df), None).to_dict('records')
        inserted_count = 0

        with self.get_connection() as conn:
            cursor = conn.cursor()
            for idx, row in enumerate(records):
                # We exclude 'key' from the duplicate check
                if not self._is_duplicate(cursor, table_name, row, exclude_cols=['key']):
                    row['key'] = self._generate_key(idx)
                    
                    columns = ", ".join([f'"{k}"' for k in row.keys()])
                    placeholders = ", ".join(["?" for _ in row.keys()])
                    query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders})'
                    
                    cursor.execute(query, list(row.values()))
                    inserted_count += 1
        
        print(f"[DB] Inserted {inserted_count} new raw records into {table_name}.")

    def insert_cleaned_data(self, df: pd.DataFrame, source_website: str):
        """
        Inserts cleaned records into the 'cleaned' table.
        Adds the 'websites' column and checks for duplicates.
        """
        if df.empty: return

        # Add marking column
        df = df.copy()
        df['websites'] = source_website
        
        # Convert to native types
        records = df.where(pd.notnull(df), None).to_dict('records')
        inserted_count = 0

        with self.get_connection() as conn:
            cursor = conn.cursor()
            for row in records:
                # Exclude 'ID' as it is auto-increment
                if not self._is_duplicate(cursor, "cleaned", row, exclude_cols=['ID']):
                    columns = ", ".join([f'"{k}"' for k in row.keys()])
                    placeholders = ", ".join(["?" for _ in row.keys()])
                    query = f'INSERT INTO "cleaned" ({columns}) VALUES ({placeholders})'
                    
                    cursor.execute(query, list(row.values()))
                    inserted_count += 1
                    
        print(f"[DB] Inserted {inserted_count} new cleaned records (Source: {source_website}).")