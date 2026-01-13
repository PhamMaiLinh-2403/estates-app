import sqlite3
import json
import hashlib
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from typing import Optional, List, Dict, Any

from .schema import * 


class DatabaseManager:
    """
    Handles SQLite database connections, schema initialization, and data operations.
    """
    def __init__(self, db_path: str = "output/real_estate.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        Ensures connections are closed and transactions committed/rolled back.
        """
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
        """Initialize database schema on startup."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # 1. Create Tables
            cursor.execute(BDS_RAW_LISTINGS_TABLE) 
            cursor.execute(ONEHOUSING_RAW_LISTINGS_TABLE)
            cursor.execute(CLEANED_LISTINGS_TABLE)
            cursor.execute(METADATA_TABLE)

            # 3. Create Indices
            for index_sql in INDICES:
                cursor.execute(index_sql)

    def _compute_content_hash(self, data: Dict[str, Any]) -> str:
        hash_fields = [
            str(data.get('title', '')),
            str(data.get('short_address', '')),
            str(data.get('main_info', '')),
            str(data.get('description', '')),
            str(data.get('other_info', '')),
        ]
        content = '|'.join(hash_fields)
        return hashlib.sha256(content.encode()).hexdigest()

    # --- Database Operations --- 
    