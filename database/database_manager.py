import sqlite3
import time
import pandas as pd
from pathlib import Path
from contextlib import contextmanager
from typing import List, Dict, Any, Optional

from .schema import * 
from commons.config import * 

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

    def create_db():
        with sqlite3.connect(DATABASE_DIR) as conn:
            cursor = conn.cursor()

            print('Creating database tables...')
            cursor.execute(BDS_RAW_TABLE)
            cursor.execute(UNIQUE_INDEX_BDS_RAW)
            cursor.execute(ONEHOUSING_RAW_TABLE)
            cursor.execute(UNIQUE_INDEX_ONEHOUSING_RAW)
            cursor.execute(CLEANED_TABLE)
            cursor.execute(UNIQUE_INDEX_CLEANED)

            conn.commit()
            print("Finished creating database!")

    def add_row_to_table(data_path, table_name):
        df = pd.read_csv(data_path)
        try:
            df.drop(columns='scraping_time', inplace=True)
        except:
            pass
        cols = list(df.columns)
        print(cols)
        placeholders = ",".join(["?"] * (len(cols)))
        quoted_cols = ",".join(f'"{col}"' for col in cols)

        sql_statement = f"""
        INSERT OR IGNORE INTO {table_name} ({quoted_cols})
        VALUES ({placeholders})
        """

        with sqlite3.connect(DATABASE_DIR) as conn:
            conn.executemany(sql_statement, df.itertuples(index=False, name=None))
            conn.commit()