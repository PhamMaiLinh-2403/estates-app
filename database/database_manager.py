import sqlite3
import time
import pandas as pd
import numpy as np
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
        cols = list(df.columns)
        placeholders = ",".join(["?"] * (len(cols)))
        quoted_cols = ",".join(f'"{col}"' for col in cols)

        sql_statement = f"""
        INSERT OR IGNORE INTO {table_name} ({quoted_cols})
        VALUES ({placeholders})
        """

        with sqlite3.connect(DATABASE_DIR) as conn:
            conn.executemany(sql_statement, df.itertuples(index=False, name=None))
            conn.commit()

    def extract_data(start_date, end_date, web):
        # start_date = datetime.strptime(start_date, "%d/%m/%Y").strftime("%Y-%m-%d")
        # end_date = datetime.strptime(end_date, "%d/%m/%Y").strftime("%Y-%m-%d")

        with sqlite3.connect(DATABASE_DIR) as conn:
            cursor = conn.cursor()
            
            if web == 'Cả hai':
                sql_statement = """
                            SELECT *
                            FROM cleaned
                            WHERE
                            date(
                                substr("Thời điểm giao dịch/rao bán", 7, 4) || '-' ||
                                substr("Thời điểm giao dịch/rao bán", 4, 2) || '-' ||
                                substr("Thời điểm giao dịch/rao bán", 1, 2)
                            )
                            BETWEEN date(?) AND date(?)
                            """
                cursor.execute(
                    sql_statement,
                    (start_date, end_date))
            else:
                sql_statement = """
                                SELECT *
                                FROM cleaned
                                WHERE
                                date(
                                    substr("Thời điểm giao dịch/rao bán", 7, 4) || '-' ||
                                    substr("Thời điểm giao dịch/rao bán", 4, 2) || '-' ||
                                    substr("Thời điểm giao dịch/rao bán", 1, 2)
                                )
                                BETWEEN date(?) AND date(?)
                                AND Web = (?)
                                """
            
                cursor.execute(
                    sql_statement,
                    (start_date, end_date, web)
                )

            rows = cursor.fetchall()

        dup = [
            'Tỉnh/Thành phố', 
            'Thành phố/Quận/Huyện/Thị xã', 
            'Xã/Phường/Thị trấn', 
            'Đường phố', 
            'Giá rao bán/giao dịch', 
            'Giá ước tính', 
            'Số tầng công trình', 
            'Tổng diện tích sàn', 
            'Đơn giá xây dựng', 
            'Chất lượng còn lại', 
            'Diện tích đất (m2)', 
            'Kích thước mặt tiền (m)', 
            'Kích thước chiều dài (m)', 
            'Số mặt tiền tiếp giáp', 
            'Hình dạng', 
            'Độ rộng ngõ/ngách nhỏ nhất (m)', 
            'Khoảng cách tới trục đường chính (m)', 
            'Mục đích sử dụng đất',
            'Web',
            'Đơn giá đất', 
            'Lợi thế kinh doanh', 
        ]

        return_df = pd.DataFrame(rows, columns=[column[0] for column in cursor.description])
        return_df.drop_duplicates(subset=dup, inplace=True)
        return return_df