import argparse
import pandas as pd
import numpy as np
import os
import sqlite3
from datetime import datetime

# --- CONFIG & DB ---
from commons.config import *
from database.database_manager import DatabaseManager
from database.schema import *

# --- PIPELINE 1: BATDONGSAN ---
from Batdongsan.orchestrator import (
    scrape_urls_multithreaded as bds_scrape_urls, 
    scrape_details_multithreaded as bds_scrape_details
)
from Batdongsan.cleaning import DataCleaner as BdsCleaner, DataImputer, FeatureEngineer

# --- PIPELINE 2: ONEHOUSING ---
from Onehousing.orchestrator import (
    scrape_onehousing_urls as oh_scrape_urls, 
    scrape_onehousing_details as oh_scrape_details
)
from Onehousing.cleaning import OneHousingDataCleaner as OhCleaner

# --- SHARED UTILS ---
from Batdongsan.address_standardizer import AddressStandardizer

# --- STANDARDIZED DATA SCHEMA ---
FINAL_SCHEMA = {
    "Tỉnh/Thành phố": "Tỉnh/Thành phố",
    "Thành phố/Quận/Huyện/Thị xã": "Thành phố/Quận/Huyện/Thị xã",
    "Xã/Phường/Thị trấn": "Xã/Phường/Thị trấn",
    "Đường phố": "Đường phố",
    "Chi tiết": "Chi tiết",
    "Nguồn thông tin": "url",
    "Tình trạng giao dịch": "Tình trạng giao dịch",
    "Thời điểm giao dịch/rao bán": "Thời điểm giao dịch/rao bán",
    "Thông tin liên hệ": "Thông tin liên hệ",
    "Giá rao bán/giao dịch": "Giá rao bán/giao dịch",
    "Giá ước tính": "Giá ước tính",
    "Loại đơn giá (đ/m2 hoặc đ/m ngang)": "Loại đơn giá (đ/m2 hoặc đ/m ngang)",
    "Đơn giá đất": "Đơn giá đất",
    "Lợi thế kinh doanh": "Lợi thế kinh doanh",
    "Số tầng công trình": "Số tầng công trình",
    "Tổng diện tích sàn": "Tổng diện tích sàn",
    "Đơn giá xây dựng": "Đơn giá xây dựng",
    "Năm xây dựng": "Năm xây dựng",
    "Chất lượng còn lại": "Chất lượng còn lại",
    "Diện tích đất (m2)": "Diện tích đất (m2)",
    "Kích thước mặt tiền (m)": "Kích thước mặt tiền (m)",
    "Kích thước chiều dài (m)": "Kích thước chiều dài (m)",
    "Số mặt tiền tiếp giáp": "Số mặt tiền tiếp giáp",
    "Hình dạng": "Hình dạng",
    "Độ rộng ngõ/ngách nhỏ nhất (m)": "Độ rộng ngõ/ngách nhỏ nhất (m)",
    "Khoảng cách tới trục đường chính (m)": "Khoảng cách tới trục đường chính (m)",
    "Mục đích sử dụng đất": "Mục đích sử dụng đất",
    "Yếu tố khác": "description",
    "Tọa độ (vĩ độ)": "latitude",
    "Tọa độ (kinh độ)": "longitude",
    "Hình ảnh của bài đăng": "image_urls",
}

def process_batdongsan_df(standardizer):
    """Cleaning and Feature Engineering logic for Batdongsan CSV data."""
    raw_path = DETAILS_CSV_PATH['Batdongsan']
    if not raw_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(raw_path).drop_duplicates()
    
    # Extraction
    df['Tỉnh/Thành phố'] = df.apply(BdsCleaner.extract_city, axis=1).apply(standardizer.standardize_province)
    df['Thành phố/Quận/Huyện/Thị xã'] = df.apply(BdsCleaner.extract_district, axis=1)
    df['Thành phố/Quận/Huyện/Thị xã'] = df.apply(standardizer.standardize_district, axis=1)
    df['Xã/Phường/Thị trấn'] = df.apply(BdsCleaner.extract_ward, axis=1)
    df['Xã/Phường/Thị trấn'] = df.apply(standardizer.standardize_ward, axis=1)
    
    df['Đường phố'] = df.apply(BdsCleaner.extract_street, axis=1)
    df['Chi tiết'] = df.apply(BdsCleaner.extract_address_details, axis=1)
    df['Thời điểm giao dịch/rao bán'] = df['main_info'].apply(BdsCleaner.extract_published_date)
    df['Giá rao bán/giao dịch'] = df.apply(BdsCleaner.extract_price, axis=1)
    df['Số mặt tiền tiếp giáp'] = df.apply(BdsCleaner.extract_facade_count, axis=1)
    df['Diện tích đất (m2)'] = df.apply(BdsCleaner.extract_total_area, axis=1)
    df['Kích thước mặt tiền (m)'] = df.apply(BdsCleaner.extract_width, axis=1)
    df['Độ rộng ngõ/ngách nhỏ nhất (m)'] = df.apply(BdsCleaner.extract_adjacent_lane_width, axis=1)
    df['Khoảng cách tới trục đường chính (m)'] = df.apply(BdsCleaner.extract_distance_to_the_main_road, axis=1)
    df['description'] = df['description'].apply(BdsCleaner.clean_description_text)
    
    # Building Specs
    df['Số tầng công trình'] = df.apply(BdsCleaner.extract_num_floors, axis=1)
    df['Hình dạng'] = df.apply(BdsCleaner.extract_land_shape, axis=1)
    df['Chất lượng còn lại'] = df.apply(BdsCleaner.estimate_remaining_quality, axis=1)
    df['Đơn giá xây dựng'] = df.apply(BdsCleaner.extract_construction_cost, axis=1)
    df['Mục đích sử dụng đất'] = df.apply(BdsCleaner.extract_land_use, axis=1)
    df['Tổng diện tích sàn'] = df.apply(BdsCleaner.extract_building_area, axis=1)

    # Imputation & Features
    df = DataImputer.fill_missing_width(df)
    df['Kích thước chiều dài (m)'] = df.apply(DataImputer.fill_missing_length, axis=1)
    df['Giá ước tính'] = df.apply(FeatureEngineer.calculate_estimated_price, axis=1)
    df['Lợi thế kinh doanh'] = df.apply(FeatureEngineer.calculate_business_advantage, axis=1)
    df['Đơn giá đất'] = df.apply(FeatureEngineer.calculate_land_unit_price, axis=1)

    # Constants
    df["Tình trạng giao dịch"] = "Đang rao bán"
    df["Thông tin liên hệ"] = np.nan
    df["Loại đơn giá (đ/m2 hoặc đ/m ngang)"] = "đ/m2"
    df["Năm xây dựng"] = np.nan

    # Rename to standardized schema
    bds_final = df.rename(columns={v: k for k, v in FINAL_SCHEMA.items() if v in df.columns})
    bds_final = bds_final[list(FINAL_SCHEMA.keys())]

    # Drop NaN and duplicated values
    na = [
        'Tỉnh/Thành phố',
        'Thành phố/Quận/Huyện/Thị xã',
        'Xã/Phường/Thị trấn',
        'Đường phố',
        'Chi tiết',
        'Nguồn thông tin', 
        'Thời điểm giao dịch/rao bán',
        'Giá rao bán/giao dịch',
        'Giá ước tính',
        'Đơn giá đất',
        'Lợi thế kinh doanh',
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
        'Tọa độ (vĩ độ)',
        'Tọa độ (kinh độ)'
    ]
    dup = [
        'Tỉnh/Thành phố', 
        'Thành phố/Quận/Huyện/Thị xã', 
        'Xã/Phường/Thị trấn', 
        'Đường phố', 
        'Giá rao bán/giao dịch', 
        'Giá ước tính', 
        'Đơn giá đất', 
        'Lợi thế kinh doanh', 
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
        'Mục đích sử dụng đất'
    ]

    old_size = bds_final.shape[0]
    bds_final.drop_duplicates(subset=dup, inplace=True)
    print(f'Dropped {old_size - bds_final.shape[0]} duplicated rows for Batdongsan.')

    old_size = bds_final.shape[0]  
    bds_final.dropna(subset=na, inplace=True)
    bds_final.reset_index(drop=True)
    print(f'Dropped {old_size - bds_final.shape[0]} NaN rows for Batdongsan.')

    print(f'Final number of rows for Batdongsan: {bds_final.shape[0]}')

    return bds_final

def process_onehousing_df():
    """Cleaning logic for OneHousing CSV data."""
    raw_path = DETAILS_CSV_PATH['Onehousing']
    if not raw_path.exists():
        return pd.DataFrame()

    df_raw = pd.read_csv(raw_path)
    df = OhCleaner.clean_onehousing_data(df_raw)
    
    # # Apply standardizer to address columns
    # df['Tỉnh/Thành phố'] = df['Tỉnh/Thành phố'].apply(standardizer.standardize_province)
    # df['Thành phố/Quận/Huyện/Thị xã'] = df.apply(standardizer.standardize_district, axis=1)
    # df['Xã/Phường/Thị trấn'] = df.apply(standardizer.standardize_ward, axis=1)
    
    # Missing coordinates in OH
    if 'latitude' not in df.columns: df['latitude'] = np.nan
    if 'longitude' not in df.columns: df['longitude'] = np.nan

    # Rename to standardized schema
    oh_final = df.rename(columns={v: k for k, v in FINAL_SCHEMA.items() if v in df.columns})
    oh_final = oh_final[list(FINAL_SCHEMA.keys())]

    # Drop NaN and duplicated values
    na = [
        'Tỉnh/Thành phố',
        'Thành phố/Quận/Huyện/Thị xã',
        'Xã/Phường/Thị trấn',
        'Đường phố',
        'Chi tiết',
        'Nguồn thông tin', 
        'Thời điểm giao dịch/rao bán',
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
        'Tọa độ (vĩ độ)',
        'Tọa độ (kinh độ)'
    ]
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
        'Mục đích sử dụng đất'
    ]

    old_size = oh_final.shape[0]
    oh_final.drop_duplicates(subset=dup, inplace=True)
    oh_final.reset_index(drop=True)
    print(f'Dropped {old_size - oh_final.shape[0]} duplicated rows for Onehousing.')

    # old_size = oh_final.shape[0]  
    # oh_final.dropna(subset=na, inplace=True)
    # print(f'Dropped {old_size - oh_final.shape[0]} NaN rows for Onehousing.')

    print(f'Final number of rows for Onehousing: {oh_final.shape[0]}')

    return oh_final

# def clean():
#     print("\n--- PHASE 2: CLEANING & DATABASE SYNC ---")
#     db = DatabaseManager()
#     standardizer = AddressStandardizer(
#         PROVINCES_SQL_FILE, DISTRICTS_SQL_FILE, 
#         WARDS_SQL_FILE, STREETS_SQL_FILE
#     )

#     # --- Handle Batdongsan ---
#     bds_csv = DETAILS_CSV_PATH['Batdongsan']
#     if bds_csv.exists():
#         df_bds_raw = pd.read_csv(bds_csv)
#         db.insert_raw_data("bds_raw", df_bds_raw) # Deduplicated insertion
        
#         df_bds_clean = process_batdongsan_df(standardizer)
#         db.insert_cleaned_data(df_bds_clean, "Batdongsan") # Deduplicated insertion
    
#     # --- Handle OneHousing ---
#     oh_csv = DETAILS_CSV_PATH['Onehousing']
#     if oh_csv.exists():
#         df_oh_raw = pd.read_csv(oh_csv)
#         db.insert_raw_data("onehousing_raw", df_oh_raw) # Deduplicated insertion
        
#         df_oh_clean = process_onehousing_df(standardizer)
#         db.insert_cleaned_data(df_oh_clean, "OneHousing") # Deduplicated insertion

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
    placeholders = ",".join(["?"] * len(cols))
    quoted_cols = "',".join(f'"{col}"' for col in cols)

    sql_statement = f"""
    INSERT OR IGNORE INTO {table_name} ({quoted_cols})
    VALUES ({placeholders})
    """

    with sqlite3.connect(DATABASE_DIR) as conn:
        conn.executemany(sql_statement, df.itertuples(index=False, name=None))
        conn.commit()

def clean():
    print("\n--- PHASE 2: CLEANING & DATABASE SYNC ---")
    if not os.path.exists(DATABASE_DIR):
        create_db()

    # Add raw data
    with sqlite3.connect(DATABASE_DIR) as conn:
        cursor = conn.cursor()    

        # Add raw data
        print("Adding raw data...")
        add_row_to_table(DETAILS_CSV_PATH['Batdongsan'], "bds_raw")
        add_row_to_table(DETAILS_CSV_PATH['Onehousing'], "onehousing_raw")
        print("Finished adding raw data!")

        # Clean data
        print("Cleaning data...")
        standardizer = AddressStandardizer(
            PROVINCES_SQL_FILE, DISTRICTS_SQL_FILE, 
            WARDS_SQL_FILE, STREETS_SQL_FILE
        )
        df_bds_clean = process_batdongsan_df(standardizer=standardizer)
        df_bds_clean['Web'] = 'Batdongsan'
        df_oh_clean = process_onehousing_df(standardizer=standardizer)
        df_oh_clean['Web'] = 'Onehousing'

        df_cleaned = pd.concat([df_bds_clean, df_oh_clean], ignore_index=True)
        df_cleaned.to_csv(CLEANED_CSV_PATH, index=False)
        add_row_to_table(CLEANED_CSV_PATH, "cleaned")
        print("Finished cleaning data!")


        


def run_pipeline():
    bds_scrape_urls()
    bds_scrape_details()
    oh_scrape_urls()
    oh_scrape_details()
    clean()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified Vietnamese Real Estate Pipeline")
    parser.add_argument("--mode", choices=["full", "scrape", "clean"], default="full")
    args = parser.parse_args()

    if args.mode == "full":
        run_pipeline()
    elif args.mode == "scrape":
        bds_scrape_urls()
        bds_scrape_details()
        oh_scrape_urls()
        oh_scrape_details()
    elif args.mode == "clean":
        # Note: Database insertion logic is part of run_pipeline/clean mode
        clean() # Simplified for this structure