import argparse
import os
import pandas as pd
import numpy as np
import json
from datetime import datetime

from commons.config import *

from Batdongsan.orchestrator import *
from Batdongsan.cleaning import * 

from Onehousing.orchestrator import * 
from Onehousing.cleaning import OneHousingDataCleaner 

from Batdongsan.address_standardizer import AddressStandardizer

# --- DATA SCHEMA (Standardized for both sources) ---
FINAL_SCHEMA = {
    "Tỉnh/Thành phố": "Tỉnh/Thành phố",
    "Thành phố/Quận/Huyện/Thị xã": "Thành phố/Quận/Huyện/Thị xã",
    "Xã/Phường/Thị trấn": "Xã/Phường/Thị trấn",
    "Đường phố": "Đường phố",
    "Chi tiết": "Chi tiết",
    "Nguồn thông tin": "url",
    "Tình trạng giao dịch": "status_const",
    "Thời điểm giao dịch/rao bán": "Thời điểm giao dịch/rao bán",
    "Thông tin liên hệ": "contact_const",
    "Giá rao bán/giao dịch": "Giá rao bán/giao dịch",
    "Giá ước tính": "Giá ước tính",
    "Loại đơn giá (đ/m2 hoặc đ/m ngang)": "unit_type_const",
    "Đơn giá đất": "Đơn giá đất",
    "Lợi thế kinh doanh": "Lợi thế kinh doanh",
    "Số tầng công trình": "Số tầng công trình",
    "Tổng diện tích sàn": "Tổng diện tích sàn",
    "Đơn giá xây dựng": "Đơn giá xây dựng",
    "Năm xây dựng": "year_const",
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

def run_scraping_phase():
    """Run Phase 1 (URLs) and Phase 2 (Details) for both sources."""
    print("\n=== STARTING BATDONGSAN PIPELINE ===")
    scrape_urls_multithreaded()
    scrape_details_multithreaded()

    print("\n=== STARTING ONEHOUSING PIPELINE ===")
    scrape_onehousing_urls()
    scrape_onehousing_details()

def process_batdongsan(standardizer):
    """Load, clean and standardize Batdongsan raw data."""
    raw_path = DETAILS_CSV_PATH['Batdongsan']
    if not raw_path.exists():
        print("[Warning] Batdongsan details not found.")
        return pd.DataFrame()

    print("[Cleaning] Processing Batdongsan data...")
    df = pd.read_csv(raw_path).drop_duplicates()
    
    # 1. Extraction & Standardization
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
    
    # Housing specific
    df['Số tầng công trình'] = df.apply(BdsCleaner.extract_num_floors, axis=1)
    df['Hình dạng'] = df.apply(BdsCleaner.extract_land_shape, axis=1)
    df['Chất lượng còn lại'] = df.apply(BdsCleaner.estimate_remaining_quality, axis=1)
    df['Đơn giá xây dựng'] = df.apply(BdsCleaner.extract_construction_cost, axis=1)
    df['Mục đích sử dụng đất'] = df.apply(BdsCleaner.extract_land_use, axis=1)
    df['Tổng diện tích sàn'] = df.apply(BdsCleaner.extract_building_area, axis=1)

    # 2. Imputation & Features
    df = DataImputer.fill_missing_width(df)
    df['Kích thước chiều dài (m)'] = df.apply(DataImputer.fill_missing_length, axis=1)
    df['Giá ước tính'] = df.apply(FeatureEngineer.calculate_estimated_price, axis=1)
    df['Lợi thế kinh doanh'] = df.apply(FeatureEngineer.calculate_business_advantage, axis=1)
    df['Đơn giá đất'] = df.apply(FeatureEngineer.calculate_land_unit_price, axis=1)

    # 3. Format Constants
    df['status_const'] = 'Đang rao bán'
    df['contact_const'] = ""
    df['unit_type_const'] = 'đ/m2'
    df['year_const'] = np.nan

    return df.rename(columns={v: k for k, v in FINAL_SCHEMA.items() if v in df.columns})

def process_onehousing(standardizer):
    """Load, clean and standardize OneHousing raw data."""
    raw_path = DETAILS_CSV_PATH['Onehousing']
    if not raw_path.exists():
        print("[Warning] OneHousing details not found.")
        return pd.DataFrame()

    print("[Cleaning] Processing OneHousing data...")
    df_raw = pd.read_csv(raw_path)
    df = OneHousingDataCleaner.clean_onehousing_data(df_raw)
    
    # Apply Standardizer (Address Mapping)
    df['Tỉnh/Thành phố'] = df['Tỉnh/Thành phố'].apply(standardizer.standardize_province)
    df['Thành phố/Quận/Huyện/Thị xã'] = df.apply(standardizer.standardize_district, axis=1)
    df['Xã/Phường/Thị trấn'] = df.apply(standardizer.standardize_ward, axis=1)
    
    # Add coordinates if missing from cleaning script logic
    if 'latitude' not in df.columns: df['latitude'] = np.nan
    if 'longitude' not in df.columns: df['longitude'] = np.nan

    return df.rename(columns={v: k for k, v in FINAL_SCHEMA.items() if v in df.columns})

def run_cleaning_pipeline():
    """Merge cleaned data from both sources and export to final Excel."""
    print("\n=== STARTING CLEANING & MERGING ===")
    standardizer = AddressStandardizer(
        PROVINCES_SQL_PATH, DISTRICTS_SQL_PATH, 
        WARDS_SQL_PATH, STREETS_SQL_PATH
    )

    df_bds = process_batdongsan(standardizer)
    df_oh = process_onehousing(standardizer)

    # Merge
    combined_df = pd.concat([df_bds, df_oh], ignore_index=True)
    
    # Filter columns to schema
    combined_df = combined_df[list(FINAL_SCHEMA.keys())]

    # Drop duplicates across sources
    combined_df.drop_duplicates(subset=['Tỉnh/Thành phố', 'Thành phố/Quận/Huyện/Thị xã', 'Đường phố', 'Giá rao bán/giao dịch', 'Diện tích đất (m2)'], inplace=True)

    # Save Output
    today = datetime.now().strftime('%d.%m.%Y')
    output_filename = f"output/standardized_realestate_{today}.xlsx"
    combined_df.to_excel(output_filename, index=False)
    
    print(f"Final dataset exported: {output_filename} ({len(combined_df)} records)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified Real Estate Scraper & Cleaner")
    parser.add_argument("--mode", choices=["scrape", "clean", "full"], required=True)
    args = parser.parse_args()

    if args.mode == "scrape":
        run_scraping_phase()
    elif args.mode == "clean":
        run_cleaning_pipeline()
    elif args.mode == "full":
        run_scraping_phase()
        run_cleaning_pipeline()