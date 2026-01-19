import argparse
import pandas as pd
import os
from datetime import datetime

from commons.config import *
from database.database_manager import DatabaseManager
from database.schema import *

from Batdongsan.orchestrator import (
    scrape_urls_multithreaded, 
    scrape_details_multithreaded,
    process_batdongsan_data 
)

from Onehousing.orchestrator import (
    scrape_onehousing_urls, 
    scrape_onehousing_details,
    process_onehousing_data
)

def clean():
    print("\n--- PHASE 2: CLEANING & DATABASE SYNC ---")
    if not os.path.exists(DATABASE_DIR):
        DatabaseManager.create_db() 

    # Add raw data
    print("Adding raw data...")
    DatabaseManager.add_row_to_table(DETAILS_CSV_PATH['Batdongsan'], "bds_raw")
    DatabaseManager.add_row_to_table(DETAILS_CSV_PATH['Onehousing'], "onehousing_raw")
    print("Finished adding raw data!")

    # Clean data
    print("Cleaning data...")
    df_bds_clean = process_batdongsan_data()
    df_bds_clean['Web'] = 'Batdongsan'
    df_oh_clean = process_onehousing_data()
    df_oh_clean['Web'] = 'Onehousing'
    df_oh_clean['Thời điểm giao dịch/rao bán'] = datetime.now().strftime("%d/%m/%Y")

    print(f'BDS: {df_bds_clean.columns}')
    print(f'OH: {df_oh_clean.columns}')

    df_cleaned = pd.concat([df_bds_clean, df_oh_clean], axis=0)
    print(f"Batdongsan original shape: {df_bds_clean.shape}")
    print(f"Onehousing original shape: {df_oh_clean.shape}")
    print(f'Final shape: {df_cleaned.shape}')
    print(f'Columns: {df_cleaned.columns}')
    df_cleaned.to_csv(CLEANED_CSV_PATH, index=False)
    DatabaseManager.add_row_to_table(CLEANED_CSV_PATH, "cleaned")
    print("Finished cleaning data!")


def run_pipeline():
    scrape_urls_multithreaded()
    scrape_details_multithreaded()
    scrape_onehousing_urls()
    scrape_onehousing_details()
    clean()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vietnamese Real Estate Pipeline")
    parser.add_argument("--mode", choices=["full", "scrape", "clean"], default="full")
    args = parser.parse_args()

    if args.mode == "full":
        run_pipeline()
    elif args.mode == "scrape":
        scrape_urls_multithreaded()
        scrape_details_multithreaded()
        scrape_onehousing_urls()
        scrape_onehousing_details()
    elif args.mode == "clean":
        clean() 