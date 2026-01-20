import argparse
import pandas as pd
import os
import sqlite3
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
    try:
        DatabaseManager.add_row_to_table(DETAILS_CSV_PATH['Batdongsan'], "bds_raw")
        DatabaseManager.add_row_to_table(DETAILS_CSV_PATH['Onehousing'], "onehousing_raw")
    except sqlite3.OperationalError as e:
        print(f"Error adding raw data: {e}")
        DatabaseManager.create_db()
        DatabaseManager.add_row_to_table(DETAILS_CSV_PATH['Batdongsan'], "bds_raw")
        DatabaseManager.add_row_to_table(DETAILS_CSV_PATH['Onehousing'], "onehousing_raw")
    print("Finished adding raw data!")

    # Clean data
    print("Cleaning data...")
    df_bds_clean = process_batdongsan_data()
    df_bds_clean['Web'] = 'Batdongsan'
    print('Finished cleaning Batdongsan.')
    df_oh_clean = process_onehousing_data()
    df_oh_clean['Web'] = 'Onehousing'
    df_oh_clean['Thời điểm giao dịch/rao bán'] = datetime.now().strftime("%d/%m/%Y")
    print('Finished cleaning Onehousing.')
    
    # print(f'BDS: {df_bds_clean.columns}')
    # print(f'OH: {df_oh_clean.columns}')

    df_cleaned = pd.concat([df_bds_clean, df_oh_clean], axis=0)
    print(f"Batdongsan original shape: {df_bds_clean.shape}")
    print(f"Onehousing original shape: {df_oh_clean.shape}")
    print(f'Final shape: {df_cleaned.shape}')
    print(f'Columns: {df_cleaned.columns}')
    df_cleaned.to_csv(CLEANED_CSV_PATH, index=False)
    DatabaseManager.add_row_to_table(CLEANED_CSV_PATH, "cleaned")
    print("Finished cleaning data!")


def run_pipeline():
    print("\n--- PHASE 1: SCRAPING ---")
    
    print("START SCRAPING URLS FOR BATDONGSAN...")
    scrape_urls_multithreaded()
    print('Finished scraping URLs for Batdongsan.')
    
    print("START SCRAPING DETAILS FOR BATDONGSAN...")
    scrape_details_multithreaded()
    print('Finished scraping details for Batdongsan.')
    
    print("START SCRAPING URLS FOR ONEHOUSING...")
    scrape_onehousing_urls()
    print('Finished scraping URLs for Onehousing.')

    print("START SCRAPING DETAILS FOR ONEHOUSING...")
    scrape_onehousing_details()
    print('Finished scraping details for Onehousing.')
    print("FINISHED SCRAPING ALL DATA.")

    clean()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vietnamese Real Estate Pipeline")
    parser.add_argument("--mode", choices=["full", "scrape", "clean"], default="full")
    args = parser.parse_args()

    if args.mode == "full":
        run_pipeline()
    elif args.mode == "scrape":
        print("START SCRAPING URLS FOR BATDONGSAN...")
        scrape_urls_multithreaded()
        print('Finished scraping URLs for Batdongsan.')
        
        print("START SCRAPING DETAILS FOR BATDONGSAN...")
        scrape_details_multithreaded()
        print('Finished scraping details for Batdongsan.')
        
        print("START SCRAPING URLS FOR ONEHOUSING...")
        scrape_onehousing_urls()
        print('Finished scraping URLs for Onehousing.')

        print("START SCRAPING DETAILS FOR ONEHOUSING...")
        scrape_onehousing_details()
        print('Finished scraping details for Onehousing.')
        print("FINISHED SCRAPING ALL DATA.")
    elif args.mode == "clean":
        clean() 