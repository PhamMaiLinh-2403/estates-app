import argparse
import pandas as pd
import os
from datetime import datetime
import traceback

from commons.config import *
from commons.state_manager import PipelineStateManager, CircuitBreaker, PipelineStopException
from database.database_manager import DatabaseManager
from database.schema import *

from Batdongsan.orchestrator import (
    scrape_urls_multithreaded as scrape_bds_urls, 
    scrape_details_multithreaded as scrape_bds_details,
    process_batdongsan_data 
)

from Onehousing.orchestrator import (
    scrape_onehousing_urls as scrape_oh_urls, 
    scrape_onehousing_details as scrape_oh_details,
    process_onehousing_data
)

def cleanup_intermediate_files():
    """
    Deletes existing CSV files to prevent file bloat and data mixing 
    before a new run. Preserves the SQLite database.
    """
    print("Cleaning up old CSV files...")
    
    # Collect all CSV paths from config
    files_to_delete = []
    
    # Add URL CSVs
    if isinstance(URLS_CSV_PATH, dict):
        files_to_delete.extend(URLS_CSV_PATH.values())
    
    # Add Details CSVs
    if isinstance(DETAILS_CSV_PATH, dict):
        files_to_delete.extend(DETAILS_CSV_PATH.values())
        
    # Add Final Cleaned CSV
    files_to_delete.append(CLEANED_CSV_PATH)
    
    for file_path in files_to_delete:
        try:
            if file_path.exists():
                os.remove(file_path)
                print(f"Deleted: {file_path}")
        except Exception as e:
            print(f"Warning: Could not delete {file_path}. Reason: {e}")

def clean():
    if not os.path.exists(DATABASE_DIR):
        DatabaseManager.create_db() 

    # Add raw data (Ignore duplicates handled by SQL)
    try:
        if DETAILS_CSV_PATH['Batdongsan'].exists():
            DatabaseManager.add_row_to_table(DETAILS_CSV_PATH['Batdongsan'], "bds_raw")
        if DETAILS_CSV_PATH['Onehousing'].exists():
            DatabaseManager.add_row_to_table(DETAILS_CSV_PATH['Onehousing'], "onehousing_raw")
    except Exception as e:
        print(f"Error syncing raw data: {e}")

    # Clean data
    try:
        df_bds_clean = process_batdongsan_data()
        if not df_bds_clean.empty:
            df_bds_clean['Web'] = 'Batdongsan'
        
        df_oh_clean = process_onehousing_data()
        if not df_oh_clean.empty:
            df_oh_clean['Web'] = 'Onehousing'
            df_oh_clean['Thời điểm giao dịch/rao bán'] = datetime.now().strftime("%d/%m/%Y")

        df_cleaned = pd.concat([df_bds_clean, df_oh_clean], axis=0)
        
        if not df_cleaned.empty:
            df_cleaned.to_csv(CLEANED_CSV_PATH, index=False)
            DatabaseManager.add_row_to_table(CLEANED_CSV_PATH, "cleaned")
            print("Finished cleaning data!")
        else:
            print("No cleaned data produced.")
            
    except Exception as e:
        print(f"Cleaning error: {e}")
        traceback.print_exc()


def run_pipeline_safe(resume=False):
    """
    Runs the pipeline with fault tolerance.
    """
    state_manager = PipelineStateManager()
    circuit_breaker = CircuitBreaker() 

    if not resume: # Nếu resume == False
        print("Starting New Pipeline Run...")
        cleanup_intermediate_files() 
        state_manager.reset_for_new_run()
    else:
        print("Resuming Pipeline...")

    try:
        # 1. Batdongsan
        print("START SCRAPING URLS FOR BATDONGSAN...")
        scrape_bds_urls(circuit_breaker, state_manager)
        print('Finished scraping URLs for Batdongsan.')
        print("START SCRAPING DETAILS FOR BATDONGSAN...")
        scrape_bds_details(circuit_breaker) 
        print('Finished scraping details for Batdongsan.')

        # 2. Onehousing
        print("START SCRAPING URLS FOR ONEHOUSING...")
        scrape_oh_urls(circuit_breaker, state_manager)
        print('Finished scraping URLs for Onehousing.')
        print("START SCRAPING DETAILS FOR ONEHOUSING...")
        scrape_oh_details(circuit_breaker)
        print('Finished scraping details for Onehousing.')

        print('FINISHED SCRAPING ALL DATA. START CLEANING...')
        # 3. Clean (Only if scraping survived)
        clean()
        print('FINISHED CLEANING.')
        
        return True, "Completed"

    except PipelineStopException as e:
        print(f"\n[!!!] Pipeline Stopped: {e}")
        return False, str(e)
    except Exception as e:
        print(f"\n[!!!] Unexpected Error: {e}")
        traceback.print_exc()
        return False, str(e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vietnamese Real Estate Pipeline")
    parser.add_argument("--mode", choices=["full", "clean"], default="full")
    parser.add_argument("--resume", action="store_true", help="Resume from last state")
    args = parser.parse_args()

    if args.mode == "full":
        success, msg = run_pipeline_safe(resume=args.resume)
        print(f"Pipeline Result: {msg}")
    elif args.mode == "scrape":
        print("START SCRAPING URLS FOR BATDONGSAN...")
        scrape_bds_urls()
        print('Finished scraping URLs for Batdongsan.')
        
        print("START SCRAPING DETAILS FOR BATDONGSAN...")
        scrape_bds_details()
        print('Finished scraping details for Batdongsan.')
        
        print("START SCRAPING URLS FOR ONEHOUSING...")
        scrape_oh_urls()
        print('Finished scraping URLs for Onehousing.')

        print("START SCRAPING DETAILS FOR ONEHOUSING...")
        scrape_oh_details()
        print('Finished scraping details for Onehousing.')

        print("FINISHED SCRAPING ALL DATA.")
    elif args.mode == "clean":
        clean()