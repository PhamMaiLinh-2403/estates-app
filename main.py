import argparse
import pandas as pd
import os
from datetime import datetime
import traceback

from commons.config import *
from commons.utils import * 
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
    clean_raw,
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
            print(f'Adding raw Batdongsan data to the database at {datetime.now()}...')
            DatabaseManager.add_row_to_table(DETAILS_CSV_PATH['Batdongsan'], "bds_raw")
            print(f'Finished adding raw data of Batdongsan at {datetime.now()}!')

        if DETAILS_CSV_PATH['Onehousing'].exists():
            print(f'Adding raw Onehousing data to the database at {datetime.now()}...')
            DatabaseManager.add_row_to_table(DETAILS_CSV_PATH['Onehousing'], "onehousing_raw", clean_raw)
            print(f'Finished adding raw data of Onehousing at {datetime.now()}!')

    except Exception as e:
        print(f"Error syncing raw data: {e}")

    # Clean data
    try:
        print(f'Cleaning Batdongsan data at {datetime.now()}...')
        df_bds_clean = process_batdongsan_data()

        if not df_bds_clean.empty:
            df_bds_clean['Web'] = 'Batdongsan'

        print(f'Finished cleaning Batdongsan data at {datetime.now()}!')

        print(f'Cleaning Onehousing data at {datetime.now()}...')
        df_oh_clean = process_onehousing_data()
        
        if not df_oh_clean.empty:
            df_oh_clean['Web'] = 'Onehousing'
            df_oh_clean['Thời điểm giao dịch/rao bán'] = '20/01/2026' # datetime.now().strftime("%d/%m/%Y")
            
        print(f'Finished cleaning Onehousing data at {datetime.now()}!')

        df_cleaned = pd.concat([df_bds_clean, df_oh_clean], axis=0)
        
        if not df_cleaned.empty:
            df_cleaned.to_csv(CLEANED_CSV_PATH, index=False)
            DatabaseManager.add_row_to_table(CLEANED_CSV_PATH, "cleaned")
            print(f"Finished adding data to the database at {datetime.now()}!")
        else:
            print("No cleaned data produced.")
            
    except Exception as e:
        print(f"Cleaning error: {e}")
        traceback.print_exc()


def run_pipeline_safe(resume=False, target_phase="full"):
    """
    Runs the pipeline with fault tolerance.
    target_phase options: "full", "urls", "details"
    """
    # Before starting (especially on retry), kill any zombies to avoid resource exhaustion.
    # print("Performing Chrome cleanup...")
    # kill_system_chrome_processes()
    # clean_scraper_temp_dirs()

    state_manager = PipelineStateManager()
    circuit_breaker = CircuitBreaker() 

    if not resume and target_phase in ["full", "urls"]:
        print("Starting New Pipeline Run (Cleanup initiated)...")
        cleanup_intermediate_files()    # Only clean up old files on a new run 
        state_manager.reset_for_new_run()
    else:
        print(f"Resuming Pipeline (Phase: {target_phase})...")

    try:
        if target_phase in ["full", "urls"]:
            # 1. Batdongsan
            print("START SCRAPING URLS FOR BATDONGSAN...")
            scrape_bds_urls(circuit_breaker, state_manager)
            print('Finished scraping URLs for Batdongsan.')

            # 2. Onehousing
            print("START SCRAPING URLS FOR ONEHOUSING...")
            scrape_oh_urls(circuit_breaker, state_manager)
            print('Finished scraping URLs for Onehousing.')
            
            if target_phase == "urls":
                print("URLs Collection Completed.")
                state_manager.set_completed()
                return True, "URLs Collected"

        if target_phase in ["full", "details"]:
            # 1. Batdongsan
            print("START SCRAPING DETAILS FOR BATDONGSAN...")
            scrape_bds_details(circuit_breaker) 
            print('Finished scraping details for Batdongsan.')

            # 2. Onehousing
            print("START SCRAPING DETAILS FOR ONEHOUSING...")
            scrape_oh_details(circuit_breaker)
            print('Finished scraping details for Onehousing.')

            print('FINISHED SCRAPING ALL DATA. START CLEANING...')
            # 3. Clean 
            clean()
            print('FINISHED CLEANING.')

            state_manager.set_completed()
        
        return True, "Completed"

    except PipelineStopException as e:
        print(f"\nPipeline Stopped: {e}")
        print("Attempting to process and save partial data to DB...")

        try:
            clean()
            print("Partial data saved successfully.")
        except Exception as clean_err:
            print(f"Warning: Failed to save partial data: {clean_err}")

        kill_system_chrome_processes()
        state_manager.set_suspended()
        return False, str(e)

    except Exception as e:
        print(f"\nUnexpected Error: {e}")
        traceback.print_exc()
        print("[System] Attempting to process and save partial data to DB...")

        try:
            clean()
            print("[System] Partial data saved successfully.")
        except Exception as clean_err:
            print(f"[System] Warning: Failed to save partial data: {clean_err}")

        kill_system_chrome_processes()
        state_manager.set_suspended()
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