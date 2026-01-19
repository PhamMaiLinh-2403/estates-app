import threading
import queue
import csv

from commons.config import * 
from .selenium_manager import *
from commons.utils import * 
from commons.writers import *


def scrape_urls_multithreaded():
    """Phase 1: Scrape listing URLs from search pages."""
    url_queue = queue.Queue()
    stop_event = threading.Event()
    
    writer_thread = threading.Thread(
        target=csv_url_writer_listener,
        args=(url_queue, stop_event, URLS_CSV_PATH['Batdongsan'])
    )
    writer_thread.start()

    page_ranges = split_page_ranges(START_PAGE_NUMBER, END_PAGE_NUMBER, MAX_WORKERS)
    
    threads = []
    for worker_id, (start_page, end_page) in enumerate(page_ranges):
        thread = threading.Thread(
            target=scrape_urls_worker,
            args=(worker_id, SEARCH_PAGE_URL['Batdongsan'], start_page, end_page, url_queue)
        )
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    url_queue.put(None)
    writer_thread.join()
    
    print("\nURL collection completed.")


def scrape_details_multithreaded():
    """Phase 2: Scrape listing details from collected URLs."""
    input_csv = URLS_CSV_PATH['Batdongsan']
    
    if not input_csv.exists():
        print(f"Error: {input_csv} not found. Run URL scraping first.")
        return

    print(f"Reading URLs from {input_csv}...")
    urls_to_scrape = []
    try:
        with open(input_csv, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            urls_to_scrape = [row['url'] for row in reader if row and row.get('url')]
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    print(f"Loaded {len(urls_to_scrape)} URLs.")

    data_queue = queue.Queue()
    stop_event = threading.Event()
    
    writer_thread = threading.Thread(
        target=csv_details_writer_listener,
        args=(data_queue, stop_event, DETAILS_CSV_PATH["Batdongsan"])
    )
    writer_thread.start()

    url_chunks = list(chunks(urls_to_scrape, MAX_WORKERS))
    
    threads = []
    for worker_id, url_subset in enumerate(url_chunks):
        if not url_subset:
            continue
        
        thread = threading.Thread(
            target=scrape_details_worker,
            args=(worker_id, url_subset, data_queue)
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    data_queue.put(None)
    writer_thread.join()

    print("\nDetail scraping completed.")