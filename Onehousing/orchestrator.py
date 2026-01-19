import threading
import queue
import time
import random
import pandas as pd
from typing import List

import warnings
warnings.filterwarnings("ignore")

from commons.config import * 
from commons.utils import * 

from .init_browser import *
from .fetch_urls import *
from .fetch_listings import *
from commons.writers import * 


def onehousing_url_worker(worker_id: int, page_range: List[int], url_queue: queue.Queue):
    """Worker thread for Phase 1: URL Collection."""
    for page_num in page_range:
        try:
            ua = get_random_user_agent()
            page_urls = fetch_search_page(page_num, user_agent=ua)
            
            if page_urls:
                url_queue.put(page_urls)
                print(f"[Worker {worker_id}] Page {page_num}: Found {len(page_urls)} URLs")
            else:
                print(f"[Worker {worker_id}] Page {page_num}: No URLs found")
                
            time.sleep(random.uniform(1.5, 3.0))
        except Exception as e:
            print(f"[Worker {worker_id}] Error on page {page_num}: {e}")


def onehousing_detail_worker(worker_id: int, urls: List[str], data_queue: queue.Queue):
    """Worker thread for Phase 2: Detail Extraction."""
    driver = None
    
    try:
        print(f"[Worker {worker_id}] Initializing driver...")
        driver = create_driver(headless=True)
        print(f"[Worker {worker_id}] Processing {len(urls)} URLs")
        
        for idx, url in enumerate(urls):
            try:
                data = extract_listing_details(driver, url)
                if data:
                    data['scraping_time'] = int(time.time())
                    data_queue.put(data)
                    print(f"[Worker {worker_id}] Progress: {idx+1}/{len(urls)}")
                
                time.sleep(random.uniform(3.0, 6.0))
            except Exception as e:
                print(f"[Worker {worker_id}] Error: {e}")
                
    except Exception as e:
        print(f"[Worker {worker_id}] Fatal error: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass


def scrape_onehousing_urls():
    """Phase 1: Scrape listing URLs."""
    url_queue = queue.Queue()
    stop_event = threading.Event()
    
    writer_thread = threading.Thread(
        target=csv_url_writer_listener, 
        args=(url_queue, stop_event, URLS_CSV_PATH['Onehousing'])
    )
    writer_thread.start()

    pages = list(range(START_PAGE_NUMBER, END_PAGE_NUMBER + 1))
    page_chunks = list(chunks(pages, MAX_WORKERS))
    
    threads = []
    print(f"[Orchestrator] Starting {len(page_chunks)} URL workers")
    for i, chunk in enumerate(page_chunks):
        t = threading.Thread(
            target=onehousing_url_worker, 
            args=(i, chunk, url_queue)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    
    url_queue.put(None)
    writer_thread.join()
    
    print("[Orchestrator] URL scraping finished")


def scrape_onehousing_details():
    """Phase 2: Scrape listing details."""
    input_file = URLS_CSV_PATH['Onehousing']
    
    if not input_file.exists():
        print(f"[Error] {input_file} not found")
        return

    try:
        urls_df = pd.read_csv(input_file)
        urls = urls_df['url'].tolist() if 'url' in urls_df.columns else urls_df[0].tolist()
    except Exception as e:
        print(f"[Error] Failed to read CSV: {e}")
        return

    print(f"[Orchestrator] Processing {len(urls)} URLs")
    
    data_queue = queue.Queue()
    stop_event = threading.Event()
    
    writer_thread = threading.Thread(
        target=csv_details_writer_listener, 
        args=(data_queue, stop_event, DETAILS_CSV_PATH['Onehousing'])
    )
    writer_thread.start()

    url_chunks = list(chunks(urls, MAX_WORKERS))
    threads = []
    print(f"[Orchestrator] Starting {len(url_chunks)} detail workers")
    for i, chunk in enumerate(url_chunks):
        t = threading.Thread(
            target=onehousing_detail_worker, 
            args=(i, chunk, data_queue)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    
    data_queue.put(None)
    writer_thread.join()
    
    print("[Orchestrator] Detail scraping finished")