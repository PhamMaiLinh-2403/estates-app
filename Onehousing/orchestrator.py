import threading
import signal
import sys
import csv
import queue
import time
import random
import pandas as pd
from typing import List

from commons.config import * 
from commons.utils import * 
from .scraping import OneHousingScraper

# Global stop event for graceful shutdown
stop_event = threading.Event()
interrupt_count = 0

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    global interrupt_count
    interrupt_count += 1
    
    if interrupt_count == 1:
        print("\n[Orchestrator] Interrupt received. Stopping workers gracefully...")
        stop_event.set()
    else:
        print("\n[Orchestrator] Force quit! Some data may be lost.")
        sys.exit(1)

signal.signal(signal.SIGINT, signal_handler)

# --- CSV Writers (Listeners) ---

def csv_url_writer_listener(url_queue: queue.Queue, stop_event: threading.Event):
    """Dedicated thread to write scraped URLs to CSV in real-time."""
    output_path = URLS_CSV_PATH['Onehousing']
    output_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = output_path.exists()
    
    total_saved = 0
    print(f"[Writer] URL Writer started. Saving to {output_path}")

    with open(output_path, mode='w') as f:
        f.write('')

    with open(output_path, mode='a', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['url'])
        
        while not stop_event.is_set() or not url_queue.empty():
            try:
                data = url_queue.get(timeout=1.0)
                if data is None: break
                
                if isinstance(data, list):
                    rows = [[u] for u in data]
                    writer.writerows(rows)
                    total_saved += len(rows)
                
                url_queue.task_done()
                f.flush() 
            except queue.Empty:
                continue
    print(f"[Writer] URL Writer finished. Total saved: {total_saved}")

def csv_details_writer_listener(data_queue: queue.Queue, stop_event: threading.Event):
    """
    Dedicated thread that pulls Listing Details from the queue and writes to CSV.
    Uses dynamic header detection to include the 'scraping_time' field.
    """
    batch_size = 5
    buffer = []
    total_saved = 0
    output_path = DETAILS_CSV_PATH['Onehousing']
    output_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = output_path.exists()

    file_is_empty = (
        not output_path.exists()
        or output_path.stat().st_size == 0
    )

    # with open(output_path, mode='w') as f:
    #     f.write('')

    with open(output_path, mode='a', newline='', encoding='utf-8-sig') as f:
        writer = None
        while not stop_event.is_set() or not data_queue.empty():
            try:
                data = data_queue.get(timeout=1.0)
                if data is None: break
                
                buffer.append(data)
                data_queue.task_done()

                # Initialize writer and fieldnames dynamically from the first record
                if writer is None and buffer:
                    fieldnames = list(buffer[0].keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    if file_is_empty:
                        writer.writeheader()
                        file_is_empty = False
                    # if not file_exists:
                    #     writer.writeheader()
                    #     file_exists = True

                if len(buffer) >= batch_size and writer:
                    writer.writerows(buffer)
                    total_saved += len(buffer)
                    print(f"[Writer] Saved details batch of {len(buffer)}. Total: {total_saved}")
                    buffer = []
                    f.flush()
            except queue.Empty:
                continue
        
        # Final flush for remaining items
        if buffer and writer:
            writer.writerows(buffer)
            total_saved += len(buffer)
            f.flush()
    print(f"[Writer] Details Writer finished. Total details saved: {total_saved}")

# --- Worker Functions ---

def onehousing_url_worker(worker_id: int, page_range: List[int], url_queue: queue.Queue):
    """Worker thread for Phase 1: URL Collection using requests."""
    scraper = OneHousingScraper(db_manager=None) 
    import requests
    
    for page_num in page_range:
        if stop_event.is_set(): break
        
        url = f"{scraper.search_url}page={page_num}"
        try:
            headers = {"User-Agent": scraper._get_random_user_agent()}
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                page_urls = scraper.get_listing_urls_from_page(response.text)
                if page_urls:
                    url_queue.put(page_urls)
                    print(f"[Worker {worker_id}] Page {page_num}: Found {len(page_urls)} URLs")
            time.sleep(random.uniform(1.5, 3.0))
        except Exception as e:
            print(f"[Worker {worker_id}] Error on page {page_num}: {e}")

def onehousing_detail_worker(worker_id: int, urls: List[str], data_queue: queue.Queue):
    """
    Worker thread for Phase 2: Detail Extraction using Selenium.
    Now includes 'scraping_time' timestamp for each record.
    """
    scraper = OneHousingScraper(db_manager=None)
    scraper.stop_requested = stop_event 
    
    print(f"[Worker {worker_id}] Started. Processing {len(urls)} URLs.")
    
    try:
        for idx, url in enumerate(urls):
            if stop_event.is_set(): break
            
            try:
                data = scraper.extract_listing_details(url)
                if data:
                    # FIX: Add scraping timestamp similar to pipeline 1
                    data['scraping_time'] = int(time.time())
                    data_queue.put(data)
                    print(f"[Worker {worker_id}] Progress: {idx+1}/{len(urls)}")
                
                # Randomized delay to mimic human behavior
                time.sleep(random.uniform(3.0, 6.0))
            except Exception as e:
                print(f"[Worker {worker_id}] Error scraping {url}: {e}")
                continue
    finally:
        scraper._close_driver()

# --- Orchestration Entry Points ---

def scrape_onehousing_urls():
    """Phase 1 Orchestrator: Scrape search pages for listing URLs."""
    stop_event.clear()
    url_queue = queue.Queue()
    
    writer_thread = threading.Thread(target=csv_url_writer_listener, args=(url_queue, stop_event))
    writer_thread.start()

    pages = list(range(START_PAGE_NUMBER, END_PAGE_NUMBER + 1))
    page_chunks = list(chunks(pages, MAX_WORKERS))
    
    threads = []
    for i, chunk in enumerate(page_chunks):
        t = threading.Thread(target=onehousing_url_worker, args=(i, chunk, url_queue))
        threads.append(t)
        t.start()

    for t in threads: t.join()
    url_queue.put(None)
    writer_thread.join()

def scrape_onehousing_details():
    """Phase 2 Orchestrator: Scrape details from collected URLs with real-time saving."""
    stop_event.clear()
    input_file = URLS_CSV_PATH['Onehousing']
    
    if not input_file.exists():
        print(f"[Error] Input file {input_file} not found!")
        return

    urls_df = pd.read_csv(input_file, header=None)
    urls = urls_df[0].tolist()

    # with open(input_file, mode='r', encoding='utf-8-sig') as f:
    #     reader = csv.DictReader(f)
    #     urls = [row['url'] for row in reader if row.get('url')]

    print(f"[Orchestrator] Total URLs to process: {len(urls)}")
    data_queue = queue.Queue()
    writer_thread = threading.Thread(target=csv_details_writer_listener, args=(data_queue, stop_event))
    writer_thread.start()

    url_chunks = list(chunks(urls, MAX_WORKERS))
    threads = []
    for i, chunk in enumerate(url_chunks):
        t = threading.Thread(target=onehousing_detail_worker, args=(i, chunk, data_queue))
        threads.append(t)
        t.start()

    # Monitor threads
    try:
        while any(t.is_alive() for t in threads):
            time.sleep(1)
            if stop_event.is_set(): break
    except KeyboardInterrupt:
        stop_event.set()

    for t in threads: t.join(timeout=30)
    data_queue.put(None)
    writer_thread.join()
    print("[Orchestrator] OneHousing Detail Scraping Pipeline Finished.")