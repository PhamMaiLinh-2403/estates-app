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
        print("\n[INTERRUPT] Ctrl+C detected! Stopping workers gracefully...")
        print("[INTERRUPT] Press Ctrl+C again to force quit (may lose data)")
        stop_event.set()
    else:
        print("\n[FORCE QUIT] Exiting immediately!")
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
        
        while True:
            # Check stop event first
            if stop_event.is_set() and url_queue.empty():
                print("[Writer] Stop event set and queue empty. Exiting.")
                break
                
            try:
                data = url_queue.get(timeout=0.5)  # Shorter timeout
                if data is None:
                    print("[Writer] Received shutdown signal.")
                    break
                
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
    """
    batch_size = 5
    buffer = []
    total_saved = 0
    output_path = DETAILS_CSV_PATH['Onehousing']
    output_path.parent.mkdir(parents=True, exist_ok=True)

    file_is_empty = (
        not output_path.exists()
        or output_path.stat().st_size == 0
    )

    with open(output_path, mode='a', newline='', encoding='utf-8-sig') as f:
        writer = None
        
        while True:
            # Check stop event first
            if stop_event.is_set() and data_queue.empty():
                print("[Writer] Stop event set and queue empty. Exiting.")
                break
                
            try:
                data = data_queue.get(timeout=0.5)  # Shorter timeout
                if data is None:
                    print("[Writer] Received shutdown signal.")
                    break
                
                buffer.append(data)
                data_queue.task_done()

                # Initialize writer dynamically from first record
                if writer is None and buffer:
                    fieldnames = list(buffer[0].keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    if file_is_empty:
                        writer.writeheader()
                        file_is_empty = False

                if len(buffer) >= batch_size and writer:
                    writer.writerows(buffer)
                    total_saved += len(buffer)
                    print(f"[Writer] Saved details batch of {len(buffer)}. Total: {total_saved}")
                    buffer = []
                    f.flush()
                    
            except queue.Empty:
                # Flush buffer when stopping
                if stop_event.is_set() and buffer and writer:
                    writer.writerows(buffer)
                    total_saved += len(buffer)
                    print(f"[Writer] Emergency flush: {len(buffer)}. Total: {total_saved}")
                    buffer = []
                    f.flush()
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
        if stop_event.is_set():
            print(f"[Worker {worker_id}] Stop event detected at page {page_num}. Exiting.")
            break
        
        url = f"{scraper.search_url}page={page_num}"
        try:
            headers = {"User-Agent": scraper._get_random_user_agent()}
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                page_urls = scraper.get_listing_urls_from_page(response.text)
                if page_urls:
                    if stop_event.is_set():
                        print(f"[Worker {worker_id}] Stop event detected before queue push.")
                        break
                        
                    url_queue.put(page_urls)
                    print(f"[Worker {worker_id}] Page {page_num}: Found {len(page_urls)} URLs")
                    
            # Interruptible sleep
            for _ in range(int(random.uniform(1.5, 3.0) * 10)):
                if stop_event.is_set():
                    return
                time.sleep(0.1)
                
        except Exception as e:
            print(f"[Worker {worker_id}] Error on page {page_num}: {e}")


def onehousing_detail_worker(worker_id: int, urls: List[str], data_queue: queue.Queue):
    """
    Worker thread for Phase 2: Detail Extraction using Selenium.
    """
    scraper = OneHousingScraper(db_manager=None)
    scraper.stop_requested = stop_event 
    
    print(f"[Worker {worker_id}] Started. Processing {len(urls)} URLs.")
    
    try:
        for idx, url in enumerate(urls):
            if stop_event.is_set():
                print(f"[Worker {worker_id}] Stop event detected. Processed {idx}/{len(urls)} URLs.")
                break
            
            try:
                data = scraper.extract_listing_details(url)
                if data:
                    if stop_event.is_set():
                        print(f"[Worker {worker_id}] Stop event detected before queue push.")
                        break
                        
                    data['scraping_time'] = int(time.time())
                    data_queue.put(data)
                    print(f"[Worker {worker_id}] Progress: {idx+1}/{len(urls)}")
                
                # Interruptible sleep
                delay = random.uniform(3.0, 6.0)
                for _ in range(int(delay * 10)):
                    if stop_event.is_set():
                        return
                    time.sleep(0.1)
                    
            except Exception as e:
                print(f"[Worker {worker_id}] Error scraping {url}: {e}")
                continue
    finally:
        scraper._close_driver()


# --- Orchestration Entry Points ---

def scrape_onehousing_urls():
    """Phase 1 Orchestrator: Scrape search pages for listing URLs."""
    global interrupt_count
    interrupt_count = 0
    stop_event.clear()
    
    print("\n=== STARTING ONEHOUSING URL COLLECTION ===")
    print("Press Ctrl+C once to stop gracefully")
    print("Press Ctrl+C twice to force quit\n")
    
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

    # Monitor threads
    try:
        while any(t.is_alive() for t in threads):
            if stop_event.is_set():
                print("\n[Monitor] Stop requested. Waiting for workers...")
                for t in threads:
                    t.join(timeout=5)
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        stop_event.set()

    for t in threads:
        t.join(timeout=5)
        
    url_queue.put(None)
    writer_thread.join(timeout=10)
    
    if stop_event.is_set():
        print("\n[STOPPED] URL collection interrupted.")
    else:
        print("\n[COMPLETE] URL collection finished.")


def scrape_onehousing_details():
    """Phase 2 Orchestrator: Scrape details from collected URLs."""
    global interrupt_count
    interrupt_count = 0
    stop_event.clear()
    
    input_file = URLS_CSV_PATH['Onehousing']
    
    if not input_file.exists():
        print(f"[ERROR] Input file {input_file} not found!")
        return

    print("\n=== STARTING ONEHOUSING DETAIL SCRAPING ===")
    print("Press Ctrl+C once to stop gracefully")
    print("Press Ctrl+C twice to force quit\n")

    urls_df = pd.read_csv(input_file, header=None)
    urls = urls_df[0].tolist()

    print(f"[Orchestrator] Total URLs to process: {len(urls)}\n")
    
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
            if stop_event.is_set():
                print("\n[Monitor] Stop requested. Waiting for workers...")
                for t in threads:
                    t.join(timeout=5)
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        stop_event.set()

    for t in threads:
        t.join(timeout=5)
        
    data_queue.put(None)
    writer_thread.join(timeout=10)
    
    if stop_event.is_set():
        print("\n[STOPPED] Detail scraping interrupted.")
    else:
        print("\n[COMPLETE] Detail scraping finished.")