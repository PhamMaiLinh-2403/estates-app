import threading
import signal
import sys
import csv
import queue
import time

from commons.config import * 
from .selenium_manager import *
from commons.utils import * 

# Global stop event for graceful shutdown
stop_event = threading.Event()
interrupt_count = 0


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    global interrupt_count
    interrupt_count += 1
    
    if interrupt_count == 1:
        print("\nInterrupt received. Stopping workers gracefully...")
        stop_event.set()
    else:
        print("\nForce quit! Some data may be lost.")
        sys.exit(1)

signal.signal(signal.SIGINT, signal_handler)

def csv_writer_listener(url_queue: queue.Queue, stop_event: threading.Event):
    """
    A dedicated thread that pulls URLs from the queue and writes them to CSV.
    """
    batch_size = 10 
    buffer = []
    total_saved = 0
    
    # Ensure directory exists
    URLS_CSV_PATH['Batdongsan'].parent.mkdir(parents=True, exist_ok=True)
    file_exists = URLS_CSV_PATH['Batdongsan'].exists()
    
    print(f"[Writer] Started. Saving to {URLS_CSV_PATH['Batdongsan']}")

    with open(URLS_CSV_PATH['Batdongsan'], mode='w') as f:
        f.write('')

    with open(URLS_CSV_PATH['Batdongsan'], mode='a', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['url']) # Header
        
        while not stop_event.is_set() or not url_queue.empty():
            try:
                data = url_queue.get(timeout=1.0)

                if data is None:
                    break
                
                # Data comes in as a list of URLs from a single page
                if isinstance(data, list):
                    buffer.extend([[u] for u in data]) 
                
                url_queue.task_done()

                # Flush if buffer is full
                if len(buffer) >= batch_size:
                    writer.writerows(buffer)
                    total_saved += len(buffer)
                    print(f"[Writer] Saved batch of {len(buffer)}. Total saved: {total_saved}")
                    buffer = [] # Clear buffer
                    f.flush()   # Ensure data hits the disk
            
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[Writer] Error writing to CSV: {e}")

        # Final flush
        if buffer:
            writer.writerows(buffer)
            total_saved += len(buffer)
            print(f"[Writer] Final flush: {len(buffer)}. Total saved: {total_saved}")

def csv_details_writer_listener(data_queue: queue.Queue, stop_event: threading.Event):
    """
    A dedicated thread that pulls Listing Details (dicts) from the queue and writes them to CSV.
    Supports real-time batch writing and dynamic header detection.
    """
    batch_size = 5
    buffer = []
    total_saved = 0
    
    # Define output path for details
    details_output_path = DETAILS_CSV_PATH["Batdongsan"]
    details_output_path.parent.mkdir(parents=True, exist_ok=True)
    
    file_exists = details_output_path.exists()
    print(f"[Writer] Started. Saving details to {details_output_path}")

    with open(details_output_path, mode='w') as f:
        f.write('')

    with open(details_output_path, mode='a', newline='', encoding='utf-8-sig') as f:
        writer = None
        
        while not stop_event.is_set() or not data_queue.empty():
            try:
                data = data_queue.get(timeout=1.0)

                if data is None:
                    break
                
                buffer.append(data)
                data_queue.task_done()

                # Initialize writer with headers from the first record if not exists
                if writer is None and buffer:
                    # If file didn't exist, we write headers. 
                    # If file exists, we assume headers match (or append anyway).
                    fieldnames = list(buffer[0].keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    
                    if not file_exists:
                        writer.writeheader()
                        file_exists = True # Prevent re-writing header

                # Flush if buffer is full
                if len(buffer) >= batch_size and writer:
                    writer.writerows(buffer)
                    total_saved += len(buffer)
                    print(f"[Writer] Saved details batch of {len(buffer)}. Total saved: {total_saved}")
                    buffer = [] 
                    f.flush()
            
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[Writer] Error writing details to CSV: {e}")

        # Final flush
        if buffer and writer:
            writer.writerows(buffer)
            total_saved += len(buffer)
            print(f"[Writer] Final details flush: {len(buffer)}. Total saved: {total_saved}")


def scrape_urls_multithreaded():
    """
    Phase 1: Scrape listing URLs from search pages using multiple workers
    and stream results to CSV immediately.
    """
    global interrupt_count
    interrupt_count = 0  
    stop_event.clear()   
    
    # 1. Setup Queue and Writer
    url_queue = queue.Queue()
    writer_thread = threading.Thread(
        target=csv_writer_listener,
        args=(url_queue, stop_event),
        daemon=False 
    )
    writer_thread.start()

    # 2. Split Work
    page_ranges = split_page_ranges(
        START_PAGE_NUMBER,
        END_PAGE_NUMBER,
        MAX_WORKERS
    )
    
    threads = []
    
    # 3. Start Workers
    for worker_id, (start_page, end_page) in enumerate(page_ranges):
        thread = threading.Thread(
            target=scrape_urls_worker,
            args=(worker_id, SEARCH_PAGE_URL['Batdongsan'], start_page, end_page, stop_event, url_queue),
            daemon=False
        )
        threads.append(thread)
        thread.start()
    
    # 4. Monitor Workers
    _monitor_threads(threads)
    
    # 5. Stop Writer
    print("\nAll workers finished. Stopping writer...")
    url_queue.put(None) 
    writer_thread.join()
    
    if stop_event.is_set():
        print("\nURL collection interrupted by user.")
    else:
        print("\nURL collection completed successfully.")

def scrape_details_multithreaded():
    """
    Phase 2: Read URLs from the CSV generated in Phase 1, scrape details,
    add timestamps, and save to a new CSV in real-time.
    """
    global interrupt_count
    interrupt_count = 0
    stop_event.clear()

    input_csv = URLS_CSV_PATH['Batdongsan']
    
    if not input_csv.exists():
        print(f"Error: Input file {input_csv} not found. Run URL scraping first.")
        return

    # 1. Read URLs from CSV
    print(f"Reading URLs from {input_csv}...")
    urls_to_scrape = []
    try:
        with open(input_csv, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            # Filter out empty rows or None
            urls_to_scrape = [row['url'] for row in reader if row and row.get('url')]
    except Exception as e:
        print(f"Error reading input CSV: {e}")
        return

    total_urls = len(urls_to_scrape)
    print(f"Loaded {total_urls} URLs. Preparing workers...")

    # 2. Setup Queue and Writer
    data_queue = queue.Queue()
    writer_thread = threading.Thread(
        target=csv_details_writer_listener,
        args=(data_queue, stop_event),
        daemon=False
    )
    writer_thread.start()

    # 3. Split Work
    # Using 'chunks' to split the list of URLs evenly among workers
    url_chunks = list(chunks(urls_to_scrape, MAX_WORKERS))
    
    threads = []

    # 4. Start Workers
    for worker_id, url_subset in enumerate(url_chunks):
        if not url_subset: continue
        
        thread = threading.Thread(
            target=scrape_details_worker,
            args=(worker_id, url_subset, stop_event, data_queue),
            daemon=False
        )
        threads.append(thread)
        thread.start()

    # 5. Monitor Workers
    _monitor_threads(threads)

    # 6. Stop Writer
    print("\nAll workers finished. Stopping writer...")
    data_queue.put(None)
    writer_thread.join()

    if stop_event.is_set():
        print("\nDetail scraping interrupted by user.")
    else:
        print("\nDetail scraping completed successfully.")

def _monitor_threads(threads):
    """Helper to join threads and handle interruptions."""
    try:
        while any(t.is_alive() for t in threads):
            for thread in threads:
                thread.join(timeout=0.5)
            
            if stop_event.is_set():
                print("\nWaiting for active workers to finish current task (max 30s)...")
                for thread in threads:
                    thread.join(timeout=30)
                break
                
    except KeyboardInterrupt:
        print("\nMain thread interrupted, signaling workers...")
        stop_event.set()
        for thread in threads:
            thread.join(timeout=30)