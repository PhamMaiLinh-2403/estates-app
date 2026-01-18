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
        print("\n[INTERRUPT] Ctrl+C detected! Stopping workers gracefully...")
        print("[INTERRUPT] Press Ctrl+C again to force quit (may lose data)")
        stop_event.set()
    else:
        print("\n[FORCE QUIT] Exiting immediately!")
        sys.exit(1)

# Register signal handler
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
            writer.writerow(['url'])
        
        while True:
            # Check stop event first
            if stop_event.is_set() and url_queue.empty():
                print("[Writer] Stop event set and queue empty. Exiting.")
                break
                
            try:
                data = url_queue.get(timeout=0.5)  # Shorter timeout for responsiveness

                if data is None:
                    print("[Writer] Received shutdown signal.")
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
                    buffer = []
                    f.flush()
            
            except queue.Empty:
                # Flush buffer even if not full when stopping
                if stop_event.is_set() and buffer:
                    writer.writerows(buffer)
                    total_saved += len(buffer)
                    print(f"[Writer] Emergency flush: {len(buffer)}. Total saved: {total_saved}")
                    buffer = []
                    f.flush()
                continue
            except Exception as e:
                print(f"[Writer] Error writing to CSV: {e}")

        # Final flush
        if buffer:
            writer.writerows(buffer)
            total_saved += len(buffer)
            print(f"[Writer] Final flush: {len(buffer)}. Total saved: {total_saved}")
            f.flush()


def csv_details_writer_listener(data_queue: queue.Queue, stop_event: threading.Event):
    """
    A dedicated thread that pulls Listing Details (dicts) from the queue and writes them to CSV.
    """
    batch_size = 5
    buffer = []
    total_saved = 0
    
    details_output_path = DETAILS_CSV_PATH["Batdongsan"]
    details_output_path.parent.mkdir(parents=True, exist_ok=True)
    
    file_exists = details_output_path.exists()
    print(f"[Writer] Started. Saving details to {details_output_path}")

    with open(details_output_path, mode='w') as f:
        f.write('')

    with open(details_output_path, mode='a', newline='', encoding='utf-8-sig') as f:
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

                # Initialize writer with headers from the first record
                if writer is None and buffer:
                    fieldnames = list(buffer[0].keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    
                    if not file_exists:
                        writer.writeheader()
                        file_exists = True

                # Flush if buffer is full
                if len(buffer) >= batch_size and writer:
                    writer.writerows(buffer)
                    total_saved += len(buffer)
                    print(f"[Writer] Saved details batch of {len(buffer)}. Total saved: {total_saved}")
                    buffer = []
                    f.flush()
            
            except queue.Empty:
                # Flush buffer when stopping
                if stop_event.is_set() and buffer and writer:
                    writer.writerows(buffer)
                    total_saved += len(buffer)
                    print(f"[Writer] Emergency flush: {len(buffer)}. Total saved: {total_saved}")
                    buffer = []
                    f.flush()
                continue
            except Exception as e:
                print(f"[Writer] Error writing details to CSV: {e}")

        # Final flush
        if buffer and writer:
            writer.writerows(buffer)
            total_saved += len(buffer)
            print(f"[Writer] Final details flush: {len(buffer)}. Total saved: {total_saved}")
            f.flush()


def scrape_urls_multithreaded():
    """
    Phase 1: Scrape listing URLs from search pages using multiple workers
    """
    global interrupt_count
    interrupt_count = 0  
    stop_event.clear()
    
    print("\n=== STARTING URL COLLECTION ===")
    print("Press Ctrl+C once to stop gracefully")
    print("Press Ctrl+C twice to force quit\n")
    
    # Setup Queue and Writer
    url_queue = queue.Queue()
    writer_thread = threading.Thread(
        target=csv_writer_listener,
        args=(url_queue, stop_event),
        daemon=False 
    )
    writer_thread.start()

    # Split Work
    page_ranges = split_page_ranges(
        START_PAGE_NUMBER,
        END_PAGE_NUMBER,
        MAX_WORKERS
    )
    
    threads = []
    
    # Start Workers
    for worker_id, (start_page, end_page) in enumerate(page_ranges):
        thread = threading.Thread(
            target=scrape_urls_worker,
            args=(worker_id, SEARCH_PAGE_URL['Batdongsan'], start_page, end_page, stop_event, url_queue),
            daemon=False
        )
        threads.append(thread)
        thread.start()
    
    # Monitor Workers
    _monitor_threads(threads, "URL collection")
    
    # Stop Writer
    print("\n[Main] All workers finished. Stopping writer...")
    url_queue.put(None)
    writer_thread.join(timeout=10)
    
    if stop_event.is_set():
        print("\n[STOPPED] URL collection interrupted by user.")
    else:
        print("\n[COMPLETE] URL collection completed successfully.")


def scrape_details_multithreaded():
    """
    Phase 2: Read URLs from CSV, scrape details, and save in real-time
    """
    global interrupt_count
    interrupt_count = 0
    stop_event.clear()

    input_csv = URLS_CSV_PATH['Batdongsan']
    
    if not input_csv.exists():
        print(f"[ERROR] Input file {input_csv} not found. Run URL scraping first.")
        return

    print("\n=== STARTING DETAIL SCRAPING ===")
    print("Press Ctrl+C once to stop gracefully")
    print("Press Ctrl+C twice to force quit\n")

    # Read URLs
    print(f"Reading URLs from {input_csv}...")
    urls_to_scrape = []
    try:
        with open(input_csv, mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            urls_to_scrape = [row['url'] for row in reader if row and row.get('url')]
    except Exception as e:
        print(f"[ERROR] reading input CSV: {e}")
        return

    total_urls = len(urls_to_scrape)
    print(f"Loaded {total_urls} URLs. Preparing workers...\n")

    # Setup Queue and Writer
    data_queue = queue.Queue()
    writer_thread = threading.Thread(
        target=csv_details_writer_listener,
        args=(data_queue, stop_event),
        daemon=False
    )
    writer_thread.start()

    # Split Work
    url_chunks = list(chunks(urls_to_scrape, MAX_WORKERS))
    threads = []

    # Start Workers
    for worker_id, url_subset in enumerate(url_chunks):
        if not url_subset:
            continue
        
        thread = threading.Thread(
            target=scrape_details_worker,
            args=(worker_id, url_subset, stop_event, data_queue),
            daemon=False
        )
        threads.append(thread)
        thread.start()

    # Monitor Workers
    _monitor_threads(threads, "Detail scraping")

    # Stop Writer
    print("\n[Main] All workers finished. Stopping writer...")
    data_queue.put(None)
    writer_thread.join(timeout=10)

    if stop_event.is_set():
        print("\n[STOPPED] Detail scraping interrupted by user.")
    else:
        print("\n[COMPLETE] Detail scraping completed successfully.")


def _monitor_threads(threads, task_name="Task"):
    """Helper to join threads and handle interruptions."""
    try:
        while any(t.is_alive() for t in threads):
            if stop_event.is_set():
                print(f"\n[Monitor] Stop requested for {task_name}. Waiting for workers to finish...")
                # Give threads time to finish current work
                for thread in threads:
                    thread.join(timeout=5)
                break
            
            # Check thread status periodically
            time.sleep(0.5)
                
    except KeyboardInterrupt:
        print(f"\n[Monitor] Keyboard interrupt in monitor for {task_name}")
        stop_event.set()
        for thread in threads:
            thread.join(timeout=5)