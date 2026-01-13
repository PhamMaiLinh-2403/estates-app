import threading
import signal
import sys
import csv
import queue

from commons.config import * 
from database.database_manager import DatabaseManager
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
    
    URLS_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    file_exists = URLS_CSV_PATH.exists()
    
    print(f"[Writer] Started. Saving to {URLS_CSV_PATH}")

    with open(URLS_CSV_PATH, mode='a', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['url']) # Header
        
        while not stop_event.is_set() or not url_queue.empty():
            try:
                # Wait for data with a timeout so we can check stop_event occasionally
                data = url_queue.get(timeout=1.0)
                
                # 'None' is the signal that all workers are done
                if data is None:
                    break
                
                # Data comes in as a list of URLs from a single page
                if isinstance(data, list):
                    buffer.extend([[u] for u in data]) # Format for csv.writer.writerows
                
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

def scrape_urls_multithreaded(db_manager: DatabaseManager):
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
        daemon=False # Wait for writer to finish flushing
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
    try:
        while any(t.is_alive() for t in threads):
            for thread in threads:
                thread.join(timeout=0.5)
            
            # If user interrupted via Ctrl+C (stop_event set in signal_handler)
            if stop_event.is_set():
                print("\nWaiting for active workers to finish current page (max 30s)...")
                for thread in threads:
                    thread.join(timeout=30)
                break
                
    except KeyboardInterrupt:
        print("\nMain thread interrupted, signaling workers...")
        stop_event.set()
        for thread in threads:
            thread.join(timeout=30)
    
    # 5. Stop Writer
    print("\nAll workers finished. Stopping writer...")
    url_queue.put(None) # Sentinel value to tell writer to exit
    writer_thread.join()
    
    if stop_event.is_set():
        print("\nURL collection interrupted by user.")
    else:
        print("\nURL collection completed successfully.")