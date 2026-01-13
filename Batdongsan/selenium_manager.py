import time 
import random
import threading
from pathlib import Path
from seleniumbase import Driver

from commons.config import *
from .scraping import Scraper
from database.database_manager import DatabaseManager


def create_stealth_driver(headless: bool = True) -> Driver:
    """
    Creates and returns a "supercharged" Selenium driver instance using seleniumbase's UC mode.
    """
    driver = Driver(
        uc=SELENIUM_CONFIG["uc_driver"],
        headless=headless,
        agent=None,
    )

    width, height = map(int, SELENIUM_CONFIG["window_size"].split(','))
    driver.set_window_size(width, height)

    return driver


def scrape_urls_worker(worker_id: int, search_page_url: str, start_page: int, 
                       end_page: int, stop_event: threading.Event) -> list[str]:
    """
    Worker to scrape a range of pagination pages.
    """
    time.sleep(worker_id * 2.0)
    
    print(f"[Worker {worker_id}] Starting URL scrape for pages {start_page} to {end_page}...")
    
    driver = create_stealth_driver(headless=SELENIUM_CONFIG["headless"])
    scraper = Scraper(driver)
    found_urls = []

    try:
        found_urls = scraper.scrape_listing_urls(search_page_url, start_page, end_page)
        print(f"[Worker {worker_id}] Finished. Found {len(found_urls)} URLs.")
        
    except Exception as e:
        print(f"[Worker {worker_id}] Critical Error: {e}")
    finally:
        driver.quit()

    return found_urls


def scrape_detail_worker(worker_id: int, url_subset: list[str], existing_ids: set[str], 
                        stop_event: threading.Event, db_manager: DatabaseManager, 
                        metadata_id: int) -> dict:
    """
    Worker that scrapes listing details and writes to database in batches.
    Now includes proper error handling and graceful shutdown on CTRL+C.
    
    Returns:
        Dictionary with statistics: {'scraped': X, 'new': Y, 'changed': Z, 'duplicate': W, 'errors': E}
    """
    stats = {'scraped': 0, 'new': 0, 'changed': 0, 'duplicate': 0, 'errors': 0}
    
    base = SCRAPING_DETAILS_CONFIG.get("stagger_step_sec", 2.0)
    start_delay = worker_id * base
    print(f"[Worker {worker_id}] Sleeping {start_delay:.1f}s before start.")
    time.sleep(start_delay)

    driver = None
    try:
        driver = create_stealth_driver(headless=SELENIUM_CONFIG["headless"])
        scraper = Scraper(driver)
        
        batch_size = SCRAPING_DETAILS_CONFIG.get("batch_size", 10)
        batch_buffer = []

        for idx, url in enumerate(url_subset, 1):
            if stop_event.is_set():
                print(f"[Worker {worker_id}] Stop event detected. Flushing remaining batch...")
                break

            print(f"[Worker {worker_id}] {idx}/{len(url_subset)} → {url}")
            
            try:
                data = scraper.scrape_listing_details(url)

                if data:
                    listing_id = str(data.get("id", "")).replace(".0", "")
                    
                    # Skip if already exists in the current session
                    if listing_id in existing_ids:
                        print(f"[Worker {worker_id}] Skipping already-scraped ID: {listing_id}")
                        stats['duplicate'] += 1
                        continue
                    
                    batch_buffer.append(data)
                    stats['scraped'] += 1
                    
                    # Add to existing IDs to prevent duplicates within this session
                    existing_ids.add(listing_id)

            except Exception as e:
                print(f"[Worker {worker_id}] Error scraping {url}: {e}")
                stats['errors'] += 1
                continue

            # Flush batch to database
            if len(batch_buffer) >= batch_size:
                batch_stats = db_manager.insert_raw_listings_batch(batch_buffer, metadata_id)
                stats['new'] += batch_stats['new']
                stats['changed'] += batch_stats['changed']
                stats['duplicate'] += batch_stats['duplicate']
                
                print(f"[Worker {worker_id}] Batch saved: {batch_stats}")
                batch_buffer = []
            
            # Sleep between requests
            if SCRAPING_DETAILS_CONFIG["stagger_mode"] == "random":
                delay = random.uniform(
                    SCRAPING_DETAILS_CONFIG["stagger_step_sec"],
                    SCRAPING_DETAILS_CONFIG["stagger_max_sec"],
                )
                time.sleep(delay)
        
        # Flush remaining items
        if batch_buffer:
            batch_stats = db_manager.insert_raw_listings_batch(batch_buffer, metadata_id)
            stats['new'] += batch_stats['new']
            stats['changed'] += batch_stats['changed']
            stats['duplicate'] += batch_stats['duplicate']
            print(f"[Worker {worker_id}] Final batch saved: {batch_stats}")

    except KeyboardInterrupt:
        print(f"[Worker {worker_id}] Keyboard interrupt received. Saving progress...")
        if batch_buffer:
            batch_stats = db_manager.insert_raw_listings_batch(batch_buffer, metadata_id)
            stats['new'] += batch_stats['new']
            stats['changed'] += batch_stats['changed']
            stats['duplicate'] += batch_stats['duplicate']
        raise
        
    except Exception as e:
        print(f"[Worker {worker_id}] Critical error: {e}")
        # Try to save whatever is in buffer
        if batch_buffer:
            try:
                batch_stats = db_manager.insert_raw_listings_batch(batch_buffer, metadata_id)
                stats['new'] += batch_stats['new']
                stats['changed'] += batch_stats['changed']
                stats['duplicate'] += batch_stats['duplicate']
            except Exception as save_error:
                print(f"[Worker {worker_id}] Failed to save final batch: {save_error}")
        
    finally:
        if driver:
            driver.quit()
        
    print(f"[Worker {worker_id}] Final stats: {stats}")
    return stats