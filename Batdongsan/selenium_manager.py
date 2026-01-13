import time 
import random
import threading
import queue 
from seleniumbase import Driver

from commons.config import *
from commons.utils import * 
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
                       end_page: int, stop_event: threading.Event, 
                       url_queue: queue.Queue) -> None: 
    """
    Worker to scrape a range of pagination pages and push results to a queue.
    """
    time.sleep(worker_id * 2.0) 
    
    print(f"[Worker {worker_id}] Starting URL scrape for pages {start_page} to {end_page}...")
    
    driver = create_stealth_driver(headless=SELENIUM_CONFIG["headless"])
    scraper = Scraper(driver)

    try:
        for i in range(start_page, end_page + 1):
            if stop_event.is_set():
                print(f"[Worker {worker_id}] Stop event received. Exiting...")
                break

            current_url = build_page_url(search_page_url, i)
            new_urls = scraper.scrape_single_page(current_url)
            
            if new_urls:
                # Push found URLs to the queue
                url_queue.put(new_urls)
                print(f"[Worker {worker_id}] Page {i}: Found {len(new_urls)} URLs. Pushed to queue.")
            else:
                print(f"[Worker {worker_id}] Page {i}: No URLs found.")

    except Exception as e:
        print(f"[Worker {worker_id}] Critical Error: {e}")
    finally:
        driver.quit()

def scrape_details_worker(worker_id: int, url_subset: list[str], 
                         stop_event: threading.Event, data_queue: queue.Queue):
    """
    Worker that scrapes listing details and pushes them to the queue.
    """
    base = SCRAPING_DETAILS_CONFIG.get("stagger_step_sec", 2.0)
    start_delay = worker_id * base
    print(f"[Worker {worker_id}] Sleeping {start_delay:.1f}s before start.")
    time.sleep(start_delay)

    driver = None
    try:
        driver = create_stealth_driver(headless=SELENIUM_CONFIG["headless"])
        scraper = Scraper(driver)
        
        print(f"[Worker {worker_id}] Started. Processing {len(url_subset)} URLs.")

        for idx, url in enumerate(url_subset, 1):
            if stop_event.is_set():
                print(f"[Worker {worker_id}] Stop event detected. Exiting...")
                break

            print(f"[Worker {worker_id}] {idx}/{len(url_subset)} → {url}")
            
            try:
                data = scraper.scrape_listing_details(url)

                if data:
                    # Add scraping timestamp (Unix epoch seconds)
                    data['scraping_time'] = int(time.time())
                    data_queue.put(data)

            except Exception as e:
                print(f"[Worker {worker_id}] Error scraping {url}: {e}")
                continue
            
            # Sleep between requests
            if SCRAPING_DETAILS_CONFIG["stagger_mode"] == "random":
                delay = random.uniform(
                    SCRAPING_DETAILS_CONFIG["stagger_step_sec"],
                    SCRAPING_DETAILS_CONFIG["stagger_max_sec"],
                )
                time.sleep(delay)

    except KeyboardInterrupt:
        print(f"[Worker {worker_id}] Keyboard interrupt received.")
        
    except Exception as e:
        print(f"[Worker {worker_id}] Critical error: {e}")
        
    finally:
        if driver:
            driver.quit()