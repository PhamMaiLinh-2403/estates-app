import time 
import random
import shutil
import tempfile
import threading
import os 
from seleniumbase import Driver

from commons.config import *
from commons.utils import * 
from .scraping import Scraper

# Global lock to prevent browsers from spawning at the exact same millisecond
# which causes CPU spikes and port binding race conditions.
DRIVER_INIT_LOCK = threading.Lock()

def create_stealth_driver(headless: bool = True):
    """
    Creates a stealth Selenium driver instance using seleniumbase's UC mode.
    """
    # Create a unique temporary directory for this specific thread/driver
    user_data_dir = tempfile.mkdtemp(prefix="bds_scraper_")

    with DRIVER_INIT_LOCK:
        try:
            driver = Driver(
                uc=SELENIUM_CONFIG["uc_driver"],
                headless=headless,
                agent=None,
                user_data_dir=user_data_dir, # Isolate the profile for thread-safe drivers 
                incognito=False, # UC mode works best without explicit incognito flag if user_data_dir is custom
            )

            driver.set_page_load_timeout(30) 
            driver.set_script_timeout(30)
            
            width, height = map(int, SELENIUM_CONFIG["window_size"].split(','))
            driver.set_window_size(width, height)
            
            time.sleep(random.uniform(0.5, 1.5))
            
            return driver, user_data_dir
        except Exception as e:
            # If init fails, clean up the temp dir immediately
            shutil.rmtree(user_data_dir, ignore_errors=True)
            raise e

def scrape_urls_worker(worker_id, url, pages, q, cb, sm):
    """Wrapper to handle specific pages list instead of range."""
    # Spawn workers cách nhau 2s để tránh race conditions 
    time.sleep(worker_id * 2.0)
    
    driver = None
    user_data_dir = None
    
    try:
        driver, user_data_dir = create_stealth_driver(headless=SELENIUM_CONFIG["headless"])
        scraper = Scraper(driver)
        
        for page in pages:
            # Iterate qua một list các pages và lần lượt scrape URLs của bài đăng từ mỗi page 
            if cb.should_stop(): 
                break
            
            try:
                current_url = build_page_url(url, page) # Tạo đường link dẫn đến menu page hiện tại 
                new_urls = scraper.scrape_single_page(current_url)
                
                if new_urls:
                    q.put(new_urls) # Put all the scraped URLs in a queue 
                    cb.record_success()
                    sm.mark_page_complete("Batdongsan", page)
                    print(f"[Worker {worker_id}] Page {page}: Found {len(new_urls)}")
                else:
                    print(f"[Worker {worker_id}] Page {page}: No URLs found")
            except (ConnectionRefusedError, MemoryError) as e:
                cb.record_failure(str(e))
                break
            except Exception as e:
                cb.record_failure(str(e))
    finally:
        safe_driver_quit(driver, user_data_dir)

def scrape_details_worker(worker_id, url_subset, data_queue, circuit_breaker):
    """Worker to scrape listing details."""
    # Flows: Chunk toàn bộ URLs cần phải scrape ra thành các subset, spawn các workers để scrape từng subset và đẩy data vào queue khi scrape xong.
    start_delay = worker_id * 3.0 
    time.sleep(start_delay)

    driver = None
    user_data_dir = None

    try:
        driver, user_data_dir = create_stealth_driver(headless=SELENIUM_CONFIG["headless"])
        scraper = Scraper(driver)
        
        for idx, url in enumerate(url_subset, 1):
            if circuit_breaker.should_stop():
                print(f"[Worker {worker_id}] Stop signal received.")
                break

            try:
                data = scraper.scrape_listing_details(url)

                if data:
                    data_queue.put(data)
                    circuit_breaker.record_success()
                else:
                    circuit_breaker.record_failure("ItemFailed3Times")
                    print(f"[Worker {worker_id}] Failed item {url}")

            except (ConnectionRefusedError, MemoryError) as e:
                circuit_breaker.record_failure(str(e))
                break
            except Exception as e:
                circuit_breaker.record_failure(str(e))
            
            # Stagger logic
            if SCRAPING_DETAILS_CONFIG["stagger_mode"] == "random":
                time.sleep(random.uniform(SCRAPING_DETAILS_CONFIG["stagger_step_sec"], SCRAPING_DETAILS_CONFIG["stagger_max_sec"]))
        
    except Exception as e:
        print(f"[Worker {worker_id}] Fatal: {e}")
    finally:
        safe_driver_quit(driver, user_data_dir)