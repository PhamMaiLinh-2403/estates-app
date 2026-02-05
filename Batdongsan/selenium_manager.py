import time 
import random
from seleniumbase import Driver

from commons.config import *
from commons.utils import * 
from .scraping import Scraper


def create_stealth_driver(headless: bool = True) -> Driver:
    """Creates a stealth Selenium driver instance using seleniumbase's UC mode."""
    driver = Driver(
        uc=SELENIUM_CONFIG["uc_driver"],
        headless=headless,
        agent=None,
    )

    width, height = map(int, SELENIUM_CONFIG["window_size"].split(','))
    driver.set_window_size(width, height)

    return driver


def scrape_urls_worker(worker_id, url, pages, q, cb, sm):
    """Wrapper to handle specific pages list instead of range."""
    time.sleep(worker_id * 1.0)
    driver = create_stealth_driver(headless=SELENIUM_CONFIG["headless"])
    scraper = Scraper(driver)
    
    try:
        for page in pages:
            if cb.should_stop(): 
                break
            
            try:
                current_url = build_page_url(url, page)
                new_urls = scraper.scrape_single_page(current_url)
                
                if new_urls:
                    q.put(new_urls)
                    cb.record_success()
                    sm.mark_page_complete("Batdongsan", page)
                    print(f"[Worker {worker_id}] Page {page}: Found {len(new_urls)}")
                else:
                    print(f"[Worker {worker_id}] Page {page}: No URLs found")
                    # Empty page is not a failure, just empty.
            except (ConnectionRefusedError, MemoryError) as e:
                cb.record_failure(str(e))
                break
            except Exception as e:
                cb.record_failure(str(e))
    finally:
        driver.quit()


def scrape_details_worker(worker_id, url_subset, data_queue, circuit_breaker):
    """Worker with Circuit Breaker integration."""
    start_delay = worker_id * 2.0
    time.sleep(start_delay)

    driver = None
    try:
        driver = create_stealth_driver(headless=SELENIUM_CONFIG["headless"])
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
        if driver: 
            driver.quit()