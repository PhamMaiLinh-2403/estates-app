import time 
import random
from seleniumbase import Driver
from selenium.common.exceptions import (
    WebDriverException,
    InvalidSessionIdException,
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    SessionNotCreatedException
)

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
    start_delay = worker_id * 2.0
    time.sleep(start_delay)
    driver = None
    scraper = None
    consecutive_failures = 0
    
    try:
        driver = create_stealth_driver(headless=SELENIUM_CONFIG["headless"])
        scraper = Scraper(driver)
        print(f"[Details Worker {worker_id}] Started with {len(url_subset)} URLs")
        
        for idx, url in enumerate(url_subset, 1):
            if circuit_breaker.should_stop():
                print(f"[Details Worker {worker_id}] Circuit breaker triggered, stopping")
                break

            # Periodic driver refresh to prevent memory leaks and slowdown
            if idx % DRIVER_REFRESH_INTERVAL == 0:
                print(f"[Details Worker {worker_id}] Periodic driver refresh at item {idx}")
                try:
                    driver.quit()
                    driver = create_stealth_driver(headless=SELENIUM_CONFIG["headless"])
                    scraper = Scraper(driver)
                except Exception as e:
                    print(f"[Details Worker {worker_id}] Failed to refresh driver: {e}")
                    circuit_breaker.record_failure(f"DriverRefresh_{type(e).__name__}")

            try:
                data = scraper.scrape_listing_details(url)
                data_queue.put(data)
                circuit_breaker.record_success()
                consecutive_failures = 0
                
                if idx % 10 == 0:
                    print(f"[Details Worker {worker_id}] Progress: {idx}/{len(url_subset)}")

            except (UnicodeDecodeError, UnicodeEncodeError) as e:
                """
                ENCODING ERRORS: These are item-specific, not system errors.
                - Log the error
                - Skip this item
                - Continue to next
                - Don't count toward consecutive failures for driver recreation
                """
                print(f"[Details Worker {worker_id}] Encoding error on item {idx} ({url}): {e}")
                circuit_breaker.record_failure(f"Encoding_{type(e).__name__}")

            except (InvalidSessionIdException, SessionNotCreatedException, WebDriverException) as e:
                """
                WEBDRIVER ERRORS: Driver session issues.
                - Log the error
                - Count toward consecutive failures
                - Recreate driver if too many consecutive failures
                """
                print(f"[Details Worker {worker_id}] WebDriver error on item {idx} ({url}): {type(e).__name__}")
                consecutive_failures += 1
                circuit_breaker.record_failure(f"WebDriver_{type(e).__name__}")
                
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    print(f"[Details Worker {worker_id}] {consecutive_failures} consecutive failures, recreating driver...")
                    try:
                        if driver:
                            driver.quit()
                    except:
                        pass
                    
                    try:
                        driver = create_stealth_driver(headless=SELENIUM_CONFIG["headless"])
                        scraper = Scraper(driver)
                        consecutive_failures = 0
                        print(f"[Details Worker {worker_id}] Driver recreated successfully")
                    except Exception as recreate_error:
                        print(f"[Details Worker {worker_id}] Failed to recreate driver: {recreate_error}")
                        circuit_breaker.record_failure(f"DriverRecreation_{type(recreate_error).__name__}")
                        break
                        
            except (TimeoutException, StaleElementReferenceException, NoSuchElementException) as e:
                """
                SELENIUM ELEMENT ERRORS: Usually transient.
                - Already retried 3 times by @retry decorator
                - Log and skip
                - Don't recreate driver
                """
                print(f"[Details Worker {worker_id}] Element error on item {idx} after retries ({url}): {type(e).__name__}")
                circuit_breaker.record_failure(f"Element_{type(e).__name__}")
                consecutive_failures += 1
                
            except (ConnectionRefusedError, MemoryError) as e:
                """
                CRITICAL SYSTEM ERRORS: Stop immediately.
                """
                print(f"[Details Worker {worker_id}] CRITICAL system error: {e}")
                circuit_breaker.record_failure(f"Critical_{type(e).__name__}")
                break
                
            except Exception as e:
                """
                UNEXPECTED ERRORS: Log with full details.
                """
                print(f"[Details Worker {worker_id}] Unexpected error on item {idx} ({url}): {type(e).__name__}: {e}")
                circuit_breaker.record_failure(f"Unexpected_{type(e).__name__}")
                consecutive_failures += 1
            
            # Stagger logic
            if SCRAPING_DETAILS_CONFIG["stagger_mode"] == "random":
                sleep_time = random.uniform(
                    SCRAPING_DETAILS_CONFIG["stagger_step_sec"], 
                    SCRAPING_DETAILS_CONFIG["stagger_max_sec"]
                )
                time.sleep(sleep_time)
        
        print(f"[Details Worker {worker_id}] Completed {idx}/{len(url_subset)} URLs")
        
    except Exception as e:
        print(f"[Details Worker {worker_id}] Fatal initialization error: {type(e).__name__}: {e}")
        circuit_breaker.record_failure(f"Fatal_{type(e).__name__}")
        
    finally:
        if driver:
            try:
                driver.quit()
                print(f"[Details Worker {worker_id}] Driver cleaned up")
            except Exception as e:
                print(f"[Details Worker {worker_id}] Error during cleanup: {e}")