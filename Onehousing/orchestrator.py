import threading
import queue
import time
import random
import pandas as pd
from typing import List

import warnings
warnings.filterwarnings("ignore")

from commons.config import * 
from commons.utils import * 
from commons.writers import *
from commons.state_manager import PipelineStateManager, CircuitBreaker, PipelineStopException

from .init_browser import *
from .fetch_urls import *
from .fetch_listings import *
from .cleaning import OneHousingDataCleaner 

def scrape_onehousing_urls(circuit_breaker: CircuitBreaker, state_manager: PipelineStateManager):
    """Phase 1: Scrape listing URLs."""
    url_queue = queue.Queue()
    stop_event = threading.Event()
    
    writer_thread = threading.Thread(
        target=csv_url_writer_listener, 
        args=(url_queue, stop_event, URLS_CSV_PATH['Onehousing'])
    )
    writer_thread.start()

    # State Check
    completed = state_manager.get_completed_pages("Onehousing")
    all_pages = list(range(START_PAGE_NUMBER, END_PAGE_NUMBER + 1))
    pending_pages = [p for p in all_pages if p not in completed]

    if not pending_pages:
        print("[Onehousing] All pages scraped.")
        url_queue.put(None)
        writer_thread.join()
        return

    page_chunks = list(chunks(pending_pages, MAX_WORKERS))
    
    threads = []
    print(f"[Orchestrator] Starting {len(page_chunks)} URL workers")
    for i, chunk in enumerate(page_chunks):
        t = threading.Thread(
            target=onehousing_url_worker, 
            args=(i, chunk, url_queue, circuit_breaker, state_manager)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    
    url_queue.put(None)
    writer_thread.join()
    
    if circuit_breaker.should_stop():
        raise PipelineStopException(circuit_breaker.stop_reason)

def onehousing_url_worker(worker_id: int, page_range: List[int], url_queue: queue.Queue, cb: CircuitBreaker, sm: PipelineStateManager):
    for page_num in page_range:
        if cb.should_stop(): 
            break

        try:
            ua = get_random_user_agent()
            page_urls = fetch_search_page(page_num, user_agent=ua)
            
            if page_urls:
                url_queue.put(page_urls)
                cb.record_success()
                sm.mark_page_complete("Onehousing", page_num)
                print(f"[Worker {worker_id}] Page {page_num}: Found {len(page_urls)}")
            else:
                print(f"[Worker {worker_id}] Page {page_num}: No URLs")
            
            time.sleep(random.uniform(1.5, 3.0))
        except (ConnectionRefusedError, MemoryError) as e:
            cb.record_failure(str(e))
            break
        except Exception as e:
            print(f"[Worker {worker_id}] Error page {page_num}: {e}")
            cb.record_failure(str(e))

def scrape_onehousing_details(circuit_breaker: CircuitBreaker):
    """Phase 2: Scrape listing details."""
    state_manager = PipelineStateManager()
    urls_to_scrape = state_manager.get_pending_details_urls("Onehousing")

    if not urls_to_scrape:
        print("[Onehousing] No pending URLs.")
        return

    print(f"[Orchestrator] Processing {len(urls_to_scrape)} URLs")
    
    data_queue = queue.Queue()
    stop_event = threading.Event()
    
    writer_thread = threading.Thread(
        target=csv_details_writer_listener, 
        args=(data_queue, stop_event, DETAILS_CSV_PATH['Onehousing'])
    )
    writer_thread.start()

    url_chunks = list(chunks(urls_to_scrape, MAX_WORKERS))
    threads = []
    
    for i, chunk in enumerate(url_chunks):
        t = threading.Thread(
            target=onehousing_detail_worker, 
            args=(i, chunk, data_queue, circuit_breaker)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    
    data_queue.put(None)
    writer_thread.join()
    
    if circuit_breaker.should_stop():
        raise PipelineStopException(circuit_breaker.stop_reason)

def onehousing_detail_worker(worker_id: int, urls: List[str], data_queue: queue.Queue, cb: CircuitBreaker):
    driver = None
    try:
        driver = create_driver(headless=True)
        for idx, url in enumerate(urls):
            if cb.should_stop(): 
                break

            try:
                data = extract_listing_details(driver, url)
                
                if data:
                    # data['scraping_time'] = int(time.time())
                    data_queue.put(data)
                    cb.record_success()
                else:
                    cb.record_failure("ItemFailed")

                time.sleep(random.uniform(3.0, 6.0))

            except (ConnectionRefusedError, MemoryError) as e:
                cb.record_failure(str(e))
                break
            except Exception as e:
                cb.record_failure(str(e))
                
    except Exception as e:
        print(f"[Worker {worker_id}] Fatal: {e}")
    finally:
        if driver:
            try: 
                driver.quit()
            except: 
                pass

def process_onehousing_data(raw_path=DETAILS_CSV_PATH['Onehousing'], final_schema=FINAL_SCHEMA):
    "Orchestrate cleaning logic for Onehousing data."
    if not raw_path.exists():
        return pd.DataFrame()

    df_raw = pd.read_csv(raw_path)
    old_size = df_raw.shape[0]
    df_raw = df_raw.drop_duplicates(subset=['property_id', 'listing_title', 'total_price', 'unit_price', 'city', 'district', 'alley_width', 'features', 'property_description'])
    print(f'Dropped {old_size - df_raw.shape[0]} duplicated raw rows for Onehousing.')
    df = OneHousingDataCleaner.clean_onehousing_data(df_raw)

    # Rename to standardized schema
    oh_final = df.rename(columns={v: k for k, v in final_schema.items() if v in df.columns})
    oh_final = oh_final[list(final_schema.keys())]

    # # Drop NaN and duplicated values
    na = [
        'Tỉnh/Thành phố',
        'Thành phố/Quận/Huyện/Thị xã',
        'Xã/Phường/Thị trấn',
        'Đường phố',
        'Chi tiết',
        'Nguồn thông tin', 
        'Giá rao bán/giao dịch',
        'Giá ước tính',
        'Số tầng công trình', 
        'Tổng diện tích sàn', 
        'Đơn giá xây dựng',
        'Chất lượng còn lại',
        'Diện tích đất (m2)',
        'Kích thước mặt tiền (m)',
        'Kích thước chiều dài (m)',
        'Số mặt tiền tiếp giáp',
        'Hình dạng',
        'Độ rộng ngõ/ngách nhỏ nhất (m)',
        'Khoảng cách tới trục đường chính (m)',
        'Mục đích sử dụng đất',
        'Tọa độ (vĩ độ)',
        'Tọa độ (kinh độ)'
    ]
    dup = [
        'Tỉnh/Thành phố', 
        'Thành phố/Quận/Huyện/Thị xã', 
        'Xã/Phường/Thị trấn', 
        'Đường phố', 
        'Giá rao bán/giao dịch', 
        'Giá ước tính',
        'Số tầng công trình', 
        'Tổng diện tích sàn', 
        'Đơn giá xây dựng', 
        'Chất lượng còn lại', 
        'Diện tích đất (m2)', 
        'Kích thước mặt tiền (m)', 
        'Kích thước chiều dài (m)', 
        'Số mặt tiền tiếp giáp', 
        'Hình dạng', 
        'Độ rộng ngõ/ngách nhỏ nhất (m)', 
        'Khoảng cách tới trục đường chính (m)', 
        'Mục đích sử dụng đất'
    ]

    old_size = oh_final.shape[0]
    oh_final.drop_duplicates(subset=dup, inplace=True)
    print(f'Dropped {old_size - oh_final.shape[0]} duplicated rows for Onehousing.')

    old_size = oh_final.shape[0]  
    oh_final.dropna(subset=na, inplace=True)
    oh_final.reset_index(drop=True)
    print(f'Dropped {old_size - oh_final.shape[0]} NaN rows for Onehousing.')

    print(f'Final number of rows for Onehousing: {oh_final.shape[0]}')

    return oh_final