import threading
import queue
import os 
import time
import random
import gc 
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
    all_pages = list(range(START_PAGE_NUMBER, ONEHOUSING_END_PAGE_NUMBER + 1))
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

    batch_size = DRIVER_RESTART_INTERVAL
    super_batches = [
        urls_to_scrape[i : i + batch_size] 
        for i in range(0, len(urls_to_scrape), batch_size)
    ]

    for batch_idx, batch_urls in enumerate(super_batches):
        if circuit_breaker.should_stop():
            break
            
        print(f"\n[Onehousing] Processing Batch {batch_idx + 1}/{len(super_batches)} "
              f"({len(batch_urls)} items)")

        url_chunks = list(chunks(batch_urls, MAX_WORKERS))
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
            
        gc.collect()
        print(f"[Onehousing] Batch {batch_idx + 1} completed. Drivers refreshed.")

        if batch_idx < len(super_batches) - 1 and not circuit_breaker.should_stop():
            print(f"[Onehousing] Cooling down for {BATCH_COOLDOWN_SECONDS} seconds...")
            time.sleep(BATCH_COOLDOWN_SECONDS)
    
    data_queue.put(None)
    writer_thread.join()
    
    if circuit_breaker.should_stop():
        raise PipelineStopException(circuit_breaker.stop_reason)

def onehousing_detail_worker(worker_id: int, urls: List[str], data_queue: queue.Queue, cb: CircuitBreaker):
    driver = None
    user_data_dir = None

    try:
        # Unpack tuple
        driver, user_data_dir = create_driver(headless=True)
        for idx, url in enumerate(urls):
            if cb.should_stop(): 
                break

            try:
                data = extract_listing_details(driver, url)
                
                if data:
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
        safe_driver_quit(driver, user_data_dir)

def clean_raw(raw_df):
    # Bỏ các cột null property_url và listing_title 
    raw_df.dropna(subset=['property_url', 'listing_title'], inplace=True)

    # Thay thế các cột có \r\n thành \n
    for col in ['listing_title', 'city', 'district', 'alley_width', 'features', 'property_description']:
        raw_df[col] = raw_df[col].str.replace('\r\n', '\n').str.strip()
    
    # Xử lý các trường hợp property_id bị null
    extracted_ids = raw_df['property_url'].str.split('.').str[-1]
    raw_df['property_id'] = raw_df['property_id'].fillna(extracted_ids)

    # Xử lý các đoạn bị null city, district
    extracted_city = raw_df['listing_title'].str.split(',').str[-1]
    extracted_district = raw_df['listing_title'].str.split(',').str[-2]
    raw_df['city'] = raw_df['city'].fillna(extracted_city)
    raw_df['district'] = raw_df['district'].fillna(extracted_district)

    # Dropna nếu dòng có giá trị NULL trong các cột bắt buộc
    raw_df.dropna(subset=['listing_title', 'total_price', 'city', 'district'], inplace=True)

    # Drop duplicates nếu có
    raw_df.drop_duplicates(inplace=True)

    return raw_df

def process_onehousing_data(raw_path=DETAILS_CSV_PATH['Onehousing'], final_schema=FINAL_SCHEMA):
    "Orchestrate cleaning logic for Onehousing data."
    if not raw_path.exists():
        return pd.DataFrame()

    df_raw = pd.read_csv(raw_path)
    df_raw = clean_raw(df_raw)
    old_size = df_raw.shape[0]
    # df_raw = df_raw.drop_duplicates(subset=['property_id', 'listing_title', 'total_price', 'unit_price', 'city', 'district', 'alley_width', 'features', 'property_description'])
    # print(f'Dropped {old_size - df_raw.shape[0]} duplicated raw rows for Onehousing.')
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