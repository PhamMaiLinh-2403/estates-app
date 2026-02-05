import threading
import queue
import pandas as pd 
import numpy as np 
import time 
import gc 

from commons.config import * 
from commons.utils import * 
from commons.writers import *
from commons.config import * 
from commons.state_manager import CircuitBreaker, PipelineStateManager, PipelineStopException

from .selenium_manager import *
from .address_standardizer import AddressStandardizer 
from .cleaning import DataCleaner, DataImputer, FeatureEngineer

def scrape_urls_multithreaded(circuit_breaker: CircuitBreaker, state_manager: PipelineStateManager):
    """Phase 1: Scrape listing URLs."""
    url_queue = queue.Queue()
    stop_event = threading.Event()
    
    writer_thread = threading.Thread(
        target=csv_url_writer_listener,
        args=(url_queue, stop_event, URLS_CSV_PATH['Batdongsan'])
    )
    writer_thread.start()

    # Filter out pages already done
    completed_pages = state_manager.get_completed_pages("Batdongsan")
    all_pages = list(range(START_PAGE_NUMBER, END_PAGE_NUMBER + 1))
    pending_pages = [p for p in all_pages if p not in completed_pages]

    if not pending_pages:
        print("[Batdongsan] All pages already scraped.")
        url_queue.put(None)
        writer_thread.join()
        return

    # Split pending pages among workers
    page_chunks = list(chunks(pending_pages, MAX_WORKERS))
    
    threads = []
    for worker_id, pages in enumerate(page_chunks):
        if not pages: 
            continue
        thread = threading.Thread(
            target=scrape_urls_worker,
            args=(worker_id, SEARCH_PAGE_URL['Batdongsan'], pages, url_queue, circuit_breaker, state_manager)
        )
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    url_queue.put(None)
    writer_thread.join()
    
    if circuit_breaker.should_stop():
        raise PipelineStopException(circuit_breaker.stop_reason)
    
    print("\n[Batdongsan] URL collection completed.")


def scrape_details_multithreaded(circuit_breaker: CircuitBreaker):
    """Phase 2: Scrape details from collected URLs."""
    state_manager = PipelineStateManager()
    
    # Get only URLs that haven't been scraped yet
    urls_to_scrape = state_manager.get_pending_details_urls("Batdongsan")
    
    if not urls_to_scrape:
        print("[Batdongsan] No pending URLs to scrape.")
        return

    print(f"[Batdongsan] Resuming with {len(urls_to_scrape)} URLs.")

    data_queue = queue.Queue()
    stop_event = threading.Event()
    
    writer_thread = threading.Thread(
        target=csv_details_writer_listener,
        args=(data_queue, stop_event, DETAILS_CSV_PATH["Batdongsan"])
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

        print(f"\n[Batdongsan] Processing Batch {batch_idx + 1}/{len(super_batches)} "
              f"({len(batch_urls)} items)")

        # Split this specific batch among workers
        url_chunks = list(chunks(batch_urls, MAX_WORKERS))
        
        threads = []
        for worker_id, url_subset in enumerate(url_chunks):
            if not url_subset: 
                continue
            thread = threading.Thread(
                target=scrape_details_worker,
                args=(worker_id, url_subset, data_queue, circuit_breaker)
            )
            threads.append(thread)
            thread.start()

        # Wait for all threads in this batch to finish (Drivers quit here)
        for thread in threads:
            thread.join()
        
        gc.collect()
        print(f"[Batdongsan] Batch {batch_idx + 1} completed. Drivers refreshed.")

        # Cooldown between superbatches 
        if batch_idx < len(super_batches) - 1 and not circuit_breaker.should_stop():
            print(f"[Batdongsan] Cooling down for {BATCH_COOLDOWN_SECONDS} seconds...")
            time.sleep(BATCH_COOLDOWN_SECONDS)

    data_queue.put(None)
    writer_thread.join()

    if circuit_breaker.should_stop():
        raise PipelineStopException(circuit_breaker.stop_reason)

    print("\n[Batdongsan] Detail scraping completed.")

def process_batdongsan_data(raw_path=DETAILS_CSV_PATH['Batdongsan'], final_schema=FINAL_SCHEMA):
    """Orchestrate cleaning logic for Batdongsan data."""
    if not raw_path.exists():
        print ("Details not found. Please run scraping first.")

    df = pd.read_csv(raw_path)
    old_size = df.shape[0]
    df = df.drop_duplicates(subset=['id','title','short_address','address_parts', 'main_info', 'description', 'other_info'])
    print(f'Dropped {old_size - df.shape[0]} duplicated raw rows for Batdongsan.')
    standardizer = AddressStandardizer(
        PROVINCES_SQL_FILE, DISTRICTS_SQL_FILE, 
        WARDS_SQL_FILE, STREETS_SQL_FILE
    )
    
    # Extraction
    df['Tỉnh/Thành phố'] = df.apply(DataCleaner.extract_city, axis=1).apply(standardizer.standardize_province)
    df['Thành phố/Quận/Huyện/Thị xã'] = df.apply(DataCleaner.extract_district, axis=1)
    df['Thành phố/Quận/Huyện/Thị xã'] = df.apply(standardizer.standardize_district, axis=1)
    df['Xã/Phường/Thị trấn'] = df.apply(DataCleaner.extract_ward, axis=1)
    df['Xã/Phường/Thị trấn'] = df.apply(standardizer.standardize_ward, axis=1)
    
    df['Đường phố'] = df.apply(DataCleaner.extract_street, axis=1)
    df['Chi tiết'] = df.apply(DataCleaner.extract_address_details, axis=1)
    df['Thời điểm giao dịch/rao bán'] = df['main_info'].apply(DataCleaner.extract_published_date)
    df['Giá rao bán/giao dịch'] = df.apply(DataCleaner.extract_price, axis=1)
    df['Số mặt tiền tiếp giáp'] = df.apply(DataCleaner.extract_facade_count, axis=1)
    df['Diện tích đất (m2)'] = df.apply(DataCleaner.extract_total_area, axis=1)
    df['Kích thước mặt tiền (m)'] = df.apply(DataCleaner.extract_width, axis=1)
    df['Độ rộng ngõ/ngách nhỏ nhất (m)'] = df.apply(DataCleaner.extract_adjacent_lane_width, axis=1)
    df['Khoảng cách tới trục đường chính (m)'] = df.apply(DataCleaner.extract_distance_to_the_main_road, axis=1)
    df['description'] = df['description'].apply(DataCleaner.clean_description_text)
    df['Số tầng công trình'] = df.apply(DataCleaner.extract_num_floors, axis=1)
    df['Hình dạng'] = df.apply(DataCleaner.extract_land_shape, axis=1)
    df['Chất lượng còn lại'] = df.apply(DataCleaner.estimate_remaining_quality, axis=1)
    df['Đơn giá xây dựng'] = df.apply(DataCleaner.extract_construction_cost, axis=1)
    df['Mục đích sử dụng đất'] = df.apply(DataCleaner.extract_land_use, axis=1)
    df['Tổng diện tích sàn'] = df.apply(DataCleaner.extract_building_area, axis=1)

    # Imputation & Features
    df = DataImputer.fill_missing_width(df)
    df['Kích thước chiều dài (m)'] = df.apply(DataImputer.fill_missing_length, axis=1)
    df['Giá ước tính'] = df.apply(FeatureEngineer.calculate_estimated_price, axis=1)
    df['Lợi thế kinh doanh'] = df.apply(FeatureEngineer.calculate_business_advantage, axis=1)
    df['Đơn giá đất'] = df.apply(FeatureEngineer.calculate_land_unit_price, axis=1)

    # Constants
    df["Tình trạng giao dịch"] = "Đang rao bán"
    df["Thông tin liên hệ"] = np.nan
    df["Loại đơn giá (đ/m2 hoặc đ/m ngang)"] = "đ/m2"
    df["Năm xây dựng"] = np.nan

    # Rename to standardized schema
    bds_final = df.rename(columns={v: k for k, v in final_schema.items() if v in df.columns})
    bds_final = bds_final[list(final_schema.keys())]

    # Drop NaN and duplicated values
    na = [
        'Tỉnh/Thành phố',
        'Thành phố/Quận/Huyện/Thị xã',
        'Xã/Phường/Thị trấn',
        'Đường phố',
        'Chi tiết',
        'Nguồn thông tin', 
        'Thời điểm giao dịch/rao bán',
        'Giá rao bán/giao dịch',
        'Giá ước tính',
        'Đơn giá đất',
        'Lợi thế kinh doanh',
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
        'Đơn giá đất', 
        'Lợi thế kinh doanh', 
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

    original_size = bds_final.shape[0]
    bds_final.drop_duplicates(subset=dup, inplace=True)
    print(f'Dropped {original_size - bds_final.shape[0]} duplicated rows for Batdongsan.')

    original_size = bds_final.shape[0]  
    bds_final.dropna(subset=na, inplace=True)
    bds_final.reset_index(drop=True)
    print(f'Dropped {original_size - bds_final.shape[0]} NaN rows for Batdongsan.')

    print(f'Final number of rows for Batdongsan: {bds_final.shape[0]}')

    return bds_final