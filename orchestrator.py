import pandas as pd
import threading
import signal
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from commons.config import *
from Batdongsan.selenium_manager import *
from Onehousing.scraping import OneHousingScraper
from database.database_manager import DatabaseManager
from Batdongsan.cleaning import DataCleaner, DataImputer, FeatureEngineer
from Onehousing.cleaning import OneHousingDataCleaner
from Batdongsan.address_standardizer import AddressStandardizer


class ScrapingPipeline:
    """
    Orchestrates the complete scraping and data processing pipeline for both
    Batdongsan.com.vn and OneHousing.vn websites.
    """
    
    def __init__(self, db_path: str = "output/real_estate.db"):
        self.db_manager = DatabaseManager(db_path)
        self.stop_event = threading.Event()
        self.address_standardizer = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle CTRL+C gracefully."""
        print("Shutdown signal received. Finishing current batch...")
        self.stop_event.set()
    
    # ===== STEP 1: SCRAPE URLs =====
    
    def scrape_listing_urls(self, website: str = 'batdongsan', search_page_url: str = None,
                           start_page: int = START_PAGE_NUMBER, 
                           end_page: int = END_PAGE_NUMBER,
                           num_workers: int = MAX_WORKERS) -> list[str]:
        """
        Scrape listing URLs from pagination pages using multiple workers.
        
        Args:
            website: 'batdongsan' or 'onehousing'
            search_page_url: Override default search URL
            start_page: Starting page number
            end_page: Ending page number
            num_workers: Number of parallel workers
            
        Returns:
            List of listing URLs
        """
        print(f"\n{'='*60}")
        print(f"STEP 1: SCRAPING LISTING URLs ({website.upper()})")
        print(f"{'='*60}")
        print(f"Pages: {start_page} to {end_page}")
        print(f"Workers: {num_workers}")
        
        # Use website-specific URL if not provided
        if search_page_url is None:
            if website.lower() == 'batdongsan':
                search_page_url = SEARCH_PAGE_URL.get('Batdongsan', '')
            elif website.lower() == 'onehousing':
                search_page_url = SEARCH_PAGE_URL.get('Onehousing', '')
        
        all_urls = []
        
        if website.lower() == 'batdongsan':
            # Use multi-threaded approach for Batdongsan
            pages_per_worker = (end_page - start_page + 1) // num_workers
            
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = []
                
                for i in range(num_workers):
                    worker_start = start_page + (i * pages_per_worker)
                    worker_end = worker_start + pages_per_worker - 1
                    
                    if i == num_workers - 1:
                        worker_end = end_page
                    
                    future = executor.submit(
                        scrape_urls_worker,
                        i + 1,
                        search_page_url,
                        worker_start,
                        worker_end,
                        self.stop_event
                    )
                    futures.append(future)
                
                for future in as_completed(futures):
                    try:
                        urls = future.result()
                        all_urls.extend(urls)
                    except Exception as e:
                        print(f"Worker failed: {e}")
        
        elif website.lower() == 'onehousing':
            # Use OneHousing scraper
            onehousing_scraper = OneHousingScraper(self.db_manager)
            try:
                all_urls = onehousing_scraper.scrape_listing_urls(start_page, end_page)
            finally:
                onehousing_scraper.shutdown()
        
        # Save URLs to CSV
        if all_urls:
            df = pd.DataFrame({'url': all_urls})
            df.drop_duplicates(inplace=True)
            
            # Add timestamp and website to filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = OUTPUT_DIR / f"listing_urls_{website}_{timestamp}.csv"
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            print(f"\n Scraped {len(all_urls)} URLs")
            print(f"Saved to: {output_file}")
        
        return all_urls
    
    # ===== STEP 2: SCRAPE DETAILS =====
    
    def scrape_listing_details(self, website: str = 'batdongsan', urls: list[str] = None, 
                               urls_file: str = None,
                               num_workers: int = MAX_WORKERS,
                               start_index: int = 0,
                               count: int = 0) -> dict:
        """
        Scrape detailed information from listing URLs and save to database.
        
        Args:
            website: 'batdongsan' or 'onehousing'
            urls: List of URLs to scrape
            urls_file: Path to CSV file containing URLs
            num_workers: Number of parallel workers
            start_index: Starting index in the URL list
            count: Number of URLs to process (0 = all)
        
        Returns:
            Dictionary with scraping statistics
        """
        print(f"\n{'='*60}")
        print(f"STEP 2: SCRAPING LISTING DETAILS ({website.upper()})")
        print(f"{'='*60}")
        
        # Load URLs
        if urls is None:
            if urls_file is None:
                # Find most recent URLs file for this website
                url_files = sorted(OUTPUT_DIR.glob(f"listing_urls_{website}_*.csv"))
                if not url_files:
                    print(f"No URL files found for {website}. Run scrape_listing_urls first.")
                    return {}
                urls_file = url_files[-1]
            
            print(f"Loading URLs from: {urls_file}")
            df = pd.read_csv(urls_file)
            urls = df['url'].tolist()
        
        # Apply filtering
        urls = urls[start_index:]
        if count > 0:
            urls = urls[:count]
        
        print(f"Total URLs to scrape: {len(urls)}")
        
        # Create metadata record with website identifier
        website_name = 'batdongsan.com.vn' if website.lower() == 'batdongsan' else 'onehousing.vn'
        metadata_id = self.db_manager.create_scraping_metadata(website_name)
        
        combined_stats = {'scraped': 0, 'new': 0, 'changed': 0, 'duplicate': 0, 'errors': 0}
        
        try:
            if website.lower() == 'batdongsan':
                # Get existing listing IDs to avoid duplicates
                existing_ids = self.db_manager.get_existing_listing_ids()
                print(f"Found {len(existing_ids)} existing listings in database")
                
                # Divide work among workers
                urls_per_worker = len(urls) // num_workers
                
                with ThreadPoolExecutor(max_workers=num_workers) as executor:
                    futures = []
                    
                    for i in range(num_workers):
                        start_idx = i * urls_per_worker
                        end_idx = start_idx + urls_per_worker if i < num_workers - 1 else len(urls)
                        
                        worker_urls = urls[start_idx:end_idx]
                        
                        future = executor.submit(
                            scrape_detail_worker,
                            i + 1,
                            worker_urls,
                            existing_ids,
                            self.stop_event,
                            self.db_manager,
                            metadata_id
                        )
                        futures.append(future)
                    
                    # Collect results
                    for future in as_completed(futures):
                        try:
                            worker_stats = future.result()
                            for key in combined_stats:
                                combined_stats[key] += worker_stats.get(key, 0)
                        except Exception as e:
                            print(f"Worker failed: {e}")
            
            elif website.lower() == 'onehousing':
                # Get existing OneHousing property IDs
                existing_ids = self.db_manager.get_existing_onehousing_property_ids()
                print(f"Found {len(existing_ids)} existing OneHousing listings in database")
                
                # Use OneHousing scraper
                onehousing_scraper = OneHousingScraper(self.db_manager)
                try:
                    combined_stats = onehousing_scraper.scrape_listing_details_batch(
                        urls,
                        existing_ids,
                        metadata_id,
                        batch_size=SCRAPING_DETAILS_CONFIG.get("batch_size", 10)
                    )
                finally:
                    onehousing_scraper.shutdown()
            
            # Update metadata with success
            status = f"Completed: {combined_stats['new']} new, {combined_stats['changed']} changed, {combined_stats['duplicate']} duplicate, {combined_stats['errors']} errors"
            self.db_manager.update_scraping_metadata(metadata_id, status)
            
        except KeyboardInterrupt:
            print("\nScraping interrupted by user")
            self.db_manager.update_scraping_metadata(
                metadata_id, 
                "INTERRUPTED", 
                "User interrupted the scraping process"
            )
        except Exception as e:
            print(f"\nScraping failed: {e}")
            self.db_manager.update_scraping_metadata(
                metadata_id,
                "FAILED",
                str(e)
            )
        
        print(f"\n{'='*60}")
        print(f"SCRAPING SUMMARY ({website.upper()})")
        print(f"{'='*60}")
        for key, value in combined_stats.items():
            print(f"{key.capitalize()}: {value}")
        
        return combined_stats
    
    # ===== STEP 3: CLEAN AND PROCESS DATA =====
    
    def clean_and_process_data(self, website: str = 'batdongsan', status_filter: list[str] = None) -> int:
        """
        Clean raw listings and save to cleaned_listings table.
        
        Args:
            website: 'batdongsan' or 'onehousing' or 'both'
            status_filter: List of statuses to process (default: ['NEW', 'CHANGED'])
        
        Returns:
            Number of records processed
        """
        print(f"\n{'='*60}")
        print(f"STEP 3: CLEANING AND PROCESSING DATA ({website.upper()})")
        print(f"{'='*60}")
        
        if status_filter is None:
            status_filter = ['NEW', 'CHANGED']
        
        total_inserted = 0
        
        # Process Batdongsan data
        if website.lower() in ['batdongsan', 'both']:
            print("\n[Batdongsan] Processing...")
            total_inserted += self._clean_batdongsan_data(status_filter)
        
        # Process OneHousing data
        if website.lower() in ['onehousing', 'both']:
            print("\n[OneHousing] Processing...")
            total_inserted += self._clean_onehousing_data(status_filter)
        
        print(f"\nTotal processed: {total_inserted} records")
        return total_inserted
    
    def _clean_batdongsan_data(self, status_filter: list[str]) -> int:
        """Clean Batdongsan raw listings."""
        # Get raw listings that need cleaning
        raw_listings = self.db_manager.get_listings_for_cleaning(status_filter)
        print(f"Found {len(raw_listings)} Batdongsan listings to clean")
        
        if not raw_listings:
            print("No new Batdongsan listings to clean")
            return 0
        
        # Convert to DataFrame for processing
        df = pd.DataFrame(raw_listings)
        
        print("Initializing address standardizer...")
        if self.address_standardizer is None:
            self.address_standardizer = AddressStandardizer(
                str(PROVINCES_SQL_FILE),
                str(DISTRICTS_SQL_FILE),
                str(WARDS_SQL_FILE),
                str(STREETS_SQL_FILE)
            )
        
        print("Cleaning Batdongsan data...")
        cleaned_df = self._apply_batdongsan_cleaning_pipeline(df)
        
        print("Saving to database...")
        cleaned_records = cleaned_df.to_dict('records')
        inserted = self.db_manager.insert_cleaned_listings_batch(cleaned_records)
        
        print(f"Processed {len(cleaned_df)} records, inserted {inserted} new records")
        
        # Also save to Excel for backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_file = OUTPUT_DIR / f"cleaned_listings_batdongsan_{timestamp}.xlsx"
        cleaned_df.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"Excel backup saved to: {excel_file}")
        
        return inserted
    
    def _clean_onehousing_data(self, status_filter: list[str]) -> int:
        """Clean OneHousing raw listings."""
        # Get raw listings that need cleaning
        raw_listings = self.db_manager.get_onehousing_listings_for_cleaning(status_filter)
        print(f"Found {len(raw_listings)} OneHousing listings to clean")
        
        if not raw_listings:
            print("No new OneHousing listings to clean")
            return 0
        
        # Convert to DataFrame for processing
        df = pd.DataFrame(raw_listings)
        
        print("Cleaning OneHousing data...")
        cleaned_df = OneHousingDataCleaner.clean_onehousing_data(df)
        
        print("Saving to database...")
        cleaned_records = cleaned_df.to_dict('records')
        inserted = self.db_manager.insert_cleaned_listings_batch(cleaned_records)
        
        print(f"Processed {len(cleaned_df)} records, inserted {inserted} new records")
        
        # Also save to Excel for backup
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_file = OUTPUT_DIR / f"cleaned_listings_onehousing_{timestamp}.xlsx"
        cleaned_df.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"Excel backup saved to: {excel_file}")
        
        return inserted
    
    def _apply_batdongsan_cleaning_pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all cleaning and feature engineering steps."""
        
        # Rename ID column
        if 'listing_id' in df.columns:
            df.rename(columns={'listing_id': 'ID'}, inplace=True)
        
        # Extract address components
        print("  → Extracting address components...")
        df['Tỉnh/Thành phố'] = df.apply(DataCleaner.extract_city, axis=1)
        df['Thành phố/Quận/Huyện/Thị xã'] = df.apply(DataCleaner.extract_district, axis=1)
        df['Xã/Phường/Thị trấn'] = df.apply(DataCleaner.extract_ward, axis=1)
        df['Đường phố'] = df.apply(DataCleaner.extract_street, axis=1)
        df['Chi tiết'] = df.apply(DataCleaner.extract_address_details, axis=1)
        
        # Standardize addresses
        print("  → Standardizing addresses...")
        df['Tỉnh/Thành phố'] = df['Tỉnh/Thành phố'].apply(
            self.address_standardizer.standardize_province
        )
        df['Thành phố/Quận/Huyện/Thị xã'] = df.apply(
            self.address_standardizer.standardize_district, axis=1
        )
        df['Xã/Phường/Thị trấn'] = df.apply(
            self.address_standardizer.standardize_ward, axis=1
        )
        
        # Extract property features
        print("  → Extracting property features...")
        df['Nguồn thông tin'] = 'batdongsan.com.vn'
        df['Tình trạng giao dịch'] = 'Đã rao bán'
        df['Thời điểm giao dịch/rao bán'] = df['main_info'].apply(
            DataCleaner.extract_published_date
        )
        df['Thông tin liên hệ'] = df['url']
        
        df['Giá rao bán/giao dịch'] = df.apply(DataCleaner.extract_price, axis=1)
        df['Diện tích đất (m2)'] = df.apply(DataCleaner.extract_total_area, axis=1)
        df['Số tầng công trình'] = df.apply(DataCleaner.extract_num_floors, axis=1)
        df['Số mặt tiền tiếp giáp'] = df.apply(DataCleaner.extract_facade_count, axis=1)
        df['Hình dạng'] = df.apply(DataCleaner.extract_land_shape, axis=1)
        df['Chất lượng còn lại'] = df.apply(DataCleaner.estimate_remaining_quality, axis=1)
        df['Đơn giá xây dựng'] = df.apply(DataCleaner.extract_construction_cost, axis=1)
        df['Kích thước mặt tiền (m)'] = df.apply(DataCleaner.extract_width, axis=1)
        df['Kích thước chiều dài (m)'] = df.apply(DataCleaner.extract_length, axis=1)
        df['Mục đích sử dụng đất'] = df.apply(DataCleaner.extract_land_use, axis=1)
        df['Diện tích xây dựng'] = df.apply(DataCleaner.extract_construction_area, axis=1)
        df['Tổng diện tích sàn'] = df.apply(DataCleaner.extract_building_area, axis=1)
        df['Độ rộng ngõ/ngách nhỏ nhất (m)'] = df.apply(
            DataCleaner.extract_adjacent_lane_width, axis=1
        )
        df['Khoảng cách tới trục đường chính (m)'] = df.apply(
            DataCleaner.extract_distance_to_the_main_road, axis=1
        )
        df['Yếu tố khác'] = df.apply(DataCleaner.extract_street_or_alley_front, axis=1)
        
        # Impute missing values
        print("  → Imputing missing values...")
        df = DataImputer.fill_missing_width(df)
        df['Kích thước chiều dài (m)'] = df.apply(DataImputer.fill_missing_length, axis=1)
        
        # Feature engineering
        print("  → Engineering features...")
        df['Giá ước tính'] = df.apply(FeatureEngineer.calculate_estimated_price, axis=1)
        df['Lợi thế kinh doanh'] = df.apply(FeatureEngineer.calculate_business_advantage, axis=1)
        df['Đơn giá đất'] = df.apply(FeatureEngineer.calculate_land_unit_price, axis=1)
        df['Loại đơn giá (Đ/m2 hoặc Đ/m ngang)'] = 'Đ/m2'
        
        # Add coordinates and images
        df['Tọa độ (vĩ độ)'] = df['latitude']
        df['Tọa độ (kinh độ)'] = df['longitude']
        df['Hình ảnh của bài đăng'] = df['image_urls']
        
        # Select only required columns for cleaned table
        cleaned_columns = [
            'ID', 'Tỉnh/Thành phố', 'Thành phố/Quận/Huyện/Thị xã', 'Xã/Phường/Thị trấn',
            'Đường phố', 'Chi tiết', 'Nguồn thông tin', 'Tình trạng giao dịch',
            'Thời điểm giao dịch/rao bán', 'Thông tin liên hệ', 'Giá rao bán/giao dịch',
            'Giá ước tính', 'Loại đơn giá (Đ/m2 hoặc Đ/m ngang)', 'Đơn giá đất',
            'Lợi thế kinh doanh', 'Số tầng công trình', 'Tổng diện tích sàn',
            'Đơn giá xây dựng', 'Chất lượng còn lại', 'Diện tích đất (m2)',
            'Kích thước mặt tiền (m)', 'Kích thước chiều dài (m)', 'Số mặt tiền tiếp giáp',
            'Hình dạng', 'Độ rộng ngõ/ngách nhỏ nhất (m)', 'Khoảng cách tới trục đường chính (m)',
            'Mục đích sử dụng đất', 'Yếu tố khác', 'Tọa độ (vĩ độ)', 'Tọa độ (kinh độ)',
            'Hình ảnh của bài đăng'
        ]
        
        return df[cleaned_columns]
    
    # ===== COMPLETE PIPELINE =====
    
    def run_complete_pipeline(self, website: str = 'both', scrape_urls: bool = True, 
                             scrape_details: bool = True,
                             clean_data: bool = True,
                             **kwargs):
        """
        Run the complete scraping and processing pipeline.
        
        Args:
            website: 'batdongsan', 'onehousing', or 'both'
            scrape_urls: Whether to scrape listing URLs
            scrape_details: Whether to scrape listing details
            clean_data: Whether to clean and process data
            **kwargs: Additional arguments for individual steps
        """
        print(f"\n{'='*60}")
        print(f"REAL ESTATE SCRAPING PIPELINE")
        print(f"{'='*60}")
        print(f"Website(s): {website.upper()}")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        websites_to_process = []
        if website.lower() == 'both':
            websites_to_process = ['batdongsan', 'onehousing']
        else:
            websites_to_process = [website.lower()]
        
        try:
            # Process each website sequentially
            for site in websites_to_process:
                if self.stop_event.is_set():
                    print(f"\nPipeline interrupted. Skipping {site}")
                    break
                
                print(f"\n{'='*60}")
                print(f"PROCESSING: {site.upper()}")
                print(f"{'='*60}")
                
                urls = None
                
                # Step 1: Scrape URLs
                if scrape_urls:
                    urls = self.scrape_listing_urls(
                        website=site,
                        search_page_url=kwargs.get('search_page_url'),
                        start_page=kwargs.get('start_page', START_PAGE_NUMBER),
                        end_page=kwargs.get('end_page', END_PAGE_NUMBER),
                        num_workers=kwargs.get('num_workers', MAX_WORKERS)
                    )
                
                # Step 2: Scrape Details
                if scrape_details:
                    self.scrape_listing_details(
                        website=site,
                        urls=urls,
                        urls_file=kwargs.get('urls_file'),
                        num_workers=kwargs.get('num_workers', MAX_WORKERS),
                        start_index=kwargs.get('start_index', 0),
                        count=kwargs.get('count', 0)
                    )
                
                # Step 3: Clean Data
                if clean_data:
                    self.clean_and_process_data(
                        website=site,
                        status_filter=kwargs.get('status_filter', ['NEW', 'CHANGED'])
                    )
            
            print(f"\n{'='*60}")
            print(f"PIPELINE COMPLETED SUCCESSFULLY")
            print(f"{'='*60}")
            
        except KeyboardInterrupt:
            print(f"\nPipeline interrupted by user")
        except Exception as e:
            print(f"\nPipeline failed: {e}")
            raise
        finally:
            print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ===== USAGE EXAMPLE =====

if __name__ == "__main__":
    # Initialize pipeline
    pipeline = ScrapingPipeline()
    
    # Option 1: Run complete pipeline for both websites
    pipeline.run_complete_pipeline(
        website='both',  # 'batdongsan', 'onehousing', or 'both'
        scrape_urls=True,
        scrape_details=True,
        clean_data=True,
        start_page=1,
        end_page=10,  # Adjust as needed
        num_workers=4
    )
    
    # Option 2: Run for specific website only
    # pipeline.run_complete_pipeline(
    #     website='onehousing',
    #     start_page=1,
    #     end_page=50,
    #     num_workers=2
    # )
    
    # Option 3: Run individual steps for specific website
    # urls = pipeline.scrape_listing_urls(website='batdongsan', start_page=1, end_page=5)
    # pipeline.scrape_listing_details(website='batdongsan', urls=urls, num_workers=4)
    # pipeline.clean_and_process_data(website='batdongsan')