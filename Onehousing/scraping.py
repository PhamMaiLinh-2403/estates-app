import json
import time
import random
import threading
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from fake_useragent import UserAgent

from commons.config import *
from database.database_manager import DatabaseManager


class OneHousingScraper:
    """
    Scraper for OneHousing website.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.ua = UserAgent()
        self.driver = None
        self.stop_requested = threading.Event()
        
        self.base_url = BASE_URL.get('Onehousing', 'https://onehousing.vn')
        self.search_url = SEARCH_PAGE_URL.get('Onehousing', f'{self.base_url}/nha-dat-ban?')
        
    def _init_driver(self):
        """Initialize Selenium WebDriver."""
        user_agent = self._get_random_user_agent()
        options = Options()
        options.add_argument(f"user-agent={user_agent}")
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920,1080")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        try:
            self.driver = webdriver.Chrome(options=options)
            print("[OneHousing] Successfully initialized WebDriver.")
        except Exception as e:
            print(f"[OneHousing] Failed to initialize WebDriver: {e}")
            raise
    
    def _get_random_user_agent(self):
        """Get random user agent."""
        try:
            return self.ua.random
        except Exception:
            fallback_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
            ]
            return random.choice(fallback_agents)
    
    def _close_driver(self):
        """Close WebDriver."""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
            except Exception as e:
                print(f"[OneHousing] Error closing driver: {e}")
    
    # Step 1: Scrape URLs 
    
    def get_listing_urls_from_page(self, html):
        """Extract listing URLs from a page's HTML."""
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select('a[data-role="property-card"]')
        
        urls = []
        for card in cards:
            href = card.get("href")
            if href:
                if not href.startswith("http"):
                    href = self.base_url + href
                urls.append(href)
        
        return urls
    
    def scrape_listing_urls(self, start_page: int = 1, end_page: int = 100):
        """
        Scrape listing URLs from pagination pages.
        """      
        all_urls = set()
        consecutive_failures = 0    # Count failed requests.
        max_consecutive_failures = 3
        
        for page_num in range(start_page, end_page + 1):
            if self.stop_requested.is_set():
                print("[OneHousing] Stop requested. Exiting URL scraping.")
                break
            
            url = f"{self.search_url}page={page_num}"
            
            try:
                headers = {"User-Agent": self._get_random_user_agent()}
                response = requests.get(url, headers=headers, timeout=10)
                
                if response.status_code >= 400:
                    consecutive_failures += 1
                    print(f"[OneHousing] HTTP {response.status_code} on page {page_num}")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        print(f"[OneHousing] Too many failures. Stopping at page {page_num}")
                        break
                    continue
                
                if not response.text:
                    consecutive_failures += 1
                    print(f"[OneHousing] Empty page content on page {page_num}")
                    
                    if consecutive_failures >= max_consecutive_failures:
                        print(f"[OneHousing] Too many empty pages. Stopping at page {page_num}")
                        break
                    continue
                
                # Reset failure counter on success
                consecutive_failures = 0
                
                # Extract URLs
                page_urls = self.get_listing_urls_from_page(response.text)
                all_urls.update(page_urls)
                
                print(f"[OneHousing] Page {page_num}: Found {len(page_urls)} URLs (Total: {len(all_urls)})")
                
                # Respectful delay
                time.sleep(random.uniform(1, 3))
            
            except Exception as e:
                print(f"[OneHousing] Unexpected error on page {page_num}: {e}")
                consecutive_failures += 1
                
                if consecutive_failures >= max_consecutive_failures:
                    break

                time.sleep(RETRY_DELAY)
        
        print(f"[OneHousing] Total URLs scraped: {len(all_urls)}")
        return set(list(all_urls)) 
    
    # Step 2: Scrape listing details. 
    
    def extract_listing_details(self, url: str) -> Optional[Dict]:
        """
        Extract detailed information from a single listing page.
        """
        if self.stop_requested.is_set():
            return None
        
        if not self.driver:
            self._init_driver()
        
        try:
            self.driver.get(url)
            wait = WebDriverWait(self.driver, 10)
            wait.until(EC.presence_of_element_located((By.XPATH, "/html/body")))
            
            def safe_text(by, selector, timeout=5):
                """Helper function for text extraction."""
                try:
                    return wait.until(
                        EC.presence_of_element_located((by, selector))
                    ).text.strip()
                except (TimeoutException, NoSuchElementException):
                    return None
            
            # Data dictionary 
            data = {
                "property_url": url,
                "listing_title": safe_text(By.XPATH, '//*[@id="detail_title"]'),
                "property_id": safe_text(By.CSS_SELECTOR, '#container-property div:nth-child(5) div.flex.cursor-pointer p'),
                "total_price": safe_text(By.XPATH, '//*[@id="total-price"]'),
                "unit_price": safe_text(By.XPATH, '//*[@id="unit-price"]'),
                "alley_width": safe_text(By.XPATH, '//*[@id="overview_content"]//div[@data-impression-index="1"]'),
                "image_url": None,
                "city": None,
                "district": None,
                "features": [],
                "latitude": None,
                "longitude": None, 
                "property_description": []
            }
            
            # Extract image URL
            try:
                img_el = self.driver.find_element(By.XPATH, '//link[@rel="preload" and @as="image"]')
                image_src = img_el.get_attribute("imagesrcset")
                if image_src:
                    data["image_url"] = image_src.split(',')[0].strip().split(' ')[0]
            except Exception:
                pass
            
            # Extract breadcrumbs for location and geolocation 
            try:
                script_elements = self.driver.find_elements(By.XPATH, '//script[@type="application/ld+json"]')
                for script_el in script_elements:
                    try:
                        json_data = json.loads(script_el.get_attribute("innerHTML"))

                        # Extract location 
                        if isinstance(json_data, dict) and json_data.get("@type") == "BreadcrumbList":
                            for item in json_data.get("itemListElement", []):
                                if item.get("position") == 2:
                                    data["city"] = item.get("name")
                                elif item.get("position") == 3:
                                    data["district"] = item.get("name")
                        
                        # Extract geolocation 
                        if isinstance(json_data, dict) and json_data.get("geo"):
                            geo = json_data.get("geo", {})
                            data["latitude"] = geo.get("latitude")
                            data["longitude"] = geo.get("longitude")

                            break
                    except Exception:
                        continue
            except Exception:
                pass
            
            # Extract features
            try:
                features = wait.until(
                    EC.presence_of_all_elements_located((By.XPATH, '//*[@id="key-feature-item"]'))
                )
                for ele in features:
                    try:
                        title_el = ele.find_element(By.XPATH, './/*[@id="item_title"]')
                        text_el = ele.find_element(By.XPATH, './/*[@id="key-feature-text"]')
                        title = title_el.text.strip() if title_el else None
                        text = text_el.text.strip() if text_el else None
                        if title and text:
                            data["features"].append(f"{title}: {text}")
                    except NoSuchElementException:
                        continue
            except Exception:
                pass

            # Extract description
            try:
                desc_div = self.driver.find_element(By.CSS_SELECTOR, 'div[data-testid="property-description"]')
                if desc_div and desc_div.text:
                    data["property_description"] = [desc_div.text.strip()]
                else:
                    desc_elements = wait.until(
                        EC.presence_of_all_elements_located(
                            (By.CSS_SELECTOR, 'ul[aria-label="description-heading"].relative li')
                        )
                    )
                    data["property_description"] = [
                        li.text.strip() for li in desc_elements if li.text.strip()
                    ]
            except Exception:
                pass
            
            # Convert lists to strings for storage
            data["features"] = "; ".join(data["features"])
            data["property_description"] = ". ".join(data["property_description"])
            
            return data

        except Exception as e:
            print(f"[OneHousing] Unexpected error for {url}: {e}")
            return None