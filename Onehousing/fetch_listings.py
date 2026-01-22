import json
import re

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

def extract_listing_details(driver, url):
    """Extract detailed information from a single listing page."""
    try:
        driver.get(url)
        
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.XPATH, "/html/body")))
        
        def safe_text(by, selector):
            try:
                return wait.until(EC.presence_of_element_located((by, selector))).text.strip()
            except (TimeoutException, NoSuchElementException):
                return None
        
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
            img_el = driver.find_element(By.XPATH, '//link[@rel="preload" and @as="image"]')
            image_src = img_el.get_attribute("imagesrcset")
            if image_src:
                data["image_url"] = image_src.split(',')[0].strip().split(' ')[0]
        except:
            pass
        
        # Extract breadcrumbs and geolocation from JSON-LD
        try:
            script_elements = driver.find_elements(By.XPATH, '//script[@type="application/ld+json"]')
            for script_el in script_elements:
                try:
                    json_content = script_el.get_attribute("innerHTML")
                    if not json_content:
                        continue
                    
                    json_data = json.loads(json_content)

                    if isinstance(json_data, dict) and json_data.get("@type") == "BreadcrumbList":
                        for item in json_data.get("itemListElement", []):
                            if item.get("position") == 2:
                                data["city"] = item.get("name")
                            elif item.get("position") == 3:
                                data["district"] = item.get("name")
                    
                    if isinstance(json_data, dict) and json_data.get("geo"):
                        geo = json_data.get("geo", {})
                        data["latitude"] = geo.get("latitude")
                        data["longitude"] = geo.get("longitude")
                except:
                    continue
        except:
            pass
        
        # Extract features
        try:
            features = driver.find_elements(By.XPATH, '//*[@id="key-feature-item"]')
            for ele in features:
                try:
                    title = ele.find_element(By.XPATH, './/*[@id="item_title"]').text.strip()
                    text = ele.find_element(By.XPATH, './/*[@id="key-feature-text"]').text.strip()
                    if title and text:
                        data["features"].append(f"{title}: {text}")
                except NoSuchElementException:
                    continue
        except:
            pass

        # Extract description
        try:
            title_meta = driver.find_element(
                By.CSS_SELECTOR,
                "span[data-testid='seo-title-meta']"
            ).text.strip()

            description_meta = driver.find_element(
                By.CSS_SELECTOR,
                "span[data-testid='seo-description-meta']"
            ).text.strip()

            li_texts = driver.execute_script("""
                const ul = document.querySelector("ul[aria-label='description-heading']");
                if (!ul) return [];
                return Array.from(ul.querySelectorAll("li"))
                    .map(li => li.innerText.trim())
                    .filter(Boolean);
            """)

            full_description = " ".join(
                [title_meta, description_meta] + li_texts
            )
            full_description = re.sub(r"\s+", " ", full_description).strip()

            data["property_description"] = full_description

        except Exception:
            pass
        
        data["features"] = "; ".join(data["features"])
        data["property_description"] = "".join(data["property_description"])
        
        return data

    except Exception as e:
        print(f"[Fetch] Error for {url}: {e}")
        return None