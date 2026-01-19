import requests
from bs4 import BeautifulSoup
from typing import List

from commons.config import BASE_URL, SEARCH_PAGE_URL

def get_base_url():
    return BASE_URL.get('Onehousing', 'https://onehousing.vn')

def get_search_url():
    base = get_base_url()
    return SEARCH_PAGE_URL.get('Onehousing', f'{base}/nha-dat-ban?')

def parse_listing_urls(html: str) -> List[str]:
    """Extract listing URLs from a raw HTML string."""
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select('a[data-role="property-card"]')
    
    base_url = get_base_url()
    urls = []
    for card in cards:
        href = card.get("href")
        if href:
            if not href.startswith("http"):
                href = base_url + href
            urls.append(href)
    return urls

def fetch_search_page(page_num, user_agent):
    """
    Fetches a single search page and returns the listing URLs found.
    """
    url = f"{get_search_url()}page={page_num}"
    
    if not user_agent:
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

    headers = {"User-Agent": user_agent}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            print(f"[Fetch URL] HTTP {response.status_code} on page {page_num}")
            return []
            
        return parse_listing_urls(response.text)
        
    except Exception as e:
        print(f"[Fetch URL] Error on page {page_num}: {e}")
        return []