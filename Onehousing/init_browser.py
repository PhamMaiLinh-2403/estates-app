import random
import tempfile
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent

def get_random_user_agent():
    """Get random user agent."""
    ua = UserAgent()
    try:
        return ua.random
    except Exception:
        fallback_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
        ]
        return random.choice(fallback_agents)

def create_driver(headless=True):
    """
    Returns tuple: (driver, user_data_dir)
    """
    user_agent = get_random_user_agent()
    options = Options()
    options.add_argument(f"user-agent={user_agent}")
    
    # Create unique profile dir
    user_data_dir = tempfile.mkdtemp()
    options.add_argument(f"--user-data-dir={user_data_dir}")
    
    if headless:
        options.add_argument("--headless=new") # Use new headless mode for better stability
    
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument("--log-level=3")
    options.add_argument("--silent")

    try:
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(30)
        driver.set_script_timeout(30)
        return driver, user_data_dir
    except Exception as e:
        shutil.rmtree(user_data_dir, ignore_errors=True)
        print(f"[Driver Init] Failed to initialize WebDriver: {e}")
        raise