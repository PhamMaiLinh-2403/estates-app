import os
import platform
import tempfile
import glob
import shutil 
import time 
import psutil 
import socket 
from datetime import datetime, time as dtime
from selenium.common.exceptions import WebDriverException


def build_page_url(search_page_url, page_number):
    """Constructs urls for pagination."""
    if page_number == 1:
        return search_page_url
    base_search_url = search_page_url.rstrip('/')
    return f"{base_search_url}/p{page_number}"


def chunks(iterable, n):
    """Split iterable into n roughly equal chunks."""
    lst = list(iterable)
    k, m = divmod(len(lst), n)
    for i in range(n):
        start = i * k + min(i, m)
        end = (i + 1) * k + min(i + 1, m)
        yield lst[start:end]

def kill_system_chrome_processes():
    """
    Forcefully kills all chrome and chromedriver processes on the system.
    """
    system_platform = platform.system()
    try:
        if system_platform == "Windows":
            # /F = Force, /IM = Image Name, /T = Kill child processes too
            os.system("taskkill /F /IM chrome.exe /T >nul 2>&1")
            os.system("taskkill /F /IM chromedriver.exe /T >nul 2>&1")
        else:
            # Linux/MacOS
            os.system("pkill -f chrome > /dev/null 2>&1")
            os.system("pkill -f chromedriver > /dev/null 2>&1")
            
        print("Executed global Chrome cleanup.")
    except Exception as e:
        print(f"Warning: Could not run cleanup command: {e}")

def clean_scraper_temp_dirs():
    tmp_dir = tempfile.gettempdir()
    targets = glob.glob(os.path.join(tmp_dir, "bds_scraper_*")) # Find folders starting with bds_scraper_
    
    count = 0
    for path in targets:
        try:
            shutil.rmtree(path, ignore_errors=True)
            count += 1
        except Exception:
            pass

    print(f"[System] Cleaned {count} temporary browser profiles.")

def safe_driver_quit(driver, user_data_dir=None):
    """
    Aggressively quits the driver. If graceful quit hangs, it forces a kill on the PID.
    """
    if not driver:
        return

    # 1. Grab PID before trying to quit
    driver_pid = None
    try:
        if hasattr(driver, 'service') and hasattr(driver.service, 'process'):
            driver_pid = driver.service.process.pid
        elif hasattr(driver, 'process'):
            driver_pid = driver.process.pid
    except Exception:
        pass

    # 2. Try Graceful Quit
    try:
        driver.quit()
    except Exception:
        pass

    # 3. Double Tap: Force Kill the Process ID if it still exists
    if driver_pid:
        try:
            if psutil.pid_exists(driver_pid):
                proc = psutil.Process(driver_pid)
                # Kill children (renderer processes)
                for child in proc.children(recursive=True):
                    try:
                        child.kill()
                    except psutil.NoSuchProcess:
                        pass
                # Kill parent (browser)
                proc.kill()
                print(f"[System] Force killed stuck driver PID: {driver_pid}")
        except (psutil.NoSuchProcess, Exception):
            pass

    # 4. Cleanup Temp Dir
    if user_data_dir and os.path.exists(user_data_dir):
        try:
            import shutil
            # Wait a tiny bit for file locks to release, then delete
            time.sleep(0.1) 
            shutil.rmtree(user_data_dir, ignore_errors=True)
        except Exception:
            pass

def check_internet_connection(host="8.8.8.8", port=53, timeout=5):
    """
    Ping Google DNS to check for active internet connection.
    """
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, "Stable"
    except OSError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Unknown Error: {e}"

def wait_for_internet(max_retries=10, wait_seconds=30):
    """
    Blocks execution until internet is available or retries are exhausted.
    """
    print("Checking internet connectivity...")
    for i in range(max_retries):
        if check_internet_connection():
            print("Internet connection confirmed.")
            return True
        print(f"No internet. Retrying in {wait_seconds}s ({i+1}/{max_retries})...")
        time.sleep(wait_seconds)
    
    print("Error: Internet is down. Aborting pipeline start.")
    return False

def is_safe_working_hour():
    """
    Returns True if current time is Mon-Fri, between 06:00 and 17:45.
    Returns False if it is weekend or night time.
    """
    now = datetime.now()
    
    # 1. Check Weekend (5=Saturday, 6=Sunday)
    if now.weekday() >= 5:
        return False
        
    # 2. Check Time Window (06:00 to 17:45)
    # We stop at 17:45 to give the system 15 mins to save data and close Chrome
    start_time = dtime(6, 0)
    end_time = dtime(17, 45)
    
    current_time = now.time()
    
    if start_time <= current_time <= end_time:
        return True
    
    return False