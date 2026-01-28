import threading
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from .config import * 

class CircuitBreaker:
    def __init__(self):
        self.threshold = THRESHOLD
        self.consecutive_failures = 0
        self.is_open = False
        self.lock = threading.Lock()
        self.stop_reason = ""

    def record_success(self):
        """
        Reset the number of consecutive failures when requests successfully. 
        """
        with self.lock:
            if self.consecutive_failures > 0:
                self.consecutive_failures = 0

    def record_failure(self, error_type: str):
        with self.lock:
            self.consecutive_failures += 1
            
            # Critical errors trigger immediate stop
            critical_prefixes = ["Critical_", "Fatal_", "MemoryError", "ConnectionRefused"]
            
            if any(error_type.startswith(prefix) for prefix in critical_prefixes):
                self.is_open = True
                self.stop_reason = f"Critical Error: {error_type}"
                print(f"[Circuit Breaker] OPEN: {self.stop_reason}")
                return True

            if self.consecutive_failures >= self.threshold:
                self.is_open = True
                self.stop_reason = f"Threshold reached: {self.consecutive_failures} consecutive failures (last: {error_type})"
                print(f"[Circuit Breaker] OPEN: {self.stop_reason}")
                return True
            else:
                print(f"[Circuit Breaker] Failure {self.consecutive_failures}/{self.threshold}: {error_type}")
            
        return False

    def should_stop(self):
        with self.lock:
            return self.is_open
        

class PipelineStateManager:
    def __init__(self):
        self.state = self._load_state()

    def _load_state(self):
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {
            "batdongsan_url_pages": [],
            "onehousing_url_pages": [],
            "last_run_status": "idle", # idle, running, suspended, completed
            "retry_count": 0
        }
    
    def save_state(self):
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_FILE, 'w') as f:
            json.dump(self.state, f)

    def mark_page_complete(self, source, page_num):
        key = f"{source.lower()}_url_pages"
        if page_num not in self.state[key]:
            self.state[key].append(page_num)
            self.save_state()

    def get_completed_pages(self, source):
        return set(self.state[f"{source.lower()}_url_pages"])
    
    def get_pending_details_urls(self, source):
        """
        Compare the URL file vs Details file  
        to return only URLs that haven't been scraped yet.
        """
        url_csv = URLS_CSV_PATH[source]
        detail_csv = DETAILS_CSV_PATH[source]

        if not url_csv.exists():
            return []

        # Load To-Do
        try:
            df_urls = pd.read_csv(url_csv)
            all_urls = set(df_urls['url'].unique()) if 'url' in df_urls.columns else set()
        except Exception:
            return []

        # Load Done
        done_urls = set()
        if detail_csv.exists():
            try:
                # Only read relevant columns to save memory
                df_details = pd.read_csv(detail_csv)
                # Handle different column names between sites
                if 'url' in df_details.columns:
                    done_urls.update(df_details['url'].dropna().unique())
                if 'property_url' in df_details.columns:
                    done_urls.update(df_details['property_url'].dropna().unique())
            except (ValueError, pd.errors.EmptyDataError):
                pass 

        # Calc Diff
        pending = list(all_urls - done_urls)
        print(f"[{source}] Total URLs: {len(all_urls)}, Done: {len(done_urls)}, Pending: {len(pending)}")
        return pending
    
    def reset_for_new_run(self):
        self.state["batdongsan_url_pages"] = []
        self.state["onehousing_url_pages"] = []
        self.state["last_run_status"] = "running"
        self.state["retry_count"] = 0
        self.save_state()

    def set_suspended(self):
        self.state["last_run_status"] = "suspended"
        self.save_state()
    
    def set_completed(self):
        self.state["last_run_status"] = "completed"
        self.state["retry_count"] = 0
        self.save_state()

    def is_suspended(self):
        """Checks if the previous run crashed or was suspended."""
        return self.state.get("last_run_status") == "suspended"

    def reset(self):
        """Resets retry count after a successful run."""
        self.state["retry_count"] = 0
        self.state["last_run_status"] = "idle" # or completed
        self.save_state()

    def increment_retry(self):
        """Increments retry count and returns current value."""
        self.state["retry_count"] = self.state.get("retry_count", 0) + 1
        self.save_state()
        return self.state["retry_count"]

class PipelineStopException(Exception):
    """Custom exception to halt the pipeline gracefully."""
    pass