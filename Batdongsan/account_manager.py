import os 
import pickle 
from pathlib import Path 
from dotenv import load_dotenv 

class AccountPool:
    """
    Class to handle account rotation and cookie persistence. 
    """
    def __init__(self, cookie_dir="output/cookies"):
        self.users = None 
        self.passwords = None 
        self.accounts = [] 
        self.idx = 0 
        
        self.cookie_dir = Path(cookie_dir)
        self.cookie_dir.mkdir(parents=True, exist_ok=True)
        
        self.load_accounts()

    def load_accounts(self):
        """Loads credentials from .env and zips them into tuples."""
        load_dotenv()

        self.users = [u.strip() for u in os.getenv("ACCOUNT_USERNAMES", "").split(",")]
        self.passwords = [p.strip() for p in os.getenv("ACCOUNT_PASSWORDS", "").split(",")]

        if not self.users or not self.passwords:
            print("ACCOUNT_USERNAMES or ACCOUNT_PASSWORDS not found in .env")
            return

        if len(self.users) != len(self.passwords):
            raise ValueError("Number of usernames does not match number of passwords.")

        # Create list of tuples: [('user1', 'pass1'), ('user2', 'pass2')]
        self.accounts = list(zip(self.users, self.passwords))
        print(f"Loaded {len(self.accounts)} accounts.")

    def get_next_account(self):
        """
        Returns the next (username, password) tuple in the rotation.
        """
        if not self.accounts:
            return None, None

        # Get current account
        account = self.accounts[self.idx]
        
        # Move index to next, loop back to 0 if at the end  
        self.idx = (self.idx + 1) % len(self.accounts)
        
        return account

    def save_cookies(self, driver, username):
        """
        Dumps the current driver's cookies to a pickle file named after the username.
        """
        try:
            cookies = driver.get_cookies()
            filepath = self.cookie_dir / f"{username}.json"
            
            with open(filepath, "wb") as f:
                pickle.dump(cookies, f)
            
            print(f"Cookies saved for user: {username}")
        except Exception as e:
            print(f"Failed to save cookies for {username}: {e}")

    def load_cookies(self, driver, username):
        """
        Loads cookies from JSON file into the driver.
        Returns True if successful, False if no cookie file exists.
        
        IMPORTANT: Driver must be on the target domain (e.g., batdongsan.com.vn) 
        BEFORE calling this.
        """
        filepath = self.cookie_dir / f"{username}.json"
        
        if not filepath.exists():
            return False

        try:
            with open(filepath, "rb") as f:
                cookies = pickle.load(f)
            
            for cookie in cookies:
                try:
                    driver.add_cookie(cookie)
                except Exception:
                    continue

            driver.refresh()
            print(f"[AccountPool] Cookies loaded for user: {username}")
            return True
            
        except Exception as e:
            print(f"[AccountPool] Error loading cookies for {username}: {e}")
            return False