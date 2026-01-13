import time 
from functools import wraps

def retry(max_tries=3, delay_seconds=3, backoff=2):
    """
    A decorator for retrying a function or method if it raises an exception.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            tries = 0
            func_name = f"{args[0].__class__.__name__}.{func.__name__}"

            while tries < max_tries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    tries += 1

                    if tries >= max_tries:
                        print(f"'{func_name}' failed after {max_tries} attempts. Last error: {e}")
                        return None

                    sleep_time = delay_seconds * (backoff ** (tries - 1))
                    print(f"'{func_name}' failed. Retrying in {sleep_time:.2f} seconds... ({tries}/{max_tries})")
                    time.sleep(sleep_time)

            return None
        return wrapper
    return decorator