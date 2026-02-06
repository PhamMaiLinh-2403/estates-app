import time 
from functools import wraps

def retry(max_tries=3, delay_seconds=3, backoff=2, retriable_exceptions=None):
    """
    A decorator for retrying a function or method if it raises a retriable exception.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            tries = 0
            func_name = f"{args[0].__class__.__name__}.{func.__name__}" if args else func.__name__

            last_exception = None
            
            while tries < max_tries:
                try:
                    return func(*args, **kwargs)
                    
                except Exception as e:
                    last_exception = e
                    tries += 1

                    # Check if this exception should be retried
                    if retriable_exceptions is not None:
                        if not isinstance(e, retriable_exceptions):
                            # Non-retriable exception - raise immediately
                            print(f"[{func_name}] Non-retriable error: {type(e).__name__}: {e}")
                            raise e

                    if tries >= max_tries:
                        # Max retries reached - raise the last exception
                        print(f"[{func_name}] Failed after {max_tries} attempts. Last error: {type(e).__name__}: {e}")
                        raise e

                    # Calculate delay and retry
                    sleep_time = delay_seconds * (backoff ** (tries - 1))
                    print(f"[{func_name}] Attempt {tries}/{max_tries} failed: {type(e).__name__}. Retrying in {sleep_time:.2f}s...")
                    time.sleep(sleep_time)

            # This should never be reached, but just in case
            if last_exception:
                raise last_exception
            return None
            
        return wrapper
    return decorator