# utils.py

from datetime import datetime, timedelta
from itertools import islice

def chunked(iterable, size):
    """Split iterable into chunks of max size."""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def is_recent(date_str):
    try:
        if not date_str:
            return False
        # Normalize date string (remove time part if needed)
        date_clean = date_str.split("T")[0]
        record_date = datetime.strptime(date_clean, "%Y-%m-%d")
        is_recent_flag = (datetime.now() - record_date) < timedelta(days=180)
        return is_recent_flag
    except Exception as e:
        print(f"❌ Error parsing date '{date_str}': {e}")
        return False