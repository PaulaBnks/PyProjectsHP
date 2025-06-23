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
    """Check if a note is recent (within last 180 days)"""
    try:
        record_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return (datetime.now() - record_date) < timedelta(days=180)
    except Exception:
        return False