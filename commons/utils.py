def split_page_ranges(start, end, n_workers):
    """Splits a range of pages (e.g. 1-100) into n chunks."""
    total_pages = end - start + 1
    if n_workers <= 0 or total_pages <= 0:
        return []

    # If fewer pages than workers, reduce workers
    n_workers = min(n_workers, total_pages)
    
    chunk_size = total_pages // n_workers
    ranges = []
    current_start = start

    for i in range(n_workers):
        current_end = current_start + chunk_size - 1
        # Add remainder to the last worker
        if i == n_workers - 1:
            current_end = end
        
        ranges.append((current_start, current_end))
        current_start = current_end + 1
    
    return ranges

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