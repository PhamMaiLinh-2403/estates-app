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