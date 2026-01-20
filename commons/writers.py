import csv
import queue
import threading
from commons.config import * 

def csv_url_writer_listener(url_queue: queue.Queue, stop_event: threading.Event, output_path):
    """Write URLs to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    batch_size = CSV_WRITER_CONFIG["url_batch_size"]
    encoding = CSV_WRITER_CONFIG["encoding"]
    buffer = []
    total_saved = 0
    
    with open(output_path, mode='w', newline='', encoding=encoding) as f:
        writer = csv.writer(f)
        writer.writerow(['url'])
        
        while True:
            try:
                data = url_queue.get(timeout=1.0)
                
                if data is None:
                    break
                
                if isinstance(data, list):
                    buffer.extend([[u] for u in data])
                
                url_queue.task_done()

                if len(buffer) >= batch_size:
                    writer.writerows(buffer)
                    total_saved += len(buffer)
                    print(f"[Writer] Saved batch. Total: {total_saved}")
                    buffer = []
                    f.flush()
            
            except queue.Empty:
                continue

        if buffer:
            writer.writerows(buffer)
            total_saved += len(buffer)
            f.flush()
    
    print(f"[Writer] Saved {total_saved} URLs")


def csv_details_writer_listener(data_queue: queue.Queue, stop_event: threading.Event, output_path):
    """Write details to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    batch_size = CSV_WRITER_CONFIG["details_batch_size"]
    encoding = CSV_WRITER_CONFIG["encoding"]
    buffer = []
    total_saved = 0
    
    with open(output_path, mode='w', newline='', encoding=encoding) as f:
        writer = None
        
        while True:
            try:
                data = data_queue.get(timeout=1.0)

                if data is None:
                    break
                
                buffer.append(data)
                data_queue.task_done()

                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=list(buffer[0].keys()))
                    writer.writeheader()

                if len(buffer) >= batch_size:
                    writer.writerows(buffer)
                    total_saved += len(buffer)
                    print(f"[Writer] Saved batch. Total: {total_saved}")
                    buffer = []
                    f.flush()
            
            except queue.Empty:
                continue

        if buffer and writer:
            writer.writerows(buffer)
            total_saved += len(buffer)
            f.flush()
    
    print(f"[Writer] Saved {total_saved} details")