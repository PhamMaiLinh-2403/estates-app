import csv
import queue
import threading
import pandas as pd 
from commons.config import * 

def validate_and_clean_csv(file_path):
    if not file_path.exists():
        return True
    
    try:
        df = pd.read_csv(file_path)

        if len(df) == 0:
            # If empty, write to files normally 
            return True 
        else:
            last_row = df.iloc[-1]
            # Check if the row have any missing column 
            if last_row.isna().any():    
                # Remove the incomplete row
                df_cleaned = df.iloc[:-1]
                # Save back to file
                df_cleaned.to_csv(file_path, index=False)
                return True
            else:
                return True
    except Exception as e:
        print("Error reading csv files!")

def csv_url_writer_listener(url_queue: queue.Queue, stop_event: threading.Event, output_path):
    """Write URLs to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    batch_size = CSV_WRITER_CONFIG["url_batch_size"]
    encoding = CSV_WRITER_CONFIG["encoding"]
    buffer = []
    total_saved = 0
    file_exists = output_path.exists()

    if file_exists:
        if validate_and_clean_csv(output_path):
            print("Resuming scraping and appending to old files...")
        else:
            print("Old files corrupted. Creating new files...")
            file_exists = False

    write_mode = 'a' if file_exists else 'w'
    write_header = not file_exists
    
    with open(output_path, mode=write_mode, newline='', encoding=encoding) as f:
        writer = csv.writer(f)

        if write_header:
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
    file_exists = output_path.exists()
    
    if file_exists:
        print(f"\n[Details Writer] Found existing {output_path.name}, validating...")
        
        if validate_and_clean_csv(output_path):
            print(f"[Details Writer] ✓ File is clean, will append new data")
        else:
            print(f"[Details Writer] ✗ File is corrupted, starting fresh")
            file_exists = False
    else:
        print(f"[Details Writer] Creating new file: {output_path.name}")
    
    write_mode = 'a' if file_exists else 'w'
    write_header = not file_exists

    with open(output_path, mode=write_mode, newline='', encoding=encoding) as f:
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
                    if write_header:
                        writer.writeheader()
                        f.flush() 

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