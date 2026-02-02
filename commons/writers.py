import csv
import queue
import threading
import pandas as pd 
from filelock import FileLock
from commons.config import * 

def validate_and_clean_csv(file_path):
    lock_path = str(file_path) + ".lock"
    lock = FileLock(lock_path)

    with lock:
        if not file_path.exists():
            return True

        try:
            df = pd.read_csv(file_path)

            if len(df) == 0:
                return True

            last_row = df.iloc[-1]

            if last_row.isna().any():
                # Remove the incomplete row
                df_cleaned = df.iloc[:-1]
                df_cleaned.to_csv(file_path, index=False)

            return True

        except Exception as e:
            print(f"Error reading csv file {file_path}: {e}")
            return False
        
def csv_url_writer_listener(url_queue: queue.Queue,
                            stop_event: threading.Event,
                            output_path):
    """
    OPTIMIZED:
    - Validation happens ONCE at start, OUTSIDE write loop
    - No FileLock (not needed - single writer)
    - Blocking queue.get() instead of timeout loop
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    batch_size = CSV_WRITER_CONFIG["url_batch_size"]
    encoding = CSV_WRITER_CONFIG["encoding"]
    buffer = []
    total_saved = 0

    file_exists = output_path.exists()
    if file_exists:
        if validate_and_clean_csv(output_path):
            print(f"[URL Writer] Resuming, appending to {output_path.name}")
        else:
            print(f"[URL Writer] File corrupted, starting fresh")
            file_exists = False

    write_mode = 'a' if file_exists else 'w'
    write_header = not file_exists

    with open(output_path, mode=write_mode, newline='', encoding=encoding) as f:
        writer = csv.writer(f)

        if write_header:
            writer.writerow(['url'])

        while True:
            try:
                data = url_queue.get()

                if data is None: 
                    break

                if isinstance(data, list):
                    buffer.extend([[u] for u in data])

                url_queue.task_done()

                if len(buffer) >= batch_size:
                    writer.writerows(buffer)
                    total_saved += len(buffer)
                    buffer.clear()
                    f.flush()

            except Exception as e:
                print(f"[URL Writer] Error: {e}")
                continue

        # Write remaining buffer
        if buffer:
            writer.writerows(buffer)
            total_saved += len(buffer)
            f.flush()

    print(f"[URL Writer] Saved {total_saved} URLs")


def csv_details_writer_listener(
    data_queue: queue.Queue,
    stop_event: threading.Event,
    output_path
):
    """
    OPTIMIZED:
    - Validation happens ONCE at start
    - No FileLock
    - Blocking queue processing
    - Better error handling
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    batch_size = CSV_WRITER_CONFIG["details_batch_size"]
    encoding = CSV_WRITER_CONFIG["encoding"]
    buffer = []
    total_saved = 0

    file_exists = output_path.exists()
    if file_exists:
        if validate_and_clean_csv(output_path):
            print(f"[Details Writer] Resuming, appending to {output_path.name}")
        else:
            print(f"[Details Writer] File corrupted, starting fresh")
            file_exists = False
    else:
        print(f"[Details Writer] Creating new file: {output_path.name}")

    write_mode = 'a' if file_exists else 'w'
    write_header = not file_exists

    try:
        with open(output_path, mode=write_mode, newline='', encoding=encoding) as f:
            writer = None

            while True:
                try:
                    data = data_queue.get()

                    if data is None: 
                        break

                    buffer.append(data)
                    data_queue.task_done()

                    # Initialize writer on first data
                    if writer is None:
                        writer = csv.DictWriter(
                            f,
                            fieldnames=list(buffer[0].keys())
                        )
                        if write_header:
                            writer.writeheader()
                            f.flush()

                    # Write batch
                    if len(buffer) >= batch_size:
                        writer.writerows(buffer)
                        total_saved += len(buffer)
                        print(f"[Details Writer] Saved batch. Total: {total_saved}")
                        buffer.clear()
                        f.flush()

                except Exception as e:
                    print(f"[Details Writer] Error processing item: {e}")
                    continue

            # Write remaining buffer
            if buffer and writer:
                writer.writerows(buffer)
                total_saved += len(buffer)
                f.flush()

    except Exception as e:
        print(f"[Details Writer] Fatal error: {e}")
        if buffer and 'f' in locals():
            try:
                if writer:
                    writer.writerows(buffer)
                    total_saved += len(buffer)
            except:
                pass

    print(f"[Details Writer] Saved {total_saved} details")