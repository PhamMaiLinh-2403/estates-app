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

    output_path.parent.mkdir(parents=True, exist_ok=True)

    lock = FileLock(str(output_path) + ".lock")

    batch_size = CSV_WRITER_CONFIG["url_batch_size"]
    encoding = CSV_WRITER_CONFIG["encoding"]
    buffer = []
    total_saved = 0

    with lock:
        file_exists = output_path.exists()

        if file_exists:
            if validate_and_clean_csv(output_path):
                print("Resuming scraping and appending to old files...")
            else:
                print("Old file corrupted. Creating new file...")
                file_exists = False

        # Appending if in reusme mode, delete all old files to start fresh if in new run 
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
                        buffer.clear()
                        f.flush()

                except queue.Empty:
                    continue

            if buffer:
                writer.writerows(buffer)
                total_saved += len(buffer)
                f.flush()

    print(f"[Writer] Saved {total_saved} URLs")


def csv_details_writer_listener(
    data_queue: queue.Queue,
    stop_event: threading.Event,
    output_path
):
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lock = FileLock(str(output_path) + ".lock")

    batch_size = CSV_WRITER_CONFIG["details_batch_size"]
    encoding = CSV_WRITER_CONFIG["encoding"]
    buffer = []
    total_saved = 0

    with lock:
        file_exists = output_path.exists()

        if file_exists:
            if validate_and_clean_csv(output_path):
                print(f"File is clean, will append new data.")
            else:
                print(f"File is corrupted, starting fresh.")
                file_exists = False
        else:
            print(f"Creating new file: {output_path.name}")

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
                        writer = csv.DictWriter(
                            f,
                            fieldnames=list(buffer[0].keys())
                        )
                        if write_header:
                            writer.writeheader()
                            f.flush()

                    if len(buffer) >= batch_size:
                        writer.writerows(buffer)
                        total_saved += len(buffer)
                        print(f"Saved batch. Total: {total_saved}")
                        buffer.clear()
                        f.flush()

                except queue.Empty:
                    continue

            if buffer and writer:
                writer.writerows(buffer)
                total_saved += len(buffer)
                f.flush()

    print(f"Saved {total_saved} details")