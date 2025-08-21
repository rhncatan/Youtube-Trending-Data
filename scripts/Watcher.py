import time
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
import os
import pandas as pd

from Transform import process_trending_topics
from Load import load_today_trending

class CSVCreatedHandler(FileSystemEventHandler):

    def on_created(self, event):
        # If event is not a directory update and event created is .csv
        if not event.is_directory and event.src_path.lower().endswith('.csv'):
            filename = os.path.basename(event.src_path)
            region_code = filename.split("_")[1]
            print(f"Detected new CSV: {filename}")

            if self._wait_until_file_ready(event.src_path):
                print(f"File is ready...")

                df = process_trending_topics(pd.read_csv(event.src_path), region_code=region_code)

                load_today_trending(df)

            else:
                print(f"File never stabilized: {filename}")

    def _wait_until_file_ready(self, filepath,wait_time=1.0, max_checks=5):
        last_size = -1
        checks = 0

        while checks < max_checks:
            current_size = os.path.getsize(filepath)
            if current_size == last_size:
                return True  # File size stopped changing → ready
            last_size = current_size
            time.sleep(wait_time)
            checks += 1
        return False  # Gave up waiting




data_dir = r'data/raw'

event_handler = CSVCreatedHandler()
observer = Observer()
observer.schedule(event_handler, data_dir, recursive=False)
observer.start()

try:
    print(f"Awaiting trending topics data: {data_dir}")
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping watcher...")
    observer.stop()

observer.join()