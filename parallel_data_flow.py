import threading
import os
import pathlib
import time


sql_files = {
    'agents.sql': [],
    'enquiries_per_day.sql': [],
    'listing_photos_quality.sql': [],
    'page_clicks_per_day.sql': [],
    'listing_performance.sql': ['page_clicks_per_day.sql','enquiries_per_day.sql'],
    'listing_features.sql': ['agents.sql', 'listing_photos_quality.sql'],
    'listings_performance.sql': ['listing_performance.sql','listing_features.sql'],
}

lock = threading.Lock()
executed_files = {}


def execute_sql_file(sql_file):
    time.sleep(2)
    print(f"Executing {sql_file}")

def execute_sql_with_dependencies(sql_file):
    for dependency in sql_files[sql_file]:
        execute_sql_with_dependencies(dependency)
        time.sleep(2)
    with lock:
        if sql_file not in executed_files:
            execute_sql_file(sql_file)
            executed_files[sql_file] = True


threads = [threading.Thread(target=execute_sql_with_dependencies, args=(sql_file,)) for sql_file in sql_files]

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()
