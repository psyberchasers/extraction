import os
import sys
import json
import signal
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from tools.DataFetcher import DataFetcher, Database

OUTPUT_DIR = Path("output")
API_KEYS = [
    "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",
    #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
]
DB_FILE = OUTPUT_DIR / "processed_files.db"
OUTPUT_JSON_FILE = OUTPUT_DIR / "output.json"
PROCESSED_FILES_JSON_FILE = OUTPUT_DIR / "processed_files.json"


def setup_output_directory():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def signal_handler(signum, frame):
    save_processed_files()
    sys.exit(0)


def save_processed_files():
    processed_files = db.get_processed_files()
    PROCESSED_FILES_JSON_FILE.write_text(json.dumps(processed_files))


def load_data(api_key):
    try:
        result = DataFetcher(
            "pdf.upload.b.k.u.c", max_docs_per_min=10, api_key=api_key
        ).load()
    except Exception as e:
        print(f"Error occurred: {e}")
        result = []
    return result


def flatten(data):
    return [item for sublist in data for item in sublist]


def load_existing_data():
    if OUTPUT_JSON_FILE.is_file():
        return json.loads(OUTPUT_JSON_FILE.read_text())
    return []


def merge_data(existing_data, new_data):
    data_by_file = {item["file"]: item for item in existing_data}
    for item in new_data:
        file = item["file"]
        if file not in data_by_file:
            data_by_file[file] = item
        else:
            for key, value in item.items():
                if value not in {None, "", 0}:
                    data_by_file[file][key] = value
    return list(data_by_file.values())


def write_output_data(data):
    OUTPUT_JSON_FILE.write_text(json.dumps(data, indent=4))


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    setup_output_directory()

    db = Database(str(DB_FILE))

    with ThreadPoolExecutor() as executor:
        new_data = flatten(executor.map(load_data, API_KEYS))

    existing_data = load_existing_data()

    merged_data = merge_data(existing_data, new_data)

    write_output_data(merged_data)

    save_processed_files()
