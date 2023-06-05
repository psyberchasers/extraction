"""Loading logic for loading documents from an s3 directory."""
import time
import boto3
from threading import Lock
from langchain.document_loaders import S3DirectoryLoader
from langchain.document_loaders.s3_file import S3FileLoader
from tools.CustomCharacterTextSplitter import CustomCharacterTextSplitter
from tools.Database import Database
from utils.extractData import extract_data


class DataFetcher(S3DirectoryLoader):
    """Loading logic for loading documents from s3."""

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        max_docs_per_min: int = 15,
        api_key: str = "",
    ):
        """Initialize with bucket and key name."""
        self.bucket = bucket
        self.prefix = prefix
        self.max_docs_per_min = max_docs_per_min
        self.api_key = api_key
        self.db = Database("output/processed_files.db")
        self.processed_files = self.load_processed_files()
        self.lock = Lock()

    def load_processed_files(self):
        """Load processed file names from local database."""
        return self.db.get_processed_files()

    def load(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket)
        json_data = []
        start_time = time.time()
        processed_docs = 0
        for obj in bucket.objects.filter(Prefix=self.prefix):
            with self.lock:
                status = self.db.get_file_status(obj.key)
                if status == "processing":
                    # This file is currently being processed by another thread
                    continue

                # Skip file if processed
                processed_files = self.load_processed_files()
                if obj.key in processed_files:
                    continue

                # Lock this file before processing
                self.db.lock_file(obj.key)

            try:
                loader = S3FileLoader(self.bucket, obj.key).load()

                text_splitter = CustomCharacterTextSplitter(
                    max_chunks=1, chunk_size=3000
                )
                documents = text_splitter.split_documents(loader)
                data = extract_data(documents, self.api_key)

                # Add file name to the result
                for item in data:
                    item["file"] = obj.key

                json_data.extend(data)

                # Save this file as processed
                with self.lock:
                    self.db.save_processed_file(obj.key)
            except Exception as e:
                # An error occurred, revert file status to 'unprocessed'
                print(f"An error occurred while processing file {obj.key}: {e}")
                with self.lock:
                    self.db.revert_file_status(obj.key)
                continue

            processed_docs += 1
            if processed_docs >= self.max_docs_per_min:
                end_time = time.time()
                elapsed_time = end_time - start_time
                if elapsed_time < 60:
                    time.sleep(60 - elapsed_time)
                start_time = time.time()
                processed_docs = 0

        return json_data
