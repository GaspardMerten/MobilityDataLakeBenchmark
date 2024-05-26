import json
import os
import shutil

from stores.base_store import BaseStore
import gzip


class AllInOneGZipFileStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Create tmp directory if it doesn't exist
        os.makedirs('tmp', exist_ok=True)
        self.memory = []

    def reset(self):
        shutil.rmtree('tmp')
        os.makedirs('tmp', exist_ok=False)
        self.memory = []

    def store_document(self, data: dict, timestamp: str):
        self.memory.append({'timestamp': timestamp, 'data': data})

    def get_document(self, timestamp: str):
        with gzip.open(f'tmp/all.json.gz', 'rt') as file:
            data = json.load(file)
            return next((doc['data'] for doc in data if doc['timestamp'] == timestamp), None)

    def flush(self):
        # Store all document in one file
        with gzip.open(f'tmp/all.json.gz', 'wt') as file:
            file.write(json.dumps(self.memory))

    def get_total_size(self):
        self.flush()

        return os.path.getsize(f'tmp/all.json.gz') if os.path.exists(f'tmp/all.json.gz') else 0
