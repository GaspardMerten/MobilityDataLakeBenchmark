import json
import os
import shutil

from stores.base_store import BaseStore
import gzip


class GZipFileStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Create tmp directory if it doesn't exist
        os.makedirs('tmp', exist_ok=True)

    def reset(self):
        shutil.rmtree('tmp')
        os.makedirs('tmp', exist_ok=False)

    def store_document(self, data: dict, timestamp: str):
        with gzip.open(f'tmp/{timestamp}.json.gz', 'wt') as file:
            file.write(json.dumps(data))

    def get_document(self, timestamp: str):
        with gzip.open(f'tmp/{timestamp}.json.gz', 'rt') as file:
            return json.load(file)

    def get_total_size(self):
        return sum(os.path.getsize(f'tmp/{f}') for f in os.listdir('tmp'))
