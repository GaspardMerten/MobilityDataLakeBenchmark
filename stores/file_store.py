import json
import os
import shutil

from stores.base_store import BaseStore


class FileStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Create tmp directory if it doesn't exist
        os.makedirs('tmp', exist_ok=True)

    def reset(self):
        shutil.rmtree('tmp')
        os.makedirs('tmp', exist_ok=False)

    def store_document(self, data: dict, timestamp: str):
        with open(f'tmp/{timestamp}.json', 'w') as file:
            file.write(json.dumps(data))

    def get_document(self, timestamp: str):
        with open(f'tmp/{timestamp}.json', 'r') as file:
            return json.load(file)

    def get_total_size(self):
        return sum(os.path.getsize(f'tmp/{f}') for f in os.listdir('tmp'))
