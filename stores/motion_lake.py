import json
from datetime import datetime

import motion_lake_client
from motion_lake_client import ContentType

from stores.base_store import BaseStore


class MotionLakeStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = motion_lake_client.BaseClient()

    def reset(self):
        try:
            self.client.delete_collection("benchmark")
        except Exception as e:
            print(e)


    def store_document(self, data: dict, timestamp: str):
        self.client.store(
            "benchmark",
            json.dumps(data).encode(),
            datetime.fromisoformat(timestamp),
            content_type=ContentType.JSON,
            create_collection=True,
        )

    def get_document(self, timestamp: str):
            return self.client.get_items_between(
            "benchmark", datetime.fromisoformat(timestamp), datetime.fromisoformat(timestamp), limit=1
        )

    def get_total_size(self):
        self.client.flush_buffer("benchmark")
        return self.client.get_collection_size("benchmark")

    def flush_buffer(self):
        self.client.flush_buffer("benchmark")