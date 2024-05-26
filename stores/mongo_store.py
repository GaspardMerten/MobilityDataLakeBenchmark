from stores.base_store import BaseStore
import pymongo


class MongoStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = pymongo.MongoClient("mongodb://localhost:27018/")
        self.db = self.client["store"]
        self.collection = self.db["documents"]

    def reset(self):
        self.client.drop_database("store")
        self.db = self.client["store"]

        self.collection.drop()
        self.collection = self.db["documents"]

    def store_document(self, data: dict, timestamp: str):
        self.collection.insert_one({"timestamp": timestamp, "data": data})

    def get_document(self, timestamp: str):
        return self.collection.find_one({"timestamp": timestamp})

    def get_total_size(self):
        # FLush changes to disk
        self.client["admin"].command("fsync")
        return self.db.command("dbstats")["storageSize"]
