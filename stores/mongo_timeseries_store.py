import datetime

from stores.base_store import BaseStore
import pymongo


class MongoTimeSeriesStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = pymongo.MongoClient("mongodb://localhost:27018/")
        self.db = self.client["store"]
        self.collection = self.db["documentsTS"]

    def reset(self):
        # Delete db and create a new one
        self.client.drop_database("store")
        self.db = self.client["store"]
        self.collection.drop()

        self.db.create_collection(
            "documentsTS", timeseries={"timeField": "timestamp", "metaField": "uuid"}
        )
        # Create index on timestamp
        self.collection.create_index([("timestamp", pymongo.ASCENDING)])
        self.collection = self.db["documentsTS"]

    def store_document(self, data: dict, timestamp: str):
        self.collection.insert_many(
            [
                {
                    "timestamp": datetime.datetime.fromisoformat(timestamp),
                    "uuid": feature["properties"]["uuid"],
                    "id": feature["properties"]["id"],
                    "color": feature["properties"]["color"],
                    "direction": bool(feature["properties"]["direction"] - 1),
                    "distance": feature["properties"]["distance"],
                    "distanceFromPoint": feature["properties"]["distanceFromPoint"],
                    "lineId": feature["properties"]["lineId"],
                    "coordinates_0": feature["geometry"]["coordinates"][0],
                    "coordinates_1": feature["geometry"]["coordinates"][1],
                }
                for feature in data["features"]
            ]
        )

    def get_document(self, timestamp: str):
        return {
            "features": list(
                self.collection.find({"timestamp": timestamp}, {"_id": 0})
            ),
            "type": "FeatureCollection",
        }

    def get_total_size(self):
        # FLush changes to disk
        self.client["admin"].command("fsync")
        return self.db.command("dbstats")["storageSize"]
