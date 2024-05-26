import os
import shutil

import pyarrow
import pyarrow.parquet

from stores.base_store import BaseStore


class ApacheParquetStore(BaseStore):

    def __init__(self, *args, **kwargs):
        self.table = None
        super().__init__(*args, **kwargs)
        self.last_timestamp = None
        self.compression = kwargs.get("compression", "SNAPPY")
        self.timestamp_group = kwargs.get("timestamp_group", 13)

    def reset(self):
        # Rm tmp folder if exists
        shutil.rmtree("tmp", ignore_errors=True)
        os.mkdir("tmp")
        self.table = None
        self.last_timestamp = None

    def store_document(self, data: dict, timestamp: str):
        if not self.table or self.last_timestamp[:self.timestamp_group] != timestamp[:self.timestamp_group]:
            self.table = pyarrow.Table.from_pylist(
                [
                    {
                        "timestamp": timestamp,
                        "uuid": feature["properties"]["uuid"],
                        "id": feature["properties"]["id"],
                        "color": feature["properties"]["color"],
                        "direction": bool(feature["properties"]["direction"] - 1),
                        "distance": feature["properties"]["distance"],
                        "distanceFromPoint": feature["properties"]["distanceFromPoint"],
                        "lineId": feature["properties"]["lineId"],
                        "coordinates_0": feature["geometry"]["coordinates"][0],
                        "coordinates_1": feature["geometry"]["coordinates"][1],
                        "uuidx": feature["properties"]["uuid"],
                    }
                    for feature in data["features"]
                ],
                schema=pyarrow.schema(
                    [
                        ("timestamp", pyarrow.string()),
                        ("uuid", pyarrow.string()),
                        ("id", pyarrow.int64()),
                        ("color", pyarrow.string()),
                        ("direction", pyarrow.bool_()),
                        ("distance", pyarrow.float32()),
                        ("distanceFromPoint", pyarrow.uint16()),
                        ("lineId", pyarrow.string()),
                        ("coordinates_0", pyarrow.float32()),
                        ("coordinates_1", pyarrow.float32()),
                        ("uuidx", pyarrow.string()),
                    ]
                ),
            )
        else:
            self.table = pyarrow.concat_tables(
                [
                    self.table,
                    pyarrow.Table.from_pylist(
                        [
                            {
                                "timestamp": timestamp,
                                "uuid": feature["properties"]["uuid"],
                                "id": feature["properties"]["id"],
                                "color": feature["properties"]["color"],
                                "direction": bool(
                                    feature["properties"]["direction"] - 1
                                ),
                                "distance": feature["properties"]["distance"],
                                "distanceFromPoint": feature["properties"][
                                    "distanceFromPoint"
                                ],
                                "lineId": feature["properties"]["lineId"],
                                "coordinates_0": feature["geometry"]["coordinates"][0],
                                "coordinates_1": feature["geometry"]["coordinates"][1],
                                "uuidx": feature["properties"]["uuid"],
                            }
                            for feature in data["features"]
                        ],
                        schema=pyarrow.schema(
                            [
                                ("timestamp", pyarrow.string()),
                                ("uuid", pyarrow.string()),
                                ("id", pyarrow.int64()),
                                ("color", pyarrow.string()),
                                ("direction", pyarrow.bool_()),
                                ("distance", pyarrow.float32()),
                                ("distanceFromPoint", pyarrow.uint16()),
                                ("lineId", pyarrow.string()),
                                ("coordinates_0", pyarrow.float32()),
                                ("coordinates_1", pyarrow.float32()),
                                ("uuidx", pyarrow.string()),
                            ]
                        ),
                    ),
                ]
            )

        pyarrow.parquet.write_table(
            self.table,
            f"tmp/table_{timestamp[:self.timestamp_group]}.parquet",
            compression=self.compression,
        )

        self.last_timestamp = timestamp

    def get_document(self, timestamp: str):
        table = pyarrow.parquet.read_table(
            f"tmp/table_{timestamp[:self.timestamp_group]}.parquet",
            filters=[("timestamp", "==", timestamp)],
        )

        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "uuid": row[1],
                        "id": row[2],
                        "color": row[3],
                        "direction": row[4],
                        "distance": row[5],
                        "distanceFromPoint": row[6],
                        "lineId": row[7],
                    },
                    "geometry": {"type": "Point", "coordinates": [row[8], row[9]]},
                }
                for row in table.to_pydict().values()
            ],
        }

    def get_total_size(self):
        return sum(os.path.getsize(f"tmp/{f}") for f in os.listdir("tmp"))

    def name(self):
        return f"ApacheParquetStore(compression={self.compression}, timestamp_group={self.timestamp_group})"

    def __str__(self):
        return f"ApacheParquetStore(compression={self.compression}, timestamp_group={self.timestamp_group})"
