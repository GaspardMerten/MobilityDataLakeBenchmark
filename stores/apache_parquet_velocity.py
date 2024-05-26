import os
import shutil

import pyarrow
import pyarrow.parquet

from stores.base_store import BaseStore


class ApacheParquetVelocityStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.main_schema = pyarrow.schema(
            [("doc_id", pyarrow.int32()), ("timestamp", pyarrow.string())]
        )
        self.l1_schema = pyarrow.schema(
            [
                ("id", pyarrow.int32()),
                ("properties.uuid", pyarrow.string()),
                ("properties.id", pyarrow.int64()),
                ("properties.color", pyarrow.string()),
                ("properties.direction", pyarrow.bool_()),
                ("properties.lineId", pyarrow.string()),
                ("properties.uuidx", pyarrow.string()),
            ]
        )
        self.l2_schema = pyarrow.schema(
            [
                ("id", pyarrow.int32()),
                ("l1_id", pyarrow.int32()),
                ("properties.distance", pyarrow.float32()),
                ("properties.pointId", pyarrow.uint16()),
                ("properties.distanceFromPoint", pyarrow.uint16()),
                ("geometry.coordinates_0", pyarrow.float32()),
                ("geometry.coordinates_1", pyarrow.float32()),
            ]
        )
        self.current_id = 0
        self.current_l1_id = 0
        self.hash_table = {}

        self.main_table = None
        self.l1_table = None
        self.l2_table = None
        self.last_timestamp = None
        self.compression = kwargs.get("compression", "SNAPPY")

    def reset(self):
        # Rm tmp folder if exists
        shutil.rmtree("tmp", ignore_errors=True)
        os.mkdir("tmp")
        # Create a new parquet file
        self.main_table = pyarrow.Table.from_pydict(
            {"doc_id": [], "timestamp": []}, schema=self.main_schema
        )
        self.l1_table = pyarrow.Table.from_pydict(
            {
                "id": [],
                "properties.uuid": [],
                "properties.uuidx": [],
                "properties.id": [],
                "properties.color": [],
                "properties.direction": [],
                "properties.lineId": [],
            },
            schema=self.l1_schema,
        )
        self.l2_table = pyarrow.Table.from_pydict(
            {
                "id": [],
                "l1_id": [],
                "properties.distance": [],
                "properties.pointId": [],
                "properties.distanceFromPoint": [],
                "geometry.coordinates_0": [],
                "geometry.coordinates_1": [],
            },
            schema=self.l2_schema,
        )

    def get_l1_id(self, uuid: str):
        if uuid not in self.hash_table:
            self.hash_table[uuid] = self.current_l1_id
            self.current_l1_id += 1
        return self.hash_table[uuid]

    def store_document(self, data: dict, timestamp: str):
        actual_timestamp = timestamp
        timestamp = timestamp[:13]

        if self.last_timestamp is not None and self.last_timestamp != timestamp:
            self.main_table = pyarrow.Table.from_pydict(
                {"doc_id": [], "timestamp": []}, schema=self.main_schema
            )
            self.l1_table = pyarrow.Table.from_pydict(
                {
                    "id": [],
                    "properties.uuid": [],
                    "properties.uuidx": [],
                    "properties.id": [],
                    "properties.color": [],
                    "properties.direction": [],
                    "properties.lineId": [],
                },
                schema=self.l1_schema,
            )
            self.l2_table = pyarrow.Table.from_pydict(
                {
                    "id": [],
                    "l1_id": [],
                    "properties.distance": [],
                    "properties.pointId": [],
                    "properties.distanceFromPoint": [],
                    "geometry.coordinates_0": [],
                    "geometry.coordinates_1": [],
                },
                schema=self.l2_schema,
            )
            self.current_id = 0
            self.current_l1_id = 0
            self.hash_table = {}

        # Create a new parquet file
        self.main_table = pyarrow.concat_tables(
            [
                self.main_table,
                pyarrow.Table.from_pydict(
                    {"doc_id": [self.current_id], "timestamp": [actual_timestamp]},
                    schema=self.main_schema,
                ),
            ]
        )

        self.l1_table = pyarrow.concat_tables(
            [
                self.l1_table,
                pyarrow.Table.from_pylist(
                    [
                        {
                            "id": self.get_l1_id(item["properties"]["uuid"]),
                            "properties.uuid": item["properties"]["uuid"],
                            "properties.id": item["properties"]["id"],
                            "properties.color": item["properties"]["color"],
                            "properties.direction": bool(
                                item["properties"]["direction"] - 1
                            ),
                            "properties.lineId": item["properties"]["lineId"],
                            "properties.uuidx": item["properties"]["uuid"],
                        }
                        for item in data["features"]
                        if item["properties"]["uuid"] not in self.hash_table
                    ],
                    schema=self.l1_schema,
                ),
            ]
        )

        self.l2_table = pyarrow.concat_tables(
            [
                self.l2_table,
                pyarrow.Table.from_pylist(
                    [
                        {
                            "id": self.current_id,
                            "l1_id": self.get_l1_id(item["properties"]["uuid"]),
                            "properties.distance": item["properties"]["distance"],
                            "properties.pointId": item["properties"]["pointId"],
                            "properties.distanceFromPoint": item["properties"][
                                "distanceFromPoint"
                            ],
                            "geometry.coordinates_0": item["geometry"]["coordinates"][
                                0
                            ],
                            "geometry.coordinates_1": item["geometry"]["coordinates"][
                                1
                            ],
                        }
                        for item in data["features"]
                    ],
                    schema=self.l2_schema,
                ),
            ]
        )

        pyarrow.parquet.write_table(
            self.main_table,
            f"tmp/main_{timestamp[:13]}.parquet",
            compression=self.compression,
        )
        pyarrow.parquet.write_table(
            self.l1_table,
            f"tmp/l1_{timestamp[:13]}.parquet",
            compression=self.compression,
        )
        pyarrow.parquet.write_table(
            self.l2_table,
            f"tmp/l2_{timestamp[:13]}.parquet",
            compression=self.compression,
        )

        self.current_id += 1
        self.last_timestamp = timestamp

    def process_row(self, row, l1_mapping):
        return {
            "type": "Feature",
            "properties": {
                "distance": row["properties.distance"],
                "pointId": row["properties.pointId"],
                "distanceFromPoint": row["properties.distanceFromPoint"],
                **l1_mapping[row["l1_id"]],
            },
            "geometry": {
                "type": "Point",
                "coordinates": [
                    row["geometry.coordinates_0"],
                    row["geometry.coordinates_1"],
                ],
            },
        }

    def get_document(self, timestamp: str):
        main_table = pyarrow.parquet.read_table(f"tmp/main_{timestamp[:13]}.parquet")
        l1_table = pyarrow.parquet.read_table(f"tmp/l1_{timestamp[:13]}.parquet")
        l2_table = pyarrow.parquet.read_table(f"tmp/l2_{timestamp[:13]}.parquet")

        main_df = main_table.to_pandas()
        l1_df = l1_table.to_pandas()
        l2_df = l2_table.to_pandas()

        main_id = main_df.loc[main_df["timestamp"] == timestamp, "doc_id"].values[0]

        l2_df = l2_df[l2_df["id"] == main_id]
        l1_df = l1_df[l1_df["id"].isin(l2_df["l1_id"])]

        l1_mapping = {
            row["id"]: {
                "properties.uuid": str(row["properties.uuid"]),
                "properties.id": str(row["properties.id"]),
                "properties.color": row["properties.color"],
                "properties.direction": int(row["properties.direction"]) + 1,
                "properties.lineId": row["properties.lineId"],
            }
            for _, row in l1_df.iterrows()
        }

        features = [self.process_row(row, l1_mapping) for _, row in l2_df.iterrows()]

        return {"type": "FeatureCollection", "features": features}

    def get_total_size(self):
        return sum(os.path.getsize(f"tmp/{f}") for f in os.listdir("tmp"))

    def name(self):
        return f"ApacheParquetVelocityStore(compression={self.compression})"

    def __str__(self):
        return f"ApacheParquetVelocityStore with {self.compression} compression"
