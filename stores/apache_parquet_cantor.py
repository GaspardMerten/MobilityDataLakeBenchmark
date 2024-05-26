import json
import math
import os
import shutil

import pyarrow
import pyarrow.parquet

from stores.base_store import BaseStore

MIN_X = 0
MIN_Y = 0
PRECISION = 16
BYTES_FOR_COORDINATES = 15
ORDER = 10**PRECISION


def cantor(x, y):
    x = round((x - MIN_X) * ORDER)
    y = round((y - MIN_Y) * ORDER)

    return (x + y) * (x + y + 1) // 2 + y


def inverse_cantor(z):
    w = math.floor((math.sqrt(8 * z + 1) - 1) / 2)
    t = (w**2 + w) // 2
    y = z - t
    x = w - y
    return (x / 10**PRECISION + MIN_X, y / ORDER + MIN_Y)


def elegant_pair(x, y):
    x_scaled = int((x - MIN_X) * ORDER)
    y_scaled = int((y - MIN_Y) * ORDER)
    if x_scaled < y_scaled:
        return y_scaled**2 + x_scaled
    else:
        return x_scaled**2 + x_scaled + y_scaled


def inverse_elegant_pair(z):
    sqrtz = int(math.sqrt(z))
    sq = sqrtz * sqrtz  # Store the squared value for reuse
    if sq == z:
        x, y = sqrtz, 0
    elif sqrtz * (sqrtz + 1) < z:
        x, y = sqrtz, z - sqrtz * (sqrtz + 1)
    else:
        x, y = z - sq, sqrtz

    x = x / ORDER + MIN_X

    y = y / ORDER + MIN_Y
    return x, y


class ApacheParquetCantorStore(BaseStore):

    def __init__(self, *args, **kwargs):
        self.table = None
        super().__init__(*args, **kwargs)
        self.last_timestamp = None
        self.compression = kwargs.get("compression", "SNAPPY")

    def reset(self):
        # Rm tmp folder if exists
        shutil.rmtree("tmp", ignore_errors=True)
        os.mkdir("tmp")
        self.table = None
        self.last_timestamp = None

    def store_document(self, data: dict, timestamp: str):
        if not self.table or self.last_timestamp[:13] != timestamp[:13]:
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
                        "coordinates": elegant_pair(
                            feature["geometry"]["coordinates"][0],
                            feature["geometry"]["coordinates"][1],
                        ).to_bytes(BYTES_FOR_COORDINATES, "little"),
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
                        ("coordinates", pyarrow.binary(BYTES_FOR_COORDINATES)),
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
                                "coordinates": elegant_pair(
                                    feature["geometry"]["coordinates"][0],
                                    feature["geometry"]["coordinates"][1],
                                ).to_bytes(BYTES_FOR_COORDINATES, "little"),
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
                                ("coordinates", pyarrow.binary(BYTES_FOR_COORDINATES)),
                                ("uuidx", pyarrow.string()),
                            ]
                        ),
                    ),
                ]
            )

        pyarrow.parquet.write_table(
            self.table,
            f"tmp/table_{timestamp[:13]}.parquet",
            compression=self.compression,
        )

        self.last_timestamp = timestamp

    def get_document(self, timestamp: str):
        table = pyarrow.parquet.read_table(
            f"tmp/table_{timestamp[:13]}.parquet",
            filters=[("timestamp", "==", timestamp)],
        )

        data = {
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
                    "geometry": {
                        "type": "Point",
                        "coordinates": inverse_elegant_pair(
                            int.from_bytes(row[8], "little")
                        ),
                    },
                }
                for row in map(lambda x: list(x.values()), table.to_pylist())
            ],
        }

        with open(f"tmp_x/{timestamp}.json", "w") as file:
            json.dump(data, file)

        return data

    def get_total_size(self):
        return sum(os.path.getsize(f"tmp/{f}") for f in os.listdir("tmp"))

    def name(self):
        return f"ApacheParquetCantorStore(compression={self.compression})"

    def __str__(self):
        return f"ApacheParquetCantorStore with {self.compression} compression"
