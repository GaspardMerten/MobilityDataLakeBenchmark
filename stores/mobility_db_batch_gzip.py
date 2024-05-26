import gzip
from collections import defaultdict

import sqlalchemy
from sqlalchemy.orm import DeclarativeBase

from stores.base_store import BaseStore


class MobilityDBBatchCompressedGZIPStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = sqlalchemy.create_engine(
            "postgresql://postgres:postgres@localhost:5434"
        )
        self.connection = self.engine.connect()
        self.connection.execute(sqlalchemy.text("commit"))
        try:
            self.connection.execute(sqlalchemy.text("CREATE DATABASE mobility"))
        except sqlalchemy.exc.ProgrammingError:
            pass
        self.connection.close()
        self.engine = sqlalchemy.create_engine(
            "postgresql://postgres:postgres@localhost:5434/mobility"
        )
        self.session = sqlalchemy.orm.sessionmaker(bind=self.engine)()
        self.last_inserted_id = 0
        self.connection = self.engine.connect()
        self.buffer = []

    def reset(self):
        self.connection.execute(sqlalchemy.text("DROP SCHEMA public CASCADE"))
        self.connection.execute(sqlalchemy.text("CREATE SCHEMA public"))
        self.connection.execute(sqlalchemy.text("commit"))

        # Create one table feature with the following columns:
        # geometry_coordinates: tgeompoint, the temporal geometry of the feature
        # properties_color: text, the color of the feature
        # properties_direction: integer, the direction of the feature
        # properties_distance: tfloat, the distance of the feature
        # properties_distance_from_point: tint, the distance from the point of the feature
        # properties_id: tint integer, the temporal id of the feature
        # properties_lineid: text, the line id of the feature
        # properties_pointid: ttext, the temporal point id of the feature
        self.connection.execute(sqlalchemy.text("CREATE extension mobilitydb CASCADE"))

        self.connection.execute(
            sqlalchemy.text(
                """
            CREATE TABLE feature (
                timerange tsrange,
                geometry_coordinates tgeompoint,
                properties_color ttext,
                properties_uuid ttext,
                properties_direction tint,
                properties_distance tfloat,
                properties_distance_from_point tint,
                properties_id tint,
                properties_lineid ttext,
                properties_pointid ttext
            )
        """
            )
        )

        self.connection.execute(
            # create bytea column
            sqlalchemy.text(
                """
                CREATE TABLE compressed_feature (
                timerange tsrange,
                    compressed_geometry_coordinates bytea,
                    compressed_properties_color bytea,
                    compressed_properties_uuid bytea,
                    compressed_properties_direction bytea,
                    compressed_properties_distance bytea,
                    compressed_properties_distance_from_point bytea,
                    compressed_properties_id bytea,
                    compressed_properties_lineid bytea,
                    compressed_properties_pointid bytea
                )
        """
            )
        )

        self.buffer = []

    def store_document(self, data: dict, timestamp: str):
        self.buffer.append((data, timestamp))

        BATCH_SIZE = 500
        if len(self.buffer) >= BATCH_SIZE:
            self.store_buffer()
            self.buffer.clear()

    def store_buffer(self):
        if not self.buffer:
            return
        features_data = defaultdict(lambda: defaultdict(list))
        min_timestamp = self.buffer[0][1]
        max_timestamp = self.buffer[-1][1]
        for data, timestamp in self.buffer:
            for feature in data["features"]:
                dic = features_data[feature["properties"]["uuid"]]
                dic["timestamp"].append(timestamp)
                dic["geometry_coordinates"].append(
                    f"POINT({' '.join(map(str, feature['geometry']['coordinates']))})"
                )
                dic["properties_color"].append(feature["properties"]["color"])
                dic["properties_uuid"].append(feature["properties"]["uuid"])
                dic["properties_direction"].append(feature["properties"]["direction"])
                dic["properties_distance"].append(feature["properties"]["distance"])
                dic["properties_distance_from_point"].append(
                    feature["properties"]["distanceFromPoint"]
                )
                dic["properties_id"].append(feature["properties"]["id"])
                dic["properties_lineid"].append(feature["properties"]["lineId"])
                dic["properties_pointid"].append(feature["properties"]["pointId"])

        for uuid, data in features_data.items():
            result = self.connection.execute(
                sqlalchemy.text(
                    f"""
                select
                    tsrange('{min_timestamp}', '{max_timestamp}'),
                    tgeompoint '[{','.join(map(lambda a: f'{a[0]}@{a[1]}', zip(data["geometry_coordinates"], data["timestamp"])))}]',
                    ttext '[{','.join(map(lambda a: f'{a[0]}@{a[1]}', zip(data["properties_color"], data["timestamp"])))}]',
                    ttext '[{','.join(map(lambda a: f'{a[0]}@{a[1]}', zip(data["properties_uuid"], data["timestamp"])))}]',
                    tint '[{','.join(map(lambda a: f'{a[0]}@{a[1]}', zip(data["properties_direction"], data["timestamp"])))}]',
                    tfloat '[{','.join(map(lambda a: f'{a[0]}@{a[1]}', zip(data["properties_distance"], data["timestamp"])))}]',
                    tint '[{','.join(map(lambda a: f'{a[0]}@{a[1]}', zip(data["properties_distance_from_point"], data["timestamp"])))}]',
                    tint '[{','.join(map(lambda a: f'{a[0]}@{a[1]}', zip(data["properties_id"], data["timestamp"])))}]',
                    ttext '[{','.join(map(lambda a: f'{a[0]}@{a[1]}', zip(data["properties_lineid"], data["timestamp"])))}]',
                    ttext '[{','.join(map(lambda a: f'{a[0]}@{a[1]}', zip(data["properties_pointid"], data["timestamp"])))}]'
                    """
                )
            ).fetchone()

            self.connection.execute(
                sqlalchemy.text(
                    f"""
                INSERT INTO compressed_feature
                VALUES (
                    '{result[0]}',
                    E'{gzip.compress(result[1].encode()).hex()}',
                    E'{gzip.compress(result[2].encode()).hex()}',
                    E'{gzip.compress(result[3].encode()).hex()}',
                    E'{gzip.compress(result[4].encode()).hex()}',
                    E'{gzip.compress(result[5].encode()).hex()}',
                    E'{gzip.compress(result[6].encode()).hex()}',
                    E'{gzip.compress(result[7].encode()).hex()}',
                    E'{gzip.compress(result[8].encode()).hex()}',
                    E'{gzip.compress(result[9].encode()).hex()}'
                )
            """
                )
            )

        # Commit the transaction
        self.connection.commit()

    def get_document(self, timestamp: str):
        document = self.connection.execute(
            sqlalchemy.text(f"SELECT * FROM document WHERE timestamp = '{timestamp}'")
        ).fetchone()[1]

        features = self.connection.execute(
            sqlalchemy.text(f"SELECT * FROM feature WHERE document_id = {document}")
        ).fetchall()

        items = self.connection.execute(
            sqlalchemy.text(
                f'SELECT * FROM item WHERE hash IN ({",".join([f"\'{feature[1]}\'" for feature in features])})'
            )
        ).fetchall()

        hash_to_item = {item[0]: item for item in items}

        features = [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row[2] / 10**8, row[3] / 10**8],
                },
                "id": str(hash_to_item[row[1]][1]),
                "properties": {
                    "color": hash_to_item[row[1]][3],
                    "direction": int(hash_to_item[row[1]][4]) + 1,
                    "distance": row[4],
                    "distanceFromPoint": row[5],
                    "id": str(hash_to_item[row[1]][1]),
                    "lineId": hash_to_item[row[1]][5],
                    "pointId": hash_to_item[row[1]][5],
                    "timestamp": timestamp,
                    "uuid": str(hash_to_item[row[1]][1]),
                },
            }
            for row in features
        ]

        return {"features": features, "type": "FeatureCollection"}

    def get_total_size(self):
        self.store_buffer()

        with self.engine.connect() as connection:
            result_2 = connection.execute(
                sqlalchemy.text("SELECT pg_relation_size('compressed_feature')")
            )

        return result_2.fetchone()[0]
