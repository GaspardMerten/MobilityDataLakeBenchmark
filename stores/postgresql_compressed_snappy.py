from collections import defaultdict

import snappy
import sqlalchemy
from sqlalchemy.orm import DeclarativeBase

from stores.base_store import BaseStore


class PostgreSQLCompressedSnappy(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = sqlalchemy.create_engine(
            "postgresql://postgres:postgres@localhost:5433"
        )
        self.connection = self.engine.connect()
        self.connection.execute(sqlalchemy.text("commit"))
        try:
            self.connection.execute(sqlalchemy.text("CREATE DATABASE super"))
        except sqlalchemy.exc.ProgrammingError:
            pass
        self.connection.close()
        self.engine = sqlalchemy.create_engine(
            "postgresql://postgres:postgres@localhost:5433/super"
        )
        self.session = sqlalchemy.orm.sessionmaker(bind=self.engine)()
        self.last_inserted_id = 0
        self.connection = self.engine.connect()
        self.buffer = []

    def reset(self):
        self.connection.execute(sqlalchemy.text("DROP SCHEMA public CASCADE"))
        self.connection.execute(sqlalchemy.text("CREATE SCHEMA public"))
        self.connection.execute(sqlalchemy.text("commit"))



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
                    f"','.join(map(str, feature['geometry']['coordinates'])"
                )
                dic["properties_color"].append(str(feature["properties"]["color"]))
                dic["properties_uuid"].append(str(feature["properties"]["uuid"]))
                dic["properties_direction"].append(str(feature["properties"]["direction"]))
                dic["properties_distance"].append(str(feature["properties"]["distance"]))
                dic["properties_distance_from_point"].append(
                    str(feature["properties"]["distanceFromPoint"])
                )
                dic["properties_id"].append(str(feature["properties"]["id"]))
                dic["properties_lineid"].append(str(feature["properties"]["lineId"]))
                dic["properties_pointid"].append(str(feature["properties"]["pointId"]))

        for uuid, data in features_data.items():


            self.connection.execute(
                sqlalchemy.text(
                    f"""
                INSERT INTO compressed_feature
                VALUES (
                   tsrange('{min_timestamp}', '{max_timestamp}'),
                    E'{snappy.compress(','.join(data['geometry_coordinates']).encode()).hex()}',
                    E'{snappy.compress(','.join(data['properties_color']).encode()).hex()}',
                    E'{snappy.compress(','.join(data['properties_uuid']).encode()).hex()}',
                    E'{snappy.compress(','.join(data['properties_direction']).encode()).hex()}',
                    E'{snappy.compress(','.join(data['properties_distance']).encode()).hex()}',
                    E'{snappy.compress(','.join(data['properties_distance_from_point']).encode()).hex()}',
                    E'{snappy.compress(','.join(data['properties_id']).encode()).hex()}',
                    E'{snappy.compress(','.join(data['properties_lineid']).encode()).hex()}',
                    E'{snappy.compress(','.join(data['properties_pointid']).encode()).hex()}'
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
