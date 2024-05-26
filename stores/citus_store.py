from datetime import datetime
from typing import Dict

from sqlalchemy import create_engine, text

from stores.base_store import BaseStore


class CitusStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = create_engine("postgresql://postgres:postgres@localhost:5432")
        with self.engine.connect() as connection:
            # Create the database "store" if it doesn't exist
            connection.execute(text("commit"))
            try:
                connection.execute(text("create database store"))
            except:
                pass
            connection.execute(text("commit"))
            self.engine = create_engine(
                "postgresql://postgres:postgres@localhost:5432/store"
            )

    def reset(self):
        with self.engine.connect() as connection:
            # Drop schema and create a new one
            connection.execute(text("DROP SCHEMA public CASCADE"))
            connection.execute(text("CREATE SCHEMA public"))
            connection.execute(text("commit"))
            connection.execute(text("CREATE EXTENSION citus CASCADE"))
            connection.execute(
                text(
                    """
            CREATE TABLE documents (
                timestamp timestamp,
                uuid text,
                id text,
                color text,
                direction boolean,
                distance float,
                distance_from_point float,
                line_id text,
                coordinates_0 float,
                coordinates_1 float
            ) USING columnar
            """
                )
            )

    def store_document(self, data: Dict, timestamp: str):
        documents = []
        for feature in data["features"]:
            documents.append(
                {
                    "timestamp": datetime.fromisoformat(timestamp),
                    "uuid": feature["properties"]["uuid"],
                    "id": feature["properties"]["id"],
                    "color": feature["properties"]["color"],
                    "direction": bool(feature["properties"]["direction"] - 1),
                    "distance": feature["properties"]["distance"],
                    "distance_from_point": feature["properties"]["distanceFromPoint"],
                    "line_id": feature["properties"]["lineId"],
                    "coordinates_0": feature["geometry"]["coordinates"][0],
                    "coordinates_1": feature["geometry"]["coordinates"][1],
                }
            )
        with self.engine.connect() as connection:
            connection.execute(
                text(
                    """
            INSERT INTO documents (
                timestamp,
                uuid,
                id,
                color,
                direction,
                distance,
                distance_from_point,
                line_id,
                coordinates_0,
                coordinates_1
            ) VALUES (
                :timestamp,
                :uuid,
                :id,
                :color,
                :direction,
                :distance,
                :distance_from_point,
                :line_id,
                :coordinates_0,
                :coordinates_1
            )
            """
                ),
                documents,
            )

            # commit the transaction
            connection.execute(text("commit"))

    def get_document(self, timestamp: str) -> Dict:
        with self.engine.connect() as connection:
            documents = connection.execute(
                text(
                    """
            SELECT * FROM documents WHERE timestamp = :timestamp
            """
                ),
                {"timestamp": datetime.fromisoformat(timestamp)},
            ).fetchall()

        return {
            "features": [
                {
                    "properties": {
                        "uuid": document.uuid,
                        "id": document.id,
                        "color": document.color,
                        "direction": int(document.direction) + 1,
                        "distance": document.distance,
                        "distanceFromPoint": document.distance_from_point,
                        "lineId": document.line_id,
                    },
                    "geometry": {"coordinates": [document.coordinates_0, document.coordinates_1]},
                }
                for document in documents
            ]
        }

    def get_total_size(self) -> int:
        with self.engine.connect() as connection:
            size = connection.execute(
                text(
                    """
            SELECT pg_total_relation_size('documents')
            """
                )
            ).fetchall()

        return size[0][0]