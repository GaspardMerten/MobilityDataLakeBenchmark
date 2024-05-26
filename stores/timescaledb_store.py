from datetime import datetime
from typing import Dict

from sqlalchemy import create_engine, Column, String, Float, Boolean, DateTime, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from stores.base_store import BaseStore

Base = declarative_base()


class Document(Base):
    __tablename__ = "documents"

    timestamp = Column(DateTime, primary_key=True)
    uuid = Column(String, primary_key=True)
    id = Column(String)
    color = Column(String)
    direction = Column(Boolean)
    distance = Column(Float)
    distance_from_point = Column(Float)
    line_id = Column(String)
    coordinates_0 = Column(Float)
    coordinates_1 = Column(Float)


class TimeScaleDBTimeSeriesStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = create_engine("postgresql://postgres:postgres@localhost:5435")
        with self.engine.connect() as connection:
            # Create the database "store" if it doesn't exist
            connection.execute(text("commit"))
            try:
                connection.execute(text("create database store"))
            except:
                pass
            connection.execute(text("commit"))
            self.engine = create_engine(
                "postgresql://postgres:postgres@localhost:5435/store"
            )
            connection.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
        Base.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def reset(self):
        # Delete existing data and recreate the table
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

        with self.engine.connect() as connection:
            print(
                connection.execute(
                    text(
                        "SELECT create_hypertable('documents', by_range('timestamp'),migrate_data := true);"
                    )
                ).fetchall()
            )
            print(connection.execute(
                text(
                    "ALTER TABLE documents SET (timescaledb.compress, timescaledb.compress_segmentby = 'uuid');"
                )
            ))
            print(connection.execute(
                text(
                    "SELECT add_compression_policy('documents', INTERVAL '10 minutes');"
                )
            ).fetchall())
            connection.execute(text("commit"))

    def store_document(self, data: Dict, timestamp: str):
        documents = []
        for feature in data["features"]:
            documents.append(
                Document(
                    timestamp=datetime.fromisoformat(timestamp),
                    uuid=feature["properties"]["uuid"],
                    id=feature["properties"]["id"],
                    color=feature["properties"]["color"],
                    direction=bool(feature["properties"]["direction"] - 1),
                    distance=feature["properties"]["distance"],
                    distance_from_point=feature["properties"]["distanceFromPoint"],
                    line_id=feature["properties"]["lineId"],
                    coordinates_0=feature["geometry"]["coordinates"][0],
                    coordinates_1=feature["geometry"]["coordinates"][1],
                )
            )
        self.session.add_all(documents)
        self.session.commit()

    def get_document(self, timestamp: str) -> Dict:
        documents = (
            self.session.query(Document)
            .filter_by(timestamp=datetime.fromisoformat(timestamp))
            .all()
        )
        return {
            "features": [doc.__dict__ for doc in documents],
            "type": "FeatureCollection",
        }

    def get_total_size(self) -> int:
        import psycopg2
        conn = psycopg2.connect(
            dbname="store",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5435",
        )
        cur = conn.cursor()
        print("yo")
        cur.execute("select compress_chunk(show_chunks('documents') )")
        print(cur.fetchall())
        cur.execute("""
        SELECT pg_size_pretty(sum(after_compression_total_bytes)) as total,
       pg_size_pretty(sum(before_compression_total_bytes)) as before,
       pg_size_pretty(sum(after_compression_total_bytes) - sum(before_compression_total_bytes)) as saved
FROM chunk_compression_stats('documents'::regclass)""")
        print(cur.fetchall())
        cur.execute("SELECT hypertable_size('documents')")

        return cur.fetchone()[0]