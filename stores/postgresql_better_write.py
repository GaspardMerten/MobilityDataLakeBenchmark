from sqlalchemy.orm import DeclarativeBase

from stores.base_store import BaseStore
import sqlalchemy


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = 'document'
    timestamp = sqlalchemy.Column(sqlalchemy.TIMESTAMP, primary_key=True)
    id = sqlalchemy.Column(sqlalchemy.Integer, autoincrement=True, unique=True)


class Feature(Base):
    __tablename__ = 'feature'

    document_id = sqlalchemy.Column(sqlalchemy.Integer,index=True, primary_key=True)
    geometry_coordinates_0 = sqlalchemy.Column(sqlalchemy.Float)
    geometry_coordinates_1 = sqlalchemy.Column(sqlalchemy.Float)
    id = sqlalchemy.Column(sqlalchemy.Uuid, primary_key=True)
    properties_color = sqlalchemy.Column(sqlalchemy.CHAR(7))
    properties_direction = sqlalchemy.Column(sqlalchemy.Boolean)
    properties_distance = sqlalchemy.Column(sqlalchemy.Float)
    properties_distance_from_point = sqlalchemy.Column(sqlalchemy.SmallInteger)
    properties_id = sqlalchemy.Column(sqlalchemy.SmallInteger)
    properties_lineid = sqlalchemy.Column(sqlalchemy.CHAR(2))
    properties_pointid = sqlalchemy.Column(sqlalchemy.SmallInteger)


class PostgreSQLPythonBetterWriteStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5433')
        self.connection = self.engine.connect()
        self.connection.execute(sqlalchemy.text('commit'))
        try:
            self.connection.execute(sqlalchemy.text('CREATE DATABASE better_write'))
        except sqlalchemy.exc.ProgrammingError:
            pass
        self.connection.close()
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5433/better_write')
        self.session = sqlalchemy.orm.sessionmaker(bind=self.engine)()
        self.last_inserted_id = 0
        self.connection = self.engine.connect()

    def reset(self):
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        self.last_inserted_id = 0

    def store_document(self, data: dict, timestamp: str):
        self.last_inserted_id += 1
        self.session.add(Document(timestamp=timestamp, id=self.last_inserted_id))
        self.session.commit()
        self.session.execute(sqlalchemy.text(
            f'INSERT INTO feature (document_id, geometry_coordinates_0, geometry_coordinates_1, id, properties_color, properties_direction, properties_distance, properties_distance_from_point, properties_id, properties_lineid, properties_pointid) VALUES '
            f'{",".join([f"({self.last_inserted_id}, {feature["geometry"]["coordinates"][0]}, {feature["geometry"]["coordinates"][1]}, \'{feature["id"]}\', \'{feature["properties"]["color"]}\', {bool(feature["properties"]["direction"] - 1)}, {feature["properties"]["distance"]}, {feature["properties"]["distanceFromPoint"]}, {feature["properties"]["id"]}, \'{feature["properties"]["lineId"]}\', {feature["properties"]["pointId"]})" for feature in data["features"]])}'
        ))
        self.session.commit()

    def get_document(self, timestamp: str):
        result = self.connection.execute(sqlalchemy.text(
            f'SELECT * FROM document WHERE timestamp = \'{timestamp}\''
        ))
        document_id = result.fetchone()[1]
        result = self.connection.execute(sqlalchemy.text(
            f'SELECT * FROM feature WHERE document_id = {document_id}'
        ))

        features = [{"type": "Feature", 'geometry': {'type': 'Point', 'coordinates': [row[1], row[2]]}, 'id': row[3],
                     'properties': {'color': row[4], 'direction': row[5] + 1, 'distance': row[6],
                                    'distanceFromPoint': row[7], 'id': row[8], 'lineId': row[9], 'pointId': row[10],
                                    'timestamp': timestamp, 'uuid': row[3]}} for row in result.fetchall()]

        return {'features': features, 'type': 'FeatureCollection'}

    def get_total_size(self):
        with self.engine.connect() as connection:
            result_1 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'document\')'))
            result_2 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'feature\')'))
        return result_1.fetchone()[0] + result_2.fetchone()[0]
