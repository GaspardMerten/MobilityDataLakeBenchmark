import hashlib

from sqlalchemy.orm import DeclarativeBase

from stores.base_store import BaseStore
import sqlalchemy


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = 'document'
    timestamp = sqlalchemy.Column(sqlalchemy.TIMESTAMP, primary_key=True)
    id = sqlalchemy.Column(sqlalchemy.Integer, autoincrement=True, unique=True)


class Item(Base):
    __tablename__ = 'item'

    hash = sqlalchemy.Column(sqlalchemy.CHAR(8), primary_key=True, index=True)
    properties_uuid = sqlalchemy.Column(sqlalchemy.Uuid)
    properties_id = sqlalchemy.Column(sqlalchemy.SmallInteger)
    properties_color = sqlalchemy.Column(sqlalchemy.CHAR(7))
    properties_direction = sqlalchemy.Column(sqlalchemy.Boolean)
    properties_lineid = sqlalchemy.Column(sqlalchemy.CHAR(2))


class Feature(Base):
    __tablename__ = 'feature'

    document_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, index=True)
    item = sqlalchemy.Column(sqlalchemy.CHAR(8), primary_key=True)
    geometry_coordinates_0 = sqlalchemy.Column(sqlalchemy.Float)
    geometry_coordinates_1 = sqlalchemy.Column(sqlalchemy.Float)
    properties_distance = sqlalchemy.Column(sqlalchemy.Float)
    properties_distance_from_point = sqlalchemy.Column(sqlalchemy.SmallInteger)
    properties_pointid = sqlalchemy.Column(sqlalchemy.SmallInteger)


class PostgreSQLVelocityStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5433')
        self.connection = self.engine.connect()
        self.connection.execute(sqlalchemy.text('commit'))
        try:
            self.connection.execute(sqlalchemy.text('CREATE DATABASE velocity'))
        except sqlalchemy.exc.ProgrammingError:
            pass
        self.connection.close()
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5433/velocity')
        self.session = sqlalchemy.orm.sessionmaker(bind=self.engine)()
        self.last_inserted_id = 0
        self.connection = self.engine.connect()

    def reset(self):
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

        self.last_inserted_id = 0

    def hash_item(self, item: dict):
        return hashlib.md5(
            f'{item["properties"]["uuid"]}{item["properties"]["color"]}{item["properties"]["direction"]}{item["properties"]["lineId"]}'.encode(
                'utf-8')).hexdigest()[:8]

    def store_document(self, data: dict, timestamp: str):
        self.last_inserted_id += 1
        self.session.add(Document(timestamp=timestamp, id=self.last_inserted_id))

        items = []

        for feature in data['features']:
            hashed = self.hash_item(feature)

            items.append({
                'hash': hashed,
                'properties_uuid': feature['properties']['uuid'],
                'properties_id': feature['properties']['id'],
                'properties_color': feature['properties']['color'],
                'properties_direction': feature['properties']['direction'] - 1,
                'properties_lineid': feature['properties']['lineId'],
            })

            self.session.add(Feature(
                document_id=self.last_inserted_id,
                item=hashed,
                geometry_coordinates_0=feature['geometry']['coordinates'][0],
                geometry_coordinates_1=feature['geometry']['coordinates'][1],
                properties_distance=feature['properties']['distance'],
                properties_pointid= feature['properties']['pointId'],
                properties_distance_from_point=feature['properties']['distanceFromPoint'],
            ))

        self.session.commit()
        # Query the database to see which hashes are not already in the database
        result = self.connection.execute(sqlalchemy.text(
            f'SELECT hash FROM item WHERE hash IN ({",".join([f"\'{item['hash']}\'" for item in items])})'
        ))
        hashes = [row[0] for row in result.fetchall()]

        for item in items:
            if item['hash'] not in hashes:
                self.session.add(Item(
                    hash=item['hash'],
                    properties_uuid=item['properties_uuid'],
                    properties_id=item['properties_id'],
                    properties_color=item['properties_color'],
                    properties_direction=item['properties_direction'],
                    properties_lineid=item['properties_lineid'],
                ))

        self.session.commit()

    def get_document(self, timestamp: str):
        document = self.connection.execute(sqlalchemy.text(
            f'SELECT * FROM document WHERE timestamp = \'{timestamp}\''
        )).fetchone()[1]

        features = self.connection.execute(sqlalchemy.text(
            f'SELECT * FROM feature WHERE document_id = {document}'
        )).fetchall()

        items = self.connection.execute(sqlalchemy.text(
            f'SELECT * FROM item WHERE hash IN ({",".join([f"\'{feature[1]}\'" for feature in features])})'
        )).fetchall()

        hash_to_item = {item[0]: item for item in items}

        features = [{"type": "Feature", 'geometry': {'type': 'Point', 'coordinates': [row[2] / 10 ** 8, row[3] / 10 ** 8]},
                     'id': str(hash_to_item[row[1]][1]),
                     'properties': {'color': hash_to_item[row[1]][3],
                                    'direction': int(hash_to_item[row[1]][4]) + 1, 'distance': row[4],
                                    'distanceFromPoint': row[5], 'id': str(hash_to_item[row[1]][1]),
                                    'lineId': hash_to_item[row[1]][5], 'pointId': hash_to_item[row[1]][5],
                                    'timestamp': timestamp, 'uuid': str(hash_to_item[row[1]][1])}} for row in
                    features]

        return {'features': features, 'type': 'FeatureCollection'}

    def get_total_size(self):
        with self.engine.connect() as connection:
            result_1 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'document\')'))
            result_2 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'feature\')'))
            result_3 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'item\')'))
        return result_1.fetchone()[0] + result_2.fetchone()[0] + result_3.fetchone()[0]
