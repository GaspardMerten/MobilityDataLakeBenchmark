import hashlib
from functools import lru_cache

import sqlalchemy
from sqlalchemy.orm import DeclarativeBase

from stores.base_store import BaseStore


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = 'document'
    timestamp = sqlalchemy.Column(sqlalchemy.TIMESTAMP, primary_key=True)
    id = sqlalchemy.Column(sqlalchemy.Integer, autoincrement=True, unique=True)
    geometry_0_min = sqlalchemy.Column(sqlalchemy.Float)
    geometry_0_max = sqlalchemy.Column(sqlalchemy.Float)
    geometry_1_min = sqlalchemy.Column(sqlalchemy.Float)
    geometry_1_max = sqlalchemy.Column(sqlalchemy.Float)
    properties_distance_min = sqlalchemy.Column(sqlalchemy.Float)
    properties_distance_max = sqlalchemy.Column(sqlalchemy.Float)


class Bus(Base):
    __tablename__ = 'bus'

    id = sqlalchemy.Column(sqlalchemy.SmallInteger, primary_key=True, autoincrement=True)
    color = sqlalchemy.Column(sqlalchemy.CHAR(7))
    direction = sqlalchemy.Column(sqlalchemy.Boolean)
    lineid = sqlalchemy.Column(sqlalchemy.CHAR(2))


class Item(Base):
    __tablename__ = 'item'

    hash = sqlalchemy.Column(sqlalchemy.CHAR(8), primary_key=True, index=True)
    properties_uuid = sqlalchemy.Column(sqlalchemy.Uuid)
    bus_id = sqlalchemy.Column(sqlalchemy.SmallInteger)


class Feature(Base):
    __tablename__ = 'feature'

    document_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, index=True)
    item = sqlalchemy.Column(sqlalchemy.CHAR(8), primary_key=True)
    geometry_coordinates_0 = sqlalchemy.Column(sqlalchemy.Integer)
    geometry_coordinates_1 = sqlalchemy.Column(sqlalchemy.Integer)
    properties_distance = sqlalchemy.Column(sqlalchemy.Float)
    properties_distance_from_point = sqlalchemy.Column(sqlalchemy.SmallInteger)
    properties_id = sqlalchemy.Column(sqlalchemy.SmallInteger)
    properties_pointid = sqlalchemy.Column(sqlalchemy.SmallInteger)


class PostgreSQLVelocitySplitStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5433')
        self.connection = self.engine.connect()
        self.connection.execute(sqlalchemy.text('commit'))
        try:
            self.connection.execute(sqlalchemy.text('CREATE DATABASE velocity_shift'))
        except sqlalchemy.exc.ProgrammingError:
            pass
        self.connection.close()
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5433/velocity_shift')
        self.session = sqlalchemy.orm.sessionmaker(bind=self.engine)()
        self.last_inserted_id = 0
        self.connection = self.engine.connect()

    def reset(self):
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

        self.last_inserted_id = 0

    @lru_cache
    def get_bus_id(self, color: str, direction: int, lineid: str):
        result = self.connection.execute(sqlalchemy.text(
            f'SELECT id FROM bus WHERE color = \'{color}\' AND direction = {bool(direction)} AND lineid = \'{lineid}\''
        )).fetchone()

        if result is None:
            self.session.add(Bus(color=color, direction=direction, lineid=lineid))
            self.session.commit()
            return self.get_bus_id(color, direction, lineid)

        return result[0]

    def hash_item(self, item: dict):
        x = hashlib.md5(
            f'{item["properties"]["uuid"]}{item["properties"]["color"]}{item["properties"]["direction"]}{item["properties"]["lineId"]}'.encode()
        ).hexdigest()

        return x[:8]

    def store_document(self, data: dict, timestamp: str):
        self.last_inserted_id += 1

        items = []

        min_geometry_0 = min([feature['geometry']['coordinates'][0] for feature in data['features']])
        max_geometry_0 = max([feature['geometry']['coordinates'][0] for feature in data['features']])
        min_geometry_1 = min([feature['geometry']['coordinates'][1] for feature in data['features']])
        max_geometry_1 = max([feature['geometry']['coordinates'][1] for feature in data['features']])

        for feature in data['features']:
            hashed = self.hash_item(feature)

            items.append({
                'hash': hashed,
                'properties_uuid': feature['properties']['uuid'],
                'bus_id': self.get_bus_id(feature['properties']['color'], feature['properties']['direction'] - 1,
                                            feature['properties']['lineId'])
            })

            self.session.add(Feature(
                document_id=self.last_inserted_id,
                item=hashed,
                geometry_coordinates_0=(feature['geometry']['coordinates'][0] - min_geometry_0) * 10 ** 8,
                geometry_coordinates_1=(feature['geometry']['coordinates'][1] - min_geometry_1) * 10 ** 8,
                properties_distance=feature['properties']['distance'],
                properties_distance_from_point=feature['properties']['distanceFromPoint'],
                properties_id=feature['properties']['id'],
                properties_pointid=feature['properties']['pointId']
            ))

        self.session.add(Document(timestamp=timestamp, id=self.last_inserted_id, geometry_0_min=min_geometry_0,
                                  geometry_0_max=max_geometry_0, geometry_1_min=min_geometry_1,
                                  geometry_1_max=max_geometry_1))
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
                    bus_id=item['bus_id']
                ))

        self.session.commit()

    def get_document(self, timestamp: str):
        document = self.connection.execute(sqlalchemy.text(
            f'SELECT * FROM document WHERE timestamp = \'{timestamp}\''
        )).fetchone()

        features = self.connection.execute(sqlalchemy.text(
            f'SELECT * FROM feature WHERE document_id = {document[1]}'
        )).fetchall()

        items = self.connection.execute(sqlalchemy.text(
            f'SELECT * FROM item WHERE hash IN ({",".join([f"\'{feature[1]}\'" for feature in features])})'
        )).fetchall()

        hash_to_item = {item[0]: item for item in items}

        buses = self.connection.execute(sqlalchemy.text(
            f'SELECT * FROM bus'
        )).fetchall()

        bus_id_to_bus = {bus[0]: bus for bus in buses}




        features = [
            {
                'geometry': {
                    'coordinates': [
                        feature[2] / 10 ** 8 + document[2],
                        feature[3] / 10 ** 8 + document[4]
                    ]
                },
                'properties': {
                    'distance': feature[4],
                    'distanceFromPoint': feature[5],
                    'id': feature[6],
                    'pointId': feature[7],
                    'uuid': hash_to_item[feature[1]][1],
                    'color': bus_id_to_bus[hash_to_item[feature[1]][2]][1],
                    'direction': int(bus_id_to_bus[hash_to_item[feature[1]][2]][2]) + 1,
                    'lineId': bus_id_to_bus[hash_to_item[feature[1]][2]][3],
                }
            }
            for feature in features
        ]

        return {'features': features, 'type': 'FeatureCollection'}

    def get_total_size(self):
        with self.engine.connect() as connection:
            result_1 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'document\')'))
            result_2 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'feature\')'))
            result_3 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'item\')'))
        return result_1.fetchone()[0] + result_2.fetchone()[0] + result_3.fetchone()[0]
