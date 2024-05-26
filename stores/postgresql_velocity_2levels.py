import hashlib
import time

from sqlalchemy.orm import DeclarativeBase

from stores.base_store import BaseStore
import sqlalchemy


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = 'document'
    timestamp = sqlalchemy.Column(sqlalchemy.TIMESTAMP, primary_key=True)
    id = sqlalchemy.Column(sqlalchemy.Integer, autoincrement=True, unique=True)


class SubItem(Base):
    __tablename__ = 'subitem'
    hash = sqlalchemy.Column(sqlalchemy.CHAR(3), primary_key=True, index=True)
    properties_color = sqlalchemy.Column(sqlalchemy.CHAR(7))
    properties_direction = sqlalchemy.Column(sqlalchemy.Boolean)
    properties_lineid = sqlalchemy.Column(sqlalchemy.CHAR(2))


class Item(Base):
    __tablename__ = 'item'

    internal_id = sqlalchemy.Column(sqlalchemy.Boolean, autoincrement=True, unique=True)
    subitem_hash = sqlalchemy.Column(sqlalchemy.CHAR(3), primary_key=True)
    hash = sqlalchemy.Column(sqlalchemy.CHAR(6), primary_key=True, index=True)
    properties_uuid = sqlalchemy.Column(sqlalchemy.Uuid)
    properties_id = sqlalchemy.Column(sqlalchemy.SmallInteger)
    properties_pointid = sqlalchemy.Column(sqlalchemy.SmallInteger)


class Feature(Base):
    __tablename__ = 'feature'

    document_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True,
                                    index=True)
    item = sqlalchemy.Column(sqlalchemy.CHAR(6), primary_key=True)

    geometry_coordinates_0 = sqlalchemy.Column(sqlalchemy.Float)
    geometry_coordinates_1 = sqlalchemy.Column(sqlalchemy.Float)
    properties_distance = sqlalchemy.Column(sqlalchemy.Float)
    properties_distance_from_point = sqlalchemy.Column(sqlalchemy.SmallInteger)


class PostgreSQLVelocity2Store(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5433')
        self.connection = self.engine.connect()
        self.connection.execute(sqlalchemy.text('commit'))
        try:
            self.connection.execute(sqlalchemy.text('CREATE DATABASE velocity_2'))
        except sqlalchemy.exc.ProgrammingError:
            pass
        self.connection.close()
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5433/velocity_2')
        self.session = sqlalchemy.orm.sessionmaker(bind=self.engine)()
        self.last_inserted_id = 0
        self.connection = self.engine.connect()

    def reset(self):
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)

        # Remove the internal_id sequence
        self.connection.execute(sqlalchemy.text('DROP SEQUENCE IF EXISTS item_internal_id_seq'))

        self.last_inserted_id = 0

    def hash_item(self, item: dict):
        return hashlib.md5(
            f'{item["properties"]["uuid"]}{item["properties"]["pointId"]}'.encode()
        ).hexdigest()[:6]

    def hash_subitem(self, item: dict):
        return hashlib.md5(
            f'{item["properties"]["color"]}{item["properties"]["direction"]}{item["properties"]["lineId"]}'.encode()
        ).hexdigest()[:3]

    def store_document(self, data: dict, timestamp: str):
        self.last_inserted_id += 1
        self.session.add(Document(timestamp=timestamp, id=self.last_inserted_id))
        items = []
        subitems = []
        for feature in data['features']:
            hashed = self.hash_item(feature)
            sub_hashed = self.hash_subitem(feature)

            items.append({
                'hash': hashed,
                'subitem_hash': sub_hashed,
                'properties_uuid': feature['properties']['uuid'],
                'properties_id': feature['properties']['id'],
                'properties_pointid': feature['properties']['pointId']
            })

            subitems.append({
                'hash': sub_hashed,
                'properties_color': feature['properties']['color'],
                'properties_direction': feature['properties']['direction'],
                'properties_lineid': feature['properties']['lineId']
            })

            self.session.add(Feature(
                document_id=self.last_inserted_id,
                item=hashed,
                geometry_coordinates_0=feature['geometry']['coordinates'][0],
                geometry_coordinates_1=feature['geometry']['coordinates'][1],
                properties_distance=feature['properties']['distance'],
                properties_distance_from_point=feature['properties']['distanceFromPoint'],
            ))

        result = self.connection.execute(sqlalchemy.text(
            f'SELECT hash FROM subitem WHERE hash IN ({",".join([f"\'{subitem['hash']}\'" for subitem in subitems])})'
        ))

        hashes = [row[0] for row in result.fetchall()]
        added_hashes = set()
        for subitem in subitems:
            if subitem['hash'] not in hashes and subitem['hash'] not in added_hashes:
                self.session.add(SubItem(
                    hash=subitem['hash'],
                    properties_color=subitem['properties_color'],
                    properties_direction=subitem['properties_direction'] - 1,
                    properties_lineid=subitem['properties_lineid']
                ))
                added_hashes.add(subitem['hash'])

        # Query the database to see which hashes are not already in the database
        result = self.connection.execute(sqlalchemy.text(
            f'SELECT hash FROM item WHERE hash IN ({",".join([f"\'{item['hash']}\'" for item in items])})'
        ))

        hashes = [row[0] for row in result.fetchall()]

        for item in items:
            if item['hash'] not in hashes:
                self.session.add(Item(
                    hash=item['hash'],
                    subitem_hash=item['subitem_hash'],
                    properties_uuid=item['properties_uuid'],
                    properties_id=item['properties_id'],
                    properties_pointid=item['properties_pointid']
                ))

        self.session.commit()

    def get_document(self, timestamp: str):
        document = self.connection.execute(sqlalchemy.text(
            f'SELECT * FROM document WHERE timestamp = \'{timestamp}\''
        )).fetchone()[1]

        features = self.connection.execute(sqlalchemy.text(
            f'SELECT * FROM feature WHERE document_id = {document}'
        ))

        items = self.connection.execute(sqlalchemy.text(
            f'SELECT * FROM item WHERE hash IN ({",".join([f"\'{feature[1]}\'" for feature in features])})'
        ))

        subitems = self.connection.execute(sqlalchemy.text(
            f'SELECT * FROM subitem WHERE hash IN ({",".join([f"\'{item[1]}\'" for item in items])})'
        ))

        hash_to_item = {item[2]: item for item in items}
        hash_to_subitem = {subitem[0]: subitem for subitem in subitems}

        return {
            'features': [
                {
                    'geometry': {
                        'coordinates': [feature[2], feature[3]]
                    },
                    'id': hash_to_item[feature[1]][2],
                    'properties': {
                        'color': hash_to_subitem[hash_to_item[feature[1]][1]][1],
                        'direction': hash_to_subitem[hash_to_item[feature[1]][1]][2] + 1,
                        'distance': feature[4],
                        'distanceFromPoint': feature[5],
                        'id': hash_to_item[feature[1]][3],
                        'lineId': hash_to_subitem[hash_to_item[feature[1]][1]][3],
                        'pointId': hash_to_item[feature[1]][4],
                        'timestamp': timestamp,
                        'uuid': hash_to_item[feature[1]][2]
                    }
                } for feature in features
            ],
            'type': 'FeatureCollection'
        }

    def get_total_size(self):
        with self.engine.connect() as connection:
            result_1 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'document\')'))
            result_2 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'feature\')'))
            result_3 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'item\')'))
        return result_1.fetchone()[0] + result_2.fetchone()[0] + result_3.fetchone()[0]
