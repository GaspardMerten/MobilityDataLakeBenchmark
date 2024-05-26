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




class Feature(Base):
    __tablename__ = 'feature'

    properties_uuid = sqlalchemy.Column(sqlalchemy.Uuid, primary_key=True)
    document_id = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.Integer))
    properties_id = sqlalchemy.Column(sqlalchemy.SmallInteger)
    properties_color = sqlalchemy.Column(sqlalchemy.CHAR(7))
    properties_direction = sqlalchemy.Column(sqlalchemy.Boolean)
    properties_lineid = sqlalchemy.Column(sqlalchemy.CHAR(2))
    properties_points_id = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.Integer))
    properties_points_distance = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.Float))
    properties_points_distance_from_point = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.SmallInteger))
    geometry_points_coordinates_0 = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.Float))
    geometry_points_coordinates_1 = sqlalchemy.Column(sqlalchemy.ARRAY(sqlalchemy.Float))


class PostgreSQLVelocityInlineStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5433')
        self.connection = self.engine.connect()
        self.connection.execute(sqlalchemy.text('commit'))
        try:
            self.connection.execute(sqlalchemy.text('CREATE DATABASE velocity_inline'))
        except sqlalchemy.exc.ProgrammingError:
            pass
        self.connection.close()
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5433/velocity_inline')
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
            f'{item["properties"]["uuid"]}'.encode()
        ).hexdigest()[:8]

    def store_document(self, data: dict, timestamp: str):
        self.last_inserted_id += 1
        self.session.add(Document(timestamp=timestamp, id=self.last_inserted_id))

        uuids = [feature['properties']['uuid'] for feature in data['features']]
        uuids_in_db_and_in_document = self.session.execute(sqlalchemy.text(
            f'''
            SELECT properties_uuid
            FROM feature
            WHERE properties_uuid in ({','.join([f"'{uuid}'" for uuid in uuids])})
            '''
        )).fetchall()

        uuids_in_db = [str(uuid[0]) for uuid in uuids_in_db_and_in_document]
        uuids_not_in_db = [uuid for uuid in uuids if uuid not in uuids_in_db]
        for feature in data['features']:
            if feature['properties']['uuid'] in uuids_not_in_db:
                self.session.add(Feature(
                    properties_uuid=feature['properties']['uuid'],
                    document_id=[self.last_inserted_id],
                    properties_id=feature['properties']['id'],
                    properties_color=feature['properties']['color'],
                    properties_direction=feature['properties']['direction'] - 1,
                    properties_lineid=feature['properties']['lineId'],
                    properties_points_id=[feature['properties']['pointId']],
                    properties_points_distance=[feature['properties']['distance']],
                    properties_points_distance_from_point=[feature['properties']['distanceFromPoint']],
                    geometry_points_coordinates_0=[feature['geometry']['coordinates'][0]],
                    geometry_points_coordinates_1=[feature['geometry']['coordinates'][1]]
                ))
            else:
                self.session.execute(sqlalchemy.text(
                    f'''
                    UPDATE feature
                    SET 
                        document_id = array_append(document_id, {self.last_inserted_id}),
                        properties_points_id = array_append(properties_points_id, {feature['properties']['pointId']}),
                        properties_points_distance = array_append(properties_points_distance, {feature['properties']['distance']}),
                        properties_points_distance_from_point = array_append(properties_points_distance_from_point, {feature['properties']['distanceFromPoint']}),
                        geometry_points_coordinates_0 = array_append(geometry_points_coordinates_0, {feature['geometry']['coordinates'][0]}),
                        geometry_points_coordinates_1 = array_append(geometry_points_coordinates_1, {feature['geometry']['coordinates'][1]})
                        
                    WHERE properties_uuid = '{feature['properties']['uuid']}'
                    '''
                ))

        self.session.commit()
    def get_document(self, timestamp: str):
        raise NotImplementedError
    def get_total_size(self):
        with self.engine.connect() as connection:
            result_1 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'document\')'))
            result_2 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'feature\')'))
        return result_1.fetchone()[0] + result_2.fetchone()[0]
