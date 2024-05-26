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

    document_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('document.id'), primary_key=True)

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


class PostgreJSONAggSQLStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5433')
        self.connection = self.engine.connect()
        self.connection.execute(sqlalchemy.text('commit'))
        try:
            self.connection.execute(sqlalchemy.text('CREATE DATABASE store'))
        except sqlalchemy.exc.ProgrammingError:
            pass
        self.connection.close()
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5433/store')
        self.session = sqlalchemy.orm.sessionmaker(bind=self.engine)()
        self.last_inserted_id = 0

    def reset(self):
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        self.last_inserted_id = 0

    def store_document(self, data: dict, timestamp: str):
        self.last_inserted_id += 1
        self.session.add(Document(timestamp=timestamp, id=self.last_inserted_id))
        for feature in data['features']:
            self.session.add(Feature(
                document_id=self.last_inserted_id,
                geometry_coordinates_0=feature['geometry']['coordinates'][0],
                geometry_coordinates_1=feature['geometry']['coordinates'][1],
                id=feature['id'],
                properties_color=feature['properties']['color'],
                properties_direction=feature['properties']['direction'] - 1,
                properties_distance=feature['properties']['distance'],
                properties_distance_from_point=feature['properties']['distanceFromPoint'],
                properties_id=feature['properties']['id'],
                properties_lineid=feature['properties']['lineId'],
                properties_pointid=feature['properties']['pointId'],
            ))
        self.session.commit()

    def get_document(self, timestamp: str):
        with self.engine.connect() as connection:
            result = connection.execute(sqlalchemy.text(
                f'''
                SELECT jsonb_build_object(
                    'type', 'FeatureCollection',
                    'features', jsonb_agg(
                        jsonb_build_object(
                            'type', 'Feature',
                            'geometry', jsonb_build_object(
                                'type', 'Point',
                                'coordinates', jsonb_build_array(feature.geometry_coordinates_0, feature.geometry_coordinates_1)
                            ),
                            'id', feature.id,
                            'properties', jsonb_build_object(
                                'color', feature.properties_color,
                                'direction', feature.properties_direction::int + 1,
                                'distance', feature.properties_distance,
                                'distanceFromPoint', feature.properties_distance_from_point,
                                'id', feature.properties_id,
                                'lineId', feature.properties_lineid,
                                'pointId', feature.properties_pointId,
                                'uuid', feature.id,
                                'timestamp', '{timestamp}'
                            )
                        )
                    )
                ) AS feature_collection
                FROM feature
                JOIN document ON feature.document_id = document.id
                WHERE document.timestamp = '{timestamp}'
                '''
            ))
            return result.fetchone()[0]

    def get_total_size(self):
        with self.engine.connect() as connection:
            result_1 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'document\')'))
            result_2 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'feature\')'))
        return result_1.fetchone()[0] + result_2.fetchone()[0]
