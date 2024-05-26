import sqlalchemy
from sqlalchemy.orm import DeclarativeBase

from stores.base_store import BaseStore


class MobilityDBStore(BaseStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5434')
        self.connection = self.engine.connect()
        self.connection.execute(sqlalchemy.text('commit'))
        try:
            self.connection.execute(sqlalchemy.text('CREATE DATABASE mobility'))
        except sqlalchemy.exc.ProgrammingError:
            pass
        self.connection.close()
        self.engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5434/mobility')
        self.session = sqlalchemy.orm.sessionmaker(bind=self.engine)()
        self.last_inserted_id = 0
        self.connection = self.engine.connect()
        self.count = 0
    def reset(self):
        self.connection.execute(sqlalchemy.text('DROP SCHEMA public CASCADE'))
        self.connection.execute(sqlalchemy.text('CREATE SCHEMA public'))
        self.count = 0
        self.connection.execute(sqlalchemy.text('commit'))

        # Create one table document with just a timestamp
        self.connection.execute(sqlalchemy.text('CREATE TABLE document (timestamp TIMESTAMP PRIMARY KEY)'))

        # Create one table feature with the following columns:
        # document_id: integer, foreign key to document.id
        # geometry_coordinates: tgeompoint, the temporal geometry of the feature
        # properties_color: text, the color of the feature
        # properties_direction: integer, the direction of the feature
        # properties_distance: tfloat, the distance of the feature
        # properties_distance_from_point: tint, the distance from the point of the feature
        # properties_id: tint integer, the temporal id of the feature
        # properties_lineid: text, the line id of the feature
        # properties_pointid: ttext, the temporal point id of the feature
        self.connection.execute(sqlalchemy.text('CREATE extension mobilitydb CASCADE'))

        self.connection.execute(sqlalchemy.text("""
            CREATE TABLE feature (
                geometry_coordinates tgeompoint,
                properties_color char(7),
                properties_uuid uuid,
                properties_direction smallint,
                properties_distance tfloat,
                properties_distance_from_point tint,
                properties_id tint,
                properties_lineid TEXT,
                properties_pointid ttext
            )
        """))

    def store_document(self, data: dict, timestamp: str):
        self.connection.execute(sqlalchemy.text(
            f'INSERT INTO document (timestamp) VALUES (\'{timestamp}\')'
        ))

        # Get all the existing uuids that are in the databsae and in the data
        existing_uuids = self.connection.execute(sqlalchemy.text(
            f'SELECT properties_uuid FROM feature WHERE properties_uuid IN ({",".join([f"\'{feature["properties"]["uuid"]}\'" for feature in data["features"]])})'
        )).fetchall()
        existing_uuids = {str(row[0]) for row in existing_uuids}
        print(len(data['features']))
        for feature in data['features']:
            if feature['properties']['uuid'] in existing_uuids:
                self.connection.execute(sqlalchemy.text(f"""
                    UPDATE feature
                    SET properties_distance = appendinstant(properties_distance,tfloat '{feature['properties']['distance']}@{timestamp}'),
                        properties_distance_from_point = appendinstant(properties_distance_from_point,tint '{feature['properties']['distanceFromPoint']}@{timestamp}'),
                        properties_id = appendinstant(properties_id,tint '{feature['properties']['id']}@{timestamp}'),
                        properties_pointid = appendinstant(properties_pointid,ttext '{feature['properties']['pointId']}@{timestamp}')
                    WHERE properties_uuid = '{feature['properties']['uuid']}'
                """))
            else:
                self.connection.execute(sqlalchemy.text(f"""
                INSERT INTO feature (geometry_coordinates, properties_color, properties_uuid, properties_direction, properties_distance, properties_distance_from_point, properties_id, properties_lineid, properties_pointid)
VALUES (
tgeompoint '[Point({feature['geometry']['coordinates'][0]} {feature['geometry']['coordinates'][1]})@{timestamp}]',
'{feature['properties']['color']}',
'{feature['properties']['uuid']}',
{feature['properties']['direction']},
tfloat '[{feature['properties']['distance']}@{timestamp}]',
tint '[{feature['properties']['distanceFromPoint']}@{timestamp}]',
tint '[{feature['properties']['id']}@{timestamp}]',
'{feature['properties']['lineId']}',
ttext '[{feature['properties']['pointId']}@{timestamp}]'
)
            """))

        self.connection.commit()

    def get_document(self, timestamp: str):
        features_rows = self.connection.execute(sqlalchemy.text(
                f"""
            SELECT
                properties_uuid,
                properties_color,
                properties_direction,
                getValue(atTimestamp(properties_distance, TIMESTAMP '{timestamp}')),
                getValue(atTimestamp(properties_distance_from_point, TIMESTAMP '{timestamp}')),
                getValue(atTimestamp(properties_id, TIMESTAMP '{timestamp}')),
                properties_lineid,
                getValue(atTimestamp(properties_pointid, TIMESTAMP '{timestamp}')),
                asText(atTimestamp(geometry_coordinates, TIMESTAMP '{timestamp}'))            
            FROM feature
            WHERE properties_pointid @> TIMESTAMP '{timestamp}'
        """

        )).fetchall()
        print(len(features_rows))
        return {'features': [
            {
                'geometry': {
                    'coordinates': row[8],
                    'type': 'Point'
                },
                'id': str(row[0]),
                'properties': {
                    'color': row[1],
                    'direction': row[2],
                    'distance': row[3],
                    'distanceFromPoint': row[4],
                    'id': row[5],
                    'lineId': row[6],
                    'pointId': row[7],
                    'timestamp': timestamp,
                    'uuid': str(row[0])
                }
            } for row in features_rows

        ], 'type': 'FeatureCollection'}

    def get_total_size(self):
        self.connection.execute(sqlalchemy.text('commit'))
        self.connection.execute(sqlalchemy.text('vacuum full public.feature'))
        with self.engine.connect() as connection:
            result_1 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'document\')'))
            result_2 = connection.execute(sqlalchemy.text('SELECT pg_relation_size(\'feature\')'))

        return result_1.fetchone()[0] + result_2.fetchone()[0]
