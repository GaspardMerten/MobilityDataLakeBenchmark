import json
import os
from datetime import datetime

import requests
import geopandas as gpd

url = "https://api.mobilitytwin.brussels/stib/vehicle-position"


def get_data_at_timestamp(_timestamp):
    response = requests.get(url + f"?timestamp={int(_timestamp)}", headers={
        'Authorization': f'Bearer {os.environ["API_KEY"]}'
    })
    return response.json(), response.headers['X-Data-Timestamp']


def get_data():
    current_timestamp = datetime.now().timestamp()
    timestamp = current_timestamp - 4 * 60 * 60

    while timestamp < current_timestamp:
        data, timestamp_text = get_data_at_timestamp(timestamp)

        # Write to file
        with open(f'data/{timestamp_text}.json', 'w') as file:
            file.write(json.dumps(data))

        timestamp += 20


if __name__ == '__main__':
    get_data()
