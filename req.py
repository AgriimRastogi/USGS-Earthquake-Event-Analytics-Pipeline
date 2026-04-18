import requests
import json
import os
from datetime import datetime

url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"

response = requests.get(url)

timestamp = datetime.now().strftime("%Y-%m-%d-%H")
file_name = f"EQdataBronze_{timestamp}.json"

with open(f"Bronze/{file_name}", "w") as file:
    json.dump(response.json(), file, indent= 4)

