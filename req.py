import requests
import json
import os
from datetime import datetime
import pandas as pd

url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

response = requests.get(url)

today_str = datetime.now().strftime("%Y-%m-%d")
os.makedirs("Bronze", exist_ok=True)
file_name = f"EQdataBronze_{today_str}.json"

with open(  f"Bronze/{file_name}", "w") as file:
    json.dump(response.json(), file, indent= 4)