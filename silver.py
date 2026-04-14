import json
import os
import pandas as pd
from datetime import datetime

today_str = datetime.now().strftime("%Y-%m-%d")
file_name = f"EQdata_{today_str}.json"

with open(  f"Bronze/{file_name}", "r") as file:
    bronze_data = json.load(file)
    
Eartquakes= bronze_data.get("features",[])
flat_records = []

for EQ in Eartquakes:
    event_id = EQ.get("id")
    properties = EQ.get("properties", {})
    coordinates = EQ.get("geometry", {}).get("coordinates",[None,None,None])
    flat_record = {
        "event_id": event_id,
        "event_time_utc": properties.get("time"),
        "updated_time_utc": properties.get("updated"),
        "magnitude": properties.get("mag"),
        "magnitude_type": properties.get("magType"),
        "place": properties.get("place"),
        "tsunami_flag": properties.get("tsunami"),
        "significance_score": properties.get("sig"),
        "longitude": Eartquakes[0],
        "latitude": Eartquakes[1],
        "depth_km": Eartquakes[2]
    }
    flat_records.append(flat_record)