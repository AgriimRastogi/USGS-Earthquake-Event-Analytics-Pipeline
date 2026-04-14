import json
import os
import pandas as pd
from datetime import datetime

#get today's file
today_str = datetime.now().strftime("%Y-%m-%d")
file_name = f"EQdataBronze_{today_str}.json"
with open(  f"Bronze/{file_name}", "r") as file:
    bronze_data = json.load(file)
    
Eartquakes= bronze_data.get("features",[])

#store each EQ event as a dictonary in a list
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
    
df = pd.DataFrame(flat_records)

#convert time to proper format
df["event_time_utc"] = pd.to_datetime(df["event_time_utc"], unit="ms")
df["updated_time_utc"] = pd.to_datetime(df["updated_time_utc"], unit="ms")

#there shuldnt be any dups, but if there are remove them
df = df.drop_duplicates(subset=["event_id"], keep="last")
    
#save flat file    
silver_path=f"Silver/EQdataSilver_{today_str}.parquet"
df.to_parquet(silver_path, index=False)