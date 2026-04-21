import json
import os
import pandas as pd
import reverse_geocoder
from datetime import datetime, timedelta
import yaml

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

if __name__ == '__main__':  # Required for Windows multiprocessing safety
    os.makedirs(config['directories']['silver'].rstrip('/'), exist_ok=True)
    now = datetime.now()
    
    print("Starting Silver Batch Processing...")

    # Loop through the last 24 hours just like the Gold script does
    for i in range(24):
        timestamp = (now - timedelta(hours=i)).strftime("%Y-%m-%d-%H")
        bronze_path = f"{config['directories']['bronze']}EQdataBronze_{timestamp}.json"
        silver_path = f"{config['directories']['silver']}EQdataSilver_{timestamp}.parquet"

        # If the bronze file doesn't exist, skip to the next hour
        if not os.path.exists(bronze_path):
            continue
            
        print(f"Processing {timestamp}...")

        with open(bronze_path, "r") as file:
            bronze_data = json.load(file)

        Earthquakes = bronze_data.get("features", [])
        
        # SAFETY NET: If there were 0 earthquakes this hour, skip to the next file!
        if len(Earthquakes) == 0:
            print(f"  -> No earthquakes found in {timestamp}. Skipping.")
            continue

        latlon = []
        flat_records = []
        
        for EQ in Earthquakes:
            event_id = EQ.get("id")
            properties = EQ.get("properties", {})
            coordinates = EQ.get("geometry", {}).get("coordinates", [None, None, None])
            
            lon = coordinates[0]
            lat = coordinates[1]
            latlon.append((lat, lon))
            
            mg = properties.get("mag") or 0.0
            
            flat_record = {
                "event_id": event_id,
                "event_time_utc": properties.get("time"),
                "updated_time_utc": properties.get("updated"),
                "magnitude": mg,
                "magnitude_type": properties.get("magType"),
                "place": properties.get("place"),
                "tsunami_flag": properties.get("tsunami"),
                "significance_score": properties.get("sig"),
                "longitude": lon,
                "latitude": lat,
                "depth_km": coordinates[2],
                "band": "none" if mg is None else "<2" if mg < 2 else "2-4" if mg < 4 else "4-6" if mg < 6 else "6-8" if mg < 8 else ">8",
            }        
            flat_records.append(flat_record)

        # Build DataFrame
        df = pd.DataFrame(flat_records)
        
        # Batch Geocode (mode=1 for speed)
        df['country'] = [x['cc'] for x in reverse_geocoder.search(latlon, mode=1)]

        # Convert time to proper format
        df["event_time_utc"] = pd.to_datetime(df["event_time_utc"], unit="ms")
        df["updated_time_utc"] = pd.to_datetime(df["updated_time_utc"], unit="ms")

        # Remove duplicates
        df = df.drop_duplicates(subset=["event_id"], keep="last")
            
        # Save flat file     
        df.to_parquet(silver_path, index=False)

    print("Silver Batch Processing Complete!")