import requests
import json
import os
from datetime import datetime, timedelta
import yaml

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Create Bronze directory if it doesn't exist
os.makedirs(config['directories']['bronze'].rstrip('/'), exist_ok=True)

now = datetime.now()
print("Starting 24-Hour Bronze Backfill...")

for i in range(24):
    # Calculate the exact hour window
    end_time = now - timedelta(hours=i)
    start_time = now - timedelta(hours=i+1)
    
    # Format times for the API (ISO 8601 format)
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")
    
    # Format the timestamp for your file naming convention
    file_timestamp = end_time.strftime("%Y-%m-%d-%H")
    file_name = f"EQdataBronze_{file_timestamp}.json"
    save_path = f"{config['directories']['bronze']}{file_name}"
    
    # The official USGS Custom Query API
    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_str}&endtime={end_str}"
    
    print(f"Fetching hour {i+1}/24: {file_timestamp}...", end=" ")
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        with open(save_path, "w") as file:
            json.dump(response.json(), file, indent=4)
        print("Success")
        
    except Exception as e:
        print(f"FAILED! Error: {e}")

print("Bronze Backfill Complete!")