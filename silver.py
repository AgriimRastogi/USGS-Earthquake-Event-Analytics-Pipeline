import json
import os
import pandas as pd
import reverse_geocoder
from datetime import datetime
import yaml
import logging
import sqlalchemy



with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

logging.basicConfig(filename=config['logging']['file_name'], level=logging.INFO, format='%(asctime)s | %(filename)s | %(levelname)s | %(message)s')
eng = sqlalchemy.create_engine(config['database']['connection_string'])
#get today's file
if __name__ == '__main__':  #We need this because of geocoder file handling issue in windows
    timestamp = datetime.now().strftime("%Y-%m-%d-%H")
    file_name = f"EQdataBronze_{timestamp}.json"

    latlon=[]
    if os.path.exists(f"{config['directories']['bronze']}{file_name}"):
        logging.info('bronze file found')
        with open(  f"{config['directories']['bronze']}{file_name}", "r") as file:
            bronze_data = json.load(file)

        Eartquakes= bronze_data.get("features",[])
        #store each EQ event as a dictonary in a list
        flat_records = []
        
        logging.info('flattening the Earthquake events in a parquet file....')
        for EQ in Eartquakes:
            event_id = EQ.get("id")
            properties = EQ.get("properties", {})
            coordinates = EQ.get("geometry", {}).get("coordinates",[None,None,None])
            lon=coordinates[0]
            lat=coordinates[1]
            latlon += [(lat,lon)]
            mg=properties.get("mag")
            
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
                "band": "none" if mg==None else "<2" if mg<2 else "2-4" if mg<4 else "4-6" if mg<6 else "6-8" if mg<8 else ">8",
            }        
            flat_records.append(flat_record)

        df = pd.DataFrame(flat_records)

        safe_latlon = [(lat if pd.notna(lat) else 0.0, lon if pd.notna(lon) else 0.0) for lat, lon in zip(df['latitude'], df['longitude'])]
        df['country'] =[x['cc'] for x in reverse_geocoder.search(safe_latlon,mode=1)] #returns list of dictionaries

        #convert time to proper format
        df["event_time_utc"] = pd.to_datetime(df["event_time_utc"], unit="ms")
        df["updated_time_utc"] = pd.to_datetime(df["updated_time_utc"], unit="ms")

        #there shuldnt be any dups, but if there are remove them
        df = df.drop_duplicates(subset=["event_id"], keep="last")
        df = df.dropna(subset=['event_id', 'latitude', 'longitude']) 
        
        #save flat file     
        silver_path=f"{config['directories']['silver']}EQdataSilver_{timestamp}.parquet"
        df.to_parquet(silver_path, index=False)
        df.to_sql('Silver_flattened_data',eng,if_exists='append',index=False)
        logging.info('Flat file saved successfully')
        logging.info('Silver Data Table Appended successfully')
    else:
        logging.error('Bronze file not Found')