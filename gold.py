import os
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy
import yaml
import logging
import pymysql

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

logging.basicConfig(filename=config['logging']['file_name'], level=logging.INFO, format='%(asctime)s | %(filename)s | %(levelname)s | %(message)s')

eng = sqlalchemy.create_engine(config['database']['connection_string'])

The_df = pd.DataFrame()

now=datetime.now()
temp=[]
today = datetime.now().strftime("%Y-%m-%d")

logging.info('Appending last 24 hour Data into a single dataframe...')
for i in range(24):
    timestamp = (now - timedelta(hours=i)).strftime("%Y-%m-%d-%H")
    silver_path = f"{config['directories']['silver']}EQdataSilver_{timestamp}.parquet"
    if os.path.exists(silver_path):
        df = pd.read_parquet(silver_path)
        df['event_timestamp'] = timestamp
        temp+=[df]
        
if len(temp) > 0:
    logging.info('Successfully got the data, now Generating SQL Tables')
    The_df=pd.concat(temp,ignore_index=True)
    
    
    gold_daily_event_summary = The_df.groupby('event_timestamp').agg(
        event_count=('event_id', 'count'),
        avg_magnitude=('magnitude', 'mean'),
        max_magnitude=('magnitude', 'max'),
        min_magnitude=('magnitude', 'min'),
        significant_events=('significance_score', lambda x: (x >= 500).sum()),
        tsunami_events=('tsunami_flag', 'sum'),
        avg_depth_km = ('depth_km', 'mean')
    ).reset_index()
    
    gold_region_activity = The_df.groupby(['event_timestamp', 'country']).agg(
        event_count=('event_id', 'count'),
        avg_magnitude=('magnitude', 'mean'),
        max_magnitude=('magnitude', 'max'),
        min_magnitude=('magnitude', 'min'),
        significant_events=('significance_score', lambda x: (x >= 500).sum()),
        tsunami_events=('tsunami_flag', 'sum'),
        avg_depth_km = ('depth_km', 'mean')
    ).reset_index()
    
    gold_magnitude_distribution = The_df.groupby(['event_timestamp','band']).agg(
        event_count=('event_id', 'count'),
    ).reset_index()                                                                                                                                                                                                                                         

    gold_daily_event_summary.to_sql('gold_daily_event_summary',eng, if_exists='append',index=False)
    logging.info('generated gold_daily_event_summary')
    gold_region_activity.to_sql('gold_region_activity',eng, if_exists='append',index=False)
    logging.info('generated gold_region_activity')
    gold_magnitude_distribution.to_sql('gold_magnitude_distribution',eng, if_exists='append',index=False)
    logging.info('generated gold_magnitude_distribution')
else:
    logging.error('No Data found')