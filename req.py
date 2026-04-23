import requests
import json
import os
from datetime import datetime
import yaml
import logging
import pandas as pd
import sqlalchemy

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

retry=config['retry']['num']

eng = sqlalchemy.create_engine(config['database']['connection_string'])
logging.basicConfig(filename=config['logging']['file_name'], level=logging.INFO, format='%(asctime)s | %(filename)s | %(levelname)s | %(message)s')


while(retry):
    url = config['api']['primary_url']
    response = requests.get(url)
    if response.ok:
        logging.info('Response succesful')
        timestamp = datetime.now().strftime("%Y-%m-%d-%H")
        file_name = f"EQdataBronze_{timestamp}.json"

        with open(f"{config['directories']['bronze']}{file_name}", "w") as file:
            json.dump(response.json(), file, indent= 4)
        
        bronze_df = pd.DataFrame(
            {'ingest_timestamp': [timestamp], 
            'raw_payload': [response.text]},)
        bronze_df.to_sql('bronze_raw_data',eng,if_exists='append',index=False)
        logging.info('Bronze Raw Data Table Appended successfully')
        break
    else:
        logging.error('Response failed, status code:', response.status_code)
        logging.info('Retrying...')
        retry-=1
        
if not retry:
    logging.info('All Retry Failed...')
    