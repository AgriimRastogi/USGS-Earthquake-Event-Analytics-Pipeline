import requests
import json
import os
from datetime import datetime
import yaml
import logging

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

logging.basicConfig(filename=config['logging']['file_name'], level=logging.INFO, format='%(asctime)s | %(filename)s | %(levelname)s | %(message)s')
url = config['api']['primary_url']

response = requests.get(url)

if response.ok:
    logging.info('Response succesful')
    timestamp = datetime.now().strftime("%Y-%m-%d-%H")
    file_name = f"EQdataBronze_{timestamp}.json"

    with open(f"{config['directories']['bronze']}{file_name}", "w") as file:
        json.dump(response.json(), file, indent= 4)
else:
    logging.error('Response failed, status code:', response.status_code)
    