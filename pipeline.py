import time
from datetime import datetime ,timedelta
import subprocess
import yaml
import logging
import os

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

logging.basicConfig(filename=config['logging']['file_name'], level=logging.INFO, format='%(asctime)s | %(filename)s | %(levelname)s | %(message)s')

def run_script(script_name):
    logging.info(f"=== Running {script_name} ===")
    result = subprocess.run(["python", script_name])
    
    if result.returncode == 0:
        logging.info(f"=== Successfully finished {script_name} ===")
    else:
        logging.error(f"=== CRITICAL: {script_name} failed! Error log below: ===")
        logging.error(result.stderr)

def delete_older(then):
    then=then.strftime("%Y-%m-%d-%H")
    path=config['directories']['bronze']
    for filename in os.listdir(path):
        timestamp = filename.split('_')[1]
        if timestamp<then:
            os.remove(os.path.join(path,filename))
            logging.info(f"=== deleted {filename} ===")
    
    path=config['directories']['silver']
    for filename in os.listdir(path):
        timestamp = filename.split('_')[1]
        if timestamp<then:
            os.remove(os.path.join(path,filename))
            logging.info(f"=== deleted {filename} ===")

#====================================
logging.info("=== Pipeline Orchestrator Started ===")    

last_hourly_run = None
last_daily_run = None

cutoff_days= 30 #sec

while(1):
    now = datetime.now()
    then = datetime.now() - timedelta(days=cutoff_days)
    
    current_hour = now.strftime("%Y-%m-%d-%H")
    current_day = now.strftime("%Y-%m-%d")
    if current_hour != last_hourly_run:
        run_script("req.py")
        run_script("silver.py")
        last_hourly_run = current_hour
        
    if current_day != last_daily_run:
        run_script("gold.py")
        last_daily_run = current_day
        delete_older(then)

    time.sleep(300)