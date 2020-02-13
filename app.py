from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import json
import time
import os
import random
import csv
from itertools import cycle
from time import sleep
import logging

BROKER_URL = os.environ['BROKER_URL']
KAFKA_TOPIC = os.environ['KAFKA_TOPIC']
MAX_RETRIES = 100

logging.info(f"Using broker at {BROKER_URL}")
logging.info(f"Using topic '{KAFKA_TOPIC}'")

def get_producer():
    retries = 0
    while retries < MAX_RETRIES:
        try:
            result = KafkaProducer(bootstrap_servers=[BROKER_URL])
            return result
        except KafkaError:
            logging.warning("No broker available. Retrying.")
            retries += 1
            sleep(2)        

producer = get_producer()

with open('data/creditcard-sample10k.csv') as f:
    reader = csv.reader(f)
    next(reader, None)  # skip the headers
    data = list(reader)

pool = cycle(data)

unique_id = 0
for transaction in pool:
    data = {
        "id" : unique_id, 
        "amount" : float(transaction[30]),
        "features" : [float(feature) for feature in transaction[2:30]]
    }
    unique_id += 1
    payload = json.dumps(data)
    print(f"Sending: {payload}")
    producer.send(KAFKA_TOPIC, payload.encode('utf-8'))
    time.sleep(1.0 + random.random()*2.0) # between 1 and 3 seconds 
