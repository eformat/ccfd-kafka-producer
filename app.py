from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
import os
import random
import csv
from itertools import cycle

producer = KafkaProducer(bootstrap_servers=[os.environ['BROKER_URL']])
topic = os.environ['KAFKA_TOPIC']

with open('data/data.csv') as f:
    reader = csv.reader(f)
    next(reader, None)  # skip the headers
    data = list(reader)

pool = cycle(data)

for transaction in pool:
    data = {"id" : int(transaction[0]), "amount" : float(transaction[4])}
    payload = json.dumps(data)
    print(f"Sending: {payload}")
    producer.send(topic, payload.encode('utf-8'))
    time.sleep(1.0 + random.random()*2.0) # between 1 and 3 seconds 
