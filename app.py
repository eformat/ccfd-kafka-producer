from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
import os
import random

producer = KafkaProducer(bootstrap_servers=[os.environ['BROKER_URL']])
topic = os.environ['KAFKA_TOPIC']

while True:
    data = {"id" : random.randint(1, 100000), "amount" : random.random() * 2500.0}
    payload = json.dumps(data)
    print(f"Sending: {payload}")
    producer.send(topic, payload.encode('utf-8'))
    time.sleep(1)
