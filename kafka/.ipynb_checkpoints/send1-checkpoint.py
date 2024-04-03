from kafka import KafkaProducer, KafkaConsumer
import numpy as np
import os
import json
from time import time, sleep

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: json.dumps(x).encode("utf-8"))

count = 0
while True:
    producer.send("topic1", value=np.random.normal())
    sleep(.5)
    count += 1
    if count % 10 == 0 :
        print("topic1 producer -------------------")