from kafka import KafkaProducer, KafkaConsumer
import numpy as np
import os
import json
from time import time, sleep
import random

with open('/root/kafka-2.12/data.txt','r', encoding='utf-8') as f:
    words = f.read().splitlines()

#프로듀서 생성
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: json.dumps(x).encode("utf-8"))
count = 0
while True:
    index = random.randint(0,len(words)-1)
    producer.send("topic2", value=words[index])
    sleep(1)
    count += 1
    if count % 5 == 0 :
        print("topic2 producer -------------------")