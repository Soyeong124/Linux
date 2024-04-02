# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %%
# !pip3 install kafka-python

# %%
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json


# %%
def read_dta_from_file(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


# %%
def create_topics(topics, bootstrap_servers = 'localhost:9092'):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topic_list = [ NewTopic(topic,num_partitions=1, replication_factor=1) for topic in topics ]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)


# %%
def producer_to_topics(data, topics, bootstrap_servers = 'localhost:9092'):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer = lambda x : json.dumps(x).encode("utf-8"))
    for topic in topics:
        producer.send(topic, value=data.get(topic))
    producer.flush()


# %%
def consumer_from_topics(topics, bootstrap_servers = 'localhost:9092'):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, auto_offset_reset = "earliest",
               value_deserializer = lambda x: json.loads(x.decode("utf-8")))
    consumer.subscribe(topics)
    for data in consumer:
        print(f'Topic : {data.topic}, Partition : {data.partition}, Offset : {data.offset}, Key : {data.key}, value : {data.value}')


# %%
file_path = 'data.json'
topics = ['p1','p2','p3']
data = read_dta_from_file(file_path)

#토픽 생성
# create_topics(topics)

producer_to_topics(data, topics)
consumer_from_topics(topics)

# %%
chmod 755 data.json

# %%
