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
import uuid

def consumer_from_topics(topics, bootstrap_servers='localhost:9092'):
    # 고유한 컨슈머 그룹 ID 생성
    # group_id = f"consumer-group-{uuid.uuid4()}"
    
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        # group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        print(f"Topic: {message.topic}, Group_id: {group_id}, Partition: {message.partition}, Offset: {message.offset}, Key: {message.key}, Value: {message.value}")


# %%
file_path = 'data2.json'
topics = ['p1','p2','p3']
data = read_dta_from_file(file_path)

#토픽 생성
# create_topics(topics)
producer_to_topics(data, topics)
consumer_from_topics(topics)

# %%
import threading

# 데이터와 토픽 읽기
file_path = 'data2.json'
topics = ['p1', 'p2', 'p3']
data = read_dta_from_file(file_path)

# 프로듀서 작업을 수행하는 함수
def run_producer(data, topics):
    # 여기서 토픽 생성이 필요하다면 주석을 해제하세요
    # create_topics(topics)
    producer_to_topics(data, topics)

# 컨슈머 작업을 수행하는 함수
def run_consumer(topics):
    consumer_from_topics(topics)

# 컨슈머 스레드 생성 및 시작
consumer_thread = threading.Thread(target=run_consumer, args=(topics,))
consumer_thread.start()

# 프로듀서 스레드 생성 및 시작
producer_thread = threading.Thread(target=run_producer, args=(data, topics))
producer_thread.start()

# 필요하다면 스레드들이 종료될 때까지 기다립니다.
producer_thread.join()
consumer_thread.join()

# %%
data

# %%
