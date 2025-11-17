#CONSUMER
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'user-events',
    bootstrap_servers=['localhost:9092','localhost:9093','localhost:9094'],
    auto_offset_reset='earliest',
    group_id='service-b',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Service B listening...")

for msg in consumer:
    print("Received:", msg.value)
