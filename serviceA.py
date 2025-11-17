# PRODUCER
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092','localhost:9093','localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {"msg": "Hello from Service A!"}
    producer.send("user-events", data)
    producer.flush()
    print("Sent:", data)
    time.sleep(1)
