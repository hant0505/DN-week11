#CONSUMER
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "user-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id="service-b"
)

print("B waiting...")

for message in consumer:
    print("Received:", message.value)
