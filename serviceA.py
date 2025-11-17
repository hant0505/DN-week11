# PRODUCER
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send("user-events", {"msg": "Hello from A!"})
producer.flush()

print("A sent message!")
