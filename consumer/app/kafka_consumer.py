from datetime import datetime, timezone
import json
import os
from confluent_kafka import Consumer
from pymongo import MongoClient

KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
    'group.id': 'user-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe([os.getenv('KAFKA_TOPIC', 'users.registered')])

print("Consumer started, waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Error:", msg.error())
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)
        print(f"📦 Received order: {order['quantity']} x {order['item']} from {order['user']}")
except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")

finally:
    consumer.close()



