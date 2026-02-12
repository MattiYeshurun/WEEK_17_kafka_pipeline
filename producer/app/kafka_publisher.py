import time
import json
from mongo_connection import collection, mongo_client
from confluent_kafka import Producer
from bson import json_util

def run_publisher():
    KAFKA_SERVERS = ['localhost:9092']
    BATCH_SIZE = 40 
    KAFKA_TOPIC = "store_events"
    
    producer = Producer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=json_util.default).encode('utf-8')
    )
    offset = 0
    while True:
        batch = list(collection.find().skip(offset).limit(BATCH_SIZE))
        
        if not batch:
            break

        for doc in batch:
            producer.send(KAFKA_TOPIC, value=doc)

            time.sleep(0.5)

        offset += BATCH_SIZE
        producer.flush()

    producer.close()
    mongo_client.close()






