import json
import os
import sqlite3
from confluent_kafka import Consumer, KafkaException

KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
    'group.id': 'user-consumer-group',
    'auto.offset.reset': 'earliest'
}

DB_NAME = 'my_db'
TOPICS = ['customers', 'orders']

def setup_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_name TEXT,
            amount REAL,
            FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
        )
    ''')
    
    conn.commit()
    return conn

def process_message(topic, data, cursor, conn):
    try:
        if topic == 'customers':
            cursor.execute(
                "INSERT OR REPLACE INTO customers (customer_id, name, email) VALUES (?, ?, ?)",
                (data['customer_id'], data['name'], data.get('email'))
            )
        elif topic == 'orders':
            cursor.execute(
                "INSERT OR REPLACE INTO orders (order_id, customer_id, product_name, amount) VALUES (?, ?, ?, ?)",
                (data['order_id'], data['customer_id'], data['product_name'], data.get('amount'))
            )
        conn.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()

def run_consumer():
    conn = setup_database()
    cursor = conn.cursor()
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(TOPICS)

    print("Consumer is running and listening for events...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue
            topic = msg.topic()
            data = json.loads(msg.value().decode('utf-8'))
            
            process_message(topic, data, cursor, conn)
            print(f"Processed event from topic '{topic}': {data}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    run_consumer()
