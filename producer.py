import time
import json
from kafka import KafkaProducer
import random

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Simulated product data
products = ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Keyboard']
categories = ['Electronics', 'Gadgets', 'Accessories']

def generate_product_data():
    return {
        'product_id': random.randint(1000, 9999),
        'product_name': random.choice(products),
        'category': random.choice(categories),
        'price': round(random.uniform(20, 500), 2),
        'stock': random.randint(1, 1000),
        'date_added': time.strftime('%Y-%m-%d %H:%M:%S')
    }

# Send random product data every 2 seconds
while True:
    product_data = generate_product_data()
    print(f'Sending product: {product_data}')
    producer.send('product_topic', value=product_data)
    time.sleep(20)
