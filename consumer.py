import json
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine

# Connect to Kafka
consumer = KafkaConsumer(
    'product_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# PostgreSQL connection
engine = create_engine('postgresql://user:password@localhost:5455/products_db')

# Read and process Kafka messages
for message in consumer:
    print(f"Received message: {message.value}")
    
    # Load message into a DataFrame
    df = pd.DataFrame([message.value])

    # 2️⃣ Transformation: Apply data cleaning logic
    # Add a new column for 'profit margin' (20% of price)
    df['profit_margin'] = df['price'] * 0.2

    # Filter products where stock is greater than 100
    filtered_df = df[df['stock'] > 100]

    # Only write to PostgreSQL if there is data after filtering
    if not filtered_df.empty:
        # 3️⃣ Load to PostgreSQL
        try:
            filtered_df.to_sql('products', engine, if_exists='append', index=False)
            print(f"Data written to PostgreSQL: \n{filtered_df}")
        except Exception as e:
            print(f"Error writing to PostgreSQL: {e}")
    else:
        print(f"No data to write - record filtered out due to stock <= 100: {df}")
