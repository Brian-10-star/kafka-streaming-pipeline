import json
import psycopg2
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

load_dotenv()

TOPIC = "mpesa_transactions"
KAFKA_BROKER = "localhost:9092"
GROUP_ID = "mpesa-consumer-group"

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)
cursor = conn.cursor()

# Connect to Kafka topic
# value_deserializer is the reverse of the producer's serializer:
# it converts the raw bytes back into a Python dictionary
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",  # If starting fresh, read from the very first message
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print(f"Consumer started. Listening to topic: {TOPIC}")
print("Press Ctrl+C to stop.\n")

try:
    for message in consumer:
        txn = message.value  # The deserialized Python dictionary

        cursor.execute("""
            INSERT INTO transactions 
                (transaction_id, sender, receiver, amount, transaction_type, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING;
        """, (
            txn["transaction_id"],
            txn["sender"],
            txn["receiver"],
            txn["amount"],
            txn["transaction_type"],
            txn["timestamp"]
        ))
        conn.commit()

        print(f"Stored: {txn['transaction_id']} | "
              f"{txn['sender']} → {txn['receiver']} | "
              f"KES {txn['amount']:,.2f} | "
              f"{txn['transaction_type']}")

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    cursor.close()
    conn.close()
    consumer.close()