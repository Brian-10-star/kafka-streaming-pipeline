import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id SERIAL PRIMARY KEY,
        transaction_id VARCHAR(50) UNIQUE NOT NULL,
        sender VARCHAR(100) NOT NULL,
        receiver VARCHAR(100) NOT NULL,
        amount NUMERIC(10, 2) NOT NULL,
        transaction_type VARCHAR(20) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        ingested_at TIMESTAMP DEFAULT NOW()
    );
""")

conn.commit()
cursor.close()
conn.close()

print("Table 'transactions' created successfully in streamingdb.")