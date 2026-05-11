import json
import logging
import psycopg2
from datetime import datetime
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os

load_dotenv()

TOPIC = "mpesa_transactions"
KAFKA_BROKER = "localhost:9092"
GROUP_ID = "mpesa-consumer-group"

# --- Dead Letter Logger Setup ---
# Any message that fails to insert into PostgreSQL gets written here
# instead of being silently dropped. This is the "dead letter log".
# logging.FileHandler writes to a file. The format includes timestamp,
# log level, and the message so failures are fully traceable.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("failed_transactions.log"),
        logging.StreamHandler()  # Also print to terminal
    ]
)
logging.getLogger("kafka").setLevel(logging.WARNING)  # It will suppress verbose Kafka logs
logger = logging.getLogger(__name__)

# --- Metrics Tracking ---
# Simple counters that accumulate as the consumer runs.
# When the consumer stops (Ctrl+C), we print a full summary.
metrics = {
    "total_received": 0,
    "total_stored": 0,
    "total_failed": 0,
    "by_type": {},          # Count per transaction type
    "latencies_ms": []      # List of latency values for averaging
}

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
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print(f"Consumer started. Listening to topic: {TOPIC}")
print("Press Ctrl+C to stop.\n")

def validate_message(txn):
    """
    Check that a message has all required fields and correct types.
    Returns True if valid, False if anything is missing or wrong.
    This prevents bad messages from crashing the INSERT statement.
    """
    required_fields = ["transaction_id", "sender", "receiver", "amount", "transaction_type", "timestamp"]
    for field in required_fields:
        if field not in txn:
            logger.error(f"Missing field '{field}' in message: {txn}")
            return False
    if not isinstance(txn["amount"], (int, float)):
        logger.error(f"Invalid amount type in message: {txn}")
        return False
    return True

def calculate_latency(timestamp_str):
    """
    Calculate how many milliseconds passed between when the producer
    generated the transaction and when the consumer received it.
    This is the real-time latency of the pipeline.
    """
    generated_at = datetime.fromisoformat(timestamp_str)
    received_at = datetime.now()
    delta = received_at - generated_at
    return delta.total_seconds() * 1000  # Convert to milliseconds

def print_metrics():
    """Print a full summary when the consumer stops."""
    print("\n" + "=" * 55)
    print("CONSUMER METRICS SUMMARY")
    print("=" * 55)
    print(f"  Total received  : {metrics['total_received']}")
    print(f"  Total stored    : {metrics['total_stored']}")
    print(f"  Total failed    : {metrics['total_failed']}")

    if metrics["latencies_ms"]:
        avg_latency = sum(metrics["latencies_ms"]) / len(metrics["latencies_ms"])
        print(f"  Avg latency     : {avg_latency:.2f} ms")

    print("\n  Breakdown by transaction type:")
    for txn_type, count in sorted(metrics["by_type"].items(), key=lambda x: x[1], reverse=True):
        print(f"    {txn_type:<22} : {count}")

    if metrics["total_failed"] > 0:
        print(f"\n  Failed messages logged to: failed_transactions.log")

    print("=" * 55)

try:
    for message in consumer:
        txn = message.value
        metrics["total_received"] += 1

        # Step 1: Validate the message before attempting any DB operation
        if not validate_message(txn):
            metrics["total_failed"] += 1
            logger.error(f"DEAD LETTER: Invalid message skipped: {txn}")
            continue  # Skip this message, move to the next one

        # Step 2: Calculate latency
        latency_ms = calculate_latency(txn["timestamp"])
        metrics["latencies_ms"].append(latency_ms)

        # Step 3: Update per-type counter
        txn_type = txn["transaction_type"]
        metrics["by_type"][txn_type] = metrics["by_type"].get(txn_type, 0) + 1

        # Step 4: Attempt database insert — wrapped in try/except
        # If the insert fails for any reason (DB down, constraint violation,
        # network issue), the error is logged and the pipeline keeps running.
        # Without this, one bad insert crashes the entire consumer.
        try:
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
            metrics["total_stored"] += 1

            print(f"Stored: {txn['transaction_id']} | "
                  f"{txn['sender']} → {txn['receiver']} | "
                  f"KES {txn['amount']:,.2f} | "
                  f"{txn_type} | "
                  f"Latency: {latency_ms:.2f}ms")

        except Exception as db_error:
            # Roll back the failed transaction so PostgreSQL stays in a clean state
            conn.rollback()
            metrics["total_failed"] += 1
            logger.error(f"DEAD LETTER: DB insert failed for {txn['transaction_id']}: {db_error}")
            logger.error(f"Failed message: {json.dumps(txn)}")

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    print_metrics()
    cursor.close()
    conn.close()
    consumer.close()