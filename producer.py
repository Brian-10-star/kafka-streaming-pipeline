import json
import time
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer


# Common Kenyan first and last names
KENYAN_FIRST_NAMES = [
    "Stacy", "Brian", "Carlton", "Mwangi", "Achieng", "Odhiambo", "Chebet", "Nathan", "Susan", "Naomi",
    "Kipchoge", "Amina", "Hassan", "Fatuma", "Baraka", "Zawadi", "Jabali", "Jeff", "Risper",
    "Makena", "Kariuki", "Otieno", "Adhiambo", "Mutua", "Wambui", "Michael", "David", "Paul"
]

KENYAN_LAST_NAMES = [
    "Kamau", "Ochieng", "Kipkorir", "Muthoni", "Waweru", "Njoroge", "Auma", "Mbugua", "Nduati",
    "Owino", "Chepkemoi", "Mugo", "Gitonga", "Onyango", "Rotich", "Karanja", "Mwangi", "Mutinda",
    "Simiyu", "Njenga", "Ogola", "Kimani", "Cheruiyot", "Ndegwa", "Katiku", "Ondieki"
]

TOPIC = "mpesa_transactions"
KAFKA_BROKER = "localhost:9092"

# Transaction types that reflect real M-Pesa usage
TRANSACTION_TYPES = ["Send Money", "Buy Goods", "Pay Bill", "Withdraw", "Pochi la Biashara"]

def kenyan_name():
    """Generate a realistic Kenyan full name."""
    return f"{random.choice(KENYAN_FIRST_NAMES)} {random.choice(KENYAN_LAST_NAMES)}"

def generate_transaction():
    """Generate one realistic M-Pesa transaction as a dictionary."""
    return {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:10].upper()}",
        "sender": kenyan_name(),
        "receiver": kenyan_name(),
        "amount": round(random.uniform(50, 50000), 2),  # KES 50 to KES 50,000
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "timestamp": datetime.now().isoformat()
    }

# Connect to Kafka broker
# value_serializer converts our Python dictionary into JSON bytes
# because Kafka only transmits raw bytes, not Python objects
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"Producer started. Sending to topic: {TOPIC}")
print("Press Ctrl+C to stop.\n")

try:
    while True:
        transaction = generate_transaction()
        producer.send(TOPIC, value=transaction)
        print(f"Sent: {transaction['transaction_id']} | "
              f"{transaction['sender']} → {transaction['receiver']} | "
              f"KES {transaction['amount']:,.2f} | "
              f"{transaction['transaction_type']}")
        time.sleep(1)  # Send one transaction per second

except KeyboardInterrupt:
    print("\nProducer stopped.")
    producer.close()