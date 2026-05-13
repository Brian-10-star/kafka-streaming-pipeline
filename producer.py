import json
import time
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer

# Common first and last names
FIRST_NAMES = [
    "Stacy", "Brian", "Carlton", "Mwangi", "Achieng", "Odhiambo", "Chebet", "Nathan", "Susan", "Naomi", "Matthew", "Martin", "Gladys", "Stephen", "Grace", "Timothy",
    "Kipchoge", "Amina", "Hassan", "Fatuma", "Baraka", "Zawadi", "Jabali", "Jeff", "Risper",
    "Mary", "Kariuki", "Otieno", "Adhiambo", "Mutua", "Wambui", "Michael", "David", "Paul"
]

LAST_NAMES = [
    "Kamau", "Ochieng", "Kipkorir", "Muthoni", "Waweru", "Njoroge", "Auma", "Mbugua", "Nduati",
    "Owino", "Chepkemoi", "Mugo", "Gitonga", "Onyango", "Rotich", "Karanja", "Mwangi", "Mutinda",
    "Simiyu", "Njenga", "Ogola", "Kimani", "Cheruiyot", "Ndegwa", "Katiku", "Ondieki", "Onyango", "Mwangi", "Hakimi", "Makena"
]

TOPIC = "mpesa_transactions"
KAFKA_BROKER = "localhost:9092"

TRANSACTION_TYPES = ["Send Money", "Buy Goods", "Pay Bill", "Withdraw", "Pochi la Biashara"]

def customer_name():
    """Generating a realistic customer full name."""
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"

def generate_transaction():
    """Generate one realistic M-Pesa transaction as a dictionary."""
    return {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:10].upper()}",
        "sender": customer_name(),
        "receiver": customer_name(),
        "amount": round(random.uniform(50, 50000), 2),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "timestamp": datetime.now().isoformat()
    }

def get_send_interval():
    """
    Return a delay in seconds based on the current hour of day.
    This simulates realistic M-Pesa traffic patterns:
    - Late night (12am-5am): very low traffic, one transaction every 5-8 seconds
    - Early morning (5am-8am): picking up, one every 2-4 seconds
    - Business hours (8am-8pm): peak traffic, one every 0.3-1 second
    - Evening (8pm-12am): winding down, one every 1-3 seconds
    random.uniform adds natural variation so intervals are never perfectly fixed.
    """
    hour = datetime.now().hour

    if 0 <= hour < 5:
        # Late night — very quiet
        return random.uniform(5.0, 8.0)
    elif 5 <= hour < 8:
        # Early morning — picking up
        return random.uniform(2.0, 4.0)
    elif 8 <= hour < 20:
        # Business hours — peak M-Pesa activity
        return random.uniform(0.3, 1.0)
    else:
        # Evening — winding down
        return random.uniform(1.0, 3.0)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"Producer started. Sending to topic: {TOPIC}")
print("Traffic pattern: LOW (12am-5am) → MEDIUM (5am-8am) → HIGH (8am-8pm) → MEDIUM (8pm-12am)")
print("Press Ctrl+C to stop.\n")

try:
    while True:
        transaction = generate_transaction()
        producer.send(TOPIC, value=transaction)

        interval = get_send_interval()
        hour = datetime.now().hour

        # Label the current traffic period so you can see it in the terminal
        if 0 <= hour < 5:
            period = "LOW TRAFFIC"
        elif 5 <= hour < 8:
            period = "MEDIUM TRAFFIC"
        elif 8 <= hour < 20:
            period = "HIGH TRAFFIC"
        else:
            period = "MEDIUM TRAFFIC"

        print(f"[{period}] Sent: {transaction['transaction_id']} | "
              f"{transaction['sender']} → {transaction['receiver']} | "
              f"KES {transaction['amount']:,.2f} | "
              f"{transaction['transaction_type']} | "
              f"Next in {interval:.2f}s")

        time.sleep(interval)

except KeyboardInterrupt:
    print("\nProducer stopped.")
    producer.close()
