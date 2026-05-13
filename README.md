# Real-Time M-Pesa Streaming Pipeline

A real-time streaming data pipeline built with Apache Kafka, Python, and PostgreSQL that simulates M-Pesa financial transactions flowing continuously from a producer through a message broker into a database. It demonstrates the full producer → broker → consumer architecture used in production fintech systems.

---

## Background

M-Pesa is East Africa's dominant mobile money platform, processing millions of transactions daily across Kenya, Tanzania, and beyond. In production, every Send Money, Pay Bill, Buy Goods, and Pochi la Biashara transaction must be captured, buffered, and persisted reliably, even under high load or partial system failure.

This project replicates that architecture at a small scale using Apache Kafka as the message broker, simulating one transaction per second with realistic Kenyan names and KES amounts, and storing every record in PostgreSQL with millisecond-level latency between generation and ingestion.

---

## Architecture
┌─────────────────┐        ┌──────────────────────────┐        ┌─────────────────┐        ┌──────────────────┐
│   producer.py   │──────▶│  Kafka Broker (Docker)    │──────▶│   consumer.py   │──────▶│   PostgreSQL     │
│                 │        │                          │        │                 │        │   streamingdb    │
│ Generates fake  │        │  Topic:                  │        │ Reads messages  │        │  transactions    │
│ M-Pesa          │        │  mpesa_transactions      │        │ the moment they │        │  table           │
│ transactions    │        │                          │        │ arrive          │        │                  │
│ every second    │        │  Managed by Zookeeper    │        │ Writes to DB    │        │                  │
└─────────────────┘        └──────────────────────────┘        └─────────────────┘        └──────────────────┘

The producer and consumer are completely decoupled — they never communicate directly. Kafka sits between them, guaranteeing message delivery, ordering, and fault tolerance.

---

## Key Concepts

| Concept | Role in This Project |
|---|---|
| **Kafka Broker** | The central message server — receives transactions from the producer and delivers them to the consumer |
| **Topic** | Named channel (`mpesa_transactions`) where all messages are published and consumed |
| **Producer** | `producer.py` — generates one M-Pesa transaction per second and publishes it to the topic |
| **Consumer** | `consumer.py` — subscribes to the topic and writes each message to PostgreSQL the moment it arrives |
| **Offset** | Kafka's bookmark — tracks which messages the consumer has already read so nothing is missed or duplicated on restart |
| **Consumer Group** | `mpesa-consumer-group` — identifies this consumer to Kafka for offset tracking |
| **Zookeeper** | Manages the Kafka broker — handles coordination, leader election, and cluster metadata |
| **Docker** | Runs both Kafka and Zookeeper in isolated containers — no manual installation required |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Message Broker | Apache Kafka 7.4.0 (Confluent) |
| Broker Manager | Apache Zookeeper 7.4.0 |
| Containerisation | Docker + Docker Compose |
| Producer / Consumer | Python 3.13 + kafka-python |
| Data Generation | faker + random (Kenyan names, KES amounts) |
| Database | PostgreSQL 18 |
| DB Driver | psycopg2-binary |
| Config Management | python-dotenv |

---

## Project Structure
kafka-streaming-pipeline/
│
├── docker-compose.yml   # Defines and starts Kafka + Zookeeper containers
├── producer.py          # Generates M-Pesa transactions and publishes to Kafka topic
├── consumer.py          # Reads from Kafka topic and writes to PostgreSQL in real time
├── db_setup.py          # Creates streamingdb database table on first run
├── requirements.txt     # Python dependencies
├── .env                 # Database credentials (not committed to version control)
├── .gitignore           # Excludes .env and pycache from Git
└── README.md

---

## Database Schema

```sql
CREATE TABLE transactions (
    id               SERIAL PRIMARY KEY,
    transaction_id   VARCHAR(50)    UNIQUE NOT NULL,  -- e.g. TXN-B70E1837AC
    sender           VARCHAR(100)   NOT NULL,          -- customer name
    receiver         VARCHAR(100)   NOT NULL,          -- customer name
    amount           NUMERIC(10,2)  NOT NULL,          -- KES 50.00 to KES 50,000.00
    transaction_type VARCHAR(20)    NOT NULL,          -- Send Money | Buy Goods | Pay Bill | Withdraw | Pochi la Biashara
    timestamp        TIMESTAMP      NOT NULL,          -- When the transaction was generated
    ingested_at      TIMESTAMP      DEFAULT NOW()      -- When the consumer wrote it to the DB
);
```

The gap between `timestamp` and `ingested_at` in production runs is consistently under 30 milliseconds — confirming true real-time ingestion.

---

## Sample Data
transaction_id  |      sender      |    receiver     |  amount   | transaction_type  |       timestamp
-----------------+------------------+-----------------+-----------+-------------------+------------------------
TXN-B70E1837AC  | Susan Mbugua     | Mutua Ondieki   | 41,426.08 | Send Money        | 2026-05-09 19:50:49
TXN-1BF8020106  | Achieng Ndegwa   | David Owino     | 48,101.74 | Withdraw          | 2026-05-09 19:50:50
TXN-A9BE29E02B  | David Ondieki    | Odhiambo Kamau  |  8,103.00 | Pochi la Biashara | 2026-05-09 19:50:51
TXN-31859FC281  | Kipchoge Simiyu  | Chebet Kimani   |  1,839.63 | Pochi la Biashara | 2026-05-09 19:50:52
TXN-D39DF8B6B7  | Brian Muthoni    | Mutua Auma      | 48,576.60 | Pochi la Biashara | 2026-05-09 19:50:53

---

## How to Run

### Prerequisites
- Docker Desktop installed and running
- PostgreSQL 18 running locally
- Python 3.13 with dependencies installed

### 1. Install Python dependencies
```bash
pip install kafka-python psycopg2-binary python-dotenv faker
```

### 2. Configure environment variables
Create a `.env` file in the project root:
```env
DB_HOST=your_postgres_host
DB_PORT=5432
DB_NAME=streamingdb
DB_USER=postgres
DB_PASSWORD=your_password
```

### 3. Start Kafka and Zookeeper
```bash
docker compose up -d
```
Verify both containers are running:
```bash
docker ps
```
You should see `kafka` and `zookeeper` both with status `Up`.

### 4. Set up the database table
```bash
python db_setup.py
```

### 5. Start the consumer (Terminal 1)
```bash
python consumer.py
```
The consumer will block and wait — it is now listening to the Kafka topic.

### 6. Start the producer (Terminal 2)
```bash
python producer.py
```
The producer begins sending one transaction per second. Switch back to Terminal 1 and watch each transaction appear within milliseconds of being sent.

### 7. Verify data in PostgreSQL
```sql
-- View latest transactions
SELECT * FROM transactions ORDER BY ingested_at DESC LIMIT 10;

-- Count by transaction type
SELECT transaction_type, COUNT(*) 
FROM transactions 
GROUP BY transaction_type 
ORDER BY COUNT(*) DESC;

-- Measure real-time latency
SELECT 
    transaction_id,
    EXTRACT(MILLISECONDS FROM (ingested_at - timestamp)) AS latency_ms
FROM transactions
ORDER BY ingested_at DESC
LIMIT 10;
```

### 8. Stop when done
```bash
# Stop producer and consumer with Ctrl+C in each terminal
# Then stop Docker containers
docker compose down
```

---

## Engineering Decisions

**Why Kafka instead of writing directly to PostgreSQL?**
A producer writing directly to a database creates tight coupling — if the database is slow or temporarily unavailable, the producer fails. Kafka acts as a buffer: the producer always succeeds by writing to Kafka, and the consumer processes at its own pace. This is the pattern used in every serious financial data platform.

**Why Docker for Kafka?**
Installing Kafka natively requires Java, manual configuration, and path management. Docker pulls a pre-built, pre-configured image and runs it in an isolated container. One command starts it, one command stops it — exactly how it runs in production cloud environments.

**Why `ON CONFLICT (transaction_id) DO NOTHING`?**
If the consumer crashes mid-run and restarts, Kafka replays unacknowledged messages from the last committed offset. Without conflict handling, replayed messages would insert duplicates. This guard ensures idempotent inserts regardless of how many times the consumer restarts.

**Why Kenyan names and M-Pesa transaction types?**
This project is built in Nairobi, Kenya. Using locally relevant data — Kenyan names, KES amounts, and real M-Pesa transaction categories including Pochi la Biashara — makes the project authentic and immediately relatable to any Kenyan engineering team reviewing the portfolio.

---

## What This Project Demonstrates

- **Streaming vs batch** — all previous projects in this portfolio move data in scheduled chunks. This project introduces continuous, event-driven data movement
- **Producer-consumer decoupling** — the two components share no code and no direct connection, communicating only through the Kafka topic
- **Offset-based fault tolerance** — the consumer can crash and restart without losing or duplicating any message
- **Containerised infrastructure** — production-grade Kafka setup using Docker Compose
- **Real-time latency** — sub-30ms gap between transaction generation and database persistence

---

## Author

**Brian Mbugua Chira**  
BSc Computer Science — Egerton University, Kenya (Expected 2028)  

[GitHub](https://github.com/Brian-10-star) · [LinkedIn](https://www.linkedin.com/in/mbuguabrian) · chirabrian1@gmail.com