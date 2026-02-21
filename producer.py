"""
producer.py — Stock Market Kafka Producer
------------------------------------------
Nitin Tewari | Real-Time Stock Market Data Engineering Project
--------------------------------------------------------------
"""

import json
import time
import argparse
import logging
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Default config ─────────────────────────────────────────────────────────────
DEFAULT_BROKER  = "localhost:9092"
DEFAULT_TOPIC   = "stock-market-data"
DEFAULT_CSV     = "indexProcessed.csv"
DEFAULT_DELAY   = 0.5  # seconds between messages


def create_producer(broker: str) -> KafkaProducer:
    """Initialize and return a KafkaProducer instance."""
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",                  # wait for all replicas to confirm
        retries=3,
        max_in_flight_requests_per_connection=1,
    )


def stream_stock_data(broker: str, topic: str, csv_path: str, delay: float):
    """
    Stream stock market data from CSV to Kafka topic.

    Args:
        broker   : Kafka broker address (host:port)
        topic    : Kafka topic name
        csv_path : Path to the stock market CSV file
        delay    : Seconds to wait between each message
    """
    logger.info(f"Loading dataset from: {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Dataset loaded — {len(df)} rows, {len(df.columns)} columns")

    producer = create_producer(broker)
    logger.info(f"Connected to Kafka broker: {broker}")
    logger.info(f"Streaming to topic: {topic}")
    logger.info("Starting data stream... Press Ctrl+C to stop.\n")

    messages_sent = 0

    try:
        while True:  # loop indefinitely to simulate continuous feed
            # Sample a random row to simulate live market data
            row = df.sample(1).iloc[0].to_dict()

            # Enrich with timestamp
            row["timestamp"] = datetime.utcnow().isoformat()
            row["event_id"]   = messages_sent

            producer.send(topic, value=row)
            messages_sent += 1

            if messages_sent % 50 == 0:
                logger.info(f"Messages sent: {messages_sent}")

            time.sleep(delay)

    except KeyboardInterrupt:
        logger.info(f"\nStream stopped. Total messages sent: {messages_sent}")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock Market Kafka Producer")
    parser.add_argument("--broker",  default=DEFAULT_BROKER,  help="Kafka broker address")
    parser.add_argument("--topic",   default=DEFAULT_TOPIC,   help="Kafka topic name")
    parser.add_argument("--csv",     default=DEFAULT_CSV,     help="Path to stock CSV file")
    parser.add_argument("--delay",   default=DEFAULT_DELAY,   type=float, help="Delay between messages (seconds)")
    args = parser.parse_args()

    stream_stock_data(args.broker, args.topic, args.csv, args.delay)
