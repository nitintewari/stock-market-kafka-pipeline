"""
consumer.py — Stock Market Kafka Consumer + AWS S3 Sink
---------------------------------------------------------
Nitin Tewari | Real-Time Stock Market Data Engineering Project
--------------------------------------------------------------
Consumes stock market messages from Kafka topic, runs anomaly
detection on each message, and stores results to AWS S3 as JSON.

Usage:
    python consumer.py --broker localhost:9092 --topic stock-market --bucket your-s3-bucket
"""

import json
import boto3
import logging
import argparse
from kafka import KafkaConsumer
from datetime import datetime
from anomaly_detector import AnomalyDetector

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Default config ─────────────────────────────────────────────────────────────
DEFAULT_BROKER  = "localhost:9092"
DEFAULT_TOPIC   = "stock-market-data"
DEFAULT_BUCKET  = "your-s3-bucket-name"
DEFAULT_PREFIX  = "stock-data/"


def create_consumer(broker: str, topic: str) -> KafkaConsumer:
    """Initialize and return a KafkaConsumer instance."""
    return KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="stock-market-consumer-group",
    )


def upload_to_s3(s3_client, bucket: str, prefix: str, data: dict):
    """Upload a single record to AWS S3 as a JSON file."""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    key = f"{prefix}{timestamp}.json"

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )
    return key


def consume_and_store(broker: str, topic: str, bucket: str, prefix: str):
    """
    Consume messages from Kafka, run anomaly detection, store to S3.

    Args:
        broker : Kafka broker address
        topic  : Kafka topic to consume from
        bucket : AWS S3 bucket name
        prefix : S3 key prefix (folder path)
    """
    consumer = create_consumer(broker, topic)
    s3_client = boto3.client("s3")
    detector  = AnomalyDetector(window_size=50, z_threshold=2.5)

    logger.info(f"Connected to Kafka broker: {broker}")
    logger.info(f"Consuming from topic: {topic}")
    logger.info(f"Storing to S3 bucket: s3://{bucket}/{prefix}")
    logger.info("Listening for messages... Press Ctrl+C to stop.\n")

    messages_consumed = 0
    anomalies_detected = 0

    try:
        for message in consumer:
            record = message.value

            # ── Run anomaly detection ──────────────────────────────────────
            price = record.get("Close") or record.get("close") or record.get("price")
            if price:
                is_anomaly, z_score, reason = detector.check(float(price))
                record["anomaly_detected"] = is_anomaly
                record["z_score"]          = round(z_score, 4)
                record["anomaly_reason"]   = reason

                if is_anomaly:
                    anomalies_detected += 1
                    logger.warning(
                        f"🚨 ANOMALY DETECTED | Price: {price} | "
                        f"Z-Score: {z_score:.2f} | Reason: {reason}"
                    )

            # ── Upload to S3 ───────────────────────────────────────────────
            s3_key = upload_to_s3(s3_client, bucket, prefix, record)
            messages_consumed += 1

            if messages_consumed % 50 == 0:
                logger.info(
                    f"Messages consumed: {messages_consumed} | "
                    f"Anomalies: {anomalies_detected}"
                )

    except KeyboardInterrupt:
        logger.info(
            f"\nConsumer stopped.\n"
            f"Total consumed: {messages_consumed}\n"
            f"Total anomalies: {anomalies_detected}"
        )
    finally:
        consumer.close()
        logger.info("Consumer connection closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stock Market Kafka Consumer")
    parser.add_argument("--broker",  default=DEFAULT_BROKER,  help="Kafka broker address")
    parser.add_argument("--topic",   default=DEFAULT_TOPIC,   help="Kafka topic name")
    parser.add_argument("--bucket",  default=DEFAULT_BUCKET,  help="AWS S3 bucket name")
    parser.add_argument("--prefix",  default=DEFAULT_PREFIX,  help="S3 key prefix")
    args = parser.parse_args()

    consume_and_store(args.broker, args.topic, args.bucket, args.prefix)
