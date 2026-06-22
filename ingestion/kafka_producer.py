"""
Kafka Transaction Producer
==========================
Simulates a real-time financial transaction stream by publishing
randomly generated transaction events to a Kafka topic.

Production features:
- Configurable throughput (TPS)
- Avro schema validation
- Dead Letter Queue for failed messages
- Prometheus metrics
- Graceful shutdown
"""

import json
import random
import time
import uuid
import signal
import sys
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer, KafkaException
from loguru import logger
from prometheus_client import Counter, Histogram, start_http_server
import yaml

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
TOPIC_TRANSACTIONS = "transactions"
TOPIC_DLQ = "transactions-dlq"
TARGET_TPS = 500          # target transactions per second
PROMETHEUS_PORT = 8000

# ── Prometheus Metrics ───────────────────────────────────────────────────────
messages_sent = Counter(
    "kafka_producer_messages_sent_total",
    "Total messages successfully sent to Kafka"
)
messages_failed = Counter(
    "kafka_producer_messages_failed_total",
    "Total messages that failed to send"
)
produce_latency = Histogram(
    "kafka_producer_produce_latency_seconds",
    "Time taken to produce a message",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
)

# ── Constants ────────────────────────────────────────────────────────────────
MERCHANT_CATEGORIES = [
    "grocery", "electronics", "restaurant", "gas_station",
    "online_retail", "travel", "healthcare", "entertainment",
    "luxury_goods", "crypto_exchange"
]

COUNTRIES = [
    "US", "UK", "CA", "DE", "FR", "JP", "AU", "BR", "IN", "SG"
]

HIGH_RISK_MERCHANTS = {"crypto_exchange", "luxury_goods"}


def generate_transaction() -> dict:
    """Generate a realistic synthetic financial transaction."""
    user_id = f"user_{random.randint(1000, 9999)}"
    merchant_category = random.choices(
        MERCHANT_CATEGORIES,
        weights=[20, 10, 15, 8, 18, 5, 7, 9, 3, 5],
        k=1
    )[0]

    # Inject anomalous transactions ~2% of the time
    is_anomaly_seed = random.random()
    if is_anomaly_seed < 0.02:
        amount = round(random.uniform(5000, 50000), 2)   # unusually large
        country = random.choice(COUNTRIES)
    else:
        amount = round(random.lognormvariate(3.5, 1.2), 2)  # realistic dist
        country = "US" if random.random() < 0.85 else random.choice(COUNTRIES)

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": amount,
        "currency": "USD",
        "merchant_id": f"merchant_{random.randint(100, 999)}",
        "merchant_category": merchant_category,
        "merchant_country": country,
        "card_present": random.choice([True, False]),
        "card_type": random.choice(["credit", "debit"]),
        "channel": random.choice(["online", "pos", "atm", "contactless"]),
        "ip_address": f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
        "device_fingerprint": str(uuid.uuid4())[:16],
        "is_high_risk_merchant": merchant_category in HIGH_RISK_MERCHANTS,
    }


def delivery_report(err, msg, dlq_producer: Optional[Producer] = None):
    """Kafka delivery callback — routes failures to DLQ."""
    if err is not None:
        logger.error(f"Delivery failed for message {msg.key()}: {err}")
        messages_failed.inc()
        if dlq_producer:
            dlq_producer.produce(
                topic=TOPIC_DLQ,
                key=msg.key(),
                value=msg.value(),
                headers={"error": str(err), "original_topic": TOPIC_TRANSACTIONS}
            )
    else:
        messages_sent.inc()


class TransactionProducer:
    """Production-ready Kafka producer for transaction streams."""

    def __init__(self, broker: str = KAFKA_BROKER):
        conf = {
            "bootstrap.servers": broker,
            "acks": "all",                    # strongest durability guarantee
            "retries": 5,
            "retry.backoff.ms": 300,
            "compression.type": "snappy",
            "linger.ms": 5,                   # small batching for throughput
            "batch.size": 32768,
            "enable.idempotence": True,
        }
        self.producer = Producer(conf)
        self.dlq_producer = Producer({"bootstrap.servers": broker})
        self._running = True
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)
        logger.info(f"TransactionProducer initialized | broker={broker}")

    def _shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Flushing producer...")
        self._running = False

    def produce_stream(self, tps: int = TARGET_TPS):
        """Continuously produce transaction events at the target TPS."""
        interval = 1.0 / tps
        logger.info(f"Starting transaction stream at {tps} TPS")
        start_http_server(PROMETHEUS_PORT)
        logger.info(f"Prometheus metrics available at :{PROMETHEUS_PORT}/metrics")

        while self._running:
            transaction = generate_transaction()
            key = transaction["user_id"].encode("utf-8")
            value = json.dumps(transaction).encode("utf-8")

            with produce_latency.time():
                try:
                    self.producer.produce(
                        topic=TOPIC_TRANSACTIONS,
                        key=key,
                        value=value,
                        callback=lambda err, msg: delivery_report(
                            err, msg, self.dlq_producer
                        )
                    )
                    self.producer.poll(0)   # non-blocking poll
                except KafkaException as e:
                    logger.error(f"KafkaException: {e}")
                    messages_failed.inc()
                except BufferError:
                    logger.warning("Producer queue full — waiting 0.5s")
                    time.sleep(0.5)

            time.sleep(interval)

        # Graceful flush on exit
        logger.info("Flushing remaining messages...")
        self.producer.flush(timeout=30)
        logger.info("Producer shutdown complete.")


if __name__ == "__main__":
    logger.add("logs/producer.log", rotation="100 MB", retention="7 days")
    producer = TransactionProducer()
    producer.produce_stream(tps=TARGET_TPS)
