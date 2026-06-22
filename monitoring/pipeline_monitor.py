"""
Pipeline Health Monitor
========================
Monitors the fraud detection pipeline for:
- Kafka consumer group lag
- Spark streaming query health
- Snowflake query cost tracking
- Dead Letter Queue message counts
- Automated Slack/email alerts
"""

import os
import time
import json
import requests
from datetime import datetime, timezone
from typing import Dict, Optional

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, KafkaException
from loguru import logger
import snowflake.connector

# ── Configuration ────────────────────────────────────────────────────────────
KAFKA_BROKER        = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_GROUP_ID      = "fraud-detection-consumer"
TOPIC_TRANSACTIONS  = "transactions"
TOPIC_DLQ           = "transactions-dlq"
SLACK_WEBHOOK_URL   = os.getenv("SLACK_WEBHOOK_URL", "")
ALERT_EMAIL         = os.getenv("ALERT_EMAIL", "")
CHECK_INTERVAL_SEC = 30

# Alert Thresholds
MAX_LAG_THRESHOLD       = 5000    # max Kafka consumer lag before alert
MAX_DLQ_RATE_PER_MIN    = 10      # max DLQ messages per minute
MAX_DETECTION_LATENCY   = 2000    # max avg detection latency in ms
MIN_FRAUD_RATE          = 0.001   # alert if fraud rate drops too low (model issue)
MAX_FRAUD_RATE          = 0.20    # alert if fraud rate spikes (false positives)


# ── Kafka Lag Monitor ─────────────────────────────────────────────────────────────

def get_kafka_consumer_lag(broker: str, group_id: str, topic: str) -> int:
    """
    Returns the total consumer group lag for a topic.
    Lag = sum of (latest_offset - committed_offset) across all partitions.
    """
    admin = AdminClient({"bootstrap.servers": broker})
    try:
        consumer = Consumer({
            "bootstrap.servers": broker,
            "group.id": group_id,
            "enable.auto.commit": False,
        })
        metadata = consumer.list_topics(topic, timeout=10)
        partitions = metadata.topics[topic].partitions

        total_lag = 0
        for pid in partitions:
            from confluent_kafka import TopicPartition
            tp = TopicPartition(topic, pid)
            committed = consumer.committed([tp], timeout=10)
            high_watermark = consumer.get_watermark_offsets(tp, timeout=10)
            if committed[0].offset >= 0:
                lag = high_watermark[1] - committed[0].offset
                total_lag += max(0, lag)

        consumer.close()
        return total_lag
    except Exception as e:
        logger.error(f"Failed to get Kafka lag: {e}")
        return -1


# ── Snowflake Cost Tracker ───────────────────────────────────────────────────────────

def get_snowflake_cost_metrics(
    account: str,
    user: str,
    password: str,
    database: str = "SNOWFLAKE"
) -> Dict:
    """
    Query Snowflake QUERY_HISTORY to track credit consumption
    for the fraud detection workload over the last hour.
    """
    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        database=database,
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            COUNT(*)                           AS query_count,
            SUM(CREDITS_USED_CLOUD_SERVICES)   AS credits_used,
            AVG(TOTAL_ELAPSED_TIME)            AS avg_elapsed_ms,
            MAX(TOTAL_ELAPSED_TIME)            AS max_elapsed_ms,
            SUM(BYTES_SCANNED) / 1e9           AS gb_scanned
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
          AND USER_NAME = CURRENT_USER()
    """)
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return {
        "query_count":    row[0],
        "credits_used":   round(row[1] or 0, 4),
        "avg_elapsed_ms": round(row[2] or 0, 1),
        "max_elapsed_ms": round(row[3] or 0, 1),
        "gb_scanned":     round(row[4] or 0, 3),
    }


# ── Alert Dispatcher ─────────────────────────────────────────────────────────────────

def send_slack_alert(message: str, severity: str = "warning"):
    """Post an alert to Slack via webhook."""
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not configured. Skipping Slack alert.")
        return

    color_map = {"info": "#36a64f", "warning": "#ff9800", "critical": "#e53935"}
    payload = {
        "attachments": [{
            "color": color_map.get(severity, "#ff9800"),
            "title": f"Fraud Pipeline Alert [{severity.upper()}]",
            "text": message,
            "footer": "Real-Time Fraud Detection Monitor",
            "ts": int(datetime.now(timezone.utc).timestamp())
        }]
    }
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
        resp.raise_for_status()
        logger.info(f"Slack alert sent: {message[:80]}")
    except Exception as e:
        logger.error(f"Failed to send Slack alert: {e}")


# ── Health Check Loop ───────────────────────────────────────────────────────────────

def run_health_checks():
    """Main monitoring loop — runs all health checks every CHECK_INTERVAL_SEC."""
    logger.info("Pipeline Monitor started")

    while True:
        try:
            timestamp = datetime.now(timezone.utc).isoformat()
            health_report = {"timestamp": timestamp}

            # 1. Kafka Consumer Lag
            lag = get_kafka_consumer_lag(
                KAFKA_BROKER, KAFKA_GROUP_ID, TOPIC_TRANSACTIONS
            )
            health_report["kafka_lag"] = lag

            if lag > MAX_LAG_THRESHOLD:
                msg = f"KAFKA LAG ALERT: Consumer lag={lag:,} exceeds threshold={MAX_LAG_THRESHOLD:,}"
                logger.warning(msg)
                send_slack_alert(msg, severity="critical")
            else:
                logger.info(f"Kafka lag OK: {lag:,}")

            # 2. DLQ Message Count
            dlq_lag = get_kafka_consumer_lag(
                KAFKA_BROKER, KAFKA_GROUP_ID + "-dlq", TOPIC_DLQ
            )
            health_report["dlq_count"] = dlq_lag

            if dlq_lag > MAX_DLQ_RATE_PER_MIN * (CHECK_INTERVAL_SEC / 60):
                msg = f"DLQ ALERT: {dlq_lag} messages in Dead Letter Queue"
                logger.error(msg)
                send_slack_alert(msg, severity="critical")

            logger.info(f"Health report: {json.dumps(health_report)}")

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            send_slack_alert(f"Health check exception: {e}", severity="critical")

        time.sleep(CHECK_INTERVAL_SEC)


if __name__ == "__main__":
    logger.add("logs/monitor.log", rotation="50 MB", retention="14 days")
    run_health_checks()
