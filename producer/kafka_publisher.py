"""
Order Intelligence Platform — Phase 2
Kafka Publisher
===============
Reads events from the order_generator and publishes them to Kafka
using Avro serialisation via the Confluent Schema Registry.

Usage:
    python -m producer.kafka_publisher
    python -m producer.kafka_publisher --tps 100 --topic order-events
"""

import argparse
import os
import signal
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

from producer.order_generator import event_stream
from utils.logger import configure_logging, get_logger
from observability import telemetry as m9

configure_logging()
log = get_logger("kafka_publisher")

# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------

SCHEMA_PATH = Path(__file__).parent / "schemas" / "order_event.avsc"


def load_schema() -> str:
    with open(SCHEMA_PATH) as f:
        return f.read()


# ---------------------------------------------------------------------------
# Delivery report callback
# ---------------------------------------------------------------------------

_delivered = 0
_failed    = 0


def delivery_report(err, msg):
    global _delivered, _failed
    m9.producer_delivery(err, msg)
    if err:
        _failed += 1
        log.error("delivery_failed",
                  topic=msg.topic(),
                  partition=msg.partition(),
                  error=str(err))
    else:
        _delivered += 1
        if _delivered % 1000 == 0:
            log.info("delivery_milestone",
                     delivered=_delivered,
                     failed=_failed,
                     topic=msg.topic())


# ---------------------------------------------------------------------------
# Publisher class
# ---------------------------------------------------------------------------

class KafkaPublisher:
    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
    ):
        self.topic = topic

        # Schema Registry client
        sr_conf = {"url": schema_registry_url}
        sr_client = SchemaRegistryClient(sr_conf)

        # Avro serialiser — registers schema automatically on first publish
        schema_str = load_schema()
        self.avro_serializer = AvroSerializer(
            sr_client,
            schema_str,
            conf={"auto.register.schemas": True},
        )
        self.string_serializer = StringSerializer("utf_8")

        # Kafka producer config
        producer_conf = {
            "bootstrap.servers": bootstrap_servers,
            "acks":              "all",          # wait for all ISR replicas
            "retries":           5,
            "retry.backoff.ms":  200,
            "compression.type":  "snappy",
            "linger.ms":         10,             # small batching window
            "batch.size":        65536,
        }
        self.producer = Producer(producer_conf)
        log.info("publisher_ready",
                 topic=topic,
                 bootstrap=bootstrap_servers,
                 schema_registry=schema_registry_url)

    @m9.observed("producer_publish")
    def publish(self, event: dict) -> None:
        """Serialise and publish a single event dict."""
        order_id = event.get("order_id") or "unknown"
        try:
            self.producer.produce(
                topic=self.topic,
                key=self.string_serializer(
                    event.get("region", "unknown"),   # partition key = region
                    SerializationContext(self.topic, MessageField.KEY),
                ),
                value=self.avro_serializer(
                    event,
                    SerializationContext(self.topic, MessageField.VALUE),
                ),
                on_delivery=delivery_report,
            )
            # Poll to serve delivery callbacks without blocking
            m9.producer_queued(event.get("event_type"))
            self.producer.poll(0)
        except Exception as exc:
            m9.record("oip_producer_events_total", component="producer", result="enqueue_error")
            log.error("publish_error",
                      order_id=order_id,
                      event_type=event.get("event_type"),
                      error=str(exc))

    def flush(self, timeout: float = 10.0) -> None:
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            log.warning("flush_incomplete", remaining_messages=remaining)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Kafka Publisher")
    parser.add_argument("--tps",        type=int,   default=10,
                        help="New orders per second (default: 10)")
    parser.add_argument("--duration",   type=int,   default=0,
                        help="Run seconds, 0=infinite (default: 0)")
    parser.add_argument("--error-rate", type=float, default=0.05,
                        help="Fraction of malformed events (default: 0.05)")
    parser.add_argument("--topic",      type=str,
                        default=os.getenv("KAFKA_TOPIC_ORDERS", "order-events"),
                        help="Kafka topic name")
    args = parser.parse_args()

    bootstrap     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

    publisher = KafkaPublisher(
        bootstrap_servers=bootstrap,
        schema_registry_url=schema_registry,
        topic=args.topic,
    )

    # Graceful shutdown
    running = True
    def _shutdown(sig, frame):
        nonlocal running
        log.info("shutdown_signal_received", signal=sig)
        running = False
    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("publisher_starting",
             tps=args.tps,
             error_rate=args.error_rate,
             topic=args.topic,
             duration=args.duration if args.duration > 0 else "infinite")

    start   = time.time()
    total   = 0

    for event in event_stream(tps=args.tps, error_rate=args.error_rate):
        if not running:
            break
        if args.duration > 0 and (time.time() - start) >= args.duration:
            break

        publisher.publish(event)
        total += 1

        if total % 500 == 0:
            elapsed = time.time() - start
            log.info("progress",
                     total_events=total,
                     elapsed_s=round(elapsed, 1),
                     events_per_sec=round(total / elapsed, 1))

    publisher.flush()
    elapsed = time.time() - start
    log.info("publisher_finished",
             total_events=total,
             elapsed_s=round(elapsed, 1),
             delivered=_delivered,
             failed=_failed)


if __name__ == "__main__":
    main()
