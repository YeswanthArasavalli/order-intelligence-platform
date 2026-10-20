"""
Order Intelligence Platform — Phase 3
Kafka Topic Configuration & Admin Client
=========================================
Defines all topic configurations and provides a one-shot admin
client that creates topics idempotently (safe to re-run).

Usage:
    python -m kafka.topic_config          # create all topics
    python -m kafka.topic_config --list   # list existing topics
    python -m kafka.topic_config --delete # delete and recreate (dev only)
"""

import argparse
import os
import sys

from confluent_kafka.admin import AdminClient, NewTopic

from utils.logger import configure_logging, get_logger

configure_logging()
log = get_logger("kafka_admin")

# ---------------------------------------------------------------------------
# Topic definitions
# Partitioning key: order-events uses region (8 partitions for regional parallelism)
#                   payment-events uses order_id (4 partitions)
#                   DLQ single partition — low volume, ordered audit log
# ---------------------------------------------------------------------------

TOPIC_CONFIGS = [
    {
        "name":              "order-events",
        "num_partitions":    8,
        "replication_factor": 1,          # 1 for dev, 3 for prod
        "config": {
            "retention.ms":        str(7 * 24 * 60 * 60 * 1000),   # 7 days
            "cleanup.policy":      "delete",
            "compression.type":    "snappy",
            "min.insync.replicas": "1",
        },
    },
    {
        "name":              "payment-events",
        "num_partitions":    4,
        "replication_factor": 1,
        "config": {
            "retention.ms":        str(7 * 24 * 60 * 60 * 1000),
            "cleanup.policy":      "delete",
            "compression.type":    "snappy",
        },
    },
    {
        "name":              "order-events-dlq",
        "num_partitions":    1,
        "replication_factor": 1,
        "config": {
            "retention.ms":        str(30 * 24 * 60 * 60 * 1000),  # 30 days
            "cleanup.policy":      "delete",
            "compression.type":    "gzip",
        },
    },
    {
        "name":              "alerts-output",
        "num_partitions":    1,
        "replication_factor": 1,
        "config": {
            "retention.ms":        str(14 * 24 * 60 * 60 * 1000),  # 14 days
            "cleanup.policy":      "delete",
        },
    },
]


# ---------------------------------------------------------------------------
# Admin functions
# ---------------------------------------------------------------------------

def get_admin_client(bootstrap_servers: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def create_topics(
    bootstrap_servers: str | None = None,
    dry_run: bool = False,
) -> None:
    """Create the configured topics using the explicit or environment bootstrap."""
    bootstrap_servers = bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    admin = get_admin_client(bootstrap_servers)

    # Fetch existing topics
    metadata = admin.list_topics(timeout=10)
    existing = set(metadata.topics.keys())

    to_create = []
    for cfg in TOPIC_CONFIGS:
        if cfg["name"] in existing:
            log.info("topic_already_exists", topic=cfg["name"])
        else:
            to_create.append(
                NewTopic(
                    cfg["name"],
                    num_partitions=cfg["num_partitions"],
                    replication_factor=cfg["replication_factor"],
                    config=cfg.get("config", {}),
                )
            )
            log.info("topic_queued_for_creation",
                     topic=cfg["name"],
                     partitions=cfg["num_partitions"])

    if not to_create:
        log.info("all_topics_exist", count=len(TOPIC_CONFIGS))
        return

    if dry_run:
        log.info("dry_run_mode_no_changes_made")
        return

    futures = admin.create_topics(to_create)
    for topic_name, future in futures.items():
        try:
            future.result()
            log.info("topic_created", topic=topic_name)
        except Exception as exc:
            log.error("topic_creation_failed", topic=topic_name, error=str(exc))
            sys.exit(1)


def list_topics(bootstrap_servers: str) -> None:
    admin = get_admin_client(bootstrap_servers)
    metadata = admin.list_topics(timeout=10)
    print(f"\n{'Topic':<35} {'Partitions':>10} {'Leader':>8}")
    print("-" * 58)
    for name, meta in sorted(metadata.topics.items()):
        if name.startswith("_"):        # skip internal topics
            continue
        partitions = len(meta.partitions)
        leader = meta.partitions[0].leader if meta.partitions else -1
        print(f"{name:<35} {partitions:>10} {leader:>8}")
    print()


def delete_topics(bootstrap_servers: str) -> None:
    """Dev-only: delete all OIP topics. Never run in production."""
    admin    = get_admin_client(bootstrap_servers)
    names    = [c["name"] for c in TOPIC_CONFIGS]
    futures  = admin.delete_topics(names, operation_timeout=30)
    for name, future in futures.items():
        try:
            future.result()
            log.info("topic_deleted", topic=name)
        except Exception as exc:
            log.warning("topic_delete_warning", topic=name, error=str(exc))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Topic Admin")
    parser.add_argument("--bootstrap",
                        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--list",      action="store_true", help="List all topics")
    parser.add_argument("--delete",    action="store_true", help="Delete OIP topics (dev only)")
    parser.add_argument("--dry-run",   action="store_true", help="Show what would be created")
    args = parser.parse_args()

    if args.list:
        list_topics(args.bootstrap)
    elif args.delete:
        print("WARNING: Deleting all OIP topics. Press Ctrl+C within 5s to abort.")
        import time
        time.sleep(5)
        delete_topics(args.bootstrap)
        create_topics(args.bootstrap)
    else:
        create_topics(args.bootstrap, dry_run=args.dry_run)
