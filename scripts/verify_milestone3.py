"""Real M3 stack verifier. Run initial, second, third, replay, replay, recovery.

Each invocation starts a separate Spark process. Never deletes fixture data.
"""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
import os
import time

from confluent_kafka.admin import AdminClient, NewTopic
from fastavro import schemaless_reader
from io import BytesIO

from producer import kafka_publisher
from producer.kafka_publisher import KafkaPublisher, load_schema
from scripts.milestone3_cases import batches, event
from scripts.verify_milestone2 import consume, read_bronze
from storage import gold_store
from storage.gold_store import current_manifest, gold_root, read_gold
from streaming.bronze_writer import read_kafka_stream, write_to_bronze
from streaming.gold_aggregator import (
    read_silver_stream, write_to_gold, write_verified_silver_to_gold,
)
from streaming.silver_transformer import read_bronze_stream, silver_path, write_to_silver
from streaming.spark_session import get_spark_session


def drain(stream, sink, checkpoint, progress):
    query = (stream.writeStream.foreachBatch(sink).option("checkpointLocation", checkpoint)
             .trigger(availableNow=True).start())
    try:
        assert query.awaitTermination(240), checkpoint
        progress.append({"checkpoint": checkpoint, "id": str(query.id), "run_id": str(query.runId),
                         "active": query.isActive, "exception": str(query.exception()),
                         "batches": [{"batchId": p["batchId"], "numInputRows": p["numInputRows"],
                                      "timestamp": p["timestamp"]} for p in query.recentProgress]})
        return sum(p["numInputRows"] for p in query.recentProgress)
    finally:
        if query.isActive:
            query.stop()


def pipeline(spark, topic, progress, *, gold_only=False, checkpoint=None):
    cp = checkpoint or f"s3a://checkpoints/{topic}"
    if not gold_only:
        drain(read_kafka_stream(spark, topic, os.environ["KAFKA_BOOTSTRAP_SERVERS"]),
              write_to_bronze, cp + "/bronze/", progress)
        drain(read_bronze_stream(spark, topic), write_to_silver, cp + "/silver/", progress)
    return drain(read_silver_stream(spark, topic), write_verified_silver_to_gold, cp + "/gold/", progress)


def publish(topic, events):
    publisher = KafkaPublisher(os.environ["KAFKA_BOOTSTRAP_SERVERS"], os.environ["SCHEMA_REGISTRY_URL"], topic)
    before = kafka_publisher._delivered
    for row in events:
        publisher.publish(row)
    publisher.flush()
    assert kafka_publisher._delivered - before == len(events)


def oracle(events):
    """Independent Python arithmetic + integer epoch window oracle for all metrics."""
    revenue, products = {}, {}
    for e in events:
        start = e["timestamp"] // 60000 * 60000
        key = (start, e["region"], e["currency"])
        r = revenue.setdefault(key, dict(order_count=0, failed_count=0, refunded_event_count=0,
            gross_revenue=Decimal(0), refund_amount=Decimal(0), cancellation_amount=Decimal(0)))
        amount = Decimal(str(e["amount"]))
        if e["event_type"] == "ORDER_PAID":
            r["order_count"] += 1
            r["gross_revenue"] += amount
            pkey = (e["timestamp"] // 300000 * 300000, e["product_id"], e["product_name"], e["currency"])
            p = products.setdefault(pkey, dict(units_sold=0, revenue=Decimal(0)))
            p["units_sold"] += 1
            p["revenue"] += amount
        if e["event_type"] == "ORDER_REFUNDED":
            r["refunded_event_count"] += 1
            r["refund_amount"] += amount
        if e["event_type"] == "ORDER_CANCELLED":
            r["failed_count"] += 1
            if json.loads(e["event_metadata"])["was_paid"]:
                r["cancellation_amount"] += amount
    for r in revenue.values():
        r["net_revenue"] = r["gross_revenue"] - r["refund_amount"] - r["cancellation_amount"]
        r["avg_order_value"] = ((r["gross_revenue"] / r["order_count"]).quantize(
            Decimal("0.00000001"), rounding=ROUND_HALF_UP) if r["order_count"] else None)
    return revenue, products


def inspect(spark, topic, expected):
    root = gold_root("gold", topic)
    manifest = current_manifest(spark, root)
    ledger = spark.read.parquet(manifest["tables"]["events"]["path"])
    assert ledger.count() == len(expected) == ledger.select("_event_id").distinct().count()
    want_revenue, want_products = oracle(expected)
    output = {}
    for name, want in (("revenue_by_minute", want_revenue), ("product_rankings", want_products)):
        rows = read_gold(spark, root, name).orderBy("window_start", "currency").collect()
        assert len(rows) == len(want), (name, len(rows), len(want))
        for row in rows:
            ms = int(row.window_start.replace(tzinfo=timezone.utc).timestamp() * 1000)
            key = ((ms, row.region, row.currency) if name == "revenue_by_minute"
                   else (ms, row.product_id, row.product_name, row.currency))
            assert key in want, key
            for metric, value in want[key].items():
                assert row[metric] == value, (name, key, metric, row[metric], value)
            assert (row.window_end - row.window_start).total_seconds() == (60 if name == "revenue_by_minute" else 300)
        output[name] = [r.asDict() for r in rows]
    sources = [r.asDict() for r in ledger.select(
        "_event_id", "_kafka_topic", "_kafka_partition", "_kafka_offset", "order_id", "event_type",
        "amount_decimal", "revenue_contribution", "event_ts", "currency",
    ).orderBy("_kafka_offset").collect()]
    dlq = [json.loads(m.value()) for m in consume(topic + "-dlq")]
    assert len(dlq) == 2
    assert {r["_event_id"] for r in sources}.isdisjoint({r["event_id"] for r in dlq})
    silver = spark.read.parquet(silver_path("silver", topic))
    return {"manifest": manifest, "outputs": output, "sources": sources,
            "bronze_count": read_bronze(spark, topic).count(), "silver_count": silver.count(),
            "gold_events": len(expected), "gold_rows": {name: len(rows) for name, rows in output.items()},
            "dlq_count": len(dlq), "dlq_source_offsets": [r["source_offset"] for r in dlq],
            "logical_digest": hashlib.sha256(json.dumps(output, default=str, sort_keys=True).encode()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--phase", choices=["initial", "second", "third", "replay", "recovery"], required=True)
    args = parser.parse_args()
    topic = args.topic
    assert topic.startswith("order-events-m3-")
    os.environ.update(KAFKA_TOPIC_ORDERS=topic, KAFKA_TOPIC_DLQ=topic + "-dlq",
                      SPARK_CHECKPOINT_DIR=f"s3a://checkpoints/{topic}")
    spark = get_spark_session("M3-" + args.phase)
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    progress = []
    try:
        if args.phase == "initial":
            admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
            for future in admin.create_topics([NewTopic(topic, 1, 1), NewTopic(topic + "-dlq", 1, 1)]).values():
                future.result()  # refuse destructive fixture reuse
            base = (int(time.time() * 1000) // 300000 - 30) * 300000
            # All-DLQ first batch: no Silver and therefore no Gold rows/snapshot.
            publish(topic, [event("bad", amount=-1, timestamp=base)])
            raw = kafka_publisher.KafkaPublisher(os.environ["KAFKA_BOOTSTRAP_SERVERS"], os.environ["SCHEMA_REGISTRY_URL"], topic)
            raw.producer.produce(topic, value=b"bad-avro", on_delivery=kafka_publisher.delivery_report)
            raw.flush()
            pipeline(spark, topic, progress)
            assert current_manifest(spark, gold_root("gold", topic)) is None
            assert read_gold(spark, gold_root("gold", topic), "revenue_by_minute").isEmpty()
            first, second, third = batches(base)
            publish(topic, first)
            pipeline(spark, topic, progress)
            expected = first
        else:
            wire = consume(topic)
            record = schemaless_reader(BytesIO(wire[2].value()[5:]), json.loads(load_schema()))
            base = record["timestamp"] - 300000
            first, second, third = batches(base)
            if args.phase in ("second", "third"):
                expected_before = first if args.phase == "second" else first + second
                before = inspect(spark, topic, expected_before)
                assert pipeline(spark, topic, progress) == 0  # separate Spark process, same checkpoints
                assert current_manifest(spark, gold_root("gold", topic)) == before["manifest"]
                publish(topic, second if args.phase == "second" else third)
                pipeline(spark, topic, progress)
                expected = expected_before + (second if args.phase == "second" else third)
            else:
                expected = first + second + third
                before = inspect(spark, topic, expected)
                if args.phase == "replay":
                    assert pipeline(spark, topic, progress) == 0
                    silver = spark.read.parquet(silver_path("silver", topic)).cache()
                    silver.count()
                    # Add a committed duplicate Silver file; Gold source must consume it once
                    # but its event identities must have no effect, even on repeated runs.
                    stamp = str(time.time_ns())
                    (silver.drop("silver_batch").write.mode("errorifexists")
                     .partitionBy("year", "month", "day", "event_type_lower", "region")
                     .parquet(silver_path("silver", topic) + "silver_batch=replay-" + stamp))
                    silver.unpersist()
                    pipeline(spark, topic, progress, gold_only=True)
                    # Direct completed callback retry and a new file-source checkpoint.
                    write_to_gold(spark.read.parquet(silver_path("silver", topic)), 0)
                    os.environ["SPARK_CHECKPOINT_DIR"] = f"s3a://checkpoints/{topic}-fresh-{stamp}"
                    pipeline(spark, topic, progress, gold_only=True,
                             checkpoint=os.environ["SPARK_CHECKPOINT_DIR"])
                else:
                    # Isolated root reuses verified source identities, fault before manifest.
                    original_root_fn = __import__("streaming.gold_aggregator", fromlist=["gold_root"])
                    source = spark.read.parquet(silver_path("silver", topic))
                    recovery_root = gold_root("gold", topic + "-recovery")
                    original = original_root_fn.gold_root
                    original_root_fn.gold_root = lambda *a: recovery_root
                    publish_manifest = gold_store._publish_manifest

                    def injected(*a):
                        raise RuntimeError("injected before manifest")

                    gold_store._publish_manifest = injected
                    try:
                        try:
                            write_to_gold(source, 0)
                            raise AssertionError("No injection")
                        except RuntimeError as exc:
                            assert "injected" in str(exc)
                        assert current_manifest(spark, recovery_root) is None
                        gold_store._publish_manifest = publish_manifest
                        write_to_gold(source, 0)
                        write_to_gold(source, 0)
                        assert current_manifest(spark, recovery_root)["version"] == 1
                        for metric in before["outputs"]:
                            got = read_gold(spark, recovery_root, metric)
                            want = read_gold(spark, gold_root("gold", topic), metric)
                            assert got.exceptAll(want).isEmpty() and want.exceptAll(got).isEmpty()
                    finally:
                        original_root_fn.gold_root = original
                        gold_store._publish_manifest = publish_manifest
                assert current_manifest(spark, gold_root("gold", topic)) == before["manifest"]
        result = inspect(spark, topic, expected)
        # Explicit expected business states, not only a generic arithmetic oracle.
        nets = defaultdict(Decimal)
        for row in result["outputs"]["revenue_by_minute"]:
            case = (int(row["window_start"].replace(tzinfo=timezone.utc).timestamp() * 1000) - base) // 300000
            if row["currency"] == "INR":
                nets[case] += row["net_revenue"]
        if args.phase in ("third", "replay", "recovery"):
            for case, value in {1: 100, 2: 70, 3: 50, 4: 0, 5: 0, 6: 60, 9: 75,
                                10: 85, 11: 140, 14: 70, 16: Decimal(".3"), 17: 10, 18: -30,
                                19: -7, 20: 0, 21: 0}.items():
                assert nets[case] == value, (case, nets[case], value)
        markers = []
        for layer in ("bronze", "silver", "gold"):
            cp = f"s3a://checkpoints/{topic}/{layer}/"
            fs, path = gold_store._filesystem(spark, cp)
            for marker in ("metadata", "offsets/0", "commits/0", "sources/0/0"):
                # Silver/Gold empty startup may create batch 0 only after data arrives.
                assert fs.exists(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(cp + marker)), cp + marker
            markers.append(cp)
        result.update(phase=args.phase, topic=topic, base_ms=base, checkpoints=markers,
                      query_progress=progress, spark=spark.version, timezone=spark.conf.get("spark.sql.session.timeZone"),
                      verified_at=datetime.now(timezone.utc).isoformat())
        print("MILESTONE3_VERIFIED " + json.dumps(result, default=str, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
