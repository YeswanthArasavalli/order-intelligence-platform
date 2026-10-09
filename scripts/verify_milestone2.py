"""Bounded real Kafka/Registry/Spark/MinIO verification; retains all fixtures.

Run initial then restart in separate spark-submit processes with the same --topic
(a new unique topic for each verification). Dependencies/packages match M1.
"""

import argparse
import json
import os
import time

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pyspark.sql import functions as F

from producer import kafka_publisher
from producer.kafka_publisher import KafkaPublisher
from data_quality.validators import annotate
from scripts.milestone2_cases import controlled_cases
from streaming.bronze_writer import BRONZE_SCHEMA, bronze_path, read_kafka_stream, write_to_bronze
from streaming import silver_transformer
from streaming.silver_transformer import (
    dq_path, read_bronze_stream, silver_path, write_to_silver,
)
from streaming.spark_session import get_spark_session


def drain(stream, callback, checkpoint):
    query = (stream.writeStream.foreachBatch(callback).option("checkpointLocation", checkpoint)
             .trigger(availableNow=True).start())
    try:
        assert query.awaitTermination(180), f"Timed out: {checkpoint}"
        return sum(p["numInputRows"] for p in query.recentProgress)
    finally:
        if query.isActive:
            query.stop()


def consume(topic):
    consumer = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
                         "group.id": topic + "-verification", "enable.auto.commit": False})
    try:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=20)
        consumer.assign([TopicPartition(topic, 0, low)])
        result = []
        deadline = time.monotonic() + 30
        while len(result) < high - low and time.monotonic() < deadline:
            msg = consumer.poll(1)
            if msg is not None:
                assert msg.error() is None, msg.error()
                result.append(msg)
        assert len(result) == high - low
        return result
    finally:
        consumer.close()


def read_bronze(spark, topic):
    return spark.read.schema(BRONZE_SCHEMA).parquet(bronze_path("bronze", topic))


def copy_source_rows(spark, topic):
    # Simulate an append/checkpoint crash replay, retaining exactly the same source
    # coordinates/payload. This adds files; it does not reset any existing data.
    duplicates = read_bronze(spark, topic).filter("_kafka_offset in (0, 1)").dropDuplicates(
        ["_kafka_topic", "_kafka_partition", "_kafka_offset"],
    ).cache()
    assert duplicates.count() == 2
    try:
        (duplicates.write.mode("append").partitionBy("year", "month", "day", "hour", "region")
         .parquet(bronze_path("bronze", topic)))
    finally:
        duplicates.unpersist()


def inspect(spark, topic):
    bronze = read_bronze(spark, topic)
    silver = spark.read.parquet(silver_path("silver", topic))
    audit = spark.read.parquet(dq_path("silver", topic) + "batches/")
    runs = spark.read.parquet(dq_path("silver", topic) + "runs/")
    messages = consume(os.environ["KAFKA_TOPIC_DLQ"])
    dlq = [dict(json.loads(m.value()), dlq_partition=m.partition(), dlq_offset=m.offset()) for m in messages]
    assert silver.filter("not _dq_passed or not _quality_passed or unix_millis(event_ts) != timestamp").count() == 0
    assert silver.select("_event_id").distinct().count() == silver.count()
    assert len({m["event_id"] for m in dlq}) == len(dlq)
    rejected = {r._event_id for r in audit.filter("_route = 'DLQ'").select("_event_id").collect()}
    assert rejected == {m["event_id"] for m in dlq}
    assert rejected.isdisjoint({r._event_id for r in silver.select("_event_id").collect()})
    for m in dlq:
        assert m["failure_reasons"] and m["failed_at"] and m["ingested_at"] and m["bronze_file"]
        assert m["source_topic"] == topic and m["source_partition"] == 0
        if m["decode_error"]:
            assert m["raw_value_b64"] and "schema_drift" in m["failure_reasons"]
    stages = [r.event_type for r in silver.filter("order_id = 'm2-lifecycle'").select("event_type").collect()]
    assert sorted(stages) == sorted(["ORDER_CREATED", "ORDER_PAID", "ORDER_SHIPPED", "ORDER_DELIVERED",
                                    "ORDER_REFUNDED", "ORDER_REFUNDED"])
    return {
        "topic": topic, "bronze_rows": bronze.count(), "silver_rows": silver.count(),
        "dlq_rows": len(dlq), "audit_rows": audit.count(), "silver_schema": silver.schema.simpleString(),
        "bronze_schema": bronze.schema.simpleString(),
        "runs": [r.asDict() for r in runs.orderBy("completed_at").collect()],
        "decisions": [r.asDict() for r in audit.select(
            "order_id", "event_type", "timestamp", "event_ts", "_kafka_topic", "_kafka_partition",
            "_kafka_offset", "_event_id", "_failure_reason", "_route", "_decode_error", "_bronze_file",
        ).orderBy("_kafka_offset", "_route").collect()],
        "dlq": dlq,
        "silver_events": [r.asDict() for r in silver.select(
            "_event_id", "order_id", "event_type", "timestamp", "event_ts", "_silver_batch_id",
        ).orderBy("_kafka_offset").collect()],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--phase", choices=["initial", "restart", "recovery", "legacy"], required=True)
    args = parser.parse_args()
    topic = args.topic
    assert topic.startswith("order-events-m2-"), "Use an isolated M2 topic"
    os.environ["KAFKA_TOPIC_ORDERS"] = topic
    os.environ["KAFKA_TOPIC_DLQ"] = topic + "-dlq"
    checkpoint = f"s3a://checkpoints/{topic}"
    os.environ["SPARK_CHECKPOINT_DIR"] = checkpoint
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    registry = os.environ["SCHEMA_REGISTRY_URL"]
    spark = get_spark_session("OIP-Milestone2Verification")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    try:
        if args.phase == "legacy":
            counts = []

            def verify_legacy(batch, batch_id):
                checked = annotate(batch)
                assert checked.filter("_kafka_topic != 'order-events-modernization'").count() == 0
                assert checked.filter("_decode_error is not null").count() == 0
                assert checked.filter("not _dq_passed").count() == 0
                assert checked.filter("unix_millis(event_ts) != timestamp").count() == 0
                counts.append(checked.count())

            drain(read_bronze_stream(spark, "order-events-modernization"), verify_legacy,
                  checkpoint + "-legacy-read/silver/")
            assert sum(counts) == 16
            print("MILESTONE2_VERIFIED " + json.dumps({
                "phase": "legacy", "topic": "order-events-modernization", "count": sum(counts),
                "legacy_bronze_explicit_schema_and_topic_fallback": True,
                "dq_passed": 16, "timestamp_equivalence": True,
            }))
            return
        if args.phase == "recovery":
            source_topic = topic
            topic += "-recovery"
            os.environ["KAFKA_TOPIC_ORDERS"] = topic
            os.environ["KAFKA_TOPIC_DLQ"] = topic + "-dlq"
            os.environ["SPARK_CHECKPOINT_DIR"] = checkpoint + "-recovery"
            admin = AdminClient({"bootstrap.servers": bootstrap})
            admin.create_topics([NewTopic(topic + "-dlq", 1, 1)])[topic + "-dlq"].result()
            batch = (read_bronze(spark, source_topic).filter("_kafka_offset in (0, 1)")
                     .dropDuplicates(["_kafka_partition", "_kafka_offset"])
                     .withColumn("_kafka_topic", F.lit(topic))
                     .withColumn("_bronze_file", F.input_file_name()))
            original_sink = silver_transformer._write_to_dlq

            def injected_failure(*args):
                raise RuntimeError("Injected failure after Silver commit, before Kafka DLQ")

            silver_transformer._write_to_dlq = injected_failure
            try:
                try:
                    write_to_silver(batch, 0)
                    raise AssertionError("Fault injection did not run")
                except RuntimeError as exc:
                    assert "Injected failure" in str(exc)
            finally:
                silver_transformer._write_to_dlq = original_sink
            assert spark.read.parquet(silver_path("silver", topic)).count() == 1
            assert len(consume(topic + "-dlq")) == 0
            # A changed checkpoint must fail rather than bypass unfinished work.
            os.environ["SPARK_CHECKPOINT_DIR"] = checkpoint + "-wrong"
            try:
                write_to_silver(batch, 0)
                raise AssertionError("Unfinished work was bypassed")
            except RuntimeError as exc:
                assert "unfinished DQ batch" in str(exc)
            os.environ["SPARK_CHECKPOINT_DIR"] = checkpoint + "-recovery"
            write_to_silver(batch, 0)
            write_to_silver(batch, 0)  # completed callback is a no-op
            assert spark.read.parquet(silver_path("silver", topic)).count() == 1
            assert len(consume(topic + "-dlq")) == 1
            write_to_silver(batch.limit(0), 1)
            empty = spark.read.parquet(dq_path("silver", topic) + "runs/").filter("batch_id = 1").first()
            assert empty.input_records == 0 and empty.quality_score_pct == 0.0
            assert drain(read_bronze_stream(spark, topic + "-empty"), write_to_silver,
                         checkpoint + "-empty/silver/") == 0
            # Partial Parquet output is never treated as successful or erased.
            partial = dq_path("silver", topic) + "partial-fixture"
            fs, location = silver_transformer._filesystem(spark, partial)
            fs.mkdirs(location)
            try:
                silver_transformer._ready(spark, partial)
                raise AssertionError("Incomplete output was accepted")
            except RuntimeError as exc:
                assert "Incomplete Parquet output" in str(exc)
            print("MILESTONE2_VERIFIED " + json.dumps({
                "phase": "recovery", "topic": topic, "silver_rows": 1, "dlq_rows": 1,
                "fault_after_silver_commit_recovered": True, "completed_batch_noop": True,
                "unfinished_batch_checkpoint_guard": True, "partial_output_guard": True,
                "empty_quality_score": empty.quality_score_pct, "empty_source_startup": True,
            }))
            return
        if args.phase == "initial":
            admin = AdminClient({"bootstrap.servers": bootstrap})
            for future in admin.create_topics([
                NewTopic(topic, 1, 1), NewTopic(topic + "-dlq", 1, 1, config={
                    "retention.ms": "2592000000", "cleanup.policy": "delete", "compression.type": "gzip",
                }),
            ]).values():
                future.result()  # refuse reuse, never clear existing fixtures
            cases = controlled_cases(int(time.time() * 1000))
            publisher = KafkaPublisher(bootstrap, registry, topic)
            before = kafka_publisher._delivered
            for case in cases:
                publisher.publish(case["event"])
            publisher.flush()
            assert kafka_publisher._delivered - before == len(cases)
            raw_cases = [("bad-header", b"not-avro"), ("bad-payload", b"\0\0\0\0\1\xff")]
            alternate = AvroSerializer(SchemaRegistryClient({"url": registry}), json.dumps({
                "type": "record", "name": "MalformedFixture", "fields": [{"name": "order_id", "type": "string"}],
            }))
            raw_cases.append(("schema-invalid", alternate({"order_id": "m2-schema-invalid"},
                SerializationContext(topic + "-schema-fixture", MessageField.VALUE))))
            for _, raw in raw_cases:
                publisher.producer.produce(topic, value=raw, on_delivery=kafka_publisher.delivery_report)
            publisher.flush()
            assert kafka_publisher._delivered - before == len(cases) + len(raw_cases)
            wire = consume(topic)
            assert all(m.value()[0] == 0 for m in wire[:len(cases)])
            assert [m.offset() for m in wire] == list(range(len(wire)))

            drain(read_kafka_stream(spark, topic, bootstrap), write_to_bronze, checkpoint + "/bronze/")
            assert read_bronze(spark, topic).count() == 30
            copy_source_rows(spark, topic)
            drain(read_bronze_stream(spark, topic), write_to_silver, checkpoint + "/silver/")
            audit = spark.read.parquet(dq_path("silver", topic) + "batches/")
            for case, msg in zip(cases, wire):
                decision = audit.filter((F.col("_kafka_offset") == msg.offset()) & (F.col("_route") != "REPLAY")).first()
                assert decision._failure_reason.split(",") == (case["expected"] or [""]), case["name"]
            result = inspect(spark, topic)
            assert (result["bronze_rows"], result["silver_rows"], result["dlq_rows"], result["audit_rows"]) == (32, 11, 19, 32)
            result["producer_delivered"] = len(wire)
            result["cases"] = [{"name": c["name"], "source_offset": i, "expected": c["expected"]} for i, c in enumerate(cases)]
            result["raw_cases"] = [name for name, _ in raw_cases]
        else:
            before = inspect(spark, topic)
            bronze_input = drain(read_kafka_stream(spark, topic, bootstrap), write_to_bronze, checkpoint + "/bronze/")
            silver_input = drain(read_bronze_stream(spark, topic), write_to_silver, checkpoint + "/silver/")
            assert bronze_input == 0 and silver_input == 0
            copy_source_rows(spark, topic)  # cross-batch valid + invalid source replay
            publisher = KafkaPublisher(bootstrap, registry, topic)
            repeat = controlled_cases(int(time.time() * 1000))[0]["event"]
            publisher.publish(repeat)  # same order/type at a NEW offset must survive
            publisher.flush()
            drain(read_kafka_stream(spark, topic, bootstrap), write_to_bronze, checkpoint + "/bronze/")
            drain(read_bronze_stream(spark, topic), write_to_silver, checkpoint + "/silver/")
            result = inspect(spark, topic)
            assert (result["bronze_rows"], result["silver_rows"], result["dlq_rows"], result["audit_rows"]) == (35, 12, 19, 35)
            # Re-read all Bronze with a fresh checkpoint, retaining the same output.
            # Completed audit history prevents new Silver or DLQ writes.
            os.environ["SPARK_CHECKPOINT_DIR"] = checkpoint + "-fresh"
            drain(read_bronze_stream(spark, topic), write_to_silver, checkpoint + "-fresh/silver/")
            result = inspect(spark, topic)
            assert (result["silver_rows"], result["dlq_rows"], result["audit_rows"]) == (12, 19, 70)
            result["restart_no_input"] = {"bronze": bronze_input, "silver": silver_input}
            result["before_restart"] = {k: before[k] for k in ("bronze_rows", "silver_rows", "dlq_rows", "audit_rows")}
            result["fresh_checkpoint_replay_rows"] = 35
        jvm = spark.sparkContext._jvm
        result["checkpoints"] = []
        for suffix in ("/bronze/", "/silver/"):
            path = checkpoint + suffix
            fs = jvm.org.apache.hadoop.fs.Path(path).getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
            for marker in ("metadata", "offsets/0", "commits/0", "sources/0/0"):
                assert fs.exists(jvm.org.apache.hadoop.fs.Path(path + marker)), path + marker
            result["checkpoints"].append(path)
        result["phase"] = args.phase
        result["spark"] = spark.version
        result["timezone"] = spark.conf.get("spark.sql.session.timeZone")
        print("MILESTONE2_VERIFIED " + json.dumps(result, default=str, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
