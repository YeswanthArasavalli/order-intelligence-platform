"""Bronze → durable DQ outcomes → Silver / Kafka DLQ.

Single writer per topic/output. Checkpointed batches and source-coordinate replay
are durable; Kafka DLQ delivery is at-least-once across a process crash. See
data_quality/CONTRACT.md for the contract and recovery limitations.
"""

import hashlib
import json
import os

from pyspark.sql import DataFrame, Window, functions as F

from data_quality.validators import FAIL_COLUMNS, annotate, compute_quality_score
from streaming.bronze_writer import BRONZE_SCHEMA, bronze_path
from streaming.spark_session import get_spark_session
from utils.logger import get_logger
from observability import telemetry as m9

log = get_logger("silver_transformer")


def silver_path(bucket: str = "silver", topic: str = "order-events") -> str:
    return f"s3a://{bucket}/{topic}/"


def dq_path(bucket: str = "silver", topic: str = "order-events") -> str:
    return f"s3a://{bucket}/_dq/{topic}/"


def _filesystem(spark, path):
    location = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    return location.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), location


def _exists(spark, path):
    fs, location = _filesystem(spark, path)
    return fs.exists(location)


def _ready(spark, path):
    if _exists(spark, path.rstrip("/") + "/_SUCCESS"):
        return True
    if _exists(spark, path):
        raise RuntimeError(f"Incomplete Parquet output; recover without resetting data: {path}")
    return False


def _write_once(df, path, partitions=()):
    if not _ready(df.sparkSession, path):
        writer = df.write.mode("errorifexists").option("compression", "zstd")
        if partitions:
            writer = writer.partitionBy(*partitions)
        writer.parquet(path)


def _completed_history(spark, root, current_key):
    base = root + "batches/"
    fs, location = _filesystem(spark, base)
    if not fs.exists(location):
        return None
    paths = []
    for status in fs.listStatus(location):
        if not status.isDirectory():
            continue
        name = status.getPath().getName()
        if not name.startswith("batch_key=") or name == f"batch_key={current_key}":
            continue
        path = status.getPath().toString()
        if not _ready(spark, root + "runs/" + name):
            raise RuntimeError(f"Recover unfinished DQ batch using its original checkpoint: {path}")
        _ready(spark, path)
        paths.append(path)
    if not paths:
        return None
    return (spark.read.option("mergeSchema", "true").parquet(*paths)
            .filter(F.col("_route") != "REPLAY")
            .select("_event_id", "_event_fingerprint"))


def add_event_identity(df):
    """No business event ID exists: immutable Kafka coordinates are authoritative."""
    return (df.withColumn("_event_id", F.when(
        ~F.col("_fail_invalid_provenance"),
        F.concat_ws(":", "_kafka_topic", "_kafka_partition", "_kafka_offset"),
    )).withColumn("_event_fingerprint", F.sha2(F.concat_ws(
        "|", "_original_event_json", "_decode_error",
        F.when(F.col("_decode_error").isNotNull(), F.col("_raw_value_b64")),
    ), 256)))


def tag_replays(checked: DataFrame, history: DataFrame | None = None) -> DataFrame:
    """Retain replay rows in the audit; never collapse different source offsets."""
    identities = checked.filter(F.col("_event_id").isNotNull()).select(
        "_event_id", "_event_fingerprint",
    )
    if history is not None:
        identities = identities.unionByName(history.join(
            identities.select("_event_id").distinct(), "_event_id", "left_semi",
        ))
    conflicts = (identities.groupBy("_event_id")
                 .agg(F.countDistinct("_event_fingerprint").alias("versions"))
                 .filter("versions > 1"))
    if not conflicts.isEmpty():
        raise RuntimeError("Conflicting payloads at identical Kafka coordinates; inspect Bronze")

    if history is not None:
        seen = history.select("_event_id").distinct().withColumn("_seen", F.lit(True))
        checked = checked.join(seen, "_event_id", "left")
    else:
        checked = checked.withColumn("_seen", F.lit(False))
    window = Window.partitionBy("_event_id").orderBy(
        F.col("_ingested_at").asc_nulls_last(), F.col("_bronze_file").asc_nulls_last(),
    )
    checked = checked.withColumn("_rank", F.row_number().over(window))
    replay = F.col("_event_id").isNotNull() & (
        F.coalesce(F.col("_seen"), F.lit(False)) | (F.col("_rank") > 1)
    )
    return (checked.withColumn("_route", F.when(replay, "REPLAY")
            .when(F.col("_dq_passed"), "SILVER").otherwise("DLQ"))
            .withColumn("_replay_reason", F.when(replay, "same_source_coordinates"))
            .drop("_seen", "_rank"))


def enrich_silver(df, batch_id):
    return (df.drop(*[c for c, _ in FAIL_COLUMNS], "_raw_value_b64", "_original_event_json",
                    "_decode_error", "_schema_mismatches", "_replay_reason")
            .withColumn("event_ts", F.timestamp_millis("timestamp"))
            .withColumn("year", F.year("event_ts"))
            .withColumn("month", F.month("event_ts"))
            .withColumn("day", F.dayofmonth("event_ts"))
            .withColumn("event_type_lower", F.lower("event_type"))
            .withColumn("_quality_passed", F.lit(True))
            .withColumn("_cleaned_at", F.col("_dq_checked_at"))
            .withColumn("_silver_batch_id", F.lit(batch_id).cast("long")))


def dlq_messages(df, batch_id):
    """Explicit diagnostic envelope, not an unbounded serialization of Spark rows."""
    payload = F.struct(
        F.lit("1.0").alias("dlq_schema_version"),
        F.col("_event_id").alias("event_id"),
        "order_id", "event_type", "timestamp", "event_ts",
        F.col("_original_event_json").alias("original_event"),
        F.when(F.col("_decode_error").isNotNull(), F.col("_raw_value_b64")).alias("raw_value_b64"),
        F.col("_decode_error").alias("decode_error"),
        F.split("_failure_reason", ",").alias("failure_reasons"),
        F.lit("ERROR").alias("severity"),
        F.col("_dq_contract_version").alias("dq_contract_version"),
        F.col("_kafka_topic").alias("source_topic"),
        F.col("_kafka_partition").alias("source_partition"),
        F.col("_kafka_offset").alias("source_offset"),
        F.col("_schema_id").alias("source_schema_id"),
        F.col("_bronze_file").alias("bronze_file"),
        F.col("_ingested_at").alias("ingested_at"),
        F.col("_dq_checked_at").alias("failed_at"),
        F.lit(batch_id).alias("silver_batch_id"),
    )
    return df.select(F.col("_event_id").alias("key"), F.to_json(
        payload, {"ignoreNullFields": "false"},
    ).alias("value"))


@m9.observed("dlq_write")
def _write_to_dlq(df, batch_id):
    (dlq_messages(df, batch_id).write.format("kafka")
     .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
     .option("topic", os.getenv("KAFKA_TOPIC_DLQ", "order-events-dlq"))
     .option("kafka.acks", "all")
     .option("kafka.enable.idempotence", "true")
     .save())


@m9.observed("silver_write")
def write_to_silver(batch_df: DataFrame, batch_id: int) -> None:
    spark = batch_df.sparkSession
    topic = os.getenv("KAFKA_TOPIC_ORDERS", "order-events")
    bucket = os.getenv("MINIO_SILVER_BUCKET", "silver")
    checkpoint = os.getenv("SPARK_CHECKPOINT_DIR", "s3a://checkpoints/streaming/").rstrip("/")
    key = hashlib.sha256(f"{checkpoint}/silver|{topic}|{batch_id}".encode()).hexdigest()
    root = dq_path(bucket, topic)
    stage_path = root + f"batches/batch_key={key}"
    run_path = root + f"runs/batch_key={key}"
    if _ready(spark, run_path):
        m9.record("oip_pipeline_batches_total", component="silver", stage="silver", result="noop")
        log.info("silver_batch_already_completed", batch_id=batch_id, batch_key=key)
        return

    if _ready(spark, stage_path):
        m9.record("oip_pipeline_batches_total", component="silver", stage="silver", result="recovered")
        staged = spark.read.parquet(stage_path)
    else:
        history = _completed_history(spark, root, key)
        for col, dtype in (("_raw_value_b64", "string"), ("_schema_id", "long"),
                           ("_bronze_file", "string"), ("_ingested_at", "timestamp")):
            if col not in batch_df.columns:
                batch_df = batch_df.withColumn(col, F.lit(None).cast(dtype))
        checked = add_event_identity(annotate(batch_df))
        staged = tag_replays(checked, history).withColumn("_dq_batch_key", F.lit(key))
        _write_once(staged, stage_path)
        staged = spark.read.parquet(stage_path)  # freeze evaluation time and decisions

    staged = staged.persist()
    try:
        counts = {r["_route"]: r["count"] for r in staged.groupBy("_route").count().collect()}
        total = sum(counts.values())
        passed = staged.filter(F.col("_dq_passed")).count()
        silver_count, dlq_count = counts.get("SILVER", 0), counts.get("DLQ", 0)
        if silver_count:
            _write_once(enrich_silver(staged.filter("_route = 'SILVER'"), batch_id),
                        silver_path(bucket, topic) + f"silver_batch={key}",
                        ("year", "month", "day", "event_type_lower", "region"))
        if dlq_count:
            _write_to_dlq(staged.filter("_route = 'DLQ'"), batch_id)
            m9.record("oip_dlq_records_written_total", dlq_count, component="silver")
        breakdown = {r["reason"]: r["count"] for r in (
            staged.filter(~F.col("_dq_passed"))
            .select(F.explode(F.split("_failure_reason", ",")).alias("reason"))
            .groupBy("reason").count().collect()
        )}
        summary = {
            "_dq_batch_key": key, "batch_id": batch_id, "source_topic": topic,
            "input_records": total, "passed_records": passed, "failed_records": total - passed,
            "silver_records": silver_count, "dlq_records": dlq_count,
            "replay_records": counts.get("REPLAY", 0),
            "quality_score_pct": compute_quality_score(total, passed),
            "failure_breakdown_json": json.dumps(breakdown, sort_keys=True),
            "checkpoint": checkpoint + "/silver/",
        }
        _write_once(spark.createDataFrame([summary]).withColumn("completed_at", F.current_timestamp()),
                    run_path)
        m9.silver(summary)
        log.info("silver_batch_completed", **summary)
    finally:
        staged.unpersist()


def read_bronze_stream(spark, topic, bucket="bronze"):
    path = bronze_path(bucket, topic)
    fs, location = _filesystem(spark, path)
    if not fs.exists(location):
        fs.mkdirs(location)  # empty prefix is a valid source; never infer from absent files
    return (spark.readStream.schema(BRONZE_SCHEMA).option("maxFilesPerTrigger", 100).parquet(path)
            .withColumn("_kafka_topic", F.coalesce(F.col("_kafka_topic"), F.lit(topic)))
            .withColumn("_bronze_file", F.input_file_name()))


def run():
    spark = get_spark_session("OIP-SilverTransformer")
    topic = os.getenv("KAFKA_TOPIC_ORDERS", "order-events")
    checkpoint = os.getenv("SPARK_CHECKPOINT_DIR", "s3a://checkpoints/streaming/").rstrip("/")
    query = (read_bronze_stream(spark, topic, os.getenv("MINIO_BRONZE_BUCKET", "bronze"))
             .writeStream.foreachBatch(write_to_silver)
             .option("checkpointLocation", checkpoint + "/silver/")
             .trigger(processingTime=f"{int(os.getenv('SPARK_TRIGGER_INTERVAL_SECONDS', '30'))} seconds")
             .start())
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
