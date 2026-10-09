"""
Order Intelligence Platform — Phase 4
Bronze Layer Writer
===================
Reads raw order events from Kafka and writes them as immutable Parquet
files to MinIO Bronze bucket — append-only, preserving the raw wire value.

Bronze is the permanent raw archive. It includes Kafka metadata
(topic, offset, partition), decoded fields, and decode failures for DQ routing.

Usage:
    python -m streaming.bronze_writer
"""

import os
import json
from io import BytesIO
from datetime import date, datetime, timezone
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import urlopen

from fastavro import parse_schema, schemaless_reader
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.spark_session import get_spark_session
from utils.logger import configure_logging, get_logger
from observability import telemetry as m9

configure_logging()
log = get_logger("bronze_writer")

# ---------------------------------------------------------------------------
# Order event schema — mirrors the Avro schema exactly
# ---------------------------------------------------------------------------

ORDER_EVENT_SCHEMA = StructType([
    StructField("order_id",       StringType(),  nullable=True),
    StructField("event_type",     StringType(),  nullable=True),
    StructField("user_id",        StringType(),  nullable=True),
    StructField("product_id",     StringType(),  nullable=True),
    StructField("product_name",   StringType(),  nullable=True),
    StructField("region",         StringType(),  nullable=True),
    StructField("amount",         DoubleType(),  nullable=True),
    StructField("currency",       StringType(),  nullable=True),
    StructField("payment_method", StringType(),  nullable=True),
    StructField("status",         StringType(),  nullable=True),
    StructField("event_metadata", StringType(),  nullable=True),
    StructField("timestamp",      LongType(),    nullable=True),
    StructField("schema_version", StringType(),  nullable=True),
])

ORDER_EVENT_FIELDS = [field.name for field in ORDER_EVENT_SCHEMA.fields]

# Additive envelope; old Parquet files read these nullable columns as null.
DECODE_SCHEMA = StructType(ORDER_EVENT_SCHEMA.fields + [
    StructField("_decode_error", StringType(), True),
])
BRONZE_SCHEMA = StructType(ORDER_EVENT_SCHEMA.fields + [
    StructField("event_ts", TimestampType(), True),
    StructField("_kafka_topic", StringType(), True),
    StructField("_kafka_partition", IntegerType(), True),
    StructField("_kafka_offset", LongType(), True),
    StructField("_kafka_timestamp", TimestampType(), True),
    StructField("_schema_id", LongType(), True),
    StructField("_raw_value_b64", StringType(), True),
    StructField("_decode_error", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_batch_id", LongType(), True),
    *[StructField(name, IntegerType(), True) for name in ("year", "month", "day", "hour")],
])
MAX_TIMESTAMP_MS = 253402300799999  # UTC 9999-12-31T23:59:59.999


class InvalidOrderEvent(ValueError):
    """A record-level error safe to quarantine (not an infrastructure outage)."""

# The producer uses Confluent's wire format:
#   1 byte magic value (0) + 4 byte schema ID + Avro payload.
# The embedded schema ID is resolved through Schema Registry and the payload
# is decoded with fastavro. The schema cache is process-local because this
# function runs inside Spark Python workers.
_schema_cache = {}


@m9.observed("schema_resolve")
def _get_schema(schema_registry_url: str, schema_id: int):
    cache_key = (schema_registry_url, schema_id)
    if cache_key not in _schema_cache:
        schema_url = (
            f"{schema_registry_url.rstrip('/')}/schemas/ids/{quote(str(schema_id))}"
        )
        with urlopen(schema_url, timeout=5) as response:
            response_body = json.load(response)

        schema = response_body["schema"]
        if isinstance(schema, str):
            schema = json.loads(schema)
        _schema_cache[cache_key] = parse_schema(schema)

    return _schema_cache[cache_key]


def _timestamp_to_epoch_millis(value) -> int:
    """Normalize Avro timestamp-millis output to the Bronze long contract."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return int(value.timestamp() * 1000)

    if isinstance(value, date):
        return int(
            datetime(
                value.year,
                value.month,
                value.day,
                tzinfo=timezone.utc,
            ).timestamp()
            * 1000
        )

    if isinstance(value, int):
        return value

    raise ValueError(f"Unsupported order event timestamp type: {type(value)!r}")


def _decode_order_event(value):
    """
    Decode one complete Confluent Avro value for use in a Spark UDF.

    The first five bytes are the Confluent magic byte and schema ID. The
    writer schema is fetched from Schema Registry using that ID, then the
    remaining bytes are decoded as Avro. Errors are raised deliberately so
    malformed records do not become silent all-null Bronze rows.
    """
    if value is None:
        raise InvalidOrderEvent("null_kafka_value")

    raw_value = bytes(value)
    if len(raw_value) < 5:
        raise InvalidOrderEvent("short_avro_header")

    if raw_value[0] != 0:
        raise InvalidOrderEvent("invalid_avro_magic_byte")

    schema_id = int.from_bytes(raw_value[1:5], byteorder="big", signed=False)
    schema_registry_url = os.getenv(
        "SCHEMA_REGISTRY_URL", "http://schema-registry:8081"
    )

    try:
        schema = _get_schema(schema_registry_url, schema_id)
    except HTTPError as exc:
        if exc.code == 404:
            raise InvalidOrderEvent("unknown_schema_id") from exc
        raise  # Authentication / server / network failures must retry the batch.

    payload = BytesIO(raw_value[5:])
    try:
        record = schemaless_reader(payload, schema)
    except (ValueError, EOFError, TypeError, IndexError, OverflowError) as exc:
        raise InvalidOrderEvent("invalid_avro_payload") from exc
    if payload.read(1):
        raise InvalidOrderEvent("trailing_avro_bytes")
    if not isinstance(record, dict):
        raise InvalidOrderEvent("invalid_record_schema")

    normalized = dict(record)
    try:
        normalized["timestamp"] = _timestamp_to_epoch_millis(normalized.get("timestamp"))
    except (ValueError, OverflowError) as exc:
        raise InvalidOrderEvent("invalid_record_timestamp_type") from exc
    for field in ORDER_EVENT_SCHEMA.fields:
        value = normalized.get(field.name)
        if value is None:
            if field.name not in {"payment_method", "event_metadata"}:
                raise InvalidOrderEvent("missing_required_avro_field")
        elif isinstance(field.dataType, StringType) and not isinstance(value, str):
            raise InvalidOrderEvent("invalid_record_field_type")
        elif isinstance(field.dataType, DoubleType):
            if type(value) not in (float, int):
                raise InvalidOrderEvent("invalid_record_field_type")
            normalized[field.name] = float(value)
        elif isinstance(field.dataType, LongType) and type(value) is not int:
            raise InvalidOrderEvent("invalid_record_field_type")
    return tuple(normalized.get(field) for field in ORDER_EVENT_FIELDS)


decode_order_event_udf = F.udf(_decode_order_event, ORDER_EVENT_SCHEMA)


def _decode_order_event_envelope(value):
    """Preserve bad records for DQ while allowing infrastructure errors to fail."""
    try:
        return (*_decode_order_event(value), None)
    except InvalidOrderEvent as exc:
        m9.record("oip_decode_failures_total", component="bronze", stage="record")
        return (*([None] * len(ORDER_EVENT_FIELDS)), str(exc))


decode_envelope_udf = F.udf(_decode_order_event_envelope, DECODE_SCHEMA)

# ---------------------------------------------------------------------------
# Bronze path builder
# ---------------------------------------------------------------------------

def bronze_path(bucket: str = "bronze", topic: str = "order-events") -> str:
    """
    Returns the base S3A path for Bronze Parquet storage.
    Spark will automatically create year=/month=/day=/hour= partition directories.
    """
    return f"s3a://{bucket}/{topic}/"


# ---------------------------------------------------------------------------
# foreachBatch sink — called by Spark for every micro-batch
# ---------------------------------------------------------------------------

@m9.observed("bronze_write")
def write_to_bronze(batch_df: DataFrame, batch_id: int) -> None:
    """
    Called by Spark's foreachBatch for each micro-batch.
    Adds Bronze metadata fields and writes partitioned Parquet to MinIO.

    Partition layout:
        s3a://bronze/order-events/
            year=2024/month=11/day=15/hour=14/region=south-india/
                part-00000-<uuid>.snappy.parquet
    """
    if batch_df.isEmpty():
        m9.record("oip_pipeline_batches_total", component="bronze", stage="bronze", result="empty")
        log.info("bronze_empty_batch", batch_id=batch_id)
        return

    count = batch_df.count()
    m9.record("oip_bronze_records_total", count, component="bronze", result="received")
    log.info("bronze_batch_received", batch_id=batch_id, record_count=count)

    bucket        = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
    topic         = os.getenv("KAFKA_TOPIC_ORDERS", "order-events")
    output_path   = bronze_path(bucket, topic)

    # Enrich with Bronze metadata
    enriched = (
        batch_df
        # Convert epoch-ms timestamp to proper timestamp type
        .withColumn("event_ts", F.when(
            F.col("timestamp").between(0, MAX_TIMESTAMP_MS),
            F.timestamp_millis("timestamp"),
        ))
        # Partition helpers
        .withColumn("year",  F.year("event_ts").cast(StringType()))
        .withColumn("month", F.lpad(F.month("event_ts").cast(StringType()), 2, "0"))
        .withColumn("day",   F.lpad(F.dayofmonth("event_ts").cast(StringType()), 2, "0"))
        .withColumn("hour",  F.lpad(F.hour("event_ts").cast(StringType()), 2, "0"))
        # Bronze audit fields
        .withColumn("_kafka_offset",    F.col("kafka_offset"))
        .withColumn("_kafka_partition", F.col("kafka_partition"))
        .withColumn("_kafka_timestamp", F.col("kafka_timestamp"))
        .withColumn("_ingested_at",     F.current_timestamp())
        .withColumn("_batch_id",        F.lit(batch_id).cast(LongType()))
        # Drop raw Kafka transport fields (kept in metadata cols above)
        .drop("kafka_offset", "kafka_partition", "kafka_timestamp")
    )

    # Write partitioned Parquet — append mode (Bronze is immutable)
    (
        enriched
        .write
        .format("parquet")
        .mode("append")
        .option("compression", "snappy")
        .partitionBy("year", "month", "day", "hour", "region")
        .save(output_path)
    )

    m9.record("oip_bronze_records_total", count, component="bronze", result="written")
    m9.record("oip_pipeline_batches_total", component="bronze", stage="bronze", result="written")
    log.info("bronze_batch_written",
             batch_id=batch_id,
             record_count=count,
             path=output_path)


# ---------------------------------------------------------------------------
# Kafka source reader
# ---------------------------------------------------------------------------

def read_kafka_stream(spark, topic: str, bootstrap: str) -> DataFrame:
    """
    Returns a streaming DataFrame reading from the Kafka topic.
    Decodes the complete Confluent Avro value and adds Kafka metadata columns.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe",               topic)
        .option("startingOffsets",         "earliest")
        .option("failOnDataLoss",          "true")
        .option("maxOffsetsPerTrigger",    10000)        # back-pressure control
        .load()
    )

    # Decode the complete Confluent Avro payload. The UDF validates the
    # embedded header and resolves its schema ID through Schema Registry.
    parsed = (
        raw
        .select(
            decode_envelope_udf(F.col("value")).alias("data"),
            F.col("topic").alias("_kafka_topic"),
            F.base64("value").alias("_raw_value_b64"),
            F.when(
                (F.length("value") >= 5) & (F.hex(F.substring("value", 1, 1)) == "00"),
                F.conv(F.hex(F.substring("value", 2, 4)), 16, 10).cast("long"),
            ).alias("_schema_id"),
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "_kafka_topic", "_raw_value_b64", "_schema_id",
                "kafka_offset", "kafka_partition", "kafka_timestamp")
    )

    return parsed


# ---------------------------------------------------------------------------
# Main streaming job
# ---------------------------------------------------------------------------

def run():
    spark         = get_spark_session("OIP-BronzeWriter")
    bootstrap     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic         = os.getenv("KAFKA_TOPIC_ORDERS", "order-events")
    checkpoint    = os.getenv("SPARK_CHECKPOINT_DIR", "s3a://checkpoints/streaming/")
    trigger_secs  = int(os.getenv("SPARK_TRIGGER_INTERVAL_SECONDS", "30"))

    log.info("bronze_writer_starting",
             topic=topic,
             bootstrap=bootstrap,
             trigger_seconds=trigger_secs,
             checkpoint=checkpoint)

    stream_df = read_kafka_stream(spark, topic, bootstrap)

    query = (
        stream_df
        .writeStream
        .foreachBatch(write_to_bronze)
        .option("checkpointLocation", f"{checkpoint}/bronze/")
        .trigger(processingTime=f"{trigger_secs} seconds")
        .start()
    )

    log.info("bronze_streaming_query_started", query_id=query.id)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("bronze_writer_stopping")
        query.stop()
    finally:
        spark.stop()
        log.info("bronze_writer_stopped")


if __name__ == "__main__":
    run()
