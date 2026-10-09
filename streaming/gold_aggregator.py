"""Silver → lifecycle/event-time Gold with durable replay-safe snapshot commits.

Retains the two existing metric families; see GOLD_CONTRACT.md for semantics,
including the explicitly additive cancellation/refund rule. No serving DB writes.
"""

import json
import os
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext
from functools import reduce

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    BooleanType, DecimalType, IntegerType, LongType, StringType, StructField,
    StructType, TimestampType,
)

from data_quality.validators import VALID_EVENT_TYPES
from storage.gold_store import commit_snapshot, current_manifest, gold_root
from streaming.bronze_writer import ORDER_EVENT_FIELDS, ORDER_EVENT_SCHEMA
from streaming.silver_transformer import _filesystem, _ready, silver_path
from streaming.spark_session import get_spark_session
from utils.logger import get_logger
from observability import telemetry as m9

log = get_logger("gold_aggregator")
MONEY = DecimalType(28, 8)
TOTAL = DecimalType(38, 8)
SILVER_INPUT_SCHEMA = StructType(ORDER_EVENT_SCHEMA.fields + [
    StructField("event_ts", TimestampType(), True),
    StructField("_kafka_topic", StringType(), True),
    StructField("_kafka_partition", IntegerType(), True),
    StructField("_kafka_offset", LongType(), True),
    StructField("_event_id", StringType(), True),
    StructField("_event_fingerprint", StringType(), True),
    StructField("_dq_passed", BooleanType(), True),
    StructField("_quality_passed", BooleanType(), True),
    StructField("_route", StringType(), True),
    StructField("_failure_reason", StringType(), True),
    StructField("_dq_contract_version", StringType(), True),
])


def _schema(fields):
    return StructType([StructField(name, dtype, True) for name, dtype in fields])


WINDOW_FIELDS = [("window_start", TimestampType()), ("window_end", TimestampType())]
OUTPUT_SCHEMAS = {
    "revenue_by_minute": _schema(WINDOW_FIELDS + [
        ("region", StringType()), ("currency", StringType()),
        ("order_count", LongType()), ("failed_count", LongType()), ("refunded_event_count", LongType()),
        ("gross_revenue", TOTAL), ("refund_amount", TOTAL), ("cancellation_amount", TOTAL),
        ("net_revenue", TOTAL), ("avg_order_value", TOTAL),
    ]),
    "product_rankings": _schema(WINDOW_FIELDS + [
        ("product_id", StringType()), ("product_name", StringType()), ("currency", StringType()),
        ("units_sold", LongType()), ("revenue", TOTAL),
    ]),
}


def _money_and_cancellation(amount, event_type, metadata):
    """Exact bounded conversion, never silent event rounding or cancellation inference."""
    try:
        with localcontext() as ctx:
            ctx.prec = 50
            value = Decimal(str(amount))
            quantized = value.quantize(Decimal("0.00000001"))
            if not value.is_finite() or value != quantized or abs(value) >= Decimal(10) ** 20:
                return None, None, "amount_not_exact_decimal_28_8"
            was_paid = None
            if event_type == "ORDER_CANCELLED":
                obj = json.loads(metadata) if metadata is not None else {}
                was_paid = obj.get("was_paid") if isinstance(obj, dict) else None
                if type(was_paid) is not bool:
                    return None, None, "ambiguous_cancellation_was_paid"
            if value < 0 or (value == 0 and not (event_type == "ORDER_CANCELLED" and was_paid is False)):
                return None, None, "invalid_gold_amount"
            return quantized, was_paid, None
    except (InvalidOperation, ValueError, TypeError, OverflowError):
        return None, None, "amount_or_metadata_not_representable"


money_and_cancellation = F.udf(_money_and_cancellation, _schema([
    ("amount", MONEY), ("was_paid", BooleanType()), ("error", StringType()),
]))


def _average_once(total, count):
    if not count:
        return None
    # Spark avg(decimal(28,8)) first rounds at scale 12. Avoid a second rounding
    # at scale 8 (which can move a value just below a half-way boundary upward).
    with localcontext() as ctx:
        ctx.prec = 80
        return (total / Decimal(count)).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


average_once = F.udf(_average_once, TOTAL)


def prepare_events(df: DataFrame, topic: str) -> DataFrame:
    """Validate the M2 boundary, retain its identity, and produce exact contributions."""
    spark = df.sparkSession
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Gold requires UTC Spark session")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    spark.conf.set("spark.sql.decimalOperations.allowPrecisionLoss", "false")
    types = {f.name: f.dataType for f in df.schema}
    mismatches = [f.name for f in SILVER_INPUT_SCHEMA if types.get(f.name) != f.dataType]
    if mismatches:
        raise ValueError(f"Gold requires verified Silver schema: {mismatches}")
    df = df.select(*[f.name for f in SILVER_INPUT_SCHEMA])
    original = F.to_json(F.struct(*[F.col(c) for c in ORDER_EVENT_FIELDS]), {"ignoreNullFields": "false"})
    valid = (
        F.col("_dq_passed") & F.col("_quality_passed") & (F.col("_route") == "SILVER")
        & (F.col("_failure_reason") == "") & (F.col("_dq_contract_version") == "1.0")
        & (F.col("_kafka_topic") == topic) & (F.col("_kafka_partition") >= 0) & (F.col("_kafka_offset") >= 0)
        & (F.col("_event_id") == F.concat_ws(":", "_kafka_topic", "_kafka_partition", "_kafka_offset"))
        & (F.col("_event_fingerprint") == F.sha2(original, 256))
        & (F.unix_millis("event_ts") == F.col("timestamp"))
        & F.col("event_type").isin(sorted(VALID_EVENT_TYPES))
    )
    if not df.filter(~F.coalesce(valid, F.lit(False))).isEmpty():
        raise ValueError("Unverified, DLQ, or corrupted Silver row; Gold publication blocked")
    df = df.withColumn("_money", money_and_cancellation("amount", "event_type", "event_metadata"))
    failures = df.filter(F.col("_money.error").isNotNull()).select("_event_id", "_money.error").take(5)
    if failures:
        raise ValueError(f"Gold monetary/lifecycle contract failure: {failures}")
    return apply_revenue_sign(df.withColumn("amount_decimal", F.col("_money.amount"))
                              .withColumn("was_paid", F.col("_money.was_paid")).drop("_money"))


def apply_revenue_sign(df: DataFrame) -> DataFrame:
    """Documented additive ledger: PAID +, REFUNDED -, paid CANCELLED -, else 0."""
    return df.withColumn("revenue_contribution",
        F.when(F.col("event_type") == "ORDER_PAID", F.col("amount_decimal"))
        .when((F.col("event_type") == "ORDER_REFUNDED")
              | ((F.col("event_type") == "ORDER_CANCELLED") & F.col("was_paid")), -F.col("amount_decimal"))
        .otherwise(F.lit(0).cast(MONEY)).cast(MONEY))


def merge_events(incoming, previous=None):
    combined = incoming if previous is None else previous.unionByName(incoming)
    conflicts = (combined.groupBy("_event_id").agg(F.countDistinct("_event_fingerprint").alias("versions"))
                 .filter("versions != 1"))
    if not conflicts.isEmpty():
        raise ValueError("Conflicting Silver payloads at identical source coordinates")
    return combined.dropDuplicates(["_event_id"])


def aggregate_events(events: DataFrame) -> dict[str, DataFrame]:
    paid = F.col("event_type") == "ORDER_PAID"
    refunded = F.col("event_type") == "ORDER_REFUNDED"
    cancelled = F.col("event_type") == "ORDER_CANCELLED"
    zero = F.lit(0).cast(MONEY)

    def money_sum(condition):
        return F.sum(F.when(condition, F.col("amount_decimal")).otherwise(zero)).cast(TOTAL)

    revenue = (events.groupBy(F.window("event_ts", "1 minute").alias("win"), "region", "currency")
        .agg(F.count(F.when(paid, 1)).alias("order_count"),
             F.count(F.when(cancelled, 1)).alias("failed_count"),
             F.count(F.when(refunded, 1)).alias("refunded_event_count"),
             money_sum(paid).alias("gross_revenue"), money_sum(refunded).alias("refund_amount"),
             money_sum(cancelled & F.col("was_paid")).alias("cancellation_amount"),
             F.sum("revenue_contribution").cast(TOTAL).alias("net_revenue"))
        .withColumn("avg_order_value", average_once("gross_revenue", "order_count"))
        .select(F.col("win.start").alias("window_start"), F.col("win.end").alias("window_end"),
                *[f.name for f in OUTPUT_SCHEMAS["revenue_by_minute"]][2:]))
    products = (events.filter(paid)
        .groupBy(F.window("event_ts", "5 minutes").alias("win"), "product_id", "product_name", "currency")
        .agg(F.count("*").alias("units_sold"), F.sum("amount_decimal").cast(TOTAL).alias("revenue"))
        .select(F.col("win.start").alias("window_start"), F.col("win.end").alias("window_end"),
                "product_id", "product_name", "currency", "units_sold", "revenue"))
    # Null sums signal overflow/corruption, never a zero. Average alone may be absent.
    if not revenue.filter(reduce(lambda a, b: a | b, [F.col(c).isNull() for c in (
        "gross_revenue", "refund_amount", "cancellation_amount", "net_revenue",
    )])).isEmpty() or not products.filter(F.col("revenue").isNull()).isEmpty():
        raise ArithmeticError("Gold decimal overflow; no snapshot published")
    return {"revenue_by_minute": revenue, "product_rankings": products}


@m9.observed("gold_batch")
def write_to_gold(batch_df: DataFrame, batch_id: int) -> None:
    topic = os.getenv("KAFKA_TOPIC_ORDERS", "order-events")
    root = gold_root(os.getenv("MINIO_GOLD_BUCKET", "gold"), topic)
    checkpoint = os.getenv("SPARK_CHECKPOINT_DIR", "s3a://checkpoints/streaming/").rstrip("/") + "/gold/"
    spark = batch_df.sparkSession
    parent = current_manifest(spark, root)
    incoming = prepare_events(batch_df, topic).persist()
    merged = None
    try:
        count = incoming.count()
        if not count:
            m9.record("oip_pipeline_batches_total", component="gold", stage="gold", result="empty")
            log.info("gold_empty_batch", batch_id=batch_id)
            return
        previous = spark.read.parquet(parent["tables"]["events"]["path"]) if parent else None
        merged = merge_events(incoming, previous).persist()
        total = merged.count()
        new_count = total - (parent["tables"]["events"]["rows"] if parent else 0)
        if new_count == 0:
            m9.record("oip_pipeline_batches_total", component="gold", stage="gold", result="noop")
            m9.record("oip_gold_events_total", count, component="gold", result="replay")
            log.info("gold_replay_noop", batch_id=batch_id, input_records=count,
                     version=parent["version"])
            return
        manifest = commit_snapshot(spark, root, {"events": merged, **aggregate_events(merged)},
            parent=parent, batch_id=batch_id, checkpoint=checkpoint, input_count=count, new_count=new_count)
        log.info("gold_snapshot_committed", batch_id=batch_id, version=manifest["version"],
                 events=total, new_events=new_count, root=root)
    finally:
        incoming.unpersist()
        if merged is not None:
            merged.unpersist()


def read_silver_stream(spark, topic, bucket="silver"):
    base = silver_path(bucket, topic)
    fs, location = _filesystem(spark, base)
    if not fs.exists(location):
        fs.mkdirs(location)
    # Schema explicitly includes region so partition values survive file streaming.
    return spark.readStream.schema(SILVER_INPUT_SCHEMA).option("maxFilesPerTrigger", 100).parquet(base)


def write_verified_silver_to_gold(batch_df, batch_id):
    """File-source callback: accept only fully committed M2 Silver batches."""
    paths = batch_df.select(F.input_file_name().alias("path")).distinct().collect()
    for row in paths:
        path = row.path
        if "/silver_batch=" not in path:
            raise ValueError(f"Gold source is not an M2 Silver batch: {path}")
        batch_path = path.split("/silver_batch=", 1)[0] + "/silver_batch=" + path.split("/silver_batch=", 1)[1].split("/", 1)[0]
        if not _ready(batch_df.sparkSession, batch_path):
            raise ValueError(f"Uncommitted Silver batch: {batch_path}")
    write_to_gold(batch_df, batch_id)


def run():
    spark = get_spark_session("OIP-GoldAggregator")
    topic = os.getenv("KAFKA_TOPIC_ORDERS", "order-events")
    checkpoint = os.getenv("SPARK_CHECKPOINT_DIR", "s3a://checkpoints/streaming/").rstrip("/")
    query = (read_silver_stream(spark, topic, os.getenv("MINIO_SILVER_BUCKET", "silver"))
             .writeStream.foreachBatch(write_verified_silver_to_gold)
             .option("checkpointLocation", checkpoint + "/gold/")
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
