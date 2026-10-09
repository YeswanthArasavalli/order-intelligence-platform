"""Auditable order-event validation. Exact rules and policies: CONTRACT.md."""

import json
import time
from functools import reduce

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import BooleanType, LongType, StructField, StructType

from data_quality.currency_codes import VALID_CURRENCIES
from streaming.bronze_writer import MAX_TIMESTAMP_MS, ORDER_EVENT_SCHEMA

CONTRACT_VERSION = "1.0"
VALID_REGIONS = {"north-india", "south-india", "east-india", "west-india", "international"}
VALID_EVENT_TYPES = {
    "ORDER_CREATED", "ORDER_PAID", "ORDER_SHIPPED",
    "ORDER_DELIVERED", "ORDER_CANCELLED", "ORDER_REFUNDED",
}
VALID_PAYMENT_METHODS = {"UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet", "COD"}
PRODUCT_ID_PATTERN = r"^prod-[0-9]+$"
MAX_PAST_SECONDS = 24 * 60 * 60
MAX_FUTURE_SECONDS = 60
OPTIONAL_FIELDS = {"payment_method", "event_metadata"}
RULE_NAMES = (
    "null_order_id", "null_user_id", "null_product_id", "invalid_product_name",
    "invalid_amount", "null_timestamp", "malformed_timestamp", "invalid_timestamp",
    "future_timestamp", "old_timestamp", "invalid_region", "invalid_product_id",
    "invalid_event_type", "schema_drift", "invalid_currency", "invalid_payment_method",
    "invalid_status", "invalid_event_metadata", "invalid_schema_version",
    "invalid_provenance", "event_ts_mismatch",
)
FAIL_COLUMNS = [(f"_fail_{name}", name) for name in RULE_NAMES]


def _reject_json_constant(value):
    raise ValueError("Non-JSON numeric constant")


def _metadata_checks(value):
    if value is None:
        return True, False
    try:
        obj = json.loads(value, parse_constant=_reject_json_constant)
        return isinstance(obj, dict), isinstance(obj, dict) and obj.get("was_paid") is False
    except (ValueError, TypeError, RecursionError):
        return False, False


metadata_checks = F.udf(_metadata_checks, StructType([
    StructField("valid", BooleanType(), False), StructField("unpaid", BooleanType(), False),
]))


def annotate(df: DataFrame, *, now_ms: int | None = None) -> DataFrame:
    """Retain every row, its original values, and every applicable failure flag.

    Physical schema mismatches are recorded before normalizing to the nullable
    event schema, avoiding Spark analysis errors or silent type coercion.
    """
    clock_ms = int(time.time() * 1000) if now_ms is None else now_ms
    original = [f.name for f in ORDER_EVENT_SCHEMA if f.name in df.columns]
    checked = df.withColumn("_original_event_json", F.to_json(
        F.struct(*[F.col(c) for c in original]), {"ignoreNullFields": "false"},
    ))
    types = {f.name: f.dataType for f in df.schema}
    null_ts = F.col("timestamp").isNull() if "timestamp" in types else F.lit(True)
    wrong_ts = "timestamp" in types and not isinstance(types["timestamp"], LongType)
    checked = checked.withColumn("_timestamp_null", null_ts).withColumn(
        "_timestamp_malformed", ~null_ts & F.lit(wrong_ts),
    )
    mismatches = []
    for field in ORDER_EVENT_SCHEMA:
        if field.name not in types or types[field.name] != field.dataType:
            if field.name in types or field.name not in OPTIONAL_FIELDS:
                mismatches.append(field.name)
            checked = checked.withColumn(field.name, F.lit(None).cast(field.dataType))
    # Legacy Bronze files have no decode envelope; absence is not a failure.
    if "_decode_error" not in checked.columns:
        checked = checked.withColumn("_decode_error", F.lit(None).cast("string"))
    for col, dtype in (("_kafka_topic", "string"), ("_kafka_partition", "int"),
                       ("_kafka_offset", "long")):
        if col not in checked.columns:
            checked = checked.withColumn(col, F.lit(None).cast(dtype))

    required_nulls = [F.col(f.name).isNull() for f in ORDER_EVENT_SCHEMA
                      if f.name not in OPTIONAL_FIELDS]
    checked = checked.withColumn("_metadata", metadata_checks("event_metadata"))
    ts = F.col("timestamp")
    representable = ts.between(1, MAX_TIMESTAMP_MS)
    reference = F.lit(clock_ms)
    if "_ingested_at" in checked.columns:
        reference = F.coalesce(F.unix_millis("_ingested_at"), reference)
    def blank(name):
        return F.col(name).isNull() | (F.trim(F.col(name)) == "")
    unpaid_cancel = (F.col("event_type") == "ORDER_CANCELLED") & F.col("_metadata.unpaid")
    amount = F.col("amount")
    rules = {
        "null_order_id": blank("order_id"),
        "null_user_id": blank("user_id"),
        "null_product_id": blank("product_id"),
        "invalid_product_name": blank("product_name"),
        "invalid_amount": amount.isNull() | F.isnan(amount) | (F.abs(amount) == float("inf"))
                          | (amount < 0) | ((amount == 0) & ~F.coalesce(unpaid_cancel, F.lit(False))),
        "null_timestamp": F.col("_timestamp_null"),
        "malformed_timestamp": F.col("_timestamp_malformed"),
        "invalid_timestamp": ts.isNotNull() & ~representable,
        "future_timestamp": representable & (ts > reference + MAX_FUTURE_SECONDS * 1000),
        "old_timestamp": representable & (ts < reference - MAX_PAST_SECONDS * 1000),
        "invalid_region": ~F.col("region").isin(sorted(VALID_REGIONS)),
        # Java $ also matches before a final newline: explicitly reject it.
        "invalid_product_id": ~F.col("product_id").rlike(PRODUCT_ID_PATTERN)
                              | F.col("product_id").contains("\n"),
        "invalid_event_type": ~F.col("event_type").isin(sorted(VALID_EVENT_TYPES)),
        "schema_drift": F.lit(bool(mismatches)) | reduce(lambda a, b: a | b, required_nulls)
                        | F.col("_decode_error").isNotNull(),
        "invalid_currency": ~F.col("currency").isin(sorted(VALID_CURRENCIES)),
        "invalid_payment_method": F.col("payment_method").isNotNull()
                                  & ~F.col("payment_method").isin(sorted(VALID_PAYMENT_METHODS)),
        "invalid_status": ~F.col("event_type").isin(sorted(VALID_EVENT_TYPES))
                          | (F.col("status") != F.regexp_replace("event_type", "^ORDER_", "")),
        "invalid_event_metadata": ~F.col("_metadata.valid"),
        "invalid_schema_version": F.col("schema_version") != "1.0",
        "invalid_provenance": blank("_kafka_topic") | F.col("_kafka_partition").isNull()
                              | (F.col("_kafka_partition") < 0) | F.col("_kafka_offset").isNull()
                              | (F.col("_kafka_offset") < 0),
        "event_ts_mismatch": F.lit(False),
    }
    if "event_ts" in checked.columns:
        rules["event_ts_mismatch"] = representable & (
            F.col("event_ts").isNull() | (F.unix_millis("event_ts") != ts)
        )
    assert tuple(rules) == RULE_NAMES
    # SQL unknown must never cause a row to disappear from both branches.
    for name, condition in rules.items():
        default_failure = name in {"invalid_region", "invalid_product_id", "invalid_event_type",
                                   "invalid_currency", "invalid_status", "invalid_schema_version"}
        checked = checked.withColumn(f"_fail_{name}", F.coalesce(condition, F.lit(default_failure)))
    checked = checked.withColumn("_failure_reason", F.concat_ws(
        ",", *[F.when(F.col(col), F.lit(name)) for col, name in FAIL_COLUMNS],
    ))
    return (checked.withColumn("_dq_passed", F.col("_failure_reason") == "")
            .withColumn("_dq_checked_at", F.timestamp_millis(F.lit(clock_ms)))
            .withColumn("_dq_contract_version", F.lit(CONTRACT_VERSION))
            .withColumn("_schema_mismatches", F.lit(",".join(mismatches)))
            .drop("_metadata", "_timestamp_null", "_timestamp_malformed"))


def validate(df: DataFrame, *, now_ms: int | None = None) -> tuple[DataFrame, DataFrame]:
    """Return disjoint, exhaustive pass/fail frames, retaining DQ evidence."""
    checked = annotate(df, now_ms=now_ms)
    return checked.filter(F.col("_dq_passed")), checked.filter(~F.col("_dq_passed"))


def compute_quality_score(total: int, passed: int) -> float:
    """Zero inputs => 0%, matching silver.dq_run_log's existing SQL definition."""
    if not 0 <= passed <= total:
        raise ValueError("Require 0 <= passed <= total")
    return round(passed / total * 100, 2) if total else 0.0
