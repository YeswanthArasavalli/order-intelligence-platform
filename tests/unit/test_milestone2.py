"""Real local Spark execution of the DQ, Silver, DLQ and replay functions."""

from datetime import datetime, timezone
from io import BytesIO
import json

import pytest
from fastavro import parse_schema, schemaless_writer
from pyspark.sql import SparkSession, functions as F

from data_quality.validators import RULE_NAMES, annotate, compute_quality_score, validate
from scripts.milestone2_cases import controlled_cases
from streaming.bronze_writer import (
    BRONZE_SCHEMA, ORDER_EVENT_FIELDS, _decode_order_event, _decode_order_event_envelope, _schema_cache,
)
from streaming.silver_transformer import add_event_identity, dlq_messages, enrich_silver, tag_replays

NOW = 1791035389489


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("M2-unit")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def frame(spark, events):
    rows = [dict(event, _kafka_topic="unit", _kafka_partition=0, _kafka_offset=i,
                 _ingested_at=datetime.fromtimestamp(NOW / 1000, timezone.utc))
            for i, event in enumerate(events)]
    return (spark.createDataFrame(rows, BRONZE_SCHEMA)
            .withColumn("event_ts", F.when(F.col("timestamp") >= 0, F.timestamp_millis("timestamp")))
            .withColumn("_bronze_file", F.lit("unit.parquet")))


def test_controlled_decisions_and_actual_outputs(spark):
    cases = controlled_cases(NOW)
    df = frame(spark, [c["event"] for c in cases])
    checked = add_event_identity(annotate(df, now_ms=NOW)).cache()
    try:
        rows = checked.orderBy("_kafka_offset").collect()
        for case, row in zip(cases, rows):
            assert row._failure_reason.split(",") == (case["expected"] or [""]), case["name"]
            assert row._dq_passed == (not case["expected"]), case["name"]
            assert all(row[f"_fail_{r}"] is not None for r in RULE_NAMES)
        clean = enrich_silver(checked.filter("_dq_passed"), 5)
        assert clean.filter("unix_millis(event_ts) != timestamp").count() == 0
        assert clean.filter("order_id = 'm2-lifecycle'").count() == 6
        assert clean.filter("event_type = 'ORDER_REFUNDED'").count() == 2
        payloads = [json.loads(r.value) for r in dlq_messages(checked.filter("not _dq_passed"), 5).collect()]
        assert len(payloads) == sum(bool(c["expected"]) for c in cases)
        assert all(p["source_topic"] == "unit" and p["failure_reasons"] and p["failed_at"] for p in payloads)
        assert all(p["original_event"] and p["source_offset"] is not None for p in payloads)
    finally:
        checked.unpersist()


def test_null_schema_and_timestamp_classes(spark):
    event = controlled_cases(NOW)[0]["event"]
    cases = [dict(event, timestamp=None), dict(event, order_id=None), dict(event, amount=None)]
    passed, failed = validate(frame(spark, cases), now_ms=NOW)
    assert passed.count() == 0 and failed.count() == 3
    reasons = [r._failure_reason for r in failed.orderBy("_kafka_offset").collect()]
    assert "null_timestamp" in reasons[0] and "future_timestamp" not in reasons[0]
    assert "schema_drift" in reasons[1] and "null_order_id" in reasons[1]
    wrong = frame(spark, [event]).withColumn("timestamp", F.lit("not-a-time"))
    row = annotate(wrong, now_ms=NOW).first()
    assert "malformed_timestamp" in row._failure_reason and "schema_drift" in row._failure_reason
    assert "null_timestamp" not in row._failure_reason
    missing = annotate(frame(spark, [event]).drop("currency"), now_ms=NOW).first()
    assert "schema_drift" in missing._failure_reason and "invalid_currency" in missing._failure_reason


def test_time_boundaries_and_replay_clock(spark):
    event = controlled_cases(NOW)[0]["event"]
    offsets = [-86400000, -86400001, 60000, 60001, 0]
    df = frame(spark, [dict(event, timestamp=NOW + t) for t in offsets])
    rows = annotate(df, now_ms=NOW + 7 * 86400000).orderBy("_kafka_offset").collect()
    assert [r._failure_reason for r in rows] == ["", "old_timestamp", "", "future_timestamp", ""]
    mismatch = annotate(frame(spark, [event]).withColumn("event_ts", F.lit(None).cast("timestamp")))
    assert "event_ts_mismatch" in mismatch.first()._failure_reason


def test_replays_do_not_remove_repeated_business_events(spark):
    event = controlled_cases(NOW)[0]["event"]
    checked = add_event_identity(annotate(frame(spark, [event, event]), now_ms=NOW)).cache()
    try:
        duplicate = checked.unionByName(checked.filter("_kafka_offset = 0"))
        tagged = tag_replays(duplicate)
        assert tagged.filter("_route = 'SILVER'").count() == 2
        assert tagged.filter("_route = 'REPLAY'").count() == 1
        history = checked.select("_event_id", "_event_fingerprint")
        assert tag_replays(checked, history).filter("_route = 'REPLAY'").count() == 2
        conflict = checked.withColumn("_event_fingerprint", F.lit("different"))
        with pytest.raises(RuntimeError, match="Conflicting payloads"):
            tag_replays(conflict, history)
    finally:
        checked.unpersist()


def test_optional_metadata_and_unpaid_cancellation(spark):
    base = controlled_cases(NOW)[0]["event"]
    events = [dict(base, event_metadata=v) for v in (None, "{}", "[]", "null", '{"n":NaN}')]
    events += [dict(base, event_type="ORDER_CANCELLED", status="CANCELLED", amount=0.0,
                    event_metadata=v) for v in ('{"was_paid":false}', '{"was_paid":"false"}', None)]
    rows = annotate(frame(spark, events), now_ms=NOW).orderBy("_kafka_offset").collect()
    assert [r._dq_passed for r in rows] == [True, True, False, False, False, True, False, False]


def test_avro_decoder_preserves_failures(monkeypatch):
    from producer.kafka_publisher import load_schema
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://unit")
    schema = parse_schema(json.loads(load_schema()))
    _schema_cache[("http://unit", 77)] = schema
    event = controlled_cases(NOW)[0]["event"]
    stream = BytesIO()
    schemaless_writer(stream, schema, event)
    wire = b"\x00" + (77).to_bytes(4, "big") + stream.getvalue()
    assert dict(zip(ORDER_EVENT_FIELDS, _decode_order_event(wire))) == event
    for value, reason in ((None, "null_kafka_value"), (b"bad", "short_avro_header"),
                          (b"123456", "invalid_avro_magic_byte"),
                          (wire + b"x", "trailing_avro_bytes"),
                          (wire[:6], "invalid_avro_payload")):
        assert _decode_order_event_envelope(value)[-1] == reason
    def unavailable(*args):
        raise ConnectionError("Registry unavailable")
    monkeypatch.setattr("streaming.bronze_writer._get_schema", unavailable)
    with pytest.raises(ConnectionError):
        _decode_order_event_envelope(wire)


def test_empty_quality():
    assert compute_quality_score(0, 0) == 0.0
    assert compute_quality_score(3, 2) == 66.67
    with pytest.raises(ValueError):
        compute_quality_score(0, 1)
