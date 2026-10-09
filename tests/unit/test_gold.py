"""Actual Spark aggregation and local Parquet/manifest persistence tests."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession, functions as F

from scripts.milestone3_cases import event
from storage import gold_store
from storage.gold_store import current_manifest, read_gold
from streaming import gold_aggregator as gold
from streaming.bronze_writer import ORDER_EVENT_FIELDS

BASE = 1791043200000  # minute and five-minute aligned


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("M3-tests")
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def silver(spark, events, offsets=None, topic="unit-gold"):
    offsets = list(range(len(events))) if offsets is None else offsets
    rows = [dict(e, event_ts=datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc),
                 _kafka_topic=topic, _kafka_partition=0, _kafka_offset=offset,
                 _event_id=f"{topic}:0:{offset}", _dq_passed=True, _quality_passed=True,
                 _route="SILVER", _failure_reason="", _dq_contract_version="1.0")
            for e, offset in zip(events, offsets)]
    return spark.createDataFrame(rows, gold.SILVER_INPUT_SCHEMA).withColumn(
        "_event_fingerprint", F.sha2(F.to_json(F.struct(*ORDER_EVENT_FIELDS),
                                              {"ignoreNullFields": "false"}), 256))


@pytest.mark.parametrize("events,net,paid,refunds,cancelled", [
    ([event()], "100", 1, 0, 0),
    ([event(), event(stage="REFUNDED", amount=30)], "70", 1, 1, 0),
    ([event(), event(stage="REFUNDED", amount=20), event(stage="REFUNDED", amount=30)], "50", 1, 2, 0),
    ([event(), event(stage="CANCELLED", event_metadata='{"was_paid":true}')], "0", 1, 0, 1),
    ([event(stage="CANCELLED", amount=0, event_metadata='{"was_paid":false}')], "0", 0, 0, 1),
    ([event(), event(stage="REFUNDED", amount=20), event(stage="REFUNDED", amount=20)], "60", 1, 2, 0),
    ([event(), event(stage="CANCELLED", event_metadata='{"was_paid":true}'),
      event(stage="REFUNDED", amount=30)], "-30", 1, 1, 1),
    ([event(stage="REFUNDED", amount=7)], "-7", 0, 1, 0),
    ([event(stage="CREATED"), event(), event(stage="SHIPPED"), event(stage="DELIVERED"),
      event(stage="REFUNDED", amount=30)], "70", 1, 1, 0),
])
def test_lifecycle_metrics(spark, events, net, paid, refunds, cancelled):
    prepared = gold.prepare_events(silver(spark, events), "unit-gold")
    output = gold.aggregate_events(prepared)
    row = output["revenue_by_minute"].first()
    assert row.net_revenue == Decimal(net)
    assert (row.order_count, row.refunded_event_count, row.failed_count) == (paid, refunds, cancelled)
    assert row.avg_order_value == (Decimal(100) if paid else None)
    assert output["product_rankings"].count() == bool(paid)
    if paid:
        product = output["product_rankings"].first()
        assert product.units_sold == paid and product.revenue == Decimal(100)


def test_money_currency_and_window_boundaries(spark):
    events = [event(amount=.1), event(amount=.2), event(amount=20, currency="USD"),
              event(amount=5, timestamp=BASE + 59999), event(amount=7, timestamp=BASE + 60000),
              event(amount=3, timestamp=BASE + 300000), event(amount=1.234, currency="BHD")]
    output = gold.aggregate_events(gold.prepare_events(silver(spark, events), "unit-gold"))
    rows = output["revenue_by_minute"].orderBy("window_start", "currency").collect()
    assert len(rows) == 5
    first = {r.currency: r for r in rows if r.window_start == rows[0].window_start}
    assert first["INR"].net_revenue == Decimal("5.30000000")
    assert first["USD"].net_revenue == Decimal("20") and first["BHD"].net_revenue == Decimal("1.234")
    assert all((r.window_end - r.window_start).total_seconds() == 60 for r in rows)
    assert output["product_rankings"].count() == 4
    assert output["revenue_by_minute"].schema["net_revenue"].dataType == gold.TOTAL


@pytest.mark.parametrize("changes", [
    {"amount": 0.30000000000000004}, {"amount": 1e21},
    {"event_type": "ORDER_CANCELLED", "status": "CANCELLED", "event_metadata": None},
    {"event_type": "ORDER_CANCELLED", "status": "CANCELLED", "event_metadata": '{"was_paid":"true"}'},
])
def test_unrepresentable_or_ambiguous_fails(spark, changes):
    with pytest.raises(ValueError, match="contract failure"):
        gold.prepare_events(silver(spark, [event(**changes)]), "unit-gold")


def test_unverified_and_conflicting_sources_fail(spark):
    good = silver(spark, [event()])
    for col, value in (("_dq_passed", False), ("_route", "DLQ"), ("_event_id", "wrong"),
                       ("_event_fingerprint", "bad")):
        with pytest.raises(ValueError, match="Unverified"):
            gold.prepare_events(good.withColumn(col, F.lit(value)), "unit-gold")
    with pytest.raises(ValueError, match="schema"):
        gold.prepare_events(good.drop("_dq_passed"), "unit-gold")
    one = gold.prepare_events(good, "unit-gold")
    different = gold.prepare_events(silver(spark, [event(amount=200)]), "unit-gold")
    with pytest.raises(ValueError, match="Conflicting"):
        gold.merge_events(one, different)


def test_multibatch_replay_and_persistence(spark, monkeypatch, tmp_path):
    root = str(tmp_path / "gold") + "/"
    monkeypatch.setenv("KAFKA_TOPIC_ORDERS", "unit-gold")
    monkeypatch.setattr(gold, "gold_root", lambda *args: root)
    batches = [silver(spark, [event()], [0]), silver(spark, [event(stage="REFUNDED", amount=20)], [1]),
               silver(spark, [event(stage="REFUNDED", amount=30)], [2])]
    for i, (df, net) in enumerate(zip(batches, (100, 80, 50))):
        gold.write_to_gold(df, i)
        assert read_gold(spark, root, "revenue_by_minute").first().net_revenue == net
    manifest = current_manifest(spark, root)
    replay = batches[0].unionByName(batches[1]).unionByName(batches[2]).unionByName(batches[1])
    gold.write_to_gold(replay, 0)  # changed checkpoint/batch-id cannot double count
    gold.write_to_gold(replay.limit(0), 9)
    assert current_manifest(spark, root) == manifest
    assert manifest["tables"]["events"]["rows"] == 3
    assert read_gold(spark, root, "revenue_by_minute").count() == 1
    # A late refund corrects an old window while preserving a later one.
    gold.write_to_gold(silver(spark, [event(timestamp=BASE + 60000)], [3]), 3)
    gold.write_to_gold(silver(spark, [event(stage="REFUNDED", amount=25)], [4]), 4)
    rows = read_gold(spark, root, "revenue_by_minute").orderBy("window_start").collect()
    assert [r.net_revenue for r in rows] == [25, 100]


def test_manifest_failure_recovery_and_empty(spark, monkeypatch, tmp_path):
    root = str(tmp_path / "gold") + "/"
    monkeypatch.setenv("KAFKA_TOPIC_ORDERS", "unit-gold")
    monkeypatch.setattr(gold, "gold_root", lambda *args: root)
    empty = silver(spark, [])
    gold.write_to_gold(empty, 0)
    assert current_manifest(spark, root) is None
    assert read_gold(spark, root, "revenue_by_minute").isEmpty()
    batch = silver(spark, [event()])
    publish = gold_store._publish_manifest

    def fail(*args):
        raise RuntimeError("injected before publication")

    monkeypatch.setattr(gold_store, "_publish_manifest", fail)
    with pytest.raises(RuntimeError, match="injected"):
        gold.write_to_gold(batch, 0)
    assert current_manifest(spark, root) is None
    monkeypatch.setattr(gold_store, "_publish_manifest", publish)
    gold.write_to_gold(batch, 0)
    assert read_gold(spark, root, "revenue_by_minute").first().net_revenue == 100
    # Callback retry after publish but before checkpoint completion.
    gold.write_to_gold(batch, 0)
    assert current_manifest(spark, root)["version"] == 1
    # An interrupted update must keep the previous committed tables visible.
    refund = silver(spark, [event(stage="REFUNDED", amount=30)], [1])
    monkeypatch.setattr(gold_store, "_publish_manifest", fail)
    with pytest.raises(RuntimeError, match="injected"):
        gold.write_to_gold(refund, 1)
    assert read_gold(spark, root, "revenue_by_minute").first().net_revenue == 100
    monkeypatch.setattr(gold_store, "_publish_manifest", publish)
    gold.write_to_gold(refund, 1)
    assert read_gold(spark, root, "revenue_by_minute").first().net_revenue == 70


def test_latest_corrupt_commit_does_not_fall_back(spark, tmp_path):
    root = str(tmp_path / "gold") + "/"
    gold_store._publish_manifest(spark, root, {"version": 1, "contract_version": "bad", "tables": {}})
    with pytest.raises(RuntimeError, match="Invalid Gold manifest"):
        current_manifest(spark, root)


def test_average_rounds_once_at_eight_places(spark):
    prepared = gold.prepare_events(silver(spark, [event(amount=.1)]), "unit-gold")
    many = (prepared.crossJoin(spark.range(10001)).withColumn("amount_decimal",
            F.when(F.col("id") < 5000, F.lit(Decimal("0.10000001")))
             .otherwise(F.lit(Decimal("0.10000000"))).cast(gold.MONEY)))
    row = gold.aggregate_events(gold.apply_revenue_sign(many))["revenue_by_minute"].first()
    assert row.gross_revenue == Decimal("1000.10005000")
    assert row.avg_order_value == Decimal("0.10000000")
