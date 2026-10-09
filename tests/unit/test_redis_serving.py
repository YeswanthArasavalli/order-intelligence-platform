"""M5 tests against real PostgreSQL/Redis, isolated disposable fixture schemas.

M4's row-serving API supplies synthetic M4-shaped fixtures. The M5 serving
entrypoint always reads PostgreSQL. Independent verification uses psycopg2 and
its own decoder in scripts.verify_milestone5 (no writer serialization helpers).
"""

from datetime import datetime, timezone
from decimal import Decimal
import json
import os
import subprocess
import sys
import uuid

import pytest
from sqlalchemy import create_engine, text

from scripts.verify_milestone5 import postgres_state, verify
from storage.gold_postgres_serving import ensure_schema, serve_rows
from storage.postgres_redis_serving import (
    InvalidSnapshot, RedisServingError, SnapshotConflict, StaleSnapshot, delete_cache,
    get_postgres_engine, get_redis_client, read_active_snapshot, serve_postgres_snapshot,
)


def revenue(region="A", currency="INR", net="-30.00000001"):
    return dict(window_start=datetime(2026, 10, 4, 0, 0, tzinfo=timezone.utc),
                window_end=datetime(2026, 10, 4, 0, 1, tzinfo=timezone.utc),
                region=region, currency=currency, order_count=1, failed_count=1, refunded_event_count=1,
                gross_revenue=Decimal("100.00000000"), refund_amount=Decimal("30.00000001"),
                cancellation_amount=Decimal("100.00000000"), net_revenue=Decimal(net),
                avg_order_value=Decimal("100.00000000"))


def product(name="Name:雪", currency="INR", minute=0):
    return dict(window_start=datetime(2026, 10, 4, 0, minute, tzinfo=timezone.utc),
                window_end=datetime(2026, 10, 4, 0, minute + 5, tzinfo=timezone.utc),
                product_id="same:id", product_name=name, currency=currency,
                units_sold=1, revenue=Decimal("999999999999999999999999999999.99999999"))


@pytest.fixture()
def world():
    engine, client = get_postgres_engine(), get_redis_client()
    suffix = uuid.uuid4().hex[:12]
    schema, namespace = "m5_test_" + suffix, "oip:m5:test-" + suffix
    ensure_schema(engine, schema)
    topic = "m5-test-topic"
    root = f"s3a://gold/{topic}/"

    def publish(version=1, revenues=None, products=None):
        revenues = [revenue("A"), revenue("B"), revenue("C")] if revenues is None else revenues
        products = [product(), product(currency="USD"), product(name="Other"), product(minute=5)] if products is None else products
        manifest = {"version": version, "contract_version": "1.0", "parent_version": version - 1, "tables": {
            name: {"path": root + f"_snapshots/test-{version}/{name}", "rows": count}
            for name, count in (("events", 0), ("revenue_by_minute", len(revenues)), ("product_rankings", len(products)))
        }}
        serve_rows(engine, manifest, topic, revenues, products, schema=schema, root=root)

    def refresh(**kwargs):
        return serve_postgres_snapshot(engine=engine, redis_client=client, schema=schema, namespace=namespace, **kwargs)

    yield engine, client, schema, namespace, publish, refresh
    # Only generated fixture resources, never M1-M4 historical schemas or other Redis keys.
    for key in client.scan_iter(match=namespace + ":*"):
        client.delete(key)
    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()
    client.close()


def test_initial_and_final_independent_equality(world):
    _, _, schema, ns, publish, refresh = world
    publish()
    assert refresh()["status"] == "SERVED"
    result = verify(schema, ns)
    assert result["postgres_digest"] == result["redis_digest"]
    assert result["active_hash_fields"] == 8


def test_same_version_noop_preserves_values_metadata_and_ttl(world):
    _, client, _, ns, publish, refresh = world
    publish()
    refresh()
    before = client.hgetall(ns + ":current")
    assert refresh()["status"] == "NOOP"
    assert client.hgetall(ns + ":current") == before
    assert client.ttl(ns + ":current") == -1


def test_new_version_replaces_removed_rows(world):
    _, client, schema, ns, publish, refresh = world
    publish()
    refresh()
    publish(2, [revenue("A"), revenue("B"), revenue("D")], [])
    refresh()
    snapshot = read_active_snapshot(client, ns)
    assert {r["region"] for r in snapshot["revenue_by_minute"]} == {"A", "B", "D"}
    assert snapshot["product_rankings"] == []
    assert snapshot["metadata"]["gold_version"] == 2
    assert verify(schema, ns)["key_count"] == 1


def test_currencies_decimal_negative_net_and_product_identity(world):
    _, client, schema, ns, publish, refresh = world
    publish(revenues=[revenue(currency="INR"), revenue(currency="USD")])
    refresh()
    snapshot = read_active_snapshot(client, ns)
    assert {r["currency"] for r in snapshot["revenue_by_minute"]} == {"INR", "USD"}
    assert all(Decimal(r["net_revenue"]) == Decimal("-30.00000001") for r in snapshot["revenue_by_minute"])
    assert len(snapshot["product_rankings"]) == 4
    assert all(Decimal(r["revenue"]) == Decimal("999999999999999999999999999999.99999999")
               for r in snapshot["product_rankings"])
    verify(schema, ns)


def test_cache_deletion_rebuild_leaves_postgres_untouched(world):
    _, client, schema, ns, publish, refresh = world
    publish()
    refresh()
    before = postgres_state(schema)
    cache = client.hgetall(ns + ":current")
    delete_cache(client, ns)
    assert read_active_snapshot(client, ns) is None
    assert refresh()["status"] == "SERVED"
    assert client.hgetall(ns + ":current") == cache
    assert before == postgres_state(schema)
    verify(schema, ns)


def test_redis_unavailable_keeps_postgres(world):
    import redis
    engine, _, schema, ns, publish, _ = world
    publish()
    before = postgres_state(schema)
    with redis.Redis(host="127.0.0.1", port=1, socket_connect_timeout=0.1) as dead:
        with pytest.raises(RedisServingError, match="Redis operation failed"):
            serve_postgres_snapshot(engine=engine, redis_client=dead, schema=schema, namespace=ns)
    assert before == postgres_state(schema)


def test_postgres_unavailable_no_fake_refresh(world):
    _, client, _, ns, publish, refresh = world
    publish()
    refresh()
    before = client.hgetall(ns + ":current")
    dead = create_engine("postgresql+psycopg2://127.0.0.1:1/unavailable", connect_args={"connect_timeout": 1})
    try:
        with pytest.raises(RedisServingError, match="PostgreSQL read failed"):
            serve_postgres_snapshot(engine=dead, redis_client=client, namespace=ns)
        assert client.hgetall(ns + ":current") == before
    finally:
        dead.dispose()


@pytest.mark.parametrize("stage", ["during_stage", "after_stage", "before_publish"])
def test_partial_refresh_keeps_complete_previous_version(world, stage):
    _, client, schema, ns, publish, refresh = world
    publish()
    refresh()
    old = client.hgetall(ns + ":current")
    publish(2, [revenue("new")])

    def fail(where):
        assert client.hgetall(ns + ":current") == old
        if where == stage:
            raise RuntimeError("injected partial refresh")

    with pytest.raises(RuntimeError, match="injected partial"):
        refresh(failure_hook=fail)
    assert client.hgetall(ns + ":current") == old
    assert len(list(client.scan_iter(match=ns + ":*"))) == 1
    assert refresh()["gold_version"] == 2
    verify(schema, ns)


def test_restart_process_is_idempotent(world):
    _, _, schema, ns, publish, refresh = world
    publish()
    refresh()
    result = subprocess.run([sys.executable, "-m", "storage.postgres_redis_serving", "--schema", schema,
                             "--namespace", ns], capture_output=True, text=True, env=os.environ.copy(), timeout=30)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "NOOP"
    verify(schema, ns)


def test_old_postgres_version_rejected(world):
    engine, client, schema, ns, publish, refresh = world
    publish(2)
    refresh()
    before = client.hgetall(ns + ":current")
    # Corrupt only this synthetic fixture's version to simulate an old PG restore.
    with engine.begin() as conn:
        conn.execute(text(f'UPDATE "{schema}".serving_state SET gold_version=1'))
    with pytest.raises(StaleSnapshot):
        refresh()
    assert client.hgetall(ns + ":current") == before


@pytest.mark.parametrize("change", ["net_revenue=0", "gross_revenue=-1", "gross_revenue='NaN'", "order_count=-1"])
def test_corrupt_postgres_rejected_without_publication(world, change):
    engine, client, schema, ns, publish, refresh = world
    publish()
    refresh()
    before = client.hgetall(ns + ":current")
    with engine.begin() as conn:
        conn.execute(text(f'UPDATE "{schema}".revenue_by_minute SET {change}'))
    with pytest.raises(InvalidSnapshot):
        refresh()
    assert before == client.hgetall(ns + ":current")


def test_empty_committed_snapshot(world):
    _, client, schema, ns, publish, refresh = world
    publish()
    refresh()
    publish(2, [], [])
    refresh()
    assert client.hlen(ns + ":current") == 1
    assert verify(schema, ns)["rows"] == {"revenue_by_minute": 0, "product_rankings": 0}


def test_namespace_isolation_and_key_encoding(world):
    _, client, schema, ns, publish, refresh = world
    other = ns + "-other:current"
    client.set(other, "unrelated")
    try:
        publish(revenues=[revenue("x:*:[雪]:currency")])
        refresh()
        verify(schema, ns)
        delete_cache(client, ns)
        assert client.get(other) == "unrelated"
        for invalid in ("*", "oip:m5:x:*", "legacy", "oip:m5:../escape"):
            with pytest.raises(ValueError):
                delete_cache(client, invalid)
    finally:
        client.delete(other)


def test_missing_cache_row_is_not_served_and_is_repaired(world):
    _, client, schema, ns, publish, refresh = world
    publish()
    refresh()
    field = next(k for k in client.hkeys(ns + ":current") if k != "@metadata")
    client.hdel(ns + ":current", field)
    with pytest.raises(InvalidSnapshot):
        read_active_snapshot(client, ns)
    assert refresh()["status"] == "SERVED"
    verify(schema, ns)


def test_overlapping_refresh_cannot_overwrite_completed_newer_snapshot(world):
    _, client, schema, ns, publish, refresh = world
    publish()
    refresh()
    publish(2)

    def overlap(stage):
        if stage == "before_publish":
            publish(3, [revenue("newest")])
            assert refresh()["gold_version"] == 3

    with pytest.raises(RedisServingError, match="changed during refresh"):
        refresh(failure_hook=overlap)
    assert read_active_snapshot(client, ns)["metadata"]["gold_version"] == 3
    assert verify(schema, ns)["key_count"] == 1


def test_stage_eviction_before_switch_preserves_previous_cache(world):
    _, client, _, ns, publish, refresh = world
    publish()
    refresh()
    before = client.hgetall(ns + ":current")
    publish(2)

    def evict(stage):
        if stage == "before_publish":
            for key in client.scan_iter(match=ns + ":stage:*"):
                client.delete(key)

    with pytest.raises(RedisServingError):
        refresh(failure_hook=evict)
    assert client.hgetall(ns + ":current") == before


def test_source_metadata_conflict_and_bounds(world, monkeypatch):
    engine, client, schema, ns, publish, refresh = world
    publish()
    refresh()
    before = client.hgetall(ns + ":current")
    monkeypatch.setenv("REDIS_GOLD_MAX_ROWS", "1")
    with pytest.raises(InvalidSnapshot):
        refresh()
    monkeypatch.delenv("REDIS_GOLD_MAX_ROWS")
    monkeypatch.setenv("REDIS_GOLD_MAX_BYTES", "1")
    with pytest.raises(InvalidSnapshot):
        refresh()
    monkeypatch.delenv("REDIS_GOLD_MAX_BYTES")
    with engine.begin() as conn:
        conn.execute(text(f'UPDATE "{schema}".serving_state SET manifest_digest=:digest'), {"digest": "f" * 64})
    with pytest.raises(SnapshotConflict):
        refresh()
    assert before == client.hgetall(ns + ":current")


def test_failure_midway_through_multichunk_load(world):
    _, client, schema, ns, publish, refresh = world
    publish()
    refresh()
    before = client.hgetall(ns + ":current")
    publish(2, [revenue(f"region-{i}") for i in range(503)], [])

    def interrupt(stage):
        if stage == "during_stage":
            staged = list(client.scan_iter(match=ns + ":stage:*"))
            assert len(staged) == 1 and client.hlen(staged[0]) == 500
            assert json.loads(client.hget(staged[0], "@metadata"))["serving_status"] == "STAGING"
            assert client.hgetall(ns + ":current") == before
            raise RuntimeError("partial 500 of 504 hash fields")

    with pytest.raises(RuntimeError, match="partial 500"):
        refresh(failure_hook=interrupt)
    assert client.hgetall(ns + ":current") == before
    refresh()
    assert verify(schema, ns)["active_hash_fields"] == 504


def test_full_negative_decimal_nullable_average_and_precise_utc(world):
    _, client, schema, ns, publish, refresh = world
    row = revenue(net="-999999999999999999999999999999.99999999")
    row.update(avg_order_value=None, window_start=datetime.fromisoformat("2026-10-04T05:30:00.123456+05:30"))
    publish(revenues=[row], products=[])
    refresh()
    cached = read_active_snapshot(client, ns)["revenue_by_minute"][0]
    assert cached["net_revenue"] == "-999999999999999999999999999999.99999999"
    assert cached["avg_order_value"] is None
    assert cached["window_start"] == "2026-10-04T00:00:00.123456+00:00"
    verify(schema, ns)
