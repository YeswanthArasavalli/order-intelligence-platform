"""PostgreSQL runtime tests for the M3 committed Gold serving contract."""

from datetime import datetime, timezone
from decimal import Decimal
import uuid

import pytest
from sqlalchemy import text

from storage.gold_postgres_serving import (
    InvalidGoldSnapshotError,
    ManifestConflictError,
    StaleGoldManifestError,
    ensure_schema,
    get_engine,
    MAX_NUMERIC_38_8,
    read_serving_state,
    serve_rows,
)


ROOT = "s3a://gold/m4-topic/"


def _manifest(version, revenue_rows, product_rows, root=ROOT):
    attempt = f"unit-attempt-{version}"
    return {
        "contract_version": "1.0",
        "version": version,
        "parent_version": version - 1,
        "tables": {
            "events": {"path": root + f"_snapshots/{attempt}/events", "rows": 1},
            "revenue_by_minute": {"path": root + f"_snapshots/{attempt}/revenue_by_minute", "rows": len(revenue_rows)},
            "product_rankings": {"path": root + f"_snapshots/{attempt}/product_rankings", "rows": len(product_rows)},
        },
    }


def _revenue(start_minute, currency="INR", net="100.00000000", region="north-india"):
    start = datetime(2026, 10, 3, 15, start_minute, tzinfo=timezone.utc)
    return {
        "window_start": start, "window_end": start.replace(minute=start.minute + 1),
        "region": region, "currency": currency, "order_count": 1,
        "failed_count": 0, "refunded_event_count": 0,
        "gross_revenue": Decimal("100.00000000"), "refund_amount": Decimal("0.00000000"),
        "cancellation_amount": Decimal("0.00000000"), "net_revenue": Decimal(net),
        "avg_order_value": Decimal("100.00000000"),
    }


def _product(start_minute, currency="INR", name="Product"):
    start = datetime(2026, 10, 3, 15, start_minute, tzinfo=timezone.utc)
    return {
        "window_start": start, "window_end": start.replace(minute=start.minute + 5),
        "product_id": "prod-3001", "product_name": name, "currency": currency,
        "units_sold": 1, "revenue": Decimal("100.00000000"),
    }


@pytest.fixture()
def db():
    engine = get_engine()
    schema = "m4_test_" + uuid.uuid4().hex[:12]
    ensure_schema(engine, schema)
    yield engine, schema
    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _rows(engine, schema, table):
    with engine.connect() as conn:
        return conn.execute(text(f'SELECT * FROM "{schema}"."{table}" ORDER BY 1,2')).mappings().all()


def test_complete_snapshot_currency_precision_and_same_manifest_noop(db):
    engine, schema = db
    first_revenue = [_revenue(10), _revenue(11, "USD", "20.00000000")]
    first_products = [_product(10), _product(11, "USD")]
    first = serve_rows(engine, _manifest(1, first_revenue, first_products), "m4-topic",
                       first_revenue, first_products, schema=schema, root=ROOT)
    assert first["status"] == "SERVED"
    assert len(_rows(engine, schema, "revenue_by_minute")) == 2
    assert _rows(engine, schema, "revenue_by_minute")[1]["currency"] == "USD"
    assert _rows(engine, schema, "revenue_by_minute")[1]["net_revenue"] == Decimal("20.00000000")
    same = serve_rows(engine, _manifest(1, first_revenue, first_products), "m4-topic",
                      first_revenue, first_products, schema=schema, root=ROOT)
    assert same["status"] == "NOOP"
    assert read_serving_state(engine, schema)["gold_version"] == 1
    assert len(_rows(engine, schema, "product_rankings")) == 2


def test_new_complete_snapshot_replaces_stale_rows_and_late_revision(db):
    engine, schema = db
    old_revenue, old_products = [_revenue(10), _revenue(11, "USD", "20.00000000")], [_product(10), _product(11, "USD")]
    serve_rows(engine, _manifest(1, old_revenue, old_products), "m4-topic", old_revenue, old_products,
               schema=schema, root=ROOT)
    new_revenue = [_revenue(10, net="85.00000000")]
    new_products = [_product(10)]
    result = serve_rows(engine, _manifest(2, new_revenue, new_products), "m4-topic",
                        new_revenue, new_products, schema=schema, root=ROOT)
    assert result["gold_version"] == 2
    assert len(_rows(engine, schema, "revenue_by_minute")) == 1
    row = _rows(engine, schema, "revenue_by_minute")[0]
    assert row["currency"] == "INR" and row["net_revenue"] == Decimal("85.00000000")
    assert len(_rows(engine, schema, "product_rankings")) == 1
    assert read_serving_state(engine, schema)["snapshot_digest"] == result["snapshot_digest"]


def test_old_manifest_conflict_and_invalid_values_do_not_change_state(db):
    engine, schema = db
    rows = [_revenue(10)]
    serve_rows(engine, _manifest(2, rows, []), "m4-topic", rows, [], schema=schema, root=ROOT)
    with pytest.raises(StaleGoldManifestError):
        serve_rows(engine, _manifest(1, rows, []), "m4-topic", rows, [], schema=schema, root=ROOT)
    with pytest.raises(ManifestConflictError):
        serve_rows(engine, _manifest(2, [_revenue(10, net="99.00000000")], []), "m4-topic",
                   [_revenue(10, net="99.00000000")], [], schema=schema, root=ROOT)
    with pytest.raises(InvalidGoldSnapshotError):
        serve_rows(engine, _manifest(3, [{**_revenue(10), "currency": "usd"}], []), "m4-topic",
                   [{**_revenue(10), "currency": "usd"}], [], schema=schema, root=ROOT)
    with pytest.raises(InvalidGoldSnapshotError):
        serve_rows(engine, {"version": 3, "contract_version": "bad", "tables": {}}, "m4-topic",
                   [], [], schema=schema, root=ROOT)
    assert read_serving_state(engine, schema)["gold_version"] == 2
    assert len(_rows(engine, schema, "revenue_by_minute")) == 1


@pytest.mark.parametrize("stage", ["after_stage", "after_delete", "after_insert", "before_commit"])
def test_failure_anywhere_in_replacement_rolls_back(db, stage):
    engine, schema = db
    old = [_revenue(10)]
    serve_rows(engine, _manifest(1, old, []), "m4-topic", old, [], schema=schema, root=ROOT)
    new = [_revenue(11, net="-30.00000000")]

    def fail(current):
        if current == stage:
            raise RuntimeError("injected serving failure")

    with pytest.raises(RuntimeError, match="injected serving failure"):
        serve_rows(engine, _manifest(2, new, []), "m4-topic", new, [], schema=schema, root=ROOT,
                   failure_hook=fail)
    state = read_serving_state(engine, schema)
    assert state["gold_version"] == 1
    assert len(_rows(engine, schema, "revenue_by_minute")) == 1
    assert _rows(engine, schema, "revenue_by_minute")[0]["window_start"].minute == 10
    # Retry the same newer snapshot after rollback.
    result = serve_rows(engine, _manifest(2, new, []), "m4-topic", new, [], schema=schema, root=ROOT)
    assert result["gold_version"] == 2
    assert _rows(engine, schema, "revenue_by_minute")[0]["net_revenue"] == Decimal("-30.00000000")


def test_product_identity_keeps_name_and_currency(db):
    engine, schema = db
    products = [_product(10, "INR", "Name A"), _product(10, "USD", "Name A"), _product(10, "INR", "Name B")]
    # Change product IDs so all three are valid distinct M3 logical identities.
    products[2]["product_id"] = "prod-3002"
    revenues = [_revenue(10), _revenue(11, "USD", "20.00000000")]
    manifest = _manifest(1, revenues, products)
    manifest["tables"]["product_rankings"]["rows"] = 3
    serve_rows(engine, manifest, "m4-topic", revenues, products, schema=schema, root=ROOT)
    with engine.connect() as conn:
        assert conn.execute(text(f'SELECT COUNT(*) FROM "{schema}".product_rankings')).scalar_one() == 3
        assert conn.execute(text(f'SELECT COUNT(DISTINCT currency) FROM "{schema}".product_rankings')).scalar_one() == 2


def test_empty_snapshot_removes_previous_rows_and_max_decimal_is_preserved(db):
    engine, schema = db
    revenue = [_revenue(10)]
    products = [_product(10)]
    serve_rows(engine, _manifest(1, revenue, products), "m4-topic", revenue, products,
               schema=schema, root=ROOT)

    maximum = str(MAX_NUMERIC_38_8)
    max_revenue = [_revenue(11, net=maximum)]
    serve_rows(engine, _manifest(2, max_revenue, []), "m4-topic", max_revenue, [],
               schema=schema, root=ROOT)
    assert _rows(engine, schema, "revenue_by_minute")[0]["net_revenue"] == MAX_NUMERIC_38_8
    assert len(_rows(engine, schema, "product_rankings")) == 0

    serve_rows(engine, _manifest(3, [], []), "m4-topic", [], [], schema=schema, root=ROOT)
    assert len(_rows(engine, schema, "revenue_by_minute")) == 0
    assert len(_rows(engine, schema, "product_rankings")) == 0


def test_negative_non_net_money_is_rejected(db):
    engine, schema = db
    bad = [{**_revenue(10), "gross_revenue": Decimal("-1.00000000")}]
    with pytest.raises(InvalidGoldSnapshotError):
        serve_rows(engine, _manifest(1, bad, []), "m4-topic", bad, [], schema=schema, root=ROOT)
