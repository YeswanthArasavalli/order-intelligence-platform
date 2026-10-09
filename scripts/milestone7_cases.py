"""Explicit synthetic M4-shaped PostgreSQL fixtures for M7 verification only."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from alerts.definitions import Rule
from storage.gold_postgres_serving import serve_rows


def revenue(net="5.00000000", *, currency="INR", minute=1, region="test-region"):
    start = datetime(2026, 10, 4, tzinfo=timezone.utc) + timedelta(minutes=minute)
    return {"window_start": start, "window_end": start + timedelta(minutes=1), "region": region,
            "currency": currency, "order_count": 1, "failed_count": 0, "refunded_event_count": 0,
            "gross_revenue": Decimal("100.00000000"), "refund_amount": Decimal("0.00000000"),
            "cancellation_amount": Decimal("0.00000000"), "net_revenue": Decimal(net),
            "avg_order_value": Decimal("100.00000000")}


def product(revenue_value="5.00000000", *, currency="INR", name="Product A", minute=0):
    start = datetime(2026, 10, 4, tzinfo=timezone.utc) + timedelta(minutes=minute)
    return {"window_start": start, "window_end": start + timedelta(minutes=5), "product_id": "product-1",
            "product_name": name, "currency": currency, "units_sold": 1, "revenue": Decimal(revenue_value)}


def rule(rule_id="inr-test", *, currency="INR", cooldown=60, **changes):
    config = {"rule_id": rule_id, "kind": "revenue_threshold", "currency": currency,
              "dimensions": {"region": "test-region"}, "metric": "net_revenue", "comparison": "lt",
              "threshold": "10.00000000", "cooldown_seconds": cooldown, "severity": "WARNING", "enabled": True}
    return Rule(**{**config, **changes})


def publish(engine, schema, version, revenues=None, products=None):
    revenues = [revenue(), revenue("20", currency="USD")] if revenues is None else revenues
    products = [product()] if products is None else products
    topic = "m7-controlled"
    root = f"s3a://gold/{topic}/"
    manifest = {"version": version, "contract_version": "1.0", "parent_version": version - 1, "tables": {
        name: {"path": root + f"_snapshots/fixture-{version}/{name}", "rows": count}
        for name, count in (("events", 0), ("revenue_by_minute", len(revenues)), ("product_rankings", len(products)))
    }}
    return serve_rows(engine, manifest, topic, revenues, products, schema=schema, root=root)
