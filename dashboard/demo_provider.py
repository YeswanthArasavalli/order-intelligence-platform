"""Synthetic demo provider for the M6 dashboard (``OIP_DEMO_MODE=synthetic``).

The demo exists so the dashboard can be demonstrated without Kafka, Spark,
MinIO, PostgreSQL, Redis, cloud services, credentials or a ``.env`` file. The
sample is a fixed literal dataset, encoded, decoded and validated through the
same M4/M5 serving-contract helpers the live read path uses, then handed to the
unchanged M6 renderer. Nothing is generated from the clock or from randomness.

Boundaries and honesty:

* Selection is explicit and fail-safe (see :func:`selected_mode`). Demo mode
  never reads a real source and real mode never falls back to this provider.
* Synthetic rows never enter real storage: this module has no writer, no cache
  refresh and no engine/client construction.
* ``source_schema``/``source_database``/``serving_status``/``gold_version`` are
  contract-shaped fields the real validator requires; they describe the shape of
  the synthetic snapshot, not a live PostgreSQL serve. ``manifest_digest`` and
  ``snapshot_identity`` are placeholder digests derived from a fixed label, not
  content digests. ``snapshot_digest`` is the genuine contract digest over the
  synthetic rows.
* ``DASHBOARD_SOURCE_TOPIC`` is ignored in demo mode: a synthetic sample has no
  source topic to pin, so pinning one would only produce a false mismatch.

Dataset specification (single authoritative size):

* 12 revenue rows = 4 one-minute windows x 3 region/currency combinations
  (``north-india``/INR, ``south-india``/INR, ``international``/USD).
* 12 product rows = one five-minute window covering those minutes, 7 INR and
  5 USD product rows taken from the producer catalog.
* 24 analytical rows in total, anchored at ``2026-10-05T12:00:00+00:00``.
* Per currency the product totals tie to the revenue totals exactly (paid-event
  count equals units sold, gross revenue equals product revenue), because the
  contract derives both tables from the same paid events.
"""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, localcontext
import hashlib
import os

from dashboard.read_service import DashboardConfig, DashboardSnapshot, _typed
from storage.postgres_redis_serving import TABLES, _decode, _digest, _fields, _row, _utc

DEMO_MODE_ENV_VAR = "OIP_DEMO_MODE"
DEMO_MODE_VALUE = "synthetic"
MODE_REAL = "real"
MODE_DEMO = "demo"
MODE_INVALID = "invalid"

DEMO_SOURCE_LABEL = "Synthetic demo data"
DEMO_REDIS_STATUS = "not used (demo)"
DEMO_DISCLOSURE = "Synthetic demo data — not a live pipeline."

# Fixed historical sample: never derived from the current clock.
ANCHOR = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)
SERVED_AT = datetime(2026, 10, 5, 12, 5, 30, tzinfo=timezone.utc)
GOLD_VERSION = 7
DEMO_TOPIC = "synthetic-order-events"
DEMO_SOURCE_ROOT = "synthetic://gold/synthetic-order-events"
DEMO_MANIFEST_PATH = "synthetic://gold/synthetic-order-events/manifests/00000007.json"

REVENUE_ROW_COUNT = 12
PRODUCT_ROW_COUNT = 12

# Region values mirror data_quality/validators.py (VALID_REGIONS). They are
# repeated here because that module imports PySpark, which the demo must not
# require. Currency codes are validated against data_quality/currency_codes.py.
DEMO_REGIONS = frozenset({"north-india", "south-india", "international"})

# (minute offset, region, currency, order_count, failed_count,
#  refunded_event_count, gross_revenue, refund_amount, cancellation_amount)
# net_revenue and avg_order_value are derived with the documented contract rules,
# so no monetary figure in the sample can contradict its own ledger.
_REVENUE_SPEC = (
    (0, "north-india", "INR", 4, 0, 0, "10000.00000000", "0.00000000", "0.00000000"),
    (0, "south-india", "INR", 2, 1, 0, "1999.50000000", "0.00000000", "499.25000000"),
    (0, "international", "USD", 0, 2, 0, "0.00000000", "0.00000000", "0.00000000"),
    (1, "north-india", "INR", 3, 0, 1, "7497.00000000", "1249.00000000", "0.00000000"),
    (1, "south-india", "INR", 5, 0, 0, "24999.99999995", "0.00000000", "0.00000000"),
    (1, "international", "USD", 3, 0, 1, "1860.00000000", "620.00000000", "0.00000000"),
    (2, "north-india", "INR", 6, 1, 2, "15000.00000000", "3000.00000000", "1500.00000000"),
    (2, "south-india", "INR", 1, 1, 1, "349.00000000", "349.00000000", "349.00000000"),
    (2, "international", "USD", 5, 0, 0, "12999.99000000", "0.00000000", "0.00000000"),
    (3, "north-india", "INR", 2, 0, 0, "2.00000001", "0.00000000", "0.00000000"),
    (3, "south-india", "INR", 3, 2, 0, "15330.50000005", "0.00000000", "2500.00000000"),
    (3, "international", "USD", 3, 1, 0, "20130.00999999", "0.00000000", "1240.00000000"),
)

# (product_id, product_name, currency, units_sold, revenue) — all rows share the
# single five-minute window that covers the revenue minutes above.
_PRODUCT_SPEC = (
    ("prod-1001", "Wireless Headphones", "INR", 6, "19998.00000000"),
    ("prod-1002", "Smartphone Case", "INR", 8, "3192.00000000"),
    ("prod-1003", "USB-C Cable", "INR", 5, "1995.00000000"),
    ("prod-1006", "Running Shoes", "INR", 2, "11998.00000000"),
    ("prod-1010", "Yoga Mat", "INR", 2, "3998.00000000"),
    ("prod-1014", "Mechanical Keyboard", "INR", 1, "8998.99999999"),
    ("prod-1015", "Monitor (24 inch)", "INR", 2, "24998.00000002"),
    ("prod-1005", "Bluetooth Speaker", "USD", 2, "5998.00000000"),
    ("prod-1007", "Cotton T-Shirt", "USD", 4, "3996.00000000"),
    ("prod-1012", "Protein Powder", "USD", 3, "5997.00000000"),
    ("prod-1013", "Desk Lamp", "USD", 1, "999.00000000"),
    ("prod-1015", "Monitor (24 inch)", "USD", 1, "17999.99999999"),
)


def selected_mode(value: str | None = None) -> str:
    """Classify the demo flag without touching any data source.

    Unset or empty means real mode; exactly ``synthetic`` means demo mode; any
    other value is invalid and must stop the application before real clients
    are constructed. Whitespace and case variants are invalid, not real mode.
    """
    if value is None:
        value = os.environ.get(DEMO_MODE_ENV_VAR)
    if value is None or value == "":
        return MODE_REAL
    return MODE_DEMO if value == DEMO_MODE_VALUE else MODE_INVALID


def mode_error_message(value: str | None = None) -> str:
    """Configuration error text for an unusable ``OIP_DEMO_MODE`` value."""
    if value is None:
        value = os.environ.get(DEMO_MODE_ENV_VAR)
    return (f"Invalid {DEMO_MODE_ENV_VAR}={value!r}: expected empty (real pipeline sources) "
            f"or exactly {DEMO_MODE_VALUE!r} (synthetic demo). No data source was initialized.")


def demo_config() -> DashboardConfig:
    """Dashboard configuration for demo mode; the source-topic pin carries no meaning here."""
    return replace(DashboardConfig.from_env(), expected_topic=None)


def _placeholder_digest(label: str) -> str:
    return hashlib.sha256(f"oip-synthetic-demo:{label}".encode()).hexdigest()


def _average(total: Decimal, count: int) -> Decimal | None:
    """Documented Gold rule: mean of paid amounts, null when there are none."""
    if not count:
        return None
    with localcontext() as context:
        context.prec = 80
        return (total / Decimal(count)).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


def _revenue_rows() -> list[dict]:
    rows = []
    for offset, region, currency, orders, failed, refunded, gross, refund, cancellation in _REVENUE_SPEC:
        gross, refund, cancellation = Decimal(gross), Decimal(refund), Decimal(cancellation)
        start = ANCHOR + timedelta(minutes=offset)
        rows.append(_row({"window_start": start, "window_end": start + timedelta(minutes=1),
                          "region": region, "currency": currency, "order_count": orders,
                          "failed_count": failed, "refunded_event_count": refunded,
                          "gross_revenue": gross, "refund_amount": refund,
                          "cancellation_amount": cancellation,
                          "net_revenue": gross - refund - cancellation,
                          "avg_order_value": _average(gross, orders)}, "revenue_by_minute"))
    return rows


def _product_rows() -> list[dict]:
    return [_row({"window_start": ANCHOR, "window_end": ANCHOR + timedelta(minutes=5),
                  "product_id": product_id, "product_name": name, "currency": currency,
                  "units_sold": units, "revenue": Decimal(revenue)}, "product_rankings")
            for product_id, name, currency, units, revenue in _PRODUCT_SPEC]


def synthetic_payload(config: DashboardConfig) -> dict:
    """Canonical serving-contract payload (Redis wire forms) for the fixed sample."""
    tables = {"revenue_by_minute": _revenue_rows(), "product_rankings": _product_rows()}
    metadata = {
        "source_topic": DEMO_TOPIC, "source_root": DEMO_SOURCE_ROOT, "gold_contract_version": "1.0",
        "gold_version": GOLD_VERSION, "manifest_path": DEMO_MANIFEST_PATH,
        "manifest_digest": _placeholder_digest("manifest"), "snapshot_identity": _placeholder_digest("snapshot"),
        "snapshot_digest": _digest(tables), "revenue_rows": len(tables["revenue_by_minute"]),
        "product_rows": len(tables["product_rankings"]), "served_at": _utc(SERVED_AT),
        "source_database": config.database, "source_schema": config.schema, "cache_contract_version": "1.0",
        "serving_status": "SERVED",
    }
    return {"metadata": metadata, **tables}


def synthetic_snapshot(config: DashboardConfig | None = None) -> DashboardSnapshot:
    """Validated demo snapshot through the real encode → decode → typed chain.

    ``_fields``/``_decode`` are the M5 serializer and decoder, so the sample is
    proven to survive the actual Redis wire format; ``_typed`` then applies the
    unchanged M6 consumer validation (metadata identity, counts, digests).
    """
    config = config or demo_config()
    payload = synthetic_payload(config)
    decoded = _decode(_fields(payload))
    if sorted(decoded) != sorted(payload) or decoded["metadata"] != payload["metadata"]:
        raise AssertionError("Synthetic payload did not survive the serving-contract round trip")
    for table in TABLES:
        if list(decoded[table]) != list(payload[table]):
            raise AssertionError("Synthetic rows did not survive the serving-contract round trip")
    return _typed(decoded, DEMO_SOURCE_LABEL, DEMO_REDIS_STATUS, config)
