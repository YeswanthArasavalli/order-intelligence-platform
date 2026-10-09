"""
Order Intelligence Platform — Phase 2
Order Event Generator
=====================
Simulates realistic e-commerce order lifecycle events.

Lifecycle state machine:
  ORDER_CREATED → ORDER_PAID (85%) → ORDER_SHIPPED (95%) → ORDER_DELIVERED (90%)
                           ↓                    ↓
                   ORDER_CANCELLED (15%)  ORDER_REFUNDED (10%)

Usage:
    python -m producer.order_generator          # default 10 TPS
    python -m producer.order_generator --tps 50
    python -m producer.order_generator --tps 100 --duration 60
"""

import json
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from faker import Faker

fake = Faker("en_IN")

# ---------------------------------------------------------------------------
# Reference data — realistic Indian e-commerce catalog
# ---------------------------------------------------------------------------

REGIONS = {
    "north-india":     0.28,
    "south-india":     0.25,
    "west-india":      0.22,
    "east-india":      0.15,
    "international":   0.10,
}

PRODUCTS = [
    {"id": "prod-1001", "name": "Wireless Headphones",      "price_range": (800,   4999)},
    {"id": "prod-1002", "name": "Smartphone Case",          "price_range": (99,    599)},
    {"id": "prod-1003", "name": "USB-C Cable",              "price_range": (149,   499)},
    {"id": "prod-1004", "name": "Laptop Stand",             "price_range": (499,   2499)},
    {"id": "prod-1005", "name": "Bluetooth Speaker",        "price_range": (999,   5999)},
    {"id": "prod-1006", "name": "Running Shoes",            "price_range": (1499,  7999)},
    {"id": "prod-1007", "name": "Cotton T-Shirt",           "price_range": (299,   999)},
    {"id": "prod-1008", "name": "Steel Water Bottle",       "price_range": (349,   1299)},
    {"id": "prod-1009", "name": "Notebook Set",             "price_range": (199,   599)},
    {"id": "prod-1010", "name": "Yoga Mat",                 "price_range": (599,   2999)},
    {"id": "prod-1011", "name": "Face Wash",                "price_range": (149,   699)},
    {"id": "prod-1012", "name": "Protein Powder",           "price_range": (799,   3499)},
    {"id": "prod-1013", "name": "Desk Lamp",                "price_range": (449,   1999)},
    {"id": "prod-1014", "name": "Mechanical Keyboard",      "price_range": (1999,  8999)},
    {"id": "prod-1015", "name": "Monitor (24 inch)",        "price_range": (7999, 24999)},
]

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet", "COD"]

# Probability weights for payment methods
PAYMENT_WEIGHTS = [0.35, 0.25, 0.15, 0.10, 0.10, 0.05]

CANCELLATION_REASONS = [
    "Changed my mind",
    "Found better price",
    "Duplicate order",
    "Delivery time too long",
    "Item not needed anymore",
]

REFUND_REASONS = [
    "Item damaged on delivery",
    "Wrong item delivered",
    "Item not as described",
    "Quality not satisfactory",
    "Missing parts",
]


# ---------------------------------------------------------------------------
# Event dataclass
# ---------------------------------------------------------------------------

@dataclass
class OrderEvent:
    order_id:       str
    event_type:     str
    user_id:        str
    product_id:     str
    product_name:   str
    region:         str
    amount:         float
    currency:       str
    payment_method: Optional[str]
    status:         str
    event_metadata: Optional[str]
    timestamp:      int           # epoch milliseconds
    schema_version: str = "1.0"

    def to_dict(self) -> dict:
        return {
            "order_id":       self.order_id,
            "event_type":     self.event_type,
            "user_id":        self.user_id,
            "product_id":     self.product_id,
            "product_name":   self.product_name,
            "region":         self.region,
            "amount":         self.amount,
            "currency":       self.currency,
            "payment_method": self.payment_method,
            "status":         self.status,
            "event_metadata": self.event_metadata,
            "timestamp":      self.timestamp,
            "schema_version": self.schema_version,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


# ---------------------------------------------------------------------------
# Order state — tracks lifecycle across events
# ---------------------------------------------------------------------------

@dataclass
class OrderState:
    order_id:       str
    user_id:        str
    product:        dict
    region:         str
    amount:         float
    payment_method: str
    created_at:     float = field(default_factory=time.time)
    paid:           bool = False
    shipped:        bool = False
    cancelled:      bool = False
    refunded:       bool = False


def _now_ms() -> int:
    """Current UTC time in epoch milliseconds."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _pick_region() -> str:
    regions = list(REGIONS.keys())
    weights = list(REGIONS.values())
    return random.choices(regions, weights=weights, k=1)[0]


def _pick_product() -> dict:
    return random.choice(PRODUCTS)


def _pick_amount(product: dict) -> float:
    lo, hi = product["price_range"]
    return round(random.uniform(lo, hi), 2)


def _pick_payment() -> str:
    return random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0]


# ---------------------------------------------------------------------------
# Event constructors
# ---------------------------------------------------------------------------

def make_created(state: OrderState) -> OrderEvent:
    return OrderEvent(
        order_id=state.order_id,
        event_type="ORDER_CREATED",
        user_id=state.user_id,
        product_id=state.product["id"],
        product_name=state.product["name"],
        region=state.region,
        amount=state.amount,
        currency="INR",
        payment_method=state.payment_method,
        status="CREATED",
        event_metadata=json.dumps({
            "session_id": str(uuid.uuid4())[:8],
            "platform":   random.choice(["android", "ios", "web"]),
        }),
        timestamp=_now_ms(),
    )


def make_paid(state: OrderState) -> OrderEvent:
    # Charged amount may differ slightly (coupons/discounts)
    discount = random.choices([0, 0.05, 0.10, 0.15], weights=[0.6, 0.2, 0.15, 0.05])[0]
    charged = round(state.amount * (1 - discount), 2)
    return OrderEvent(
        order_id=state.order_id,
        event_type="ORDER_PAID",
        user_id=state.user_id,
        product_id=state.product["id"],
        product_name=state.product["name"],
        region=state.region,
        amount=charged,          # revenue recognised at this amount
        currency="INR",
        payment_method=state.payment_method,
        status="PAID",
        event_metadata=json.dumps({
            "payment_id":  f"pay-{uuid.uuid4().hex[:10]}",
            "gateway":     random.choice(["razorpay", "paytm", "stripe", "cashfree"]),
            "discount_pct": discount * 100,
        }),
        timestamp=_now_ms(),
    )


def make_shipped(state: OrderState) -> OrderEvent:
    return OrderEvent(
        order_id=state.order_id,
        event_type="ORDER_SHIPPED",
        user_id=state.user_id,
        product_id=state.product["id"],
        product_name=state.product["name"],
        region=state.region,
        amount=state.amount,
        currency="INR",
        payment_method=state.payment_method,
        status="SHIPPED",
        event_metadata=json.dumps({
            "shipment_id": f"shp-{uuid.uuid4().hex[:8]}",
            "carrier":     random.choice(["Delhivery", "BlueDart", "DTDC", "Ecom Express"]),
            "warehouse":   random.choice(["DEL-01", "MUM-02", "BLR-03", "HYD-01"]),
        }),
        timestamp=_now_ms(),
    )


def make_delivered(state: OrderState) -> OrderEvent:
    return OrderEvent(
        order_id=state.order_id,
        event_type="ORDER_DELIVERED",
        user_id=state.user_id,
        product_id=state.product["id"],
        product_name=state.product["name"],
        region=state.region,
        amount=state.amount,
        currency="INR",
        payment_method=state.payment_method,
        status="DELIVERED",
        event_metadata=json.dumps({
            "delivered_at": datetime.now(timezone.utc).isoformat(),
        }),
        timestamp=_now_ms(),
    )


def make_cancelled(state: OrderState) -> OrderEvent:
    was_paid = state.paid
    return OrderEvent(
        order_id=state.order_id,
        event_type="ORDER_CANCELLED",
        user_id=state.user_id,
        product_id=state.product["id"],
        product_name=state.product["name"],
        region=state.region,
        amount=state.amount if was_paid else 0.0,
        currency="INR",
        payment_method=state.payment_method,
        status="CANCELLED",
        event_metadata=json.dumps({
            "reason":   random.choice(CANCELLATION_REASONS),
            "was_paid": was_paid,
        }),
        timestamp=_now_ms(),
    )


def make_refunded(state: OrderState) -> OrderEvent:
    # Partial or full refund
    refund_pct = random.choices([1.0, 0.5, 0.75], weights=[0.7, 0.2, 0.1])[0]
    refund_amount = round(state.amount * refund_pct, 2)
    return OrderEvent(
        order_id=state.order_id,
        event_type="ORDER_REFUNDED",
        user_id=state.user_id,
        product_id=state.product["id"],
        product_name=state.product["name"],
        region=state.region,
        amount=refund_amount,
        currency="INR",
        payment_method=state.payment_method,
        status="REFUNDED",
        event_metadata=json.dumps({
            "refund_id":     f"ref-{uuid.uuid4().hex[:8]}",
            "refund_reason": random.choice(REFUND_REASONS),
            "refund_pct":    refund_pct * 100,
        }),
        timestamp=_now_ms(),
    )


def make_malformed() -> dict:
    """Returns intentionally malformed event for DLQ testing (5% injection rate)."""
    bad_type = random.choice(["null_order_id", "negative_amount", "bad_timestamp", "unknown_region"])
    base = {
        "order_id":       None if bad_type == "null_order_id" else f"ord-{uuid.uuid4().hex[:8]}",
        "event_type":     "ORDER_CREATED",
        "user_id":        f"usr-{random.randint(10000, 99999)}",
        "product_id":     "prod-9999",
        "product_name":   "Test Product",
        "region":         "INVALID_REGION" if bad_type == "unknown_region" else "north-india",
        "amount":         -999.99 if bad_type == "negative_amount" else 499.0,
        "currency":       "INR",
        "payment_method": "UPI",
        "status":         "CREATED",
        "event_metadata": None,
        "timestamp":      -1 if bad_type == "bad_timestamp" else _now_ms(),
        "schema_version": "1.0",
        "_malformed":     True,
        "_bad_type":      bad_type,
    }
    return base


# ---------------------------------------------------------------------------
# Full lifecycle generator — yields events for one order
# ---------------------------------------------------------------------------

def generate_order_lifecycle(error_rate: float = 0.05):
    """
    Generator: yields a sequence of events for a single order following
    the probabilistic lifecycle state machine.

    Args:
        error_rate: Fraction of orders that produce a malformed event (0.0–1.0)

    Yields:
        dict — event payload ready for Kafka publishing
    """
    # Inject malformed event
    if random.random() < error_rate:
        yield make_malformed()
        return

    # Build order state
    product = _pick_product()
    state = OrderState(
        order_id=f"ord-{uuid.uuid4().hex[:8]}",
        user_id=f"usr-{random.randint(10000, 99999)}",
        product=product,
        region=_pick_region(),
        amount=_pick_amount(product),
        payment_method=_pick_payment(),
    )

    # 1. Always emit ORDER_CREATED
    yield make_created(state).to_dict()

    # 2. ORDER_PAID or ORDER_CANCELLED (before payment)
    if random.random() < 0.15:          # 15% cancel before paying
        yield make_cancelled(state).to_dict()
        return

    paid_event = make_paid(state)
    state.paid = True
    state.amount = paid_event.amount    # update to charged amount
    yield paid_event.to_dict()

    # 3. ORDER_SHIPPED or ORDER_CANCELLED (after payment)
    if random.random() < 0.05:          # 5% cancel after paying
        yield make_cancelled(state).to_dict()
        # cancelled-after-paid orders may get a refund
        if random.random() < 0.80:
            state.refunded = True
            yield make_refunded(state).to_dict()
        return

    state.shipped = True
    yield make_shipped(state).to_dict()

    # 4. ORDER_DELIVERED
    if random.random() < 0.90:          # 90% reach delivery
        yield make_delivered(state).to_dict()

        # 5. Optional: refund after delivery
        if random.random() < 0.08:      # 8% post-delivery refund
            state.refunded = True
            yield make_refunded(state).to_dict()


# ---------------------------------------------------------------------------
# Continuous event stream — used by kafka_publisher.py
# ---------------------------------------------------------------------------

def event_stream(tps: int = 10, error_rate: float = 0.05):
    """
    Infinite generator that produces events at approximately `tps` orders/second.
    Lifecycle events for the same order are emitted with small delays between
    them to simulate realistic timing.

    Args:
        tps:        Target new orders per second
        error_rate: Fraction of orders that inject a malformed event

    Yields:
        dict — event payload
    """
    interval = 1.0 / tps
    while True:
        for event in generate_order_lifecycle(error_rate=error_rate):
            yield event
        time.sleep(interval)


# ---------------------------------------------------------------------------
# CLI entry point — print events to stdout (useful for testing without Kafka)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Order Event Generator")
    parser.add_argument("--tps",      type=int,   default=10,   help="New orders per second")
    parser.add_argument("--duration", type=int,   default=30,   help="Run duration in seconds (0=infinite)")
    parser.add_argument("--error-rate", type=float, default=0.05, help="Fraction of malformed events")
    parser.add_argument("--pretty",   action="store_true",       help="Pretty-print JSON")
    args = parser.parse_args()

    print(f"Starting order event generator: {args.tps} TPS, error_rate={args.error_rate}")
    print("Press Ctrl+C to stop.\n")

    start = time.time()
    count = 0

    try:
        for event in event_stream(tps=args.tps, error_rate=args.error_rate):
            if args.pretty:
                print(json.dumps(event, indent=2))
            else:
                print(json.dumps(event))
            count += 1

            if args.duration > 0 and (time.time() - start) >= args.duration:
                break
    except KeyboardInterrupt:
        pass

    elapsed = time.time() - start
    print(f"\nGenerated {count} events in {elapsed:.1f}s ({count/elapsed:.1f} events/sec)")
