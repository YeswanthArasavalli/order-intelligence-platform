"""Deterministic event fixtures; offsets are assigned only by Kafka/runtime."""

import json


def event(order="single", stage="PAID", amount=100.0, timestamp=1791043200000, **changes):
    row = dict(order_id=f"m3-{order}", event_type=f"ORDER_{stage}", user_id="usr-m3",
               product_id="prod-3001", product_name="M3 product", region="north-india",
               amount=float(amount), currency="INR", payment_method=None, status=stage,
               event_metadata=None, timestamp=timestamp, schema_version="1.0")
    row.update(changes)
    return row


def batches(base_ms):
    """Three arrivals; business timestamps stay stable and span distinct windows."""
    def e(case, stage="PAID", amount=100, shift=0, **changes):
        return event(str(case), stage, amount, base_ms + case * 300000 + shift,
                     product_id=f"prod-{3000 + case}", product_name=f"Case {case}", **changes)

    initial = [e(1), e(2), e(3), e(4),
               e(5, "CANCELLED", 0, event_metadata=json.dumps({"was_paid": False})),
               e(6), e(6, "REFUNDED", 20), e(6, "REFUNDED", 20), e(9),
               e(10), e(11), e(11, amount=40, shift=60000),
               e(14, "CREATED"), e(14), e(14, "SHIPPED"), e(14, "DELIVERED"),
               e(16, amount=0.1), e(16, amount=0.2),
               e(17, amount=10), e(17, amount=20, currency="USD"),
               e(18), e(19, "REFUNDED", 7),
               e(20, "CANCELLED", 0, event_metadata='{"was_paid":false}'),
               e(21, "CREATED", 5)]
    second = [e(2, "REFUNDED", 30), e(3, "REFUNDED", 20),
              e(4, "CANCELLED", 100, event_metadata='{"was_paid":true}'),
              e(9, "REFUNDED", 25), e(10, "REFUNDED", 15),
              e(14, "REFUNDED", 30),
              e(18, "CANCELLED", 100, event_metadata='{"was_paid":true}')]
    third = [e(3, "REFUNDED", 30), e(18, "REFUNDED", 30)]
    return initial, second, third
