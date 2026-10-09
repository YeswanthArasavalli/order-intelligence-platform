"""Controlled fixtures shared by real Spark tests and Kafka verification."""

from producer.order_generator import OrderState, make_created


def controlled_cases(now_ms):
    base = make_created(OrderState(
        order_id="m2-valid", user_id="usr-m2", product={"id": "prod-1001", "name": "M2 fixture"},
        region="north-india", amount=125.5, payment_method="UPI",
    )).to_dict()
    base.update(timestamp=now_ms, event_metadata=None)
    cases = []

    def add(name, expected=(), **changes):
        event = dict(base, order_id=f"m2-{name}")
        event.update(changes)
        cases.append({"name": name, "event": event, "expected": list(expected)})

    add("valid")
    add("amount", ["invalid_amount"], amount=-1.0)
    add("product", ["invalid_product_id"], product_id="prod-123abc")
    add("timestamp", ["invalid_timestamp"], timestamp=-1)
    add("future", ["future_timestamp"], timestamp=now_ms + 3600000)
    add("currency", ["invalid_currency"], currency="ZZZ")
    add("payment", ["invalid_payment_method"], payment_method="NOT_A_METHOD")
    add("status", ["invalid_status"], status="PAID")
    add("metadata", ["invalid_event_metadata"], event_metadata="{broken")
    add("old", ["old_timestamp"], timestamp=now_ms - 2 * 86400000)
    add("late", timestamp=now_ms - 3600000)
    add("unpaid-cancel", event_type="ORDER_CANCELLED", status="CANCELLED", amount=0.0,
        event_metadata='{"was_paid":false}')
    add("refund-negative", ["invalid_amount"], event_type="ORDER_REFUNDED", status="REFUNDED", amount=-1.0)
    add("nan", ["invalid_amount"], amount=float("nan"))
    add("infinity", ["invalid_amount"], amount=float("inf"))
    add("optional", payment_method=None, event_metadata=None)
    add("usd", currency="USD", region="international")
    add("version", ["invalid_schema_version"], schema_version="99.0")
    add("blank-user", ["null_user_id"], user_id="  ")
    add("region", ["invalid_region"], region="unknown")
    add("name", ["invalid_product_name"], product_name="")
    for stage in ("CREATED", "PAID", "SHIPPED", "DELIVERED", "REFUNDED", "REFUNDED"):
        add(f"lifecycle-{stage}-{len(cases)}", order_id="m2-lifecycle",
            event_type=f"ORDER_{stage}", status=stage)
    return cases
