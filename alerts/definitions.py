"""Explicit M7 rules: finite Decimal thresholds, exact dimensions, no executable SQL."""

from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
from pathlib import Path
import re


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def identity(value) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


@dataclass(frozen=True)
class Rule:
    rule_id: str
    kind: str
    currency: str
    dimensions: dict
    metric: str
    comparison: str
    threshold: str
    cooldown_seconds: int
    severity: str
    enabled: bool

    def __post_init__(self):
        if not isinstance(self.rule_id, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", self.rule_id):
            raise ValueError("Invalid rule_id")
        if self.kind not in {"revenue_threshold", "revenue_drop", "product_threshold"}:
            raise ValueError("Unsupported rule kind")
        if not isinstance(self.currency, str) or not re.fullmatch(r"[A-Z]{3}", self.currency):
            raise ValueError("Each rule requires an explicit uppercase currency")
        dimensions = {"product_id", "product_name"} if self.kind == "product_threshold" else {"region"}
        if (not isinstance(self.dimensions, dict) or set(self.dimensions) != dimensions
                or any(not isinstance(v, str) or not v or "\x00" in v for v in self.dimensions.values())):
            raise ValueError("Rule requires exact region or product_id/product_name dimensions")
        allowed = {"revenue", "units_sold"} if self.kind == "product_threshold" else {"net_revenue"}
        if self.metric not in allowed or self.comparison not in {"lt", "lte", "gt", "gte"}:
            raise ValueError("Unsupported metric or comparison")
        if (type(self.cooldown_seconds) is not int or not 1 <= self.cooldown_seconds <= 604800
                or self.severity not in {"INFO", "WARNING", "CRITICAL"} or type(self.enabled) is not bool):
            raise ValueError("Invalid cooldown, severity or enabled setting")
        if not isinstance(self.threshold, str):
            raise ValueError("Threshold must be an exact decimal string")
        try:
            value = Decimal(self.threshold)
            with localcontext() as ctx:
                ctx.prec = 80
                if (not value.is_finite() or value.copy_abs() > Decimal("999999999999999999999999999999.99999999")
                        or value.quantize(Decimal("0.00000001")) != value):
                    raise ValueError("Threshold must be exactly NUMERIC(38,8)")
        except InvalidOperation as exc:
            raise ValueError("Invalid threshold") from exc
        if self.metric == "units_sold" and (value < 0 or value != value.to_integral_value() or value > 9223372036854775807):
            raise ValueError("Unit threshold must be a nonnegative BIGINT")
        if self.kind == "product_threshold" and value < 0:
            raise ValueError("Product threshold must be nonnegative")
        if self.kind == "revenue_drop" and (not 0 < value < 1 or self.comparison != "lt"):
            raise ValueError("Revenue drop requires lt and a retained-revenue ratio strictly between 0 and 1")

    @property
    def document(self) -> dict:
        document = asdict(self)
        document["threshold"] = format(Decimal(self.threshold), ".8f")
        document.update(contract_version="1.0", window_policy="latest_stored_window_per_exact_dimension")
        return document

    @property
    def digest(self) -> str:
        return identity(self.document)


def load_rules(path: str) -> list[Rule]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, list) or not 1 <= len(value) <= 1000:
        raise ValueError("Rules file must be a list of 1–1000 explicit rule definitions")
    try:
        rules = [Rule(**entry) for entry in value]
    except TypeError as exc:
        raise ValueError("Rule fields do not match the M7 contract") from exc
    validate_rules(rules)
    return rules


def validate_rules(rules: list[Rule]) -> None:
    if not 1 <= len(rules) <= 1000 or any(not isinstance(r, Rule) for r in rules):
        raise ValueError("Supply 1–1000 validated rules")
    if len({r.rule_id for r in rules}) != len(rules):
        raise ValueError("Duplicate rule_id in invocation")
    for rule in rules:
        rule.__post_init__()  # also protect against mutation of caller-supplied dimensions


def condition(rule: Rule, snapshot: dict) -> dict:
    """Evaluate one latest stored row; drop uses only the immediately adjacent minute.

    Snapshot time is not wall time. No interpolation, cross-dimension totals,
    historical backfill emissions or assumed zero rows when data is absent.
    """
    table = "product_rankings" if rule.kind == "product_threshold" else "revenue_by_minute"
    rows = [r for r in snapshot[table] if r["currency"] == rule.currency
            and all(r[key] == value for key, value in rule.dimensions.items())]
    current = max(rows, key=lambda r: (r["window_start"], r["window_end"])) if rows else None
    result = {"current_row": current, "previous_row": None, "qualifies": None, "reason": "NO_DATA"}
    if not rule.enabled:
        return {**result, "reason": "DISABLED"}
    if current is None:
        return result
    value = Decimal(str(current[rule.metric]))
    boundary = Decimal(rule.threshold)
    if rule.kind == "revenue_drop":
        prior = [r for r in rows if r["window_end"] == current["window_start"]]
        if len(prior) != 1 or Decimal(prior[0]["net_revenue"]) <= 0:
            return {**result, "reason": "NO_BASELINE"}
        result["previous_row"] = prior[0]
        # Exact comparison current < previous * ratio; no rounded percentage.
        with localcontext() as ctx:
            ctx.prec = 80
            boundary *= Decimal(prior[0]["net_revenue"])
    qualified = {"lt": value < boundary, "lte": value <= boundary,
                 "gt": value > boundary, "gte": value >= boundary}[rule.comparison]
    return {**result, "qualifies": qualified, "reason": "MATCH" if qualified else "CLEAR",
            "metric_value": str(value), "comparison_boundary": format(boundary, "f")}
