"""Provider-neutral M8 identity, policy and persisted-event mapping."""

from dataclasses import asdict, dataclass
from datetime import timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re


class NotificationError(RuntimeError):
    """Sanitized operation failure; an external result may remain ambiguous."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def identity(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def schema_name(value, source=False):
    if value != ("alerts_serving" if source else "notifications_serving") and not re.fullmatch(
            r"m7_[a-z0-9_]{1,50}" if source else r"m8_[a-z0-9_]{1,50}", value):
        raise ValueError("Invalid notification/source schema")
    return value


@dataclass(frozen=True)
class Policy:
    max_attempts: int = 3
    base_delay: int = 2
    max_delay: int = 30
    lease_seconds: int = 30

    def __post_init__(self):
        if (any(type(v) is not int for v in asdict(self).values()) or not 1 <= self.max_attempts <= 10
                or not 1 <= self.base_delay <= self.max_delay <= 3600 or not 1 <= self.lease_seconds <= 600):
            raise ValueError("Invalid bounded notification policy")

    def delay(self, attempt):
        return min(self.max_delay, self.base_delay * 2 ** (attempt - 1))


def notification_id(database, source, event_id, route):
    return identity({"contract_version": "1.0", "source_database": database, "source_schema": source,
                     "alert_event_id": event_id, **{k: route[k] for k in ("provider", "channel", "destination")}})


def payload_for(row, notification):
    """Copy evidence from a persisted firing. Never evaluate the business condition."""
    try:
        definition, evidence, provenance = row["definition"], row["evidence"], row["provenance"]
        if (row["event_type"] != "FIRED" or row["outcome"] != "FIRED" or row["qualifies"] is not True
                or row["currency"] != definition["currency"] or row["dimensions"] != definition["dimensions"]
                or row["severity"] != definition["severity"] or provenance["gold_version"] != row["gold_version"]
                or row["event_id"] != identity({"evaluation_id": row["evaluation_id"], "event_type": "FIRED"})):
            raise ValueError("Invalid persisted firing")
        for key in ("event_id", "evaluation_id"):
            if not re.fullmatch(r"[0-9a-f]{64}", row[key]):
                raise ValueError("Invalid source identifier")
        currency = row["currency"]
        if not re.fullmatch(r"[A-Z]{3}", currency):
            raise ValueError("Invalid source currency")
        value, boundary, threshold = evidence["metric_value"], evidence["comparison_boundary"], definition["threshold"]
        if any(not isinstance(v, str) or len(v) > 100 or not Decimal(v).is_finite() for v in (value, boundary, threshold)):
            raise ValueError("Invalid persisted financial text")
        unit = "units" if definition["metric"] == "units_sold" else currency
        comparison = {"lt": "<", "lte": "<=", "gt": ">", "gte": ">="}[definition["comparison"]]
        return {"contract_version": "1.0", "notification_id": notification, "alert_event_id": row["event_id"],
                "evaluation_id": row["evaluation_id"], "rule_id": row["rule_id"], "severity": row["severity"],
                "title": f'{row["severity"]}: {row["rule_id"]}',
                "message": f'{currency} {definition["metric"]}: {unit} {value} {comparison} {unit} {boundary}',
                "currency": currency, "dimensions": row["dimensions"], "metric": definition["metric"],
                "metric_value": value, "threshold": threshold,
                "threshold_unit": "ratio" if definition["kind"] == "revenue_drop" else unit,
                "comparison": definition["comparison"], "comparison_boundary": boundary, "value_unit": unit,
                "fired_at": row["fired_at"].astimezone(timezone.utc).isoformat(),
                "evaluated_at": row["evaluated_at"].astimezone(timezone.utc).isoformat(),
                "window_start": row["window_start"].astimezone(timezone.utc).isoformat(),
                "window_end": row["window_end"].astimezone(timezone.utc).isoformat(),
                "source": {k: provenance[k] for k in ("gold_version", "snapshot_digest", "snapshot_identity",
                                                      "manifest_path", "manifest_digest", "served_at")}}
    except (KeyError, TypeError, ValueError, AttributeError, InvalidOperation) as exc:
        raise NotificationError("Invalid M7 firing event or evidence") from exc
