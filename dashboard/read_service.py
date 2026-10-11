"""M6 read-only adapter: M5 Redis preferred, authoritative gold_serving fallback.

One load returns one whole source snapshot; never merge Redis/SQL rows. No cache
population, source writes, metrics recomputation, or data-lake dependencies.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, localcontext
import os
import re

import redis
from sqlalchemy.exc import SQLAlchemyError
from observability import telemetry as m9

from storage.postgres_redis_serving import (
    InvalidSnapshot, MONEY, TABLES, get_postgres_engine, get_redis_client,
    namespace_name, read_active_snapshot, read_postgres_snapshot, schema_name,
)


@dataclass(frozen=True)
class DashboardConfig:
    schema: str = "gold_serving"
    namespace: str = "oip:m5:gold-serving"
    database: str = "order_intelligence"
    expected_topic: str | None = None
    default_currency: str | None = None
    refresh_seconds: int = 10

    def __post_init__(self):
        if schema_name(self.schema) != "gold_serving":
            raise ValueError("Dashboard analytical source must be gold_serving")
        namespace_name(self.namespace)
        if not self.database or (self.expected_topic is not None and not self.expected_topic.strip()):
            raise ValueError("Invalid dashboard source identity configuration")
        if self.default_currency is not None and not re.fullmatch(r"[A-Z]{3}", self.default_currency):
            raise ValueError("DASHBOARD_DEFAULT_CURRENCY must be a three-letter uppercase currency")
        if type(self.refresh_seconds) is not int or not 1 <= self.refresh_seconds <= 3600:
            raise ValueError("DASHBOARD_REFRESH_SECONDS must be between 1 and 3600")

    @classmethod
    def from_env(cls):
        return cls(
            schema=os.getenv("POSTGRES_GOLD_SCHEMA", "gold_serving"),
            namespace=os.getenv("REDIS_GOLD_NAMESPACE", "oip:m5:gold-serving"),
            database=os.getenv("POSTGRES_DB", "order_intelligence"),
            expected_topic=os.getenv("DASHBOARD_SOURCE_TOPIC") or None,
            default_currency=os.getenv("DASHBOARD_DEFAULT_CURRENCY") or None,
            refresh_seconds=int(os.getenv("DASHBOARD_REFRESH_SECONDS", "10")),
        )


class DashboardUnavailable(RuntimeError):
    def __init__(self, redis_status: str):
        self.redis_status = redis_status
        super().__init__(f"Dashboard data unavailable: Redis {redis_status}; authoritative PostgreSQL unavailable or invalid.")


@dataclass(frozen=True)
class DashboardSnapshot:
    metadata: dict
    revenue_by_minute: tuple[dict, ...]
    product_rankings: tuple[dict, ...]
    source: str
    redis_status: str
    namespace: str

    @property
    def currencies(self):
        return sorted({row["currency"] for row in (*self.revenue_by_minute, *self.product_rankings)})

    @property
    def empty(self):
        return not self.revenue_by_minute and not self.product_rankings

    @property
    def provenance(self):
        return {**self.metadata, "data_source": self.source, "redis_status": self.redis_status,
                "redis_namespace": self.namespace,
                "freshness": ("Cached source version; current PostgreSQL freshness not checked"
                              if self.source == "Redis cache" else "Authoritative PostgreSQL snapshot at read time")}


def _validate_metadata(snapshot, config):
    """M5 validates rows/keys/digest; M6 additionally validates consumer provenance."""
    meta = snapshot["metadata"]
    if not isinstance(meta, dict):
        raise InvalidSnapshot("Metadata must be an object")
    if (meta.get("serving_status") != "SERVED" or meta.get("cache_contract_version") != "1.0"
            or meta.get("gold_contract_version") != "1.0"
            or meta.get("source_schema") != config.schema or meta.get("source_database") != config.database):
        raise InvalidSnapshot("Snapshot source/contract differs from dashboard configuration")
    for name in ("source_topic", "source_root", "manifest_path"):
        if not isinstance(meta.get(name), str) or not meta[name].strip():
            raise InvalidSnapshot("Missing source provenance")
    if config.expected_topic is not None and meta["source_topic"] != config.expected_topic:
        raise InvalidSnapshot("Unexpected source topic")
    if type(meta.get("gold_version")) is not int or meta["gold_version"] < 1:
        raise InvalidSnapshot("Invalid source version")
    for name in ("snapshot_digest", "manifest_digest", "snapshot_identity"):
        if not isinstance(meta.get(name), str) or not re.fullmatch(r"[0-9a-f]{64}", meta[name]):
            raise InvalidSnapshot("Invalid source digest")
    served_at = datetime.fromisoformat(meta["served_at"])
    if served_at.tzinfo is None:
        raise InvalidSnapshot("Source timestamp must have timezone")
    for table, count in (("revenue_by_minute", "revenue_rows"), ("product_rankings", "product_rows")):
        if type(meta.get(count)) is not int or meta[count] < 0 or meta[count] != len(snapshot[table]):
            raise InvalidSnapshot("Invalid metadata count")


def _typed(snapshot, source, status, config):
    _validate_metadata(snapshot, config)
    tables = {}
    for table in TABLES:
        rows = []
        for raw in snapshot[table]:
            row = dict(raw)
            for column, value in row.items():
                if column in MONEY and value is not None:
                    row[column] = Decimal(value)
                elif column in {"window_start", "window_end"}:
                    row[column] = datetime.fromisoformat(value).astimezone(timezone.utc)
            rows.append(row)
        tables[table] = tuple(rows)
    return DashboardSnapshot(dict(snapshot["metadata"]), source=source, redis_status=status,
                             namespace=config.namespace, **tables)


class DashboardReadService:
    def __init__(self, config=None, *, redis_factory=get_redis_client, postgres_factory=get_postgres_engine):
        self.config = config or DashboardConfig.from_env()
        self._redis_factory = redis_factory
        self._postgres_factory = postgres_factory
        self._redis = None
        self._engine = None

    @m9.observed("dashboard_load")
    def load(self) -> DashboardSnapshot:
        try:
            if self._redis is None:
                self._redis = self._redis_factory()
            snapshot = read_active_snapshot(self._redis, self.config.namespace)
            if snapshot is not None:
                return _typed(snapshot, "Redis cache", "valid", self.config)
            status = "missing"
        except (redis.ConnectionError, redis.TimeoutError):
            status = "unavailable"
        except (redis.RedisError, InvalidSnapshot, KeyError, ValueError, TypeError, AttributeError):
            # Also contain malformed JSON object shapes from the M5 decoder.
            # Never expose raw driver exceptions/DSNs through the UI.
            status = "invalid"
        try:
            if self._engine is None:
                self._engine = self._postgres_factory()
            snapshot = read_postgres_snapshot(self._engine, self.config.schema)
            return _typed(snapshot, "PostgreSQL fallback", status, self.config)
        except (SQLAlchemyError, InvalidSnapshot, ValueError, KeyError, TypeError, AttributeError):
            raise DashboardUnavailable(status) from None

    def close(self):
        if self._redis is not None:
            self._redis.close()
            self._redis = None
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None


def filter_rows(snapshot: DashboardSnapshot, *, currency=None, region=None, start=None, end=None):
    """Select stored rows only. Time is window_start in [start,end); no prorating.

    Region applies only to revenue: M4 products have no region dimension.
    None means all; nonexisting selections legitimately return zero matching rows.
    """
    if currency is not None and not re.fullmatch(r"[A-Z]{3}", currency):
        raise ValueError("Invalid currency filter")
    for bound in (start, end):
        if bound is not None and (not isinstance(bound, datetime) or bound.tzinfo is None):
            raise ValueError("Time filters must be timezone-aware")
    if start is not None and end is not None and end <= start:
        raise ValueError("End must be after start")

    def keep(row):
        return ((currency is None or row["currency"] == currency)
                and (start is None or row["window_start"] >= start)
                and (end is None or row["window_start"] < end))

    return {
        "revenue_by_minute": sorted(
            (r for r in snapshot.revenue_by_minute if keep(r) and (region is None or r["region"] == region)),
            key=lambda r: (r["window_start"], r["window_end"], r["region"], r["currency"])),
        "product_rankings": sorted((r for r in snapshot.product_rankings if keep(r)),
                                   key=lambda r: (r["window_start"], r["window_end"], r["product_id"],
                                                  r["product_name"], r["currency"])),
    }


# Presentation-only glyph for a null analytical value. The Gold contract lets
# avg_order_value be null (no applicable paid events); a raw Python ``None`` in an
# Arrow cell reads like an application bug, so it is shown as an em dash instead.
NULL_DISPLAY = "\u2014"

# The single money-display boundary (Phase 2 polish). The Gold contract stores
# amounts at exactly eight decimal places, and stored ledgers legitimately carry
# float-derived dust (e.g. 12,830.50000005) produced by upstream accumulation;
# every *display* of money — KPI cards, chart hover text, table cells and
# insight lines — renders fixed two-decimal amounts with thousands separators
# through ``format_amount``/``format_money`` below. The rounding is
# presentation-only: no row, aggregate, filter or stored value is ever
# quantized by these helpers, so all calculations keep the full contract
# precision. Two decimals is the only documented display precision; there are
# no per-section exceptions — sections that must prove the stored exactness
# (the provenance JSON) show raw contract strings, not formatter output.
MONEY_DISPLAY_DECIMALS = 2


def _round_for_display(value: Decimal, decimals: int) -> Decimal:
    """Half-up rounding at ``decimals`` places, immune to context precision."""
    with localcontext() as ctx:
        # ``quantize`` raises InvalidOperation when the result needs more digits
        # than the context precision (the contract type reaches 30 integer
        # digits), so size the context to the value's own digit count.
        ctx.prec = max(len(value.as_tuple().digits), value.adjusted() + decimals + 1)
        return value.quantize(Decimal(1).scaleb(-decimals), rounding=ROUND_HALF_UP)


def format_amount(value: Decimal | int, decimals: int = MONEY_DISPLAY_DECIMALS) -> str:
    """Format one stored money value for display, without a currency prefix.

    Accepts ``Decimal`` and ``int`` only; anything else raises ``TypeError``
    instead of coercing into a plausible-looking number (rows reaching the
    renderer have already been validated as exact ``Decimal`` by ``_typed``).
    ``None`` is a null and is handled by the callers (``NULL_DISPLAY`` /
    ``format_money``), never rendered as a misleading zero.
    """
    if isinstance(value, bool) or not isinstance(value, (Decimal, int)):
        raise TypeError(f"Money display requires Decimal, got {type(value).__name__}")
    return f"{_round_for_display(Decimal(value), decimals):,.{decimals}f}"


def format_money(value: Decimal | None, currency: str, decimals: int = MONEY_DISPLAY_DECIMALS) -> str:
    """Currency-prefixed display string; a null stays null with explicit wording."""
    if value is None:
        return f"{currency} {NULL_DISPLAY} (not applicable)"
    return f"{currency} {format_amount(value, decimals)}"


def display_rows(rows):
    """Render contract rows for Streamlit/Arrow tables without numeric coercion.

    Decimal money cells go through the shared two-decimal ``format_amount``
    (presentation-only rounding of float-derived stored dust; the row objects
    are never mutated), datetimes keep their exact ISO text, and nulls render
    as ``NULL_DISPLAY`` so a null cell is never confused with a genuine zero
    (which formats as ``0.00``). Ints and strings pass through untouched.
    """
    return [{k: (format_amount(v) if isinstance(v, Decimal) else
                 v.isoformat(timespec="microseconds") if isinstance(v, datetime) else
                 NULL_DISPLAY if v is None else v)
             for k, v in row.items()} for row in rows]
