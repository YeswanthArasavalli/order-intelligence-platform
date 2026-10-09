"""M5: one bounded, rebuildable Redis hash derived only from M4 PostgreSQL.

Stage a complete hash, verify it, then WATCH/RENAME it over the active hash.
HGETALL reads rows plus metadata atomically. Eviction loses the entire cache,
never individual rows while leaving a misleading complete-state marker.
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import os
import re
from typing import Callable
import uuid

import redis
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from observability import telemetry as m9

DEFAULT_NAMESPACE = "oip:m5:gold-serving"
TABLES = {
    "revenue_by_minute": (
        "window_start", "window_end", "region", "currency", "order_count", "failed_count",
        "refunded_event_count", "gross_revenue", "refund_amount", "cancellation_amount",
        "net_revenue", "avg_order_value",
    ),
    "product_rankings": (
        "window_start", "window_end", "product_id", "product_name", "currency", "units_sold", "revenue",
    ),
}
KEYS = {
    "revenue_by_minute": ("window_start", "window_end", "region", "currency"),
    "product_rankings": ("window_start", "window_end", "product_id", "product_name", "currency"),
}
FAMILIES = {"revenue_by_minute": "revenue", "product_rankings": "product"}
MONEY = {"gross_revenue", "refund_amount", "cancellation_amount", "net_revenue", "avg_order_value", "revenue"}
COUNTS = {"order_count", "failed_count", "refunded_event_count", "units_sold"}
MAX_MONEY = Decimal("999999999999999999999999999999.99999999")
STATE_COLUMNS = (
    "source_topic", "source_root", "gold_contract_version", "gold_version", "manifest_path",
    "manifest_digest", "snapshot_identity", "snapshot_digest", "revenue_rows", "product_rows", "served_at",
)


class RedisServingError(RuntimeError):
    """A cache refresh could not be completed; PostgreSQL has not been written."""


class InvalidSnapshot(RedisServingError):
    """Source or cache data violates the serving contract."""


class StaleSnapshot(RedisServingError):
    """An older PostgreSQL version cannot replace a newer active cache."""


class SnapshotConflict(RedisServingError):
    """Source identity or same-version metadata differs."""


def namespace_name(namespace: str | None = None) -> str:
    value = namespace or os.getenv("REDIS_GOLD_NAMESPACE", DEFAULT_NAMESPACE)
    if not re.fullmatch(r"oip:m5:[a-z0-9][a-z0-9_-]{0,79}", value):
        raise ValueError("Namespace must be oip:m5:<lowercase slug>; nested namespaces/globs are forbidden")
    return value


def schema_name(schema: str | None = None) -> str:
    value = schema or os.getenv("POSTGRES_GOLD_SCHEMA", "gold_serving")
    if not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", value) or value in {"gold", "public"}:
        raise ValueError("Select an M4 serving schema with a valid PostgreSQL identifier")
    return value


def get_postgres_engine() -> Engine:
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise ValueError("POSTGRES_PASSWORD is required")
    return create_engine(URL.create(
        "postgresql+psycopg2", username=os.getenv("POSTGRES_USER", "oip_user"), password=password,
        host=os.getenv("POSTGRES_HOST", "localhost"), port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "order_intelligence"),
    ), pool_pre_ping=True, pool_size=2, max_overflow=1, hide_parameters=True,
        connect_args={"connect_timeout": 5})


def get_redis_client() -> redis.Redis:
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"), port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")), username=os.getenv("REDIS_USERNAME") or None,
        password=os.getenv("REDIS_PASSWORD") or None, decode_responses=True,
        socket_connect_timeout=5, socket_timeout=5, retry_on_timeout=False, max_connections=4,
    )


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _utc(value):
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise InvalidSnapshot("PostgreSQL timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _money(value, column):
    if value is None and column == "avg_order_value":
        return None
    if not isinstance(value, Decimal) or not value.is_finite():
        raise InvalidSnapshot("Money must be finite PostgreSQL Decimal, never float")
    try:
        with localcontext() as context:
            context.prec = 80
            scaled = value.quantize(Decimal("0.00000001"))
            if value != scaled or value.copy_abs() > MAX_MONEY or (column != "net_revenue" and value < 0):
                raise InvalidSnapshot("Money violates NUMERIC(38,8) or nonnegative metric semantics")
            return format(scaled, "f")
    except InvalidOperation as exc:
        raise InvalidSnapshot("Money is outside NUMERIC(38,8)") from exc


def _row(row, table):
    if set(row) != set(TABLES[table]):
        raise InvalidSnapshot("Analytical columns do not match M4")
    output = {}
    for column in TABLES[table]:
        value = row[column]
        if column in MONEY:
            value = _money(value, column)
        elif column in COUNTS:
            if type(value) is not int or not 0 <= value <= 9223372036854775807:
                raise InvalidSnapshot("Invalid BIGINT count")
        elif column in {"window_start", "window_end"}:
            value = _utc(value)
        elif not isinstance(value, str) or "\x00" in value:
            raise InvalidSnapshot("Invalid identity dimension")
        output[column] = value
    if output["window_end"] <= output["window_start"] or not re.fullmatch(r"[A-Z]{3}", output["currency"]):
        raise InvalidSnapshot("Invalid window or currency")
    return output


def _digest(tables):
    # Match the M4 logical digest, not a hash of Redis-specific key names.
    payload = {table: [[row[column] for column in TABLES[table]] for row in sorted(
        tables[table], key=lambda row: tuple(str(row[column]) for column in KEYS[table]))] for table in TABLES}
    return hashlib.sha256(_json(payload).encode()).hexdigest()


def _positive_setting(name, default):
    result = int(os.getenv(name, str(default)))
    if result < 1:
        raise ValueError(f"{name} must be positive")
    return result


def read_postgres_snapshot(engine: Engine, schema: str | None = None) -> dict:
    """Only SELECTs in a single REPEATABLE READ, READ ONLY transaction; no DDL."""
    schema = schema_name(schema)
    max_rows = _positive_setting("REDIS_GOLD_MAX_ROWS", 100000)
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            state = conn.execute(text(f'SELECT * FROM "{schema}".serving_state WHERE singleton_id=1')).mappings().first()
            if state is None or state["serving_status"] != "SERVED":
                raise InvalidSnapshot("PostgreSQL has no successfully served snapshot")
            if state["gold_contract_version"] != "1.0" or type(state["gold_version"]) is not int or state["gold_version"] < 1:
                raise InvalidSnapshot("Unsupported source contract/version")
            for name in ("manifest_digest", "snapshot_digest", "snapshot_identity"):
                if not isinstance(state[name], str) or not re.fullmatch(r"[0-9a-f]{64}", state[name]):
                    raise InvalidSnapshot("Invalid source digest metadata")
            for name in ("source_topic", "source_root", "manifest_path"):
                if not isinstance(state[name], str) or not state[name]:
                    raise InvalidSnapshot("Missing source identity metadata")
            tables = {}
            for table, count in (("revenue_by_minute", "revenue_rows"), ("product_rankings", "product_rows")):
                if type(state[count]) is not int or not 0 <= state[count] <= max_rows:
                    raise InvalidSnapshot("PostgreSQL row count exceeds REDIS_GOLD_MAX_ROWS or is invalid")
                rows = conn.execute(text(
                    f'SELECT {", ".join(TABLES[table])} FROM "{schema}".{table} LIMIT :limit'
                ), {"limit": max_rows + 1}).mappings().all()
                if len(rows) != state[count]:
                    raise InvalidSnapshot("PostgreSQL row counts differ from serving_state")
                tables[table] = [_row(row, table) for row in rows]
                identities = [tuple(row[c] for c in KEYS[table]) for row in tables[table]]
                if len(set(identities)) != len(identities):
                    raise InvalidSnapshot("Duplicate analytical identity")
            metadata = {c: (_utc(state[c]) if c == "served_at" else state[c]) for c in STATE_COLUMNS}
            metadata.update(source_database=conn.execute(text("SELECT current_database()")).scalar_one(),
                            source_schema=schema, cache_contract_version="1.0", serving_status="SERVED")
    if _digest(tables) != metadata["snapshot_digest"]:
        raise InvalidSnapshot("PostgreSQL rows differ from serving_state snapshot_digest")
    return {"metadata": metadata, **tables}


def _field(table, row):
    def segment(value):
        return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=") or "_"
    # Every dimension is encoded independently; currency stays visible.
    return ":".join([FAMILIES[table], *[segment(row[c]) for c in KEYS[table][:-1]], row["currency"]])


def _fields(snapshot):
    fields = {"@metadata": _json(snapshot["metadata"])}
    for table in TABLES:
        for row in snapshot[table]:
            fields[_field(table, row)] = _json(row)
    size = sum(len(k.encode()) + len(v.encode()) for k, v in fields.items())
    if size > _positive_setting("REDIS_GOLD_MAX_BYTES", 16 * 1024 * 1024):
        raise InvalidSnapshot("Serialized snapshot exceeds REDIS_GOLD_MAX_BYTES")
    return fields


def _decode(fields):
    try:
        metadata = json.loads(fields["@metadata"])
        if metadata["serving_status"] != "SERVED" or metadata["cache_contract_version"] != "1.0":
            raise InvalidSnapshot("Unsupported Redis snapshot metadata")
        tables = {table: [] for table in TABLES}
        for field, raw in fields.items():
            if field == "@metadata":
                continue
            table = next((t for t, family in FAMILIES.items() if field.startswith(family + ":")), None)
            if table is None:
                raise InvalidSnapshot("Unexpected Redis snapshot field")
            row = json.loads(raw)
            typed = dict(row)
            for column, value in row.items():
                if column in MONEY and value is not None:
                    if not isinstance(value, str):
                        raise InvalidSnapshot("Redis monetary values must be strings")
                    typed[column] = Decimal(value)
                elif column in {"window_start", "window_end"}:
                    typed[column] = datetime.fromisoformat(value)
            normal = _row(typed, table)
            if normal != row or _field(table, row) != field:
                raise InvalidSnapshot("Redis row identity or canonical serialization differs")
            tables[table].append(row)
        if (len(tables["revenue_by_minute"]) != metadata["revenue_rows"]
                or len(tables["product_rankings"]) != metadata["product_rows"]
                or _digest(tables) != metadata["snapshot_digest"]):
            raise InvalidSnapshot("Incomplete or corrupt Redis snapshot")
        return {"metadata": metadata, **tables}
    except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
        raise InvalidSnapshot("Invalid Redis snapshot encoding") from exc


@m9.observed("redis_read")
def read_active_snapshot(client: redis.Redis, namespace: str | None = None) -> dict | None:
    """Atomic full-cache read. None means miss; corrupt cache raises, never serves partial rows."""
    fields = client.hgetall(namespace_name(namespace) + ":current")
    return _decode(fields) if fields else None


def delete_cache(client: redis.Redis, namespace: str | None = None) -> int:
    """Invalidate exactly the active cache. Concurrent publishers fail their WATCH."""
    return client.delete(namespace_name(namespace) + ":current")


@m9.observed("redis_refresh")
def serve_postgres_snapshot(
    *, engine: Engine | None = None, redis_client: redis.Redis | None = None,
    schema: str | None = None, namespace: str | None = None,
    failure_hook: Callable[[str], None] | None = None,
) -> dict:
    """One-shot refresh; caller retries explicit failures. No PostgreSQL writes.

    WATCH fences overlapping refreshes (no lease/distributed-lock claim). A lost
    response to RENAME is ambiguous; retry compares complete state and resolves it.
    """
    namespace = namespace_name(namespace)
    schema = schema_name(schema)
    current = namespace + ":current"
    stage = namespace + ":stage:" + uuid.uuid4().hex
    owned_engine, owned_client = engine is None, redis_client is None
    client = None
    staged = False
    try:
        engine = engine if engine is not None else get_postgres_engine()
        client = redis_client if redis_client is not None else get_redis_client()
        with client.pipeline() as guard:
            guard.watch(current)
            existing = guard.hgetall(current)
            snapshot = read_postgres_snapshot(engine, schema)
            wanted = _fields(snapshot)
            meta = snapshot["metadata"]
            if existing:
                try:
                    old = json.loads(existing["@metadata"])
                    for key in ("source_database", "source_schema", "source_topic", "source_root"):
                        if old[key] != meta[key]:
                            raise SnapshotConflict("Redis namespace is bound to another PostgreSQL source")
                    if type(old["gold_version"]) is not int:
                        raise InvalidSnapshot("Invalid cached version")
                    if old["gold_version"] > meta["gold_version"]:
                        raise StaleSnapshot("Refusing to roll Redis backward")
                    if old["gold_version"] == meta["gold_version"] and old != meta:
                        raise SnapshotConflict("Same-version source metadata differs")
                except (KeyError, TypeError, ValueError) as exc:
                    raise InvalidSnapshot("Invalid cached metadata; explicitly invalidate before rebuilding") from exc
                if existing == wanted:
                    # EXEC a read to validate WATCH even for the NOOP path.
                    guard.multi()
                    guard.hlen(current)
                    guard.execute()
                    return {"status": "NOOP", "namespace": namespace, **meta}
            stage_fields = dict(wanted)
            stage_fields["@metadata"] = _json({**meta, "serving_status": "STAGING"})
            entries = list(stage_fields.items())
            staged = True
            for offset in range(0, len(entries), 500):
                client.hset(stage, mapping=dict(entries[offset:offset + 500]))
                if failure_hook:
                    failure_hook("during_stage")
            if failure_hook:
                failure_hook("after_stage")
            if client.hgetall(stage) != stage_fields:
                raise InvalidSnapshot("Incomplete Redis staging data")
            # Completion metadata is written only after a full staging readback.
            # This remains private until the watched, atomic RENAME below.
            client.hset(stage, "@metadata", wanted["@metadata"])
            guard.watch(stage)
            if guard.hgetall(stage) != wanted:
                raise InvalidSnapshot("Redis staging data differs from PostgreSQL")
            if failure_hook:
                failure_hook("before_publish")
            guard.multi()
            guard.rename(stage, current)  # single atomic mutating command, metadata included
            guard.execute()
            return {"status": "SERVED", "namespace": namespace, **meta}
    except redis.WatchError as exc:
        raise RedisServingError("Cache changed during refresh; retry from current PostgreSQL state") from exc
    except redis.RedisError as exc:
        raise RedisServingError("Redis operation failed; retry checks whether publication completed") from exc
    except SQLAlchemyError as exc:
        raise RedisServingError("Authoritative PostgreSQL read failed; cache was not refreshed") from exc
    finally:
        if client is not None:
            if staged:
                try:
                    client.delete(stage)  # only this attempt, NEVER the published current key
                except redis.RedisError:
                    pass  # abandoned staging is inactive; explicit maintenance may remove it
            if owned_client:
                client.close()
        if owned_engine and engine is not None:
            engine.dispose()


def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema")
    parser.add_argument("--namespace")
    args = parser.parse_args()
    try:
        print(_json(serve_postgres_snapshot(schema=args.schema, namespace=args.namespace)))
    except (RedisServingError, ValueError) as exc:
        # Do not render chained driver errors, connection strings or credentials.
        parser.exit(1, f"{type(exc).__name__}: {exc}\n")


if __name__ == "__main__":
    main()
