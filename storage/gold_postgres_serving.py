"""Serve one committed M3 Gold snapshot as one PostgreSQL-visible state.

The legacy ``gold.*`` tables remain untouched: they predate M3 and cannot
represent currency-aware Decimal(38,8) identities. This module uses an additive
schema (default ``gold_serving``), one source topic per schema, and a complete
snapshot replacement inside one PostgreSQL transaction.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from typing import Callable, Iterable, Mapping

from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from observability import telemetry as m9

SERVING_CONTRACT_VERSION = "1.0"
DEFAULT_SCHEMA = "gold_serving"
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
REVENUE_COLUMNS = (
    "window_start", "window_end", "region", "currency", "order_count", "failed_count",
    "refunded_event_count", "gross_revenue", "refund_amount", "cancellation_amount",
    "net_revenue", "avg_order_value",
)
PRODUCT_COLUMNS = (
    "window_start", "window_end", "product_id", "product_name", "currency", "units_sold", "revenue",
)
MONEY_COLUMNS = {
    "gross_revenue", "refund_amount", "cancellation_amount", "net_revenue", "avg_order_value", "revenue",
}
NONNEGATIVE_MONEY_COLUMNS = {
    "gross_revenue", "refund_amount", "cancellation_amount", "avg_order_value", "revenue",
}
MAX_NUMERIC_38_8 = Decimal("999999999999999999999999999999.99999999")
MONEY_SCALE = Decimal("0.00000001")


class GoldServingError(RuntimeError):
    """Base error for a serving request that did not change PostgreSQL."""


class StaleGoldManifestError(GoldServingError):
    """A previously served or older manifest cannot roll the state backward."""


class ManifestConflictError(GoldServingError):
    """The same manifest version identifies different content or a topic."""


class InvalidGoldSnapshotError(GoldServingError):
    """The committed Gold rows do not satisfy the M3 serving contract."""


def _quote_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe PostgreSQL identifier: {value!r}")
    return f'"{value}"'


def _schema_name(schema: str | None) -> str:
    value = schema or os.getenv("POSTGRES_GOLD_SCHEMA", DEFAULT_SCHEMA)
    _quote_identifier(value)
    if value in {"gold", "public"}:
        raise ValueError(f"Refusing to serve into reserved legacy schema: {value}")
    return value


def get_engine() -> Engine:
    """Create a password-safe SQLAlchemy engine from existing configuration."""
    password = os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise ValueError("POSTGRES_PASSWORD must be supplied through the existing environment configuration")
    url = URL.create(
        "postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER", "oip_user"),
        password=password,
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "order_intelligence"),
    )
    return create_engine(
        url,
        poolclass=QueuePool,
        pool_size=int(os.getenv("POSTGRES_GOLD_POOL_SIZE", "2")),
        max_overflow=int(os.getenv("POSTGRES_GOLD_MAX_OVERFLOW", "1")),
        pool_pre_ping=True,
        pool_recycle=3600,
    )


def _ddl(schema: str) -> str:
    q = _quote_identifier(schema)
    return f"""
    CREATE SCHEMA IF NOT EXISTS {q};
    CREATE TABLE IF NOT EXISTS {q}.revenue_by_minute (
        window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL,
        region TEXT NOT NULL, currency TEXT NOT NULL,
        order_count BIGINT NOT NULL, failed_count BIGINT NOT NULL,
        refunded_event_count BIGINT NOT NULL, gross_revenue NUMERIC(38,8) NOT NULL,
        refund_amount NUMERIC(38,8) NOT NULL, cancellation_amount NUMERIC(38,8) NOT NULL,
        net_revenue NUMERIC(38,8) NOT NULL, avg_order_value NUMERIC(38,8),
        CONSTRAINT pk_m4_revenue_by_minute PRIMARY KEY (window_start, window_end, region, currency),
        CONSTRAINT ck_m4_revenue_currency CHECK (currency ~ '^[A-Z]{{3}}$')
    );
    CREATE TABLE IF NOT EXISTS {q}.product_rankings (
        window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL,
        product_id TEXT NOT NULL, product_name TEXT NOT NULL,
        currency TEXT NOT NULL, units_sold BIGINT NOT NULL, revenue NUMERIC(38,8) NOT NULL,
        CONSTRAINT pk_m4_product_rankings PRIMARY KEY
            (window_start, window_end, product_id, product_name, currency),
        CONSTRAINT ck_m4_product_currency CHECK (currency ~ '^[A-Z]{{3}}$')
    );
    CREATE TABLE IF NOT EXISTS {q}.serving_state (
        singleton_id SMALLINT PRIMARY KEY CHECK (singleton_id = 1),
        source_topic TEXT NOT NULL, source_root TEXT NOT NULL, gold_contract_version TEXT NOT NULL,
        gold_version BIGINT NOT NULL, manifest_path TEXT NOT NULL, manifest_digest CHAR(64) NOT NULL,
        snapshot_identity TEXT NOT NULL, snapshot_digest CHAR(64) NOT NULL,
        revenue_rows BIGINT NOT NULL, product_rows BIGINT NOT NULL,
        served_at TIMESTAMPTZ NOT NULL,
        serving_status VARCHAR(16) NOT NULL CHECK (serving_status = 'SERVED')
    );
    CREATE TABLE IF NOT EXISTS {q}.serving_history (
        source_topic TEXT NOT NULL, source_root TEXT NOT NULL, gold_contract_version TEXT NOT NULL,
        gold_version BIGINT NOT NULL, manifest_path TEXT NOT NULL UNIQUE, manifest_digest CHAR(64) NOT NULL,
        snapshot_identity TEXT NOT NULL,
        snapshot_digest CHAR(64) NOT NULL, revenue_rows BIGINT NOT NULL,
        product_rows BIGINT NOT NULL, served_at TIMESTAMPTZ NOT NULL,
        serving_status VARCHAR(16) NOT NULL CHECK (serving_status = 'SERVED'),
        PRIMARY KEY (source_topic, gold_version)
    );
    CREATE INDEX IF NOT EXISTS idx_m4_history_served_at
        ON {q}.serving_history (served_at DESC);
    """


def ensure_schema(engine: Engine, schema: str | None = None) -> str:
    schema = _schema_name(schema)
    with engine.begin() as conn:
        conn.execute(text(_ddl(schema)))
        q = _quote_identifier(schema)
        # Additive migration for a schema created by an earlier M4 process. New
        # schemas get NOT NULL columns from _ddl; old isolated schemas retain
        # their rows and are upgraded without dropping business data.
        conn.execute(text(f"ALTER TABLE {q}.serving_state ADD COLUMN IF NOT EXISTS source_root TEXT"))
        conn.execute(text(f"ALTER TABLE {q}.serving_state ADD COLUMN IF NOT EXISTS gold_contract_version TEXT"))
        conn.execute(text(f"ALTER TABLE {q}.serving_state ADD COLUMN IF NOT EXISTS manifest_digest CHAR(64)"))
        conn.execute(text(f"ALTER TABLE {q}.serving_history ADD COLUMN IF NOT EXISTS source_root TEXT"))
        conn.execute(text(f"ALTER TABLE {q}.serving_history ADD COLUMN IF NOT EXISTS gold_contract_version TEXT"))
        conn.execute(text(f"ALTER TABLE {q}.serving_history ADD COLUMN IF NOT EXISTS manifest_digest CHAR(64)"))
        # Earlier isolated M4 schemas used bounded strings. Convert only the
        # additive serving tables so M3 identifiers are not truncated.
        conn.execute(text(f"ALTER TABLE {q}.revenue_by_minute ALTER COLUMN region TYPE TEXT"))
        conn.execute(text(f"ALTER TABLE {q}.revenue_by_minute ALTER COLUMN currency TYPE TEXT"))
        conn.execute(text(f"ALTER TABLE {q}.product_rankings ALTER COLUMN product_id TYPE TEXT"))
        conn.execute(text(f"ALTER TABLE {q}.product_rankings ALTER COLUMN product_name TYPE TEXT"))
        conn.execute(text(f"ALTER TABLE {q}.product_rankings ALTER COLUMN currency TYPE TEXT"))
    return schema


def _utc(value):
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise InvalidGoldSnapshotError(f"Expected datetime, got {type(value).__name__}")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _canonical(value):
    if isinstance(value, datetime):
        return _utc(value).isoformat(timespec="microseconds")
    if isinstance(value, Decimal):
        with localcontext() as context:
            context.prec = 80
            return format(value.quantize(MONEY_SCALE), "f")
    if isinstance(value, bool):
        return value
    if value is None or isinstance(value, (str, int, float)):
        return value
    raise TypeError(f"Unsupported Gold value: {type(value).__name__}")


def canonical_snapshot_payload(revenue_rows: Iterable[Mapping], product_rows: Iterable[Mapping]) -> dict:
    """Canonical content representation used for serving state and equality checks."""
    def table(rows, columns, key):
        return [
            [_canonical(row.get(column)) for column in columns]
            for row in sorted(rows, key=lambda row: tuple(str(_canonical(row.get(c))) for c in key))
        ]

    return {
        "revenue_by_minute": table(
            revenue_rows, REVENUE_COLUMNS, ("window_start", "window_end", "region", "currency"),
        ),
        "product_rankings": table(
            product_rows, PRODUCT_COLUMNS,
            ("window_start", "window_end", "product_id", "product_name", "currency"),
        ),
    }


def snapshot_digest(revenue_rows: Iterable[Mapping], product_rows: Iterable[Mapping]) -> str:
    payload = canonical_snapshot_payload(revenue_rows, product_rows)
    return hashlib.sha256(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()).hexdigest()


def snapshot_identity(manifest: Mapping) -> str:
    tables = {
        name: {"path": value["path"], "rows": value["rows"]}
        for name, value in sorted(manifest["tables"].items())
    }
    return hashlib.sha256(json.dumps(tables, separators=(",", ":"), sort_keys=True).encode()).hexdigest()


def _as_dict(row):
    return row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)


def _normalise_rows(rows, columns, table_name):
    result = []
    keys = set()
    for original in rows:
        row = _as_dict(original)
        missing = [column for column in columns if column not in row]
        if missing:
            raise InvalidGoldSnapshotError(f"{table_name} row missing {missing}")
        item = {column: row[column] for column in columns}
        item["window_start"] = _utc(item["window_start"])
        item["window_end"] = _utc(item["window_end"])
        if item["window_end"] <= item["window_start"]:
            raise InvalidGoldSnapshotError(f"{table_name} has an invalid window")
        if not isinstance(item["currency"], str) or not re.fullmatch(r"[A-Z]{3}", item["currency"]):
            raise InvalidGoldSnapshotError(f"{table_name} has invalid currency")
        for column in columns:
            if column in MONEY_COLUMNS:
                if column == "avg_order_value" and item[column] is None:
                    continue
                try:
                    item[column] = Decimal(str(item[column]))
                except Exception as exc:
                    raise InvalidGoldSnapshotError(f"{table_name} has invalid money") from exc
                try:
                    with localcontext() as context:
                        context.prec = 80
                        quantized = item[column].quantize(MONEY_SCALE)
                        invalid = (
                            not item[column].is_finite()
                            or item[column] != quantized
                            or abs(item[column]) > MAX_NUMERIC_38_8
                            or (column in NONNEGATIVE_MONEY_COLUMNS and item[column] < 0)
                        )
                except InvalidOperation as exc:
                    raise InvalidGoldSnapshotError(f"{table_name} has non-finite money") from exc
                if invalid:
                    raise InvalidGoldSnapshotError(f"{table_name} has non-finite money")
        for column in ("order_count", "failed_count", "refunded_event_count", "units_sold"):
            if column in item and (not isinstance(item[column], int) or isinstance(item[column], bool)
                                   or item[column] < 0):
                raise InvalidGoldSnapshotError(f"{table_name} has invalid count")
        key_columns = (("window_start", "window_end", "region", "currency")
                       if table_name == "revenue_by_minute" else
                       ("window_start", "window_end", "product_id", "product_name", "currency"))
        key = tuple(_canonical(item[column]) for column in key_columns)
        if key in keys:
            raise InvalidGoldSnapshotError(f"{table_name} contains duplicate logical keys")
        keys.add(key)
        result.append(item)
    return result


def _manifest_metadata(manifest: Mapping, source_topic: str, root: str | None = None):
    try:
        version = int(manifest["version"])
        tables = manifest["tables"]
        if manifest["contract_version"] != "1.0" or set(tables) != {"events", "revenue_by_minute", "product_rankings"}:
            raise KeyError
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidGoldSnapshotError("Invalid committed Gold manifest metadata") from exc
    if version < 1 or not source_topic:
        raise InvalidGoldSnapshotError("Invalid Gold version or source topic")
    root = root.rstrip("/") if root else None
    if not root:
        raise InvalidGoldSnapshotError("Serving requires the committed Gold root")
    if root.rsplit("/", 1)[-1] != source_topic:
        raise InvalidGoldSnapshotError("Serving root does not match the source topic")
    for table in tables.values():
        if not table.get("path", "").startswith(root + "/_snapshots/"):
            raise InvalidGoldSnapshotError("Manifest table is outside the committed Gold root")
    manifest_path = (root + f"/_commits/{version:020d}.json") if root else str(manifest.get("manifest_path", ""))
    manifest_digest = hashlib.sha256(
        json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    return {
        "version": version,
        "source_root": root,
        "gold_contract_version": manifest["contract_version"],
        "manifest_path": manifest_path,
        "manifest_digest": manifest_digest,
        "snapshot_identity": snapshot_identity(manifest),
    }


def _failure(failure_hook: Callable[[str], None] | None, stage: str):
    if failure_hook:
        failure_hook(stage)


@m9.observed("postgres_commit")
def serve_rows(
    engine: Engine,
    manifest: Mapping,
    source_topic: str,
    revenue_rows: Iterable[Mapping],
    product_rows: Iterable[Mapping],
    *,
    schema: str | None = None,
    root: str | None = None,
    failure_hook: Callable[[str], None] | None = None,
) -> dict:
    """Atomically make PostgreSQL equal to one complete committed snapshot.

    The authoritative tables are replaced only inside one transaction. An older
    manifest, corrupt rows, or any injected/runtime error leaves the previous
    committed state visible after rollback.
    """
    schema = ensure_schema(engine, schema)
    q = _quote_identifier(schema)
    revenue = _normalise_rows(revenue_rows, REVENUE_COLUMNS, "revenue_by_minute")
    products = _normalise_rows(product_rows, PRODUCT_COLUMNS, "product_rankings")
    if manifest.get("tables", {}).get("revenue_by_minute", {}).get("rows") != len(revenue):
        raise InvalidGoldSnapshotError("Revenue row count differs from committed manifest")
    if manifest.get("tables", {}).get("product_rankings", {}).get("rows") != len(products):
        raise InvalidGoldSnapshotError("Product row count differs from committed manifest")
    metadata = _manifest_metadata(manifest, source_topic, root)
    digest = snapshot_digest(revenue, products)
    max_rows = int(os.getenv("POSTGRES_GOLD_MAX_ROWS", "100000"))
    if len(revenue) > max_rows or len(products) > max_rows:
        raise InvalidGoldSnapshotError(f"Gold snapshot exceeds POSTGRES_GOLD_MAX_ROWS={max_rows}")
    served_at = datetime.now(timezone.utc)
    lock_key = f"oip.gold-serving.{schema}"

    with engine.begin() as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"), {"lock_key": lock_key})
        state = conn.execute(text(f"SELECT * FROM {q}.serving_state WHERE singleton_id = 1 FOR UPDATE")).mappings().first()
        if state:
            if state["source_topic"] != source_topic:
                raise ManifestConflictError("Serving schema is already assigned to another source topic")
            if metadata["version"] < state["gold_version"]:
                raise StaleGoldManifestError(
                    f"Manifest v{metadata['version']} is older than served v{state['gold_version']}"
                )
            if metadata["version"] == state["gold_version"]:
                if (state["manifest_path"] != metadata["manifest_path"]
                        or state["snapshot_identity"] != metadata["snapshot_identity"]
                        or state["snapshot_digest"] != digest):
                    raise ManifestConflictError("Manifest version identifies different Gold content")
                db_revenue = [dict(row) for row in conn.execute(text(f"""SELECT
                    window_start, window_end, region, currency, order_count, failed_count,
                    refunded_event_count, gross_revenue, refund_amount, cancellation_amount,
                    net_revenue, avg_order_value FROM {q}.revenue_by_minute""")).mappings()]
                db_products = [dict(row) for row in conn.execute(text(f"""SELECT
                    window_start, window_end, product_id, product_name, currency, units_sold, revenue
                    FROM {q}.product_rankings""")).mappings()]
                if snapshot_digest(db_revenue, db_products) != state["snapshot_digest"]:
                    raise GoldServingError("PostgreSQL serving rows differ from recorded snapshot digest")
                if not state.get("source_root") or not state.get("manifest_digest"):
                    conn.execute(text(f"""UPDATE {q}.serving_state SET
                        source_root=:source_root, gold_contract_version=:gold_contract_version,
                        manifest_digest=:manifest_digest WHERE singleton_id=1"""), metadata)
                    return {"status": "NOOP_MIGRATED", "schema": schema, "source_topic": source_topic,
                            "gold_version": state["gold_version"], "snapshot_digest": state["snapshot_digest"],
                            "revenue_rows": state["revenue_rows"], "product_rows": state["product_rows"]}
                return {"status": "NOOP", "schema": schema, "source_topic": source_topic,
                        "gold_version": state["gold_version"], "snapshot_digest": state["snapshot_digest"],
                        "revenue_rows": state["revenue_rows"], "product_rows": state["product_rows"]}

        conn.execute(text(f"""
            CREATE TEMP TABLE pg_temp.m4_revenue_stage (
                LIKE {q}.revenue_by_minute INCLUDING DEFAULTS
            ) ON COMMIT DROP
        """))
        conn.execute(text(f"""
            CREATE TEMP TABLE pg_temp.m4_product_stage (
                LIKE {q}.product_rankings INCLUDING DEFAULTS
            ) ON COMMIT DROP
        """))
        revenue_sql = text("""INSERT INTO pg_temp.m4_revenue_stage
            (window_start, window_end, region, currency, order_count, failed_count,
             refunded_event_count, gross_revenue, refund_amount, cancellation_amount,
             net_revenue, avg_order_value)
            VALUES (:window_start, :window_end, :region, :currency, :order_count, :failed_count,
                    :refunded_event_count, :gross_revenue, :refund_amount, :cancellation_amount,
                    :net_revenue, :avg_order_value)""")
        product_sql = text("""INSERT INTO pg_temp.m4_product_stage
            (window_start, window_end, product_id, product_name, currency, units_sold, revenue)
            VALUES (:window_start, :window_end, :product_id, :product_name, :currency, :units_sold, :revenue)""")
        if revenue:
            for start in range(0, len(revenue), 1000):
                conn.execute(revenue_sql, revenue[start:start + 1000])
        if products:
            for start in range(0, len(products), 1000):
                conn.execute(product_sql, products[start:start + 1000])
        _failure(failure_hook, "after_stage")

        conn.execute(text(f"DELETE FROM {q}.revenue_by_minute"))
        conn.execute(text(f"DELETE FROM {q}.product_rankings"))
        _failure(failure_hook, "after_delete")
        conn.execute(text(f"""INSERT INTO {q}.revenue_by_minute
            SELECT window_start, window_end, region, currency, order_count, failed_count,
                   refunded_event_count, gross_revenue, refund_amount, cancellation_amount,
                   net_revenue, avg_order_value FROM pg_temp.m4_revenue_stage"""))
        conn.execute(text(f"""INSERT INTO {q}.product_rankings
            SELECT window_start, window_end, product_id, product_name, currency, units_sold, revenue
            FROM pg_temp.m4_product_stage"""))
        _failure(failure_hook, "after_insert")
        state_values = {
            "source_topic": source_topic, "gold_version": metadata["version"],
            "source_root": metadata["source_root"], "gold_contract_version": metadata["gold_contract_version"],
            "manifest_path": metadata["manifest_path"], "snapshot_identity": metadata["snapshot_identity"],
            "manifest_digest": metadata["manifest_digest"],
            "snapshot_digest": digest, "revenue_rows": len(revenue), "product_rows": len(products),
            "served_at": served_at,
        }
        conn.execute(text(f"""INSERT INTO {q}.serving_state
            (singleton_id, source_topic, source_root, gold_contract_version, gold_version, manifest_path,
             manifest_digest, snapshot_identity, snapshot_digest, revenue_rows, product_rows, served_at, serving_status)
            VALUES (1, :source_topic, :source_root, :gold_contract_version, :gold_version, :manifest_path,
                    :manifest_digest, :snapshot_identity, :snapshot_digest, :revenue_rows, :product_rows, :served_at, 'SERVED')
            ON CONFLICT (singleton_id) DO UPDATE SET
                source_topic = EXCLUDED.source_topic, source_root = EXCLUDED.source_root,
                gold_contract_version = EXCLUDED.gold_contract_version, gold_version = EXCLUDED.gold_version,
                manifest_path = EXCLUDED.manifest_path, snapshot_identity = EXCLUDED.snapshot_identity,
                manifest_digest = EXCLUDED.manifest_digest,
                snapshot_digest = EXCLUDED.snapshot_digest, revenue_rows = EXCLUDED.revenue_rows,
                product_rows = EXCLUDED.product_rows, served_at = EXCLUDED.served_at,
                serving_status = EXCLUDED.serving_status"""), state_values)
        conn.execute(text(f"""INSERT INTO {q}.serving_history
            (source_topic, source_root, gold_contract_version, gold_version, manifest_path, manifest_digest,
             snapshot_identity, snapshot_digest, revenue_rows, product_rows, served_at, serving_status)
            VALUES (:source_topic, :source_root, :gold_contract_version, :gold_version, :manifest_path, :manifest_digest,
                    :snapshot_identity, :snapshot_digest, :revenue_rows, :product_rows, :served_at, 'SERVED')
            ON CONFLICT (source_topic, gold_version) DO NOTHING"""), state_values)
        _failure(failure_hook, "before_commit")
    return {"status": "SERVED", "schema": schema, "source_topic": source_topic,
            "gold_version": metadata["version"], "manifest_path": metadata["manifest_path"],
            "snapshot_identity": metadata["snapshot_identity"], "snapshot_digest": digest,
            "revenue_rows": len(revenue), "product_rows": len(products)}


@m9.observed("postgres_serve")
def serve_current_gold(
    spark,
    root: str,
    source_topic: str,
    *,
    engine: Engine | None = None,
    schema: str | None = None,
    failure_hook: Callable[[str], None] | None = None,
) -> dict:
    """Read only the current committed manifest and its two pinned tables."""
    from storage.gold_store import current_manifest, read_manifest_table

    manifest = current_manifest(spark, root)
    if manifest is None:
        return {"status": "NO_COMMITTED_MANIFEST", "schema": _schema_name(schema),
                "source_topic": source_topic}
    max_rows = int(os.getenv("POSTGRES_GOLD_MAX_ROWS", "100000"))
    manifest_rows = [manifest["tables"][name]["rows"] for name in ("revenue_by_minute", "product_rankings")]
    if any(rows > max_rows for rows in manifest_rows):
        raise InvalidGoldSnapshotError(f"Gold snapshot exceeds POSTGRES_GOLD_MAX_ROWS={max_rows}")
    # Both tables are pinned to this exact manifest object. Do not call read_gold
    # independently: a newer manifest may otherwise be observed between reads.
    revenue = [row.asDict(recursive=True) for row in read_manifest_table(
        spark, manifest, "revenue_by_minute").collect()]
    products = [row.asDict(recursive=True) for row in read_manifest_table(
        spark, manifest, "product_rankings").collect()]
    latest = current_manifest(spark, root)
    if latest != manifest:
        raise GoldServingError("Gold manifest changed while snapshot was being read; retry")
    owned_engine = engine is None
    serving_engine = engine or get_engine()
    try:
        return serve_rows(
            serving_engine, manifest, source_topic, revenue, products,
            schema=schema, root=root, failure_hook=failure_hook,
        )
    finally:
        if owned_engine:
            serving_engine.dispose()


def read_serving_state(engine: Engine, schema: str | None = None):
    schema = _schema_name(schema)
    q = _quote_identifier(schema)
    ensure_schema(engine, schema)
    with engine.connect() as conn:
        row = conn.execute(text(f"SELECT * FROM {q}.serving_state WHERE singleton_id = 1")).mappings().first()
    return dict(row) if row else None
