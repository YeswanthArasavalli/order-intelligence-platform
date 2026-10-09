"""Independent psycopg2/Redis equality oracle: no production serializer/key builder.

Read PG in a read-only repeatable-read transaction; independently decode hash
field dimensions and JSON money/time values. Compare every column, then hash.
"""

import argparse
import base64
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
import re

import psycopg2
import redis

SHAPES = {
    "revenue_by_minute": (
        "revenue", 4,
        ["window_start", "window_end", "region", "currency", "order_count", "failed_count",
         "refunded_event_count", "gross_revenue", "refund_amount", "cancellation_amount",
         "net_revenue", "avg_order_value"],
    ),
    "product_rankings": (
        "product", 5,
        ["window_start", "window_end", "product_id", "product_name", "currency", "units_sold", "revenue"],
    ),
}
FINANCIAL = {"gross_revenue", "refund_amount", "cancellation_amount", "net_revenue", "avg_order_value", "revenue"}


def pg_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"), port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "order_intelligence"), user=os.getenv("POSTGRES_USER", "oip_user"),
        password=os.environ["POSTGRES_PASSWORD"], connect_timeout=5,
    )


def redis_connection():
    return redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=int(os.getenv("REDIS_PORT", "6379")),
                       db=int(os.getenv("REDIS_DB", "0")), username=os.getenv("REDIS_USERNAME") or None,
                       password=os.getenv("REDIS_PASSWORD") or None, decode_responses=True,
                       socket_connect_timeout=5, socket_timeout=5)


def postgres_state(schema):
    assert re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", schema)
    conn = pg_connection()
    try:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with conn.cursor() as cursor:
            cursor.execute(f'SELECT * FROM "{schema}".serving_state WHERE singleton_id=1')
            state = dict(zip([c.name for c in cursor.description], cursor.fetchone()))
            tables = {}
            for table, (_, _, columns) in SHAPES.items():
                cursor.execute(f'SELECT {", ".join(columns)} FROM "{schema}".{table}')
                tables[table] = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.execute(f'SELECT * FROM "{schema}".serving_history ORDER BY gold_version')
            history = cursor.fetchall()
        return state, tables, history
    finally:
        conn.close()


def canonical(tables, *, cache=False):
    output = {}
    for table, (_, key_length, columns) in SHAPES.items():
        rows = []
        identities = set()
        for original in tables[table]:
            assert set(original) == set(columns)
            row = []
            for column in columns:
                value = original[column]
                if column.startswith("window_"):
                    parsed = datetime.fromisoformat(value) if cache else value
                    assert parsed.tzinfo is not None
                    value = parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")
                elif column in FINANCIAL and value is not None:
                    assert isinstance(value, str if cache else Decimal)
                    parsed = Decimal(value)
                    assert parsed.is_finite()
                    value = format(parsed, ".8f")
                    assert Decimal(value) == parsed
                row.append(value)
            identity = tuple(row[:key_length])
            assert identity not in identities
            identities.add(identity)
            rows.append(row)
        output[table] = sorted(rows, key=lambda row: tuple(str(v) for v in row[:key_length]))
    digest = hashlib.sha256(json.dumps(output, separators=(",", ":"), sort_keys=True).encode()).hexdigest()
    return digest, output


def verify(schema, namespace):
    assert re.fullmatch(r"oip:m5:[a-z0-9][a-z0-9_-]{0,79}", namespace)
    state, tables, _ = postgres_state(schema)
    client = redis_connection()
    try:
        fields = client.hgetall(namespace + ":current")
        metadata = json.loads(fields.pop("@metadata"))
        redis_tables = {table: [] for table in SHAPES}
        for field, raw in fields.items():
            parts = field.split(":")
            table = next(t for t, shape in SHAPES.items() if shape[0] == parts[0])
            _, key_length, columns = SHAPES[table]
            assert len(parts) == key_length + 1
            dimensions = [base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)).decode()
                          if s != "_" else "" for s in parts[1:-1]] + [parts[-1]]
            row = json.loads(raw)
            assert dimensions == [row[c] for c in columns[:key_length]]
            redis_tables[table].append(row)
        pg_digest, pg_payload = canonical(tables)
        cache_digest, cache_payload = canonical(redis_tables, cache=True)
        assert pg_payload == cache_payload, "PostgreSQL/Redis full row comparison differs"
        assert pg_digest == cache_digest == state["snapshot_digest"] == metadata["snapshot_digest"]
        assert metadata["source_schema"] == schema and metadata["cache_contract_version"] == "1.0"
        for name in ("source_topic", "source_root", "gold_version", "manifest_path", "manifest_digest",
                     "snapshot_identity", "gold_contract_version", "revenue_rows", "product_rows", "serving_status"):
            assert metadata[name] == state[name], name
        assert metadata["served_at"] == state["served_at"].astimezone(timezone.utc).isoformat(timespec="microseconds")
        assert len(redis_tables["revenue_by_minute"]) == state["revenue_rows"]
        assert len(redis_tables["product_rankings"]) == state["product_rows"]
        assert client.ttl(namespace + ":current") == -1
        return {
            "schema": schema, "namespace": namespace, "postgres_version": state["gold_version"],
            "postgres_digest": pg_digest, "redis_digest": cache_digest, "metadata": metadata,
            "key_count": len(list(client.scan_iter(match=namespace + ":*"))),
            "active_hash_fields": len(fields) + 1, "ttl": -1,
            "rows": {t: len(rows) for t, rows in redis_tables.items()},
            "representative_inr": [r for r in cache_payload["revenue_by_minute"] if r[3] == "INR"][:2],
            "representative_usd": [r for r in cache_payload["revenue_by_minute"] if r[3] == "USD"][:2],
            "representative_products": cache_payload["product_rankings"][:3],
            "verified_at": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default="gold_serving")
    parser.add_argument("--namespace", default="oip:m5:gold-serving")
    parser.add_argument("--phase", choices=["serve", "verify", "rebuild", "redis-down", "postgres-down"],
                        default="serve")
    args = parser.parse_args()
    from storage.postgres_redis_serving import RedisServingError, serve_postgres_snapshot

    client = redis_connection()
    try:
        if args.phase == "redis-down":
            before = postgres_state(args.schema)
            try:
                serve_postgres_snapshot(schema=args.schema, namespace=args.namespace)
                raise AssertionError("Unavailable Redis did not fail")
            except RedisServingError:
                pass
            assert before == postgres_state(args.schema)
            output = {"redis_failure_reported": True, "postgres_unchanged": True}
        elif args.phase == "postgres-down":
            before = client.hgetall(args.namespace + ":current")
            try:
                serve_postgres_snapshot(schema=args.schema, namespace=args.namespace)
                raise AssertionError("Unavailable PostgreSQL did not fail")
            except RedisServingError:
                pass
            assert before == client.hgetall(args.namespace + ":current")
            output = {"postgres_failure_reported": True, "redis_unchanged": True}
        else:
            before = postgres_state(args.schema)
            outcome = None
            if args.phase == "rebuild":
                # Exact isolated active key only; never a database-wide deletion.
                assert args.namespace.startswith("oip:m5:verify-")
                client.delete(args.namespace + ":current")
                assert not client.exists(args.namespace + ":current")
            if args.phase in {"serve", "rebuild"}:
                outcome = serve_postgres_snapshot(schema=args.schema, namespace=args.namespace)
            output = verify(args.schema, args.namespace)
            assert before == postgres_state(args.schema)
            output.update(outcome=outcome, postgres_unchanged=True)
        output.update(phase=args.phase, verified_at=datetime.now(timezone.utc).isoformat())
        print("MILESTONE5_VERIFIED " + json.dumps(output, sort_keys=True))
    finally:
        client.close()


if __name__ == "__main__":
    main()
