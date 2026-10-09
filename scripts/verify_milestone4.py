"""Real MinIO committed-Gold → PostgreSQL serving verifier.

The verifier reads only the exact committed manifest/table paths and independently
queries PostgreSQL with psycopg2 for the equality check. It never drops a schema.
Run ``initial`` once, then ``retry``, ``failure``, ``old`` and ``corrupt`` in
separate processes using the same isolated serving schema.
"""

from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import json
import os
import time

import psycopg2
from storage.gold_postgres_serving import (
    StaleGoldManifestError,
    get_engine,
    read_serving_state,
    serve_current_gold,
    serve_rows,
)
from storage.gold_store import current_manifest, manifest_at_version
from streaming.gold_aggregator import OUTPUT_SCHEMAS
from streaming.spark_session import get_spark_session


def _normal(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, Decimal):
        with localcontext() as context:
            context.prec = 80
            return format(value.quantize(Decimal("0.00000001")), "f")
    return value


def independent_digest(revenue_rows, product_rows):
    def table(rows, columns, keys):
        values = [[_normal(row[column]) for column in columns] for row in rows]
        indexes = [columns.index(key) for key in keys]
        return sorted(values, key=lambda row: tuple(str(row[index]) for index in indexes))

    payload = {
        "revenue_by_minute": table(
            revenue_rows,
            ["window_start", "window_end", "region", "currency", "order_count", "failed_count",
             "refunded_event_count", "gross_revenue", "refund_amount", "cancellation_amount",
             "net_revenue", "avg_order_value"],
            ["window_start", "window_end", "region", "currency"],
        ),
        "product_rankings": table(
            product_rows,
            ["window_start", "window_end", "product_id", "product_name", "currency", "units_sold", "revenue"],
            ["window_start", "window_end", "product_id", "product_name", "currency"],
        ),
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest(), payload


def exact_rows(spark, manifest):
    revenue = [r.asDict(recursive=True) for r in spark.read.schema(
        OUTPUT_SCHEMAS["revenue_by_minute"]
    ).parquet(manifest["tables"]["revenue_by_minute"]["path"]).collect()]
    products = [r.asDict(recursive=True) for r in spark.read.schema(
        OUTPUT_SCHEMAS["product_rankings"]
    ).parquet(manifest["tables"]["product_rankings"]["path"]).collect()]
    return revenue, products


def pg_rows(schema):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"), port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "order_intelligence"), user=os.getenv("POSTGRES_USER", "oip_user"),
        password=os.environ["POSTGRES_PASSWORD"],
    )
    try:
        cur = conn.cursor()
        cur.execute(f'''SELECT window_start, window_end, region, currency, order_count, failed_count,
            refunded_event_count, gross_revenue, refund_amount, cancellation_amount, net_revenue,
            avg_order_value FROM "{schema}".revenue_by_minute ORDER BY 1,2,3,4''')
        revenue_columns = [
            "window_start", "window_end", "region", "currency", "order_count", "failed_count",
            "refunded_event_count", "gross_revenue", "refund_amount", "cancellation_amount",
            "net_revenue", "avg_order_value",
        ]
        revenue = [dict(zip(revenue_columns, row)) for row in cur.fetchall()]
        cur.execute(f'''SELECT window_start, window_end, product_id, product_name, currency,
            units_sold, revenue FROM "{schema}".product_rankings ORDER BY 1,2,3,4,5''')
        product_columns = ["window_start", "window_end", "product_id", "product_name", "currency", "units_sold", "revenue"]
        products = [dict(zip(product_columns, row)) for row in cur.fetchall()]
        return revenue, products
    finally:
        conn.close()


def report_state(spark, root, schema, manifest):
    revenue, products = exact_rows(spark, manifest)
    pg_revenue, pg_products = pg_rows(schema)
    gold_digest, gold_payload = independent_digest(revenue, products)
    pg_digest, pg_payload = independent_digest(pg_revenue, pg_products)
    assert gold_digest == pg_digest, (gold_digest, pg_digest)
    assert gold_payload == pg_payload
    state = read_serving_state(get_engine(), schema)
    assert state["gold_version"] == manifest["version"]
    assert state["snapshot_digest"] == gold_digest
    assert state["revenue_rows"] == len(revenue)
    assert state["product_rows"] == len(products)
    return {
        "gold_version": manifest["version"], "manifest_path": state["manifest_path"],
        "snapshot_digest": gold_digest, "gold_rows": {"revenue": len(revenue), "product": len(products)},
        "postgres_rows": {"revenue": len(pg_revenue), "product": len(pg_products)},
        "representative_revenue": gold_payload["revenue_by_minute"][:2],
        "representative_product": gold_payload["product_rankings"][:2],
    }


def corrupt_object(spark, root):
    path = root.rstrip("/") + "/_commits/99999999999999999999.json"
    fs, location = __import__("storage.gold_store", fromlist=["_filesystem"])._filesystem(spark, path)
    fs.mkdirs(location.getParent())
    stream = fs.create(location, False)
    try:
        stream.write(bytearray(b"{not-json"))
    finally:
        stream.close()
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["initial", "retry", "failure", "old", "corrupt"], required=True)
    parser.add_argument("--topic", default="order-events-m3-20261003b")
    parser.add_argument("--schema", default="gold_m4_runtime")
    args = parser.parse_args()
    root = f"s3a://gold/{args.topic}/"
    os.environ.update(POSTGRES_HOST=os.getenv("POSTGRES_HOST", "postgres"),
                      POSTGRES_GOLD_SCHEMA=args.schema)
    spark = get_spark_session("M4-" + args.phase)
    engine = get_engine()
    try:
        current = current_manifest(spark, root)
        assert current is not None and current["version"] == 3, current
        if args.phase == "initial":
            old = manifest_at_version(spark, root, 1)
            old_revenue, old_products = exact_rows(spark, old)
            first = serve_rows(engine, old, args.topic, old_revenue, old_products,
                               schema=args.schema, root=root)
            assert first["status"] == "SERVED" and first["gold_version"] == 1
            result = serve_current_gold(spark, root, args.topic, engine=engine, schema=args.schema)
            assert result["status"] == "SERVED" and result["gold_version"] == 3
            output = report_state(spark, root, args.schema, current)
            output.update(phase=args.phase, initial_version=1, final_version=3,
                          stale_rows_removed=True, late_revision_served=True)
        elif args.phase == "retry":
            first = serve_current_gold(spark, root, args.topic, engine=engine, schema=args.schema)
            second = serve_current_gold(spark, root, args.topic, engine=engine, schema=args.schema)
            assert first["status"] in {"NOOP", "NOOP_MIGRATED"}
            assert second["status"] == "NOOP"
            output = report_state(spark, root, args.schema, current)
            output.update(phase=args.phase, first_status=first["status"], second_status=second["status"],
                          same_manifest_noop=True)
        elif args.phase == "failure":
            before = report_state(spark, root, args.schema, current)
            fake = copy.deepcopy(current)
            fake["version"] = 4
            fake["parent_version"] = 3
            fake["tables"]["revenue_by_minute"]["rows"] = max(0, fake["tables"]["revenue_by_minute"]["rows"] - 1)
            revenue, products = exact_rows(spark, current)
            revenue = revenue[:-1]

            def fail(stage):
                if stage == "after_delete":
                    raise RuntimeError("injected replacement failure")

            try:
                serve_rows(engine, fake, args.topic, revenue, products, schema=args.schema, root=root,
                           failure_hook=fail)
                raise AssertionError("failure injection did not run")
            except RuntimeError as exc:
                assert "injected replacement failure" in str(exc)
            after = report_state(spark, root, args.schema, current)
            assert before["snapshot_digest"] == after["snapshot_digest"]
            output = {"phase": args.phase, "failure_before_commit_preserved": True,
                      "before": before, "after": after}
        elif args.phase == "old":
            old = manifest_at_version(spark, root, 1)
            old_revenue, old_products = exact_rows(spark, old)
            try:
                serve_rows(engine, old, args.topic, old_revenue, old_products,
                           schema=args.schema, root=root)
                raise AssertionError("old manifest rolled state backward")
            except StaleGoldManifestError:
                pass
            output = report_state(spark, root, args.schema, current)
            output.update(phase=args.phase, old_manifest_rejected=True)
        else:
            uncommitted_root = f"s3a://gold/m4-uncommitted-{int(time.time())}/"
            no_commit = serve_current_gold(spark, uncommitted_root, args.topic, engine=engine, schema=args.schema)
            assert no_commit["status"] == "NO_COMMITTED_MANIFEST"
            corrupt_root = f"s3a://gold/m4-corrupt-{int(time.time())}/"
            corrupt_path = corrupt_object(spark, corrupt_root)
            try:
                serve_current_gold(spark, corrupt_root, args.topic, engine=engine, schema=args.schema)
                raise AssertionError("corrupt manifest was served")
            except Exception as exc:
                assert "JSON" in type(exc).__name__ or "json" in str(exc).lower()
            output = report_state(spark, root, args.schema, current)
            output.update(phase=args.phase, uncommitted_status=no_commit["status"],
                          corrupt_manifest_rejected=True, corrupt_path=corrupt_path)
        output["spark"] = spark.version
        output["schema"] = args.schema
        output["topic"] = args.topic
        output["verified_at"] = datetime.now(timezone.utc).isoformat()
        print("MILESTONE4_VERIFIED " + json.dumps(output, default=str, sort_keys=True))
    finally:
        engine.dispose()
        spark.stop()


if __name__ == "__main__":
    main()
