"""Independent M6 row/UI/runtime verifier. Destructive cache tests are isolated.

Reuses the independent M5 psycopg2/Redis oracle, not dashboard transformations.
AppTest executes the actual Streamlit app against actual services, and a real
Streamlit server is additionally started/HTTP-probed for startup verification.
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from urllib.request import urlopen

from streamlit.testing.v1 import AppTest

from dashboard.read_service import DashboardConfig, DashboardReadService, DashboardUnavailable
from scripts.verify_milestone5 import canonical, postgres_state, redis_connection, verify
from storage.postgres_redis_serving import serve_postgres_snapshot

APP = str(Path(__file__).resolve().parents[1] / "dashboard" / "app.py")


def dashboard_read(namespace):
    reader = DashboardReadService(DashboardConfig(namespace=namespace))
    try:
        return reader.load()
    finally:
        reader.close()


def check_ui(expected_source=None, unavailable=False):
    app = AppTest.from_file(APP).run(timeout=30)
    assert not app.exception, [e.message for e in app.exception]
    if unavailable:
        assert app.error and "data unavailable" in app.error[0].value
        assert not app.dataframe and not app.metric
        return {"unavailable_visible": True, "rendered_tables": 0}
    assert not app.error
    captions = [c.value for c in app.caption]
    assert any("Data source: " + expected_source in c for c in captions)
    assert any("PostgreSQL source: gold_serving" in c for c in captions)
    assert any("Gold version:" in c for c in captions)
    # Reconstruct exact rendered table values, independent of display_rows().
    tables = {"revenue_by_minute": [], "product_rankings": []}
    for frame in app.dataframe:
        rows = frame.value.to_dict("records")
        table = "product_rankings" if "product_id" in frame.value.columns else "revenue_by_minute"
        tables[table].extend(rows)
    digest, _ = canonical(tables, cache=True)
    assert {s.value for s in app.subheader} >= {"Revenue — INR", "Revenue — USD", "Products — INR", "Products — USD"}
    # Widget interactions run the real source path again.
    app.selectbox(key="currency").select("USD").run(timeout=30)
    assert not app.exception and all(set(frame.value["currency"]) == {"USD"} for frame in app.dataframe)
    return {"rendered_digest": digest, "captions": captions,
            "rows": {t: len(rows) for t, rows in tables.items()}, "usd_filter_verified": True}


def server_smoke():
    command = [sys.executable, "-m", "streamlit", "run", APP, "--server.headless=true",
               "--server.address=127.0.0.1", "--server.port=18506", "--browser.gatherUsageStats=false"]
    with tempfile.TemporaryFile(mode="w+") as output:
        proc = subprocess.Popen(command, stdout=output, stderr=output)
        try:
            for _ in range(60):
                if proc.poll() is not None:
                    raise AssertionError("Streamlit server exited before ready")
                try:
                    with urlopen("http://127.0.0.1:18506/_stcore/health", timeout=1) as response:
                        health = response.read().decode()
                    with urlopen("http://127.0.0.1:18506", timeout=1) as response:
                        assert response.status == 200 and b"streamlit" in response.read().lower()
                    return {"command": command, "health": health, "http_status": 200}
                except OSError:
                    time.sleep(0.5)
            raise AssertionError("Streamlit health timeout")
        finally:
            proc.terminate()
            proc.wait(timeout=20)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["production", "healthy", "missing", "rebuild", "corrupt",
                                           "redis-down", "postgres-down", "both-down"], required=True)
    parser.add_argument("--namespace", default="oip:m5:gold-serving")
    parser.add_argument("--expected-digest")
    args = parser.parse_args()
    os.environ.update(POSTGRES_GOLD_SCHEMA="gold_serving", REDIS_GOLD_NAMESPACE=args.namespace)
    if args.phase in {"missing", "corrupt", "rebuild"}:
        assert args.namespace.startswith("oip:m5:verify-m6-")
    if args.phase == "production":
        assert args.namespace == "oip:m5:gold-serving"
    result = {"phase": args.phase, "schema": "gold_serving", "namespace": args.namespace}
    if args.phase == "both-down":
        try:
            dashboard_read(args.namespace)
            raise AssertionError("Both-down returned analytical data")
        except DashboardUnavailable:
            pass
        result["ui"] = check_ui(unavailable=True)
    else:
        client = redis_connection()
        try:
            before = None if args.phase == "postgres-down" else postgres_state("gold_serving")
            if args.phase in {"production", "healthy", "rebuild"}:
                result["m5_publication"] = serve_postgres_snapshot()  # actual env/default source path
            if args.phase == "missing":
                client.delete(args.namespace + ":current")
            elif args.phase == "corrupt":
                client.hset(args.namespace + ":current", "@metadata", "malformed-json")
            snapshot = dashboard_read(args.namespace)
            result["provenance"] = snapshot.provenance
            tables = {"revenue_by_minute": snapshot.revenue_by_minute, "product_rankings": snapshot.product_rankings}
            digest, payload = canonical(tables)
            result.update(dashboard_digest=digest, rows={t: len(r) for t, r in tables.items()},
                          currencies=snapshot.currencies)
            if args.phase in {"missing", "corrupt", "redis-down"}:
                assert snapshot.source == "PostgreSQL fallback"
                assert snapshot.redis_status == {"missing": "missing", "corrupt": "invalid", "redis-down": "unavailable"}[args.phase]
            else:
                assert snapshot.source == "Redis cache"
            if before is not None:
                pg_digest, pg_payload = canonical(before[1])
                assert payload == pg_payload and digest == pg_digest == before[0]["snapshot_digest"]
                result["postgres_digest"] = pg_digest
                assert before == postgres_state("gold_serving"), "Dashboard changed authoritative PostgreSQL"
            else:
                assert args.expected_digest and digest == args.expected_digest
            if snapshot.source == "Redis cache":
                if before is not None:
                    result["independent_m5"] = verify("gold_serving", args.namespace)
                else:
                    raw = client.hgetall(args.namespace + ":current")
                    cache_tables = {"revenue_by_minute": [], "product_rankings": []}
                    for field, value in raw.items():
                        if field.startswith("revenue:"):
                            cache_tables["revenue_by_minute"].append(json.loads(value))
                        elif field.startswith("product:"):
                            cache_tables["product_rankings"].append(json.loads(value))
                    cache_digest, cache_payload = canonical(cache_tables, cache=True)
                    assert cache_digest == digest and cache_payload == payload
                    result["redis_digest"] = cache_digest
            result["ui"] = check_ui(snapshot.source)
            assert result["ui"]["rendered_digest"] == digest
        finally:
            if args.phase == "corrupt":
                client.delete(args.namespace + ":current")
                serve_postgres_snapshot()  # restore isolated cache even if assertion fails
            client.close()
    if args.phase == "production":
        result["http_server"] = server_smoke()
    result["verified_at"] = datetime.now(timezone.utc).isoformat()
    print("MILESTONE6_VERIFIED " + json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
