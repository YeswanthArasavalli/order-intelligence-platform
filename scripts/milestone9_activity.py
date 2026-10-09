"""Controlled M9 activity using frozen APIs; retained business state is read-only."""

import argparse
import json
import os
from pathlib import Path
import time

from sqlalchemy import create_engine, text

from alerts.definitions import load_rules
from alerts.evaluator import evaluate
from dashboard.read_service import DashboardReadService
from notifications.model import NotificationError, Policy
from notifications.provider import ControlledHTTPProvider
from notifications.service import NotificationService
from observability import telemetry
from observability.metrics_exporter import Collector, Probes, MetricsServer
from scripts.milestone8_receiver import running_receiver
from storage.postgres_redis_serving import get_postgres_engine, serve_postgres_snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--run", default="20261004a")
    args = parser.parse_args()
    if not args.run.isalnum():
        parser.error("Run label must be alphanumeric")
    engine = get_postgres_engine()
    output = {"phase": args.phase}
    try:
        if args.phase == "serving":
            # An isolated cache namespace may be populated; authoritative SQL is unchanged.
            output["refresh"] = serve_postgres_snapshot(engine=engine, schema="gold_serving", namespace="oip:m5:verify-m9-" + args.run)
            output["default_cache"] = serve_postgres_snapshot(engine=engine, schema="gold_serving", namespace="oip:m5:gold-serving")
            service = DashboardReadService()
            try:
                snapshot = service.load()
                assert snapshot.source == "Redis cache"
                output["dashboard_source"] = snapshot.source
            finally:
                service.close()
        elif args.phase == "alerts":
            rules = load_rules(str(Path(__file__).resolve().parents[1] / "alerts" / "rules.example.json"))
            target = "m7_m9_" + args.run
            first = evaluate(rules, engine=engine, alert_schema=target)
            again = evaluate(rules, engine=engine, alert_schema=target)
            assert first["counts"]["FIRED"] == 2 and again["counts"]["NOOP"] == 4
            output.update(first=first["counts"], again=again["counts"])
        elif args.phase == "notifications":
            with engine.connect() as conn:
                events = conn.execute(text("SELECT event_id FROM alerts_serving.alert_events ORDER BY event_id")).scalars().all()
            ledger = Path("/tmp/omnirush") / ("m9-provider-" + args.run + ".sqlite")
            ledger.parent.mkdir(parents=True, exist_ok=True)
            with running_receiver(ledger) as receiver:
                policy = Policy(base_delay=1, max_delay=2, lease_seconds=1)
                service = NotificationService(engine, ControlledHTTPProvider(receiver.endpoint, "m9-test", 0.1),
                                              schema="m8_m9_" + args.run, policy=policy)
                receiver.plan("m9-test", ["503", "success"])
                failed = service.process([events[0]])
                assert failed["notifications_retryable_failure"] == 1
                time.sleep(1.1)
                delivered = service.process([events[0]])
                assert delivered["notifications_delivered"] == 1
                assert service.process([events[0]])["attempts"] == 0
                service.generate([events[1]])
                claim = service.claim()
                assert claim and not claim.get("recovered")
                time.sleep(1.1)
                recovered = service.process([events[1]])
                assert recovered["claims_recovered"] == 1
                time.sleep(1.1)
                assert service.process([events[1]])["notifications_delivered"] == 1
                output.update(created=2, delivered=2, retryable_failure=1, unknown_attempt=1,
                              provider_receipts=len(receiver.records()["receipts"]))
        elif args.phase in {"redis-down", "postgres-down", "independence"}:
            service = DashboardReadService()
            try:
                snapshot = service.load()
                output["dashboard_source"] = snapshot.source
                if args.phase == "redis-down":
                    assert snapshot.source == "PostgreSQL fallback" and snapshot.redis_status == "unavailable"
                else:
                    assert snapshot.source == "Redis cache"
            finally:
                service.close()
            if args.phase == "postgres-down":
                provider = ControlledHTTPProvider("http://127.0.0.1:1/notify", "m9-outage")
                try:
                    NotificationService(engine, provider, schema="m8_m9_outage_" + args.run).process()
                    raise AssertionError("PostgreSQL outage did not fail")
                except NotificationError:
                    output["m8_failure_explicit"] = True
            elif args.phase == "independence":
                rules = load_rules(str(Path(__file__).resolve().parents[1] / "alerts" / "rules.example.json"))
                result = evaluate(rules, engine=engine)
                assert result["counts"]["NOOP"] == 4
                output["m7_noop"] = 4
        elif args.phase == "no-data":
            # Real source/cache absence in an isolated fixture database, not production mutations.
            from storage.gold_postgres_serving import ensure_schema
            from scripts.milestone7_cases import publish
            import httpx
            import threading

            url = engine.url.set(database="m9_empty_" + args.run)
            with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text(f'CREATE DATABASE "{url.database}"'))
            empty = create_engine(url)
            try:
                ensure_schema(empty, "gold_serving")
                namespace = "oip:m5:verify-m9-empty-" + args.run
                os.environ["POSTGRES_DB"] = url.database
                os.environ["REDIS_GOLD_NAMESPACE"] = namespace
                collector = Collector()
                probes = Probes(collector)
                probes.collect()
                assert collector.metrics.registry.get_sample_value("oip_snapshot_present", {"component": "postgres"}) == 0
                publish(empty, "gold_serving", 1, [], [])
                serve_postgres_snapshot(engine=empty, schema="gold_serving", namespace=namespace)
                probes.collect()
                server = MetricsServer(("127.0.0.1", 0), collector)
                worker = threading.Thread(target=server.serve_forever, daemon=True)
                worker.start()
                try:
                    with httpx.Client(trust_env=False) as client:
                        response = client.get(f"http://127.0.0.1:{server.server_port}/metrics")
                        assert response.status_code == 200
                    for component in ("postgres", "redis"):
                        assert collector.metrics.registry.get_sample_value("oip_snapshot_present", {"component": component}) == 1
                        assert collector.metrics.registry.get_sample_value("oip_snapshot_rows", {"component": component, "table": "revenue"}) == 0
                    assert collector.ready
                    output.update(no_commit_distinguished=True, committed_empty_present=True, rows=0,
                                  metrics_http_status=200, ready=True)
                finally:
                    server.shutdown()
                    server.server_close()
                    probes.close()
            finally:
                empty.dispose()
        else:
            raise ValueError("Unsupported activity phase")
        telemetry.flush()
        print("MILESTONE9_VERIFIED " + json.dumps(output, default=str, sort_keys=True))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
