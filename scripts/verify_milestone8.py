"""M8 independent SQL/HTTP verification. Retains evidence; never mutates retained M7."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from sqlalchemy import text

from storage.postgres_redis_serving import get_postgres_engine


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def source_integrity(engine, schema="alerts_serving"):
    assert schema == "alerts_serving" or schema.startswith("m7_") and schema.replace("_", "").isalnum()
    result = {}
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            for table in ("alert_events", "alert_evaluations", "alert_state", "alert_definitions", "source_snapshots"):
                rows = [dict(r) for r in conn.execute(text(f'SELECT * FROM "{schema}".{table} ORDER BY 1')).mappings()]
                result[table] = {"count": len(rows), "sha256": digest(rows)}
    return result


def frozen_files():
    root = Path(__file__).resolve().parents[1]
    result = {}
    for directory in ("alerts", "storage", "streaming", "data_quality", "dashboard", "producer", "kafka", "scripts", "tests"):
        for file in (root / directory).rglob("*"):
            if file.suffix not in {".py", ".sql", ".md"} or "milestone8" in file.name or file.name == "test_notifications.py":
                continue
            result[str(file.relative_to(root))] = hashlib.sha256(file.read_bytes()).hexdigest()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--run", default="20261004a")
    args = parser.parse_args()
    if not args.run.isalnum():
        parser.error("Run label must be alphanumeric")
    engine = get_postgres_engine()
    try:
        if args.phase == "inspect":
            with engine.connect() as conn:
                version = conn.execute(text("SELECT version()")).scalar_one()
            import importlib.metadata as metadata
            output = {"postgres_version": version, "m7_source": source_integrity(engine), "frozen_files": frozen_files(),
                      "provider_configured": {key: bool(os.getenv(key)) for key in
                          ("SLACK_WEBHOOK_URL", "PAGERDUTY_INTEGRATION_KEY", "SMTP_HOST", "TELEGRAM_BOT_TOKEN")},
                      "dependencies": {p: metadata.version(p) for p in ("httpx", "sqlalchemy", "psycopg2-binary")}}
        else:
            output = run_phase(engine, args.phase, args.run)
        print("MILESTONE8_VERIFIED " + json.dumps({"phase": args.phase,
              "verified_at": datetime.now(timezone.utc).isoformat(), **output}, sort_keys=True, default=str))
    finally:
        engine.dispose()


def run_phase(engine, phase, run):
    from notifications.model import NotificationError, Policy
    from notifications.provider import ControlledHTTPProvider
    from notifications.service import NotificationService
    from scripts.milestone8_receiver import running_receiver

    ledger = Path("/tmp/omnirush") / f"m8-{run}.sqlite"
    ledger.parent.mkdir(parents=True, exist_ok=True)
    endpoint = "http://127.0.0.1:18088/notify"
    baseline_path = next((Path(__file__).resolve().parents[1] / "milestone8_evidence" / run).glob("inspect-*.json"))
    baseline = json.loads(baseline_path.read_text())["result"]
    if phase == "integrity":
        assert source_integrity(engine) == baseline["m7_source"]
        assert frozen_files() == baseline["frozen_files"]
        with running_receiver(ledger, 18088) as receiver:
            records = receiver.records()
            histories = {}
            for schema in ("notifications_serving", "m8_runtime_" + run, "m8_outage_" + run):
                history = independent_notifications(engine, schema, records)
                histories[schema] = {k: v for k, v in history.items() if k not in {"rows", "attempts"}}
                histories[schema]["attempt_outcomes"] = {outcome: sum(a["outcome"] == outcome for a in history["attempts"])
                    for outcome in ("DELIVERED", "RETRYABLE_FAILURE", "PERMANENT_FAILURE", "UNKNOWN")}
        return {"m7_source_unchanged": True, "frozen_m1_m7_files_unchanged": True,
                "m7_source": source_integrity(engine), "frozen_file_count": len(frozen_files()),
                "notification_histories": histories, "provider_receipts": len(records["receipts"]),
                "provider_http_requests": len(records["calls"]), "external_provider_certified": False}
    if phase == "postgres-down":
        with running_receiver(ledger, 18088) as receiver:
            before = receiver.records()
            service = NotificationService(engine, ControlledHTTPProvider(endpoint, "outage-inbox"), schema="m8_outage_" + run)
            try:
                service.process()
                raise AssertionError("Database outage did not fail")
            except NotificationError:
                assert receiver.records() == before
                return {"postgres_failure_explicit": True, "provider_calls_during_outage": 0}
    before = source_integrity(engine)
    assert before == baseline["m7_source"]
    if phase in {"redis-down", "recovery"}:
        with running_receiver(ledger, 18088) as receiver:
            service = NotificationService(engine, ControlledHTTPProvider(endpoint, "outage-inbox"), schema="m8_outage_" + run)
            result = service.process()
            history = independent_notifications(engine, service.schema, receiver.records())
            assert history["delivery_counts"] == {"DELIVERED": 2}
            assert source_integrity(engine) == before
            return {"result": result, "history": history, "m7_source_unchanged": True,
                    "redis_independent": phase == "redis-down", "external_provider_certified": False}
    if phase != "runtime":
        raise ValueError("Unsupported verification phase")

    policy = Policy(base_delay=1, max_delay=2, lease_seconds=1)
    target = "m8_runtime_" + run
    phases = {}
    with engine.connect() as conn:
        events = conn.execute(text("SELECT event_id FROM alerts_serving.alert_events ORDER BY event_id")).scalars().all()
    assert len(events) == 2

    def child(destination, schema, *, mode="deliver", crash=False, short=True):
        env = {**os.environ, "NOTIFICATION_PROVIDER": "controlled_http", "NOTIFICATION_ENDPOINT": endpoint,
               "NOTIFICATION_DESTINATION": destination}
        if short:
            env.update(NOTIFICATION_TIMEOUT_SECONDS="0.1", NOTIFICATION_LEASE_SECONDS="1",
                       NOTIFICATION_BASE_DELAY_SECONDS="1", NOTIFICATION_MAX_DELAY_SECONDS="2")
        if crash:
            code = ("import os; from notifications.service import NotificationService; "
                    "from notifications.provider import ControlledHTTPProvider; from notifications.model import Policy; "
                    "from storage.postgres_redis_serving import get_postgres_engine; "
                    f"s=NotificationService(get_postgres_engine(),ControlledHTTPProvider('{endpoint}','{destination}',0.1),"
                    f"schema='{schema}',policy=Policy(base_delay=1,max_delay=2,lease_seconds=1),"
                    "failure_hook=lambda stage: os._exit(73) if stage=='after_provider' else None); s.process(generate=False)")
            command = [sys.executable, "-c", code]
        else:
            command = [sys.executable, "/opt/oip/main.py", "notifications", "--schema", schema, "--mode", mode]
        proc = subprocess.run(command, env=env, capture_output=True, text=True, timeout=30)
        assert proc.returncode == (73 if crash else 0), proc.stderr
        return {"returncode": proc.returncode, "result": None if crash else json.loads(proc.stdout.splitlines()[-1])}

    with running_receiver(ledger, 18088) as receiver:
        # Default production source/target and real main dispatcher, two retained M7 events.
        phases["default_cli"] = child("m8-verification", "notifications_serving", mode="process", short=False)
        phases["default_restart"] = child("m8-verification", "notifications_serving", mode="process", short=False)
        assert phases["default_restart"]["result"]["attempts"] == 0
        assert independent_notifications(engine, "notifications_serving", receiver.records())["delivery_counts"] == {"DELIVERED": 2}

        def service(destination, max_attempts=3):
            return NotificationService(engine, ControlledHTTPProvider(endpoint, destination, 0.1), schema=target,
                                       policy=Policy(max_attempts=max_attempts, base_delay=1, max_delay=2, lease_seconds=1))

        retry = service("retry-inbox")
        receiver.plan("retry-inbox", ["503", "success"])
        phases["retry_failure"] = retry.process([events[0]])
        assert phases["retry_failure"]["notifications_retryable_failure"] == 1
        phases["retry_not_due"] = retry.process([events[0]])
        assert phases["retry_not_due"]["attempts"] == 0
        wait_due(engine, target, "retry-inbox")
        phases["retry_success"] = retry.process([events[0]])
        assert phases["retry_success"]["notifications_delivered"] == 1

        permanent = service("permanent-inbox")
        receiver.plan("permanent-inbox", ["400"])
        phases["permanent"] = permanent.process([events[0]])
        assert phases["permanent"]["notifications_permanent_failure"] == 1
        assert permanent.process([events[0]])["attempts"] == 0

        exhausted = service("exhausted-inbox")
        receiver.plan("exhausted-inbox", ["503"])
        phases["exhausted"] = []
        for _ in range(3):
            phases["exhausted"].append(exhausted.process([events[0]]))
            wait_due(engine, target, "exhausted-inbox")
        assert phases["exhausted"][-1]["notifications_permanent_failure"] == 1
        assert exhausted.process([events[0]])["attempts"] == 0

        ambiguous = service("accepted-timeout")
        receiver.plan("accepted-timeout", ["accepted_timeout", "success"])
        phases["accepted_timeout"] = ambiguous.process([events[0]])
        assert phases["accepted_timeout"]["notifications_retryable_failure"] == 1
        wait_due(engine, target, "accepted-timeout")
        phases["accepted_timeout_recovery"] = ambiguous.process([events[0]])
        assert phases["accepted_timeout_recovery"]["notifications_delivered"] == 1

        orphan = service("crash-inbox")
        orphan.generate([events[0]])
        phases["crash_after_acceptance"] = child("crash-inbox", target, crash=True)
        wait_due(engine, target, "crash-inbox")
        phases["stale_recovery"] = child("crash-inbox", target)
        assert phases["stale_recovery"]["result"]["claims_recovered"] == 1
        wait_due(engine, target, "crash-inbox")
        phases["restart_delivery"] = child("crash-inbox", target)
        assert phases["restart_delivery"]["result"]["notifications_delivered"] == 1

        def concurrent(_):
            own = get_postgres_engine()
            try:
                return NotificationService(own, ControlledHTTPProvider(endpoint, "concurrent-inbox", 0.1),
                                           schema=target, policy=policy).process([events[0]])
            finally:
                own.dispose()

        with ThreadPoolExecutor(max_workers=2) as pool:
            phases["concurrent"] = list(pool.map(concurrent, range(2)))
        assert sum(r["notifications_created"] for r in phases["concurrent"]) == 1
        assert sum(r["notifications_delivered"] for r in phases["concurrent"]) == 1
        before_restart = receiver.records()

    # The receiver is actually stopped here; real connection refusal, then same-ledger restart.
    offline = service("offline-inbox")
    phases["provider_offline"] = offline.process([events[1]])
    assert phases["provider_offline"]["notifications_retryable_failure"] == 1
    with running_receiver(ledger, 18088) as receiver:
        assert receiver.records() == before_restart
        wait_due(engine, target, "offline-inbox")
        phases["provider_restored"] = offline.process([events[1]])
        assert phases["provider_restored"]["notifications_delivered"] == 1
        phases["final_default_repeat"] = child("m8-verification", "notifications_serving", mode="process", short=False)
        records = receiver.records()
        history = independent_notifications(engine, target, records)
        default = independent_notifications(engine, "notifications_serving", records)
        assert history["delivery_counts"] == {"DELIVERED": 5, "PERMANENT_FAILURE": 2}
        assert history["attempt_count"] == 13
        # Seven successful logical acceptances across default and controlled scenarios.
        assert len(records["receipts"]) == 7
        assert source_integrity(engine) == before
    return {"m7_alert_schema": "alerts_serving", "m7_events_available": len(events), "phases": phases,
            "history": history, "default_history": default, "provider_records": records,
            "m7_before": before, "m7_after": source_integrity(engine), "m7_source_unchanged": True,
            "provider": "controlled_http", "channel": "http", "external_provider_certified": False,
            "receiver_ledger": str(ledger)}


def wait_due(engine, schema, destination):
    with engine.connect() as conn:
        delay = conn.execute(text(f'''SELECT greatest(0,extract(epoch FROM min(COALESCE(next_attempt_at,lease_until))
            -clock_timestamp())) FROM "{schema}".notification_deliveries
            WHERE destination=:dest AND status IN ('SENDING','RETRYABLE_FAILURE')'''), {"dest": destination}).scalar_one()
    if delay is not None:
        time.sleep(float(delay) + 0.05)


def independent_notifications(engine, schema, records):
    """Independent SQL joins + raw provider receipts, not service result counters."""
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            binding = conn.execute(text(f'SELECT * FROM "{schema}".source_binding')).mappings().one()
            source = binding["source_schema"]
            rows = [dict(r) for r in conn.execute(text(f'SELECT * FROM "{schema}".notification_deliveries ORDER BY notification_id')).mappings()]
            attempts = [dict(r) for r in conn.execute(text(f'SELECT * FROM "{schema}".notification_attempts ORDER BY notification_id,attempt_number')).mappings()]
            counts = {}
            for row in rows:
                scope = {"contract_version": "1.0", "source_database": binding["source_database"], "source_schema": source,
                         "alert_event_id": row["alert_event_id"], **{k: row[k] for k in ("provider", "channel", "destination")}}
                assert digest(scope) == row["notification_id"]
                original = conn.execute(text(f'''SELECT e.*,v.currency,v.evidence,v.rule_id,v.gold_version,v.evaluation_id,
                    s.provenance FROM "{source}".alert_events e JOIN "{source}".alert_evaluations v USING(evaluation_id)
                    JOIN "{source}".source_snapshots s USING(gold_version) WHERE e.event_id=:id'''),
                    {"id": row["alert_event_id"]}).mappings().one()
                payload = row["payload"]
                for field in ("currency", "severity", "rule_id", "evaluation_id"):
                    assert payload[field] == original[field]
                assert payload["metric_value"] == original["evidence"]["metric_value"]
                assert payload["comparison_boundary"] == original["evidence"]["comparison_boundary"]
                assert all(original["provenance"][k] == v for k, v in payload["source"].items())
                history = [a for a in attempts if a["notification_id"] == row["notification_id"]]
                assert [a["attempt_number"] for a in history] == list(range(1, row["attempt_count"] + 1))
                assert row["attempt_count"] <= row["max_attempts"]
                for previous, current in zip(history, history[1:]):
                    policy = row["configuration"]["policy"]
                    delay = min(policy["max_delay"], policy["base_delay"] * 2 ** (previous["attempt_number"] - 1))
                    assert (current["requested_at"] - previous["completed_at"]).total_seconds() >= delay
                if row["status"] == "DELIVERED":
                    receipt = [r for r in records["receipts"] if r["notification_id"] == row["notification_id"]]
                    assert len(receipt) == 1 and receipt[0]["reference"] == row["provider_reference"]
                    assert json.loads(receipt[0]["payload"]) == {"destination": row["destination"], "notification": payload}
                    assert history[-1]["outcome"] == "DELIVERED"
                counts[row["status"]] = counts.get(row["status"], 0) + 1
    return {"schema": schema, "notification_count": len(rows), "attempt_count": len(attempts),
            "delivery_counts": counts, "rows": rows, "attempts": attempts}


if __name__ == "__main__":
    main()
