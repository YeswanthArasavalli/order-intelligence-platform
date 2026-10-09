"""Real M7 PostgreSQL evaluation + independent SQL persistence verification.

Production phase uses retained gold_serving and explicit example rule thresholds.
Controlled transitions use only new m7_source_* schemas, not production mutations.
"""

import argparse
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

from sqlalchemy import text

from alerts.evaluator import AlertEvaluationError, evaluate, read_results
from alerts.definitions import load_rules
from scripts.milestone7_cases import publish, revenue, rule
from scripts.verify_milestone5 import postgres_state
from storage.postgres_redis_serving import get_postgres_engine


def independent_history(engine, schema):
    """Read SQL directly; recompute conditions, identities and cooldown decisions."""
    with engine.connect() as conn:
        rows = conn.execute(text(f'''SELECT v.*,s.provenance,d.definition,d.definition_digest,
            e.event_id,e.severity FROM "{schema}".alert_evaluations v
            JOIN "{schema}".source_snapshots s USING(gold_version)
            JOIN "{schema}".alert_definitions d USING(rule_id)
            LEFT JOIN "{schema}".alert_events e USING(evaluation_id)
            ORDER BY v.gold_version,v.rule_id''')).mappings().all()
        states = conn.execute(text(f'SELECT * FROM "{schema}".alert_state ORDER BY rule_id')).mappings().all()

    def digest(value):
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()

    events, by_outcome = [], {}
    for record in rows:
        row = dict(record)
        definition, meta, evidence = row["definition"], row["provenance"], row["evidence"]
        assert row["currency"] == definition["currency"]
        scope = digest({"rule_id": row["rule_id"], "currency": row["currency"], "dimensions": row["dimensions"],
                        **{key: meta[key] for key in ("source_database", "source_schema", "source_topic", "source_root")}})
        current = evidence["current_row"]
        assert row["definition_digest"] == digest(definition)
        expected = digest({"scope_id": scope, "definition_digest": row["definition_digest"], "provenance": meta,
                           "window": [current["window_start"], current["window_end"]] if current else None})
        assert scope == row["scope_id"] and expected == row["evaluation_id"]
        if row["qualifies"] is not None:
            assert current["currency"] == row["currency"]
            assert all(current[key] == value for key, value in row["dimensions"].items())
            amount = Decimal(str(current[definition["metric"]]))
            threshold = Decimal(definition["threshold"])
            if definition["kind"] == "revenue_drop":
                previous = evidence["previous_row"]
                assert previous["window_end"] == current["window_start"] and previous["currency"] == current["currency"]
                assert Decimal(previous["net_revenue"]) > 0
                with localcontext() as ctx:
                    ctx.prec = 80
                    threshold *= Decimal(previous["net_revenue"])
            conditions = {"lt": amount < threshold, "lte": amount <= threshold, "gt": amount > threshold, "gte": amount >= threshold}
            assert conditions[definition["comparison"]] == row["qualifies"]
        if row["outcome"] == "FIRED":
            assert row["qualifies"] is True and row["event_id"] == digest({"evaluation_id": expected, "event_type": "FIRED"})
            assert row["cooldown_until"] is None or row["evaluated_at"] >= row["cooldown_until"]
            assert row["severity"] == definition["severity"]
            events.append(row["event_id"])
        else:
            assert row["event_id"] is None
        if row["outcome"] == "SUPPRESSED":
            assert row["qualifies"] is True and row["evaluated_at"] < row["cooldown_until"]
        by_outcome[row["outcome"]] = by_outcome.get(row["outcome"], 0) + 1
    return {"evaluation_count": len(rows), "event_count": len(events), "state_count": len(states),
            "outcome_counts": by_outcome, "event_ids": events,
            "evaluations": [dict(row) for row in rows], "states": [dict(r) for r in states]}


def controlled(engine, suffix):
    source, target = "m7_source_runtime_" + suffix, "m7_runtime_" + suffix
    # Retain this deterministic evidence fixture; refuse accidental reuse.
    with engine.connect() as conn:
        assert conn.execute(text("SELECT to_regnamespace(:schema)"), {"schema": source}).scalar_one() is None
    rules = [rule(cooldown=5), rule("usd-test", currency="USD", cooldown=5)]
    phases = []
    publish(engine, source, 1, [revenue("20"), revenue("20", currency="USD")])
    first = evaluate(rules, engine=engine, source_schema=source, alert_schema=target)
    assert first["counts"]["OK"] == 2
    phases.append(first)
    publish(engine, source, 2, [revenue("5"), revenue("20", currency="USD")])
    fired = evaluate(rules, engine=engine, source_schema=source, alert_schema=target)
    assert fired["counts"]["FIRED"] == 1 and fired["counts"]["OK"] == 1
    phases.append(fired)
    repeat = evaluate(rules, engine=engine, source_schema=source, alert_schema=target)
    assert repeat["counts"]["NOOP"] == 2
    phases.append(repeat)
    publish(engine, source, 3, [revenue("4", minute=2), revenue("20", currency="USD")])
    suppressed = evaluate(rules, engine=engine, source_schema=source, alert_schema=target)
    assert suppressed["counts"]["SUPPRESSED"] == 1
    phases.append(suppressed)
    # Real PostgreSQL clock, no injected/rewritten alert timestamps.
    time.sleep(5.1)
    publish(engine, source, 4, [revenue("3", minute=3), revenue("20", currency="USD")])
    expired = evaluate(rules, engine=engine, source_schema=source, alert_schema=target)
    assert expired["counts"]["FIRED"] == 1
    phases.append(expired)
    publish(engine, source, 5, [revenue("10", minute=4), revenue("20", currency="USD")])
    recovery = evaluate(rules, engine=engine, source_schema=source, alert_schema=target)
    assert recovery["counts"]["RECOVERED"] == 1
    phases.append(recovery)
    publish(engine, source, 6, [], [])
    empty = evaluate(rules, engine=engine, source_schema=source, alert_schema=target)
    assert empty["counts"]["NO_DATA"] == 2
    phases.append(empty)
    history = independent_history(engine, target)
    assert history["event_count"] == 2 and history["evaluation_count"] == 12
    return {"phases": phases, "independent_history": history}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["production", "restart", "controlled", "postgres-down", "redis-down"], required=True)
    parser.add_argument("--run", default="20261004a")
    args = parser.parse_args()
    if not args.run.isalnum():
        parser.error("Run label must be alphanumeric")
    target = "m7_verify_" + args.run
    rule_path = Path(__file__).resolve().parents[1] / "alerts" / "rules.example.json"
    rules = load_rules(str(rule_path))
    engine = get_postgres_engine()
    output = {"phase": args.phase, "alert_schema": target, "verified_at": datetime.now(timezone.utc).isoformat()}
    try:
        if args.phase == "postgres-down":
            try:
                evaluate(rules, engine=engine, alert_schema=target)
                raise AssertionError("PostgreSQL outage was not rejected")
            except AlertEvaluationError:
                output["failure_explicit"] = True
        elif args.phase == "controlled":
            output.update(controlled(engine, args.run))
        else:
            before = postgres_state("gold_serving")
            if args.phase == "restart":
                prior = independent_history(engine, target)
                command = [sys.executable, "-m", "alerts.evaluator", "--rules", str(rule_path), "--alert-schema", target]
                result = subprocess.run(command, capture_output=True, text=True, timeout=30)
                assert result.returncode == 0, result.stderr
                result = json.loads(result.stdout)
                assert result["status"] == "NOOP"
                assert independent_history(engine, target) == prior
                output["new_process_restart"] = True
                output["command"] = command
            else:
                result = evaluate(rules, engine=engine, alert_schema=target)
            history = independent_history(engine, target)
            assert before == postgres_state("gold_serving"), "Alert evaluation mutated analytical source"
            assert history["outcome_counts"] == {"FIRED": 2, "OK": 1, "NO_BASELINE": 1}
            assert history["event_count"] == 2 and history["evaluation_count"] == 4
            assert len(read_results(engine, target)["events"]) == 2
            output.update(result=result, independent_history=history, analytical_source_unchanged=True)
            if args.phase == "redis-down":
                output["redis_not_required"] = True
        output["finished_at"] = datetime.now(timezone.utc).isoformat()
        print("MILESTONE7_VERIFIED " + json.dumps(output, default=str, sort_keys=True))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
