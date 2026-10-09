"""M7 one-shot, PostgreSQL-only analytical alert evaluation and persistence.

No router, notifications, Redis calls, data-lake reads, polling or scheduler.
"""

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import re
from time import monotonic
from typing import Callable

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from observability import telemetry as m9

from alerts.definitions import Rule, canonical, condition, identity, load_rules, validate_rules
from storage.postgres_redis_serving import InvalidSnapshot, get_postgres_engine, read_postgres_snapshot


class AlertEvaluationError(RuntimeError):
    """Explicit, sanitized failure; no successful alert transaction is claimed."""


class InvalidAlertSource(AlertEvaluationError):
    pass


def alert_schema_name(value: str | None = None) -> str:
    value = value or os.getenv("ALERTS_SCHEMA", "alerts_serving")
    if value != "alerts_serving" and not re.fullmatch(r"m7_[a-z0-9_]{1,50}", value):
        raise ValueError("Alert target must be alerts_serving or an isolated m7_<slug> schema")
    return value


def _source_schema(value: str) -> str:
    # Isolated generated M4-schema fixtures are supported only by the Python test API.
    # The CLI source is always gold_serving.
    if value != "gold_serving" and not re.fullmatch(r"m7_source_[a-z0-9_]{1,40}", value):
        raise ValueError("Alert source must be gold_serving or an isolated M7 test source")
    return value


def initialize(engine: Engine, alert_schema: str | None = None) -> str:
    schema = alert_schema_name(alert_schema)
    ddl = Path(__file__).with_name("persistence.sql").read_text(encoding="utf-8").replace("{schema}", f'"{schema}"')
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '10s'"))
        conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": f"oip.m7.{schema}"})
        conn.execute(text(ddl))
    return schema


def _database_now(conn) -> datetime:
    return conn.execute(text("SELECT clock_timestamp()")).scalar_one().astimezone(timezone.utc)


def _check_source(conn, source_schema, snapshot):
    meta = snapshot["metadata"]
    if meta["source_schema"] != source_schema or meta["source_root"].rstrip("/").rsplit("/", 1)[-1] != meta["source_topic"]:
        raise InvalidAlertSource("Source provenance is inconsistent")
    expected_path = meta["source_root"].rstrip("/") + f"/_commits/{meta['gold_version']:020d}.json"
    if meta["manifest_path"] != expected_path:
        raise InvalidAlertSource("Source manifest identity is inconsistent")
    history = conn.execute(text(f'SELECT * FROM "{source_schema}".serving_history '
                                'WHERE source_topic=:topic AND gold_version=:version'),
                           {"topic": meta["source_topic"], "version": meta["gold_version"]}).mappings().first()
    if not history:
        raise InvalidAlertSource("Source has no successfully committed serving_history entry")
    latest = conn.execute(text(f'SELECT max(gold_version) FROM "{source_schema}".serving_history '
                               'WHERE source_topic=:topic'), {"topic": meta["source_topic"]}).scalar_one()
    if latest != meta["gold_version"]:
        raise InvalidAlertSource("Serving state is older than its successfully served history")
    for key in ("source_topic", "source_root", "gold_contract_version", "gold_version", "manifest_path",
                "manifest_digest", "snapshot_identity", "snapshot_digest", "revenue_rows", "product_rows", "serving_status"):
        if history[key] != meta[key]:
            raise InvalidAlertSource("Current source differs from its committed history")
    if history["served_at"].astimezone(timezone.utc).isoformat(timespec="microseconds") != meta["served_at"]:
        raise InvalidAlertSource("Source serving timestamp differs from history")
    # Rules require actual comparable 1-minute/5-minute analytical windows.
    for table, seconds in (("revenue_by_minute", 60), ("product_rankings", 300)):
        for row in snapshot[table]:
            if (datetime.fromisoformat(row["window_end"]) - datetime.fromisoformat(row["window_start"])).total_seconds() != seconds:
                raise InvalidAlertSource("Unexpected analytical window duration")


@m9.observed("alerts_evaluate")
def evaluate(rules: list[Rule], *, engine: Engine | None = None, alert_schema: str | None = None,
             source_schema: str = "gold_serving", expected_topic: str | None = None,
             failure_hook: Callable[[str], None] | None = None) -> dict:
    """Commit definitions, provenance, evaluations, transitions and events together.

    Shared M4 advisory lock holds the committed source stable across the reused
    M5 read-only source transaction and the alert write transaction. M7 evaluators
    serialize per target schema. Two database connections are used, no Redis I/O.
    """
    validate_rules(rules)
    source_schema = _source_schema(source_schema)
    schema = alert_schema_name(alert_schema)
    owned = engine is None
    engine = engine if engine is not None else get_postgres_engine()
    started = monotonic()
    q = f'"{schema}"'
    try:
        initialize(engine, schema)
        with engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout = '10s'"))
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": f"oip.m7.{schema}"})
            conn.execute(text("SELECT pg_advisory_xact_lock_shared(hashtext(:key))"),
                         {"key": f"oip.gold-serving.{source_schema}"})
            snapshot = read_postgres_snapshot(engine, source_schema)
            _check_source(conn, source_schema, snapshot)
            meta = snapshot["metadata"]
            if expected_topic is not None and meta["source_topic"] != expected_topic:
                raise InvalidAlertSource("Unexpected authoritative source topic")
            head = conn.execute(text(f'SELECT * FROM {q}.source_snapshots ORDER BY gold_version DESC LIMIT 1')).mappings().first()
            if head:
                for key in ("source_database", "source_schema", "source_topic", "source_root"):
                    if head["provenance"][key] != meta[key]:
                        raise InvalidAlertSource("Alert schema is bound to another analytical source")
                if head["gold_version"] > meta["gold_version"]:
                    raise InvalidAlertSource("Stale source version would roll alert evaluation backward")
                if head["gold_version"] == meta["gold_version"] and head["provenance"] != meta:
                    raise InvalidAlertSource("Same source version has conflicting provenance")
            conn.execute(text(f'INSERT INTO {q}.source_snapshots (gold_version,provenance) '
                              'VALUES (:version,CAST(:meta AS JSONB)) ON CONFLICT DO NOTHING'),
                         {"version": meta["gold_version"], "meta": canonical(meta)})
            now = _database_now(conn)
            outcomes = []
            for rule in sorted(rules, key=lambda r: r.rule_id):
                old_definition = conn.execute(text(f'SELECT definition_digest FROM {q}.alert_definitions WHERE rule_id=:id'),
                                              {"id": rule.rule_id}).scalar_one_or_none()
                if old_definition is not None and old_definition != rule.digest:
                    raise AlertEvaluationError("Rule ID is immutable; use a new explicit ID for a changed definition")
                conn.execute(text(f'INSERT INTO {q}.alert_definitions(rule_id,definition_digest,definition) '
                                  'VALUES (:id,:digest,CAST(:definition AS JSONB)) ON CONFLICT DO NOTHING'),
                             {"id": rule.rule_id, "digest": rule.digest, "definition": canonical(rule.document)})
                evidence = condition(rule, snapshot)
                scope = identity({"rule_id": rule.rule_id, "currency": rule.currency, "dimensions": rule.dimensions,
                                  **{key: meta[key] for key in ("source_database", "source_schema", "source_topic", "source_root")}})
                row = evidence["current_row"]
                evaluation_id = identity({"scope_id": scope, "definition_digest": rule.digest, "provenance": meta,
                                          "window": [row["window_start"], row["window_end"]] if row else None})
                previous = conn.execute(text(f'SELECT * FROM {q}.alert_evaluations WHERE evaluation_id=:id'),
                                        {"id": evaluation_id}).mappings().first()
                if previous:
                    outcomes.append({"rule_id": rule.rule_id, "evaluation_id": evaluation_id,
                                     "outcome": "NOOP", "original_outcome": previous["outcome"]})
                    continue
                state = conn.execute(text(f'SELECT * FROM {q}.alert_state WHERE scope_id=:scope FOR UPDATE'),
                                     {"scope": scope}).mappings().first()
                before = state["status"] if state else "UNKNOWN"
                last_fired = state["last_fired_at"] if state else None
                if state and now < state["last_evaluated_at"]:
                    raise AlertEvaluationError("Database clock moved backward; retry after clock correction")
                deadline = last_fired + timedelta(seconds=rule.cooldown_seconds) if last_fired else None
                qualifies = evidence["qualifies"]
                if qualifies is None:
                    outcome, after = evidence["reason"], "UNKNOWN"
                elif not qualifies:
                    outcome, after = ("RECOVERED" if before == "ACTIVE" else "OK"), "OK"
                elif deadline is not None and now < deadline:
                    outcome, after = "SUPPRESSED", "ACTIVE"
                else:
                    outcome, after = "FIRED", "ACTIVE"
                values = {"id": evaluation_id, "rule": rule.rule_id, "version": meta["gold_version"],
                          "scope": scope, "currency": rule.currency, "dimensions": canonical(rule.dimensions),
                          "start": row["window_start"] if row else None, "end": row["window_end"] if row else None,
                          "evidence": canonical(evidence), "qualifies": qualifies, "outcome": outcome,
                          "before": before, "after": after, "now": now, "last": last_fired, "deadline": deadline}
                conn.execute(text(f'''INSERT INTO {q}.alert_evaluations
                    (evaluation_id,rule_id,gold_version,scope_id,currency,dimensions,window_start,window_end,
                     evidence,qualifies,outcome,previous_status,resulting_status,evaluated_at,last_fired_before,cooldown_until)
                    VALUES (:id,:rule,:version,:scope,:currency,CAST(:dimensions AS JSONB),:start,:end,
                            CAST(:evidence AS JSONB),:qualifies,:outcome,:before,:after,:now,:last,:deadline)'''), values)
                if outcome == "FIRED":
                    conn.execute(text(f'''INSERT INTO {q}.alert_events (event_id,evaluation_id,severity,fired_at,event_type)
                        VALUES (:event,:id,:severity,:now,'FIRED')'''),
                                 {"event": identity({"evaluation_id": evaluation_id, "event_type": "FIRED"}),
                                  "id": evaluation_id, "severity": rule.severity, "now": now})
                    values["last"] = now
                conn.execute(text(f'''INSERT INTO {q}.alert_state
                    (scope_id,rule_id,currency,dimensions,status,last_fired_at,last_evaluated_at,last_evaluation_id)
                    VALUES (:scope,:rule,:currency,CAST(:dimensions AS JSONB),:after,:last,:now,:id)
                    ON CONFLICT (scope_id) DO UPDATE SET status=EXCLUDED.status,last_fired_at=EXCLUDED.last_fired_at,
                    last_evaluated_at=EXCLUDED.last_evaluated_at,last_evaluation_id=EXCLUDED.last_evaluation_id'''), values)
                outcomes.append({"rule_id": rule.rule_id, "evaluation_id": evaluation_id, "outcome": outcome,
                                 "state": after, "currency": rule.currency})
                if failure_hook:
                    failure_hook("after_rule")
            if failure_hook:
                failure_hook("before_commit")
        counts = {name: sum(r["outcome"] == name for r in outcomes) for name in (
            "FIRED", "SUPPRESSED", "NOOP", "OK", "RECOVERED", "NO_DATA", "NO_BASELINE", "DISABLED")}
        return {"status": "NOOP" if counts["NOOP"] == len(rules) else "EVALUATED", "alert_schema": schema,
                "source": meta, "rules_evaluated": len(rules), "outcomes": outcomes, "counts": counts,
                "evaluated_at": now.isoformat(), "duration_ms": round((monotonic() - started) * 1000, 3),
                "currencies_present": sorted({row["currency"] for t in ("revenue_by_minute", "product_rankings") for row in snapshot[t]})}
    except (SQLAlchemyError, InvalidSnapshot) as exc:
        raise AlertEvaluationError("PostgreSQL unavailable or source invalid; no alert transaction confirmed") from exc
    finally:
        if owned:
            engine.dispose()


def read_results(engine: Engine, alert_schema: str | None = None, limit: int = 100) -> dict:
    """Read-only backend result contract for future consumers, not delivery."""
    q = f'"{alert_schema_name(alert_schema)}"'
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("Result limit must be 1–1000")
    with engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn:
        with conn.begin():
            conn.execute(text("SET TRANSACTION READ ONLY"))
            events = conn.execute(text(f'''SELECT e.*, v.rule_id,v.currency,v.dimensions,v.window_start,v.window_end,
                v.evidence,v.gold_version,s.provenance,d.definition FROM {q}.alert_events e
                JOIN {q}.alert_evaluations v USING(evaluation_id)
                JOIN {q}.source_snapshots s USING(gold_version)
                JOIN {q}.alert_definitions d USING(rule_id)
                ORDER BY e.fired_at DESC,e.event_id LIMIT :limit'''), {"limit": limit}).mappings().all()
            state = conn.execute(text(f'SELECT * FROM {q}.alert_state ORDER BY rule_id LIMIT :limit'),
                                 {"limit": limit}).mappings().all()
    return {"events": [dict(r) for r in events], "state": [dict(r) for r in state]}


def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rules", default=os.getenv("ALERT_RULES_FILE"))
    parser.add_argument("--alert-schema", default=os.getenv("ALERTS_SCHEMA", "alerts_serving"))
    args = parser.parse_args()
    if not args.rules:
        parser.error("Supply --rules or ALERT_RULES_FILE; there are no implicit currency thresholds")
    try:
        result = evaluate(load_rules(args.rules), alert_schema=args.alert_schema,
                          expected_topic=os.getenv("ALERT_SOURCE_TOPIC") or None)
        print(canonical(result))
    except (AlertEvaluationError, ValueError, OSError) as exc:
        # A transport error after commit is ambiguous; repeat resolves deterministically.
        parser.exit(1, f"M7 {type(exc).__name__}: evaluation failed; check source, configuration and database availability\n")


if __name__ == "__main__":
    main()
