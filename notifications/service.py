"""Bounded M8 generation, claim, send, and fenced-result transactions."""

from dataclasses import asdict
from datetime import timedelta
import os
from pathlib import Path
from time import monotonic
import uuid

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from observability import telemetry as m9

from notifications.model import NotificationError, Policy, canonical, notification_id, payload_for, schema_name
from notifications.provider import ControlledHTTPProvider, DeliveryResult, Provider
from storage.postgres_redis_serving import get_postgres_engine


class NotificationService:
    def __init__(self, engine, provider: Provider, *, schema="notifications_serving", source="alerts_serving",
                 policy=None, batch_size=100, failure_hook=None):
        self.engine, self.provider = engine, provider
        self.schema, self.source = schema_name(schema), schema_name(source, source=True)
        self.policy = policy or Policy()
        if type(batch_size) is not int or not 1 <= batch_size <= 1000:
            raise ValueError("Batch size must be 1–1000")
        self.batch_size, self.failure_hook = batch_size, failure_hook
        self.q, self.s = f'"{self.schema}"', f'"{self.source}"'
        self.configuration = {"route": provider.route, "policy": asdict(self.policy)}
        if self.policy.lease_seconds <= 4 * provider.route["timeout"]:
            raise ValueError("Lease must exceed four provider phase timeouts")

    def hook(self, stage):
        if self.failure_hook:
            self.failure_hook(stage)

    def initialize(self):
        ddl = Path(__file__).with_name("persistence.sql").read_text(encoding="utf-8")
        with self.engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout='10s'"))
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": "oip.m8.init." + self.schema})
            conn.execute(text(ddl.replace("{schema}", self.q).replace("{source}", self.s)))
            database = conn.execute(text("SELECT current_database()")).scalar_one()
            conn.execute(text(f'INSERT INTO {self.q}.source_binding VALUES (TRUE,:db,:source) ON CONFLICT DO NOTHING'),
                         {"db": database, "source": self.source})
            binding = conn.execute(text(f'SELECT * FROM {self.q}.source_binding')).mappings().one()
            if binding["source_database"] != database or binding["source_schema"] != self.source:
                raise NotificationError("Notification schema is bound to another M7 source")
        return database

    @m9.observed("notifications_generate")
    def generate(self, event_ids=None):
        """Commit bounded immutable requests. No provider calls or M7 updates."""
        if event_ids is not None:
            import re
            if (not 1 <= len(event_ids) <= self.batch_size or len(set(event_ids)) != len(event_ids)
                    or any(not isinstance(v, str) or not re.fullmatch(r"[0-9a-f]{64}", v) for v in event_ids)):
                raise ValueError("Explicit events must be a bounded list of unique SHA-256 IDs")
        database = self.initialize()
        route = self.provider.route
        values = {**route, "limit": self.batch_size, "ids": event_ids}
        match = 'd.provider=:provider AND d.channel=:channel AND d.destination=:destination'
        with self.engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout='10s'"))
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": "oip.m8.generate." + self.schema})
            conflict = conn.execute(text(f'''SELECT 1 FROM {self.q}.notification_deliveries d
                WHERE {match} AND configuration<>CAST(:config AS JSONB) LIMIT 1'''),
                {**values, "config": canonical(self.configuration)}).first()
            if conflict:
                raise NotificationError("Existing route has different configuration or retry policy")
            duplicate_count = conn.execute(text(f'''SELECT count(*) FROM (
                SELECT 1 FROM {self.q}.notification_deliveries d WHERE {match}
                {"AND alert_event_id=ANY(:ids)" if event_ids else ""} LIMIT :limit) x'''), values).scalar_one()
            selection = "e.event_id=ANY(:ids)" if event_ids else f'''NOT EXISTS (
                SELECT 1 FROM {self.q}.notification_deliveries d WHERE d.alert_event_id=e.event_id AND {match})'''
            rows = conn.execute(text(f'''SELECT e.*,v.rule_id,v.gold_version,v.currency,v.dimensions,v.evidence,
                v.window_start,v.window_end,v.evaluated_at,v.outcome,v.qualifies,s.provenance,d.definition
                FROM {self.s}.alert_events e LEFT JOIN {self.s}.alert_evaluations v USING(evaluation_id)
                LEFT JOIN {self.s}.source_snapshots s USING(gold_version)
                LEFT JOIN {self.s}.alert_definitions d USING(rule_id)
                WHERE {selection} ORDER BY e.fired_at,e.event_id LIMIT :limit'''), values).mappings().all()
            if event_ids and len(rows) != len(event_ids):
                raise NotificationError("Requested M7 firing event does not exist")
            created = []
            for row in rows:
                key = notification_id(database, self.source, row["event_id"], route)
                payload = payload_for(row, key)
                old = conn.execute(text(f'SELECT payload FROM {self.q}.notification_deliveries WHERE notification_id=:id'),
                                   {"id": key}).scalar_one_or_none()
                if old is not None:
                    if old != payload:
                        raise NotificationError("Immutable M7 event differs from existing notification")
                    continue
                conn.execute(text(f'''INSERT INTO {self.q}.notification_deliveries
                    (notification_id,alert_event_id,provider,channel,destination,configuration,payload,status,
                     max_attempts,created_at,updated_at,next_attempt_at)
                    VALUES (:id,:event,:provider,:channel,:destination,CAST(:config AS JSONB),CAST(:payload AS JSONB),
                     'PENDING',:max,clock_timestamp(),clock_timestamp(),clock_timestamp())'''),
                    {**values, "id": key, "event": row["event_id"], "config": canonical(self.configuration),
                     "payload": canonical(payload), "max": self.policy.max_attempts})
                created.append(key)
                self.hook("after_create")
            self.hook("before_generation_commit")
        return {"notifications_created": len(created), "notifications_suppressed_duplicate": duplicate_count,
                "created_ids": created}

    @m9.observed("notifications_claim")
    def claim(self):
        """One short SQL transaction; expired work is closed and rescheduled, not stuck."""
        route = self.provider.route
        with self.engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout='10s'"))
            row = conn.execute(text(f'''SELECT * FROM {self.q}.notification_deliveries
                WHERE provider=:provider AND channel=:channel AND destination=:destination AND
                 ((status IN ('PENDING','RETRYABLE_FAILURE') AND next_attempt_at<=clock_timestamp()) OR
                  (status='SENDING' AND lease_until<=clock_timestamp()))
                ORDER BY COALESCE(next_attempt_at,lease_until),notification_id
                LIMIT 1 FOR UPDATE SKIP LOCKED'''), route).mappings().first()
            if row is None:
                return None
            if row["configuration"] != self.configuration:
                raise NotificationError("Claim configuration conflicts with persisted request")
            now = conn.execute(text("SELECT clock_timestamp()")).scalar_one()
            key = row["notification_id"]
            if row["status"] == "SENDING":
                conn.execute(text(f'''UPDATE {self.q}.notification_attempts SET outcome='UNKNOWN',completed_at=:now,
                    error_class='lease_expired',sanitized_message='Claim expired; provider outcome unknown',retryable=TRUE
                    WHERE notification_id=:id AND attempt_number=:attempt AND outcome='SENDING' '''),
                    {"id": key, "attempt": row["attempt_count"], "now": now})
                exhausted = row["attempt_count"] >= row["max_attempts"]
                conn.execute(text(f'''UPDATE {self.q}.notification_deliveries SET status=:status,updated_at=:now,
                    next_attempt_at=:next,claimed_at=NULL,lease_until=NULL,claim_token=NULL,failed_at=:failed,
                    terminal_reason=:reason WHERE notification_id=:id'''),
                    {"id": key, "now": now, "status": "PERMANENT_FAILURE" if exhausted else "RETRYABLE_FAILURE",
                     "next": None if exhausted else now + timedelta(seconds=self.policy.delay(row["attempt_count"])),
                     "failed": now if exhausted else None, "reason": "attempts_exhausted_outcome_unknown" if exhausted else None})
                return {"recovered": True, "notification_id": key, "status": "PERMANENT_FAILURE" if exhausted else "RETRYABLE_FAILURE"}
            number, token = row["attempt_count"] + 1, uuid.uuid4().hex
            conn.execute(text(f'''UPDATE {self.q}.notification_deliveries SET status='SENDING',attempt_count=:n,
                claimed_at=:now,updated_at=:now,lease_until=:lease,claim_token=:token,next_attempt_at=NULL
                WHERE notification_id=:id'''), {"id": key, "n": number, "token": token, "now": now,
                                               "lease": now + timedelta(seconds=self.policy.lease_seconds)})
            conn.execute(text(f'''INSERT INTO {self.q}.notification_attempts
                (notification_id,attempt_number,claim_token,provider,requested_at,outcome)
                VALUES (:id,:n,:token,:provider,:now,'SENDING')'''),
                {"id": key, "n": number, "token": token, "provider": route["provider"], "now": now})
            self.hook("before_claim_commit")
            return {"notification_id": key, "attempt_number": number, "claim_token": token, "payload": row["payload"]}

    @m9.observed("notifications_finish")
    def finish(self, claim, result, duration_ms):
        with self.engine.begin() as conn:
            row = conn.execute(text(f'SELECT * FROM {self.q}.notification_deliveries WHERE notification_id=:id FOR UPDATE'),
                               {"id": claim["notification_id"]}).mappings().one()
            if row["status"] != "SENDING" or row["claim_token"] != claim["claim_token"]:
                return "STALE_RESULT"
            now = conn.execute(text("SELECT clock_timestamp()")).scalar_one()
            retryable = result.outcome == "RETRYABLE_FAILURE"
            exhausted = retryable and row["attempt_count"] >= row["max_attempts"]
            status = "PERMANENT_FAILURE" if exhausted else result.outcome
            exhausted_reason = ("attempts_exhausted_outcome_unknown" if result.error_class in
                                {"timeout", "network", "invalid_receipt", "provider_exception"} else "attempts_exhausted")
            values = {"id": claim["notification_id"], "n": claim["attempt_number"], "now": now,
                      "status": status, "outcome": result.outcome, "http": result.provider_status,
                      "ref": result.reference, "error": result.error_class, "duration": duration_ms,
                      "retryable": retryable, "message": result.error_class,
                      "next": now + timedelta(seconds=self.policy.delay(row["attempt_count"])) if retryable and not exhausted else None,
                      "delivered": now if status == "DELIVERED" else None,
                      "failed": now if status == "PERMANENT_FAILURE" else None,
                      "reason": exhausted_reason if exhausted else result.error_class if status == "PERMANENT_FAILURE" else None}
            conn.execute(text(f'''UPDATE {self.q}.notification_attempts SET completed_at=:now,outcome=:outcome,
                provider_status=:http,provider_reference=:ref,error_class=:error,sanitized_message=:message,
                duration_ms=:duration,retryable=:retryable WHERE notification_id=:id AND attempt_number=:n'''), values)
            conn.execute(text(f'''UPDATE {self.q}.notification_deliveries SET status=:status,updated_at=:now,
                next_attempt_at=:next,delivered_at=:delivered,failed_at=:failed,provider_reference=:ref,
                terminal_reason=:reason,claimed_at=NULL,lease_until=NULL,claim_token=NULL WHERE notification_id=:id'''), values)
            self.hook("before_result_commit")
        return status

    @m9.observed("notifications_process")
    def process(self, event_ids=None, *, generate=True):
        started = monotonic()
        result = {"notifications_created": 0, "notifications_suppressed_duplicate": 0, "notifications_delivered": 0,
                  "notifications_retryable_failure": 0, "notifications_permanent_failure": 0,
                  "claims_recovered": 0, "attempts": 0, "stale_results": 0, "provider_latency_ms": [], "outcomes": []}
        try:
            if generate:
                result.update(self.generate(event_ids))
            else:
                self.initialize()
            for _ in range(self.batch_size):
                claim = self.claim()
                if claim is None:
                    break
                if claim.get("recovered"):
                    result["claims_recovered"] += 1
                    result["notifications_" + claim["status"].lower()] += 1
                    continue
                self.hook("after_claim_commit")
                before = monotonic()
                try:
                    delivery = self.provider.send(claim["payload"])
                    if not isinstance(delivery, DeliveryResult):
                        raise TypeError("Invalid provider result")
                except Exception:
                    # Never persist/log provider exceptions, URLs, headers or response bodies.
                    delivery = DeliveryResult("RETRYABLE_FAILURE", error_class="provider_exception")
                latency = round((monotonic() - before) * 1000, 3)
                self.hook("after_provider")
                status = self.finish(claim, delivery, latency)
                result["attempts"] += 1
                result["provider_latency_ms"].append(latency)
                result["stale_results" if status == "STALE_RESULT" else "notifications_" + status.lower()] += 1
                result["outcomes"].append({"notification_id": claim["notification_id"], "status": status,
                                           "attempt_number": claim["attempt_number"]})
            result.update(processing_duration_ms=round((monotonic() - started) * 1000, 3),
                          provider=self.provider.route["provider"], channel=self.provider.route["channel"],
                          external_provider_certified=False)
            return result
        except SQLAlchemyError as exc:
            raise NotificationError("PostgreSQL operation failed; delivery outcome requires deterministic retry") from exc


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default=os.getenv("NOTIFICATIONS_SCHEMA", "notifications_serving"))
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("NOTIFICATION_BATCH_SIZE", "100")))
    parser.add_argument("--mode", choices=["process", "generate", "deliver"], default="process")
    args = parser.parse_args()
    engine = None
    try:
        if os.getenv("NOTIFICATION_PROVIDER") != "controlled_http":
            raise ValueError("Explicit controlled_http provider selection required")
        provider = ControlledHTTPProvider(os.getenv("NOTIFICATION_ENDPOINT", ""), os.getenv("NOTIFICATION_DESTINATION", ""),
                                          float(os.getenv("NOTIFICATION_TIMEOUT_SECONDS", "2")))
        policy = Policy(max_attempts=int(os.getenv("NOTIFICATION_MAX_ATTEMPTS", "3")),
                        base_delay=int(os.getenv("NOTIFICATION_BASE_DELAY_SECONDS", "2")),
                        max_delay=int(os.getenv("NOTIFICATION_MAX_DELAY_SECONDS", "30")),
                        lease_seconds=int(os.getenv("NOTIFICATION_LEASE_SECONDS", "30")))
        engine = get_postgres_engine()
        service = NotificationService(engine, provider, schema=args.schema, policy=policy, batch_size=args.batch_size)
        result = service.generate() if args.mode == "generate" else service.process(generate=args.mode != "deliver")
        print(canonical(result))
    except (ValueError, NotificationError, SQLAlchemyError, OSError):
        parser.exit(1, "M8 notification processing failed; check database, source and provider configuration\n")
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    main()
