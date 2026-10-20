"""M8 contract tests: actual PostgreSQL and loopback HTTP with durable receipts."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
import uuid

import httpx
import pytest
from sqlalchemy import create_engine, text

from notifications.model import NotificationError, Policy
from notifications.provider import ControlledHTTPProvider
from notifications.service import NotificationService
from scripts.milestone8_receiver import running_receiver
from scripts.verify_milestone8 import source_integrity
from storage.postgres_redis_serving import get_postgres_engine


def drop_generated_schemas(engine, schemas):
    """Best-effort teardown of this file's own UUID-generated fixture schemas.

    Each DROP SCHEMA runs in its own transaction, so a failure on one schema
    rolls back only that drop and every remaining generated schema is still
    attempted. The first cleanup failure is re-raised after all attempts, so
    cleanup problems are surfaced rather than suppressed; when teardown runs
    while a test error is propagating, the original test error is retained as
    the raised exception's context rather than being lost. Schemas outside the
    m7_*/m8_* generated fixture prefixes are refused outright, so cleanup can
    never drop alerts_serving, notifications_serving, gold_serving or any
    shared schema.
    """
    failure = None
    for schema in schemas:
        if not schema.startswith(("m7_", "m8_")):
            raise AssertionError(f"Refusing to drop non-generated schema {schema!r}")
        try:
            with engine.begin() as conn:
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        except Exception as error:  # recorded, re-raised below; never hidden
            failure = failure or error
    if failure is not None:
        raise failure


@pytest.fixture()
def world(tmp_path):
    """Self-seed two disposable M7 firing events and bind M8 to the generated source.

    Publishes an isolated M4-shaped source, runs the frozen evaluator into an
    isolated m7_* alert schema, then constructs the service against that source
    and a unique m8_test_* target, so the fixture seeds its own source and its
    generated events no longer depend on retained alerts_serving rows. That is
    a source-level property established by inspecting this file and
    NotificationService's bindings; behavioral isolation is claimed only after
    the suite executes against a disposable PostgreSQL instance.
    """
    from alerts.evaluator import evaluate
    from scripts.milestone7_cases import publish, rule
    engine = get_postgres_engine()
    suffix = uuid.uuid4().hex[:12]
    source, alerts, target = "m7_source_" + suffix, "m7_m8_" + suffix, "m8_test_" + suffix
    try:
        publish(engine, source, 1)
        evaluated = evaluate([rule("inr-a"), rule("inr-b")], engine=engine, source_schema=source, alert_schema=alerts)
        # Fixture contract, derived from the supported evaluate() result rather
        # than re-read from the snapshot table the payload later joins: the
        # evaluator recorded exactly the published version requested here.
        assert evaluated["source"]["gold_version"] == 1, "Self-seeded fixture must publish source version 1"
        with engine.connect() as conn:
            events = conn.execute(text(f'SELECT event_id FROM "{alerts}".alert_events ORDER BY event_id')).scalars().all()
        assert len(events) == 2, "Self-seeded M7 fixture must produce exactly two firing events"
        with running_receiver(tmp_path / "receiver.sqlite") as receiver:
            provider = ControlledHTTPProvider(receiver.endpoint, "test-inbox", timeout=0.2)
            service = NotificationService(engine, provider, source=alerts, schema=target,
                                          policy=Policy(base_delay=1, max_delay=2, lease_seconds=1))

            def rows(table):
                with engine.connect() as conn:
                    return [dict(r) for r in conn.execute(text(f'SELECT * FROM "{target}".{table} ORDER BY 1,2')).mappings()]

            yield service, receiver, events, rows
    finally:
        try:
            # Disposable generated fixtures only, never retained production source/history.
            drop_generated_schemas(engine, (target, alerts, source))
        finally:
            engine.dispose()


def wait_due(service):
    with service.engine.connect() as conn:
        delay = conn.execute(text(f'''SELECT greatest(0,extract(epoch FROM min(
            COALESCE(next_attempt_at,lease_until))-clock_timestamp())) FROM {service.q}.notification_deliveries
            WHERE status IN ('SENDING','RETRYABLE_FAILURE')''')).scalar_one()
    if delay is not None:
        time.sleep(float(delay) + 0.03)


def decode_child_stdout(proc):
    """Parse a restarted-worker child's JSON stdout with full-process diagnostics."""
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as error:
        raise AssertionError(f"child stdout was not JSON: stdout={proc.stdout!r} stderr={proc.stderr!r}") from error


def test_generation_mapping_identity_destination_and_immutability(world):
    service, receiver, events, rows = world
    before = source_integrity(service.engine, service.source)
    assert service.generate()["notifications_created"] == 2
    assert receiver.records()["calls"] == []
    for row in rows("notification_deliveries"):
        scope = {"contract_version": "1.0", "source_database": "order_intelligence", "source_schema": service.source,
                 "alert_event_id": row["alert_event_id"], "provider": "controlled_http", "channel": "http", "destination": "test-inbox"}
        expected = hashlib.sha256(json.dumps(scope, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        assert row["notification_id"] == expected and row["status"] == "PENDING"
        # 1 is the fixture-guaranteed published version: the world fixture
        # publishes version 1 and asserts the evaluator recorded gold_version
        # 1, so this checks M8 propagation of the published source version
        # instead of echoing the same source_snapshots row the payload joins.
        assert row["payload"]["source"]["gold_version"] == 1 and row["payload"]["currency"] == "INR"
        assert "INR" in row["payload"]["message"] and row["payload"]["alert_event_id"] in events
    again = service.generate()
    assert again["notifications_created"] == 0 and again["notifications_suppressed_duplicate"] == 2
    assert len(rows("notification_deliveries")) == 2
    assert before == source_integrity(service.engine, service.source)


def test_success_and_rerun_no_provider_call(world):
    service, receiver, _, rows = world
    first = service.process()
    assert first["notifications_delivered"] == 2 and first["attempts"] == 2
    assert service.process()["attempts"] == 0
    assert len(receiver.records()["receipts"]) == len(receiver.records()["calls"]) == 2
    assert all(r["status"] == "DELIVERED" and r["provider_reference"] for r in rows("notification_deliveries"))
    assert all(r["outcome"] == "DELIVERED" and r["attempt_number"] == 1 for r in rows("notification_attempts"))


@pytest.mark.parametrize("mode,expected", [("503", "RETRYABLE_FAILURE"), ("429", "RETRYABLE_FAILURE"),
    ("408", "RETRYABLE_FAILURE"), ("425", "RETRYABLE_FAILURE"), ("400", "PERMANENT_FAILURE"),
    ("401", "PERMANENT_FAILURE"), ("403", "PERMANENT_FAILURE"), ("404", "PERMANENT_FAILURE"),
    ("409", "PERMANENT_FAILURE"), ("302", "PERMANENT_FAILURE"), ("malformed", "RETRYABLE_FAILURE"),
    ("timeout", "RETRYABLE_FAILURE")])
def test_http_classification_and_sanitization(world, mode, expected):
    service, receiver, events, rows = world
    receiver.plan("test-inbox", [mode])
    result = service.process([events[0]])
    assert result["notifications_" + expected.lower()] == 1, f"result={result}"
    attempt = rows("notification_attempts")[0]
    assert attempt["outcome"] == expected and attempt["retryable"] == (expected == "RETRYABLE_FAILURE")
    assert "CONTROLLED_SECRET_CANARY" not in json.dumps(rows("notification_attempts"), default=str)
    assert "127.0.0.1" not in json.dumps(result)


def test_retry_policy_success_and_due_time(world):
    service, receiver, events, rows = world
    receiver.plan("test-inbox", ["503", "success"])
    assert service.process([events[0]])["notifications_retryable_failure"] == 1
    delivery, attempt = rows("notification_deliveries")[0], rows("notification_attempts")[0]
    assert (delivery["next_attempt_at"] - attempt["completed_at"]).total_seconds() == 1
    assert service.process([events[0]])["attempts"] == 0
    wait_due(service)
    assert service.process([events[0]])["notifications_delivered"] == 1
    assert len(receiver.records()["calls"]) == 2 and len(receiver.records()["receipts"]) == 1
    assert [r["attempt_number"] for r in rows("notification_attempts")] == [1, 2]


def test_permanent_no_retry(world):
    service, receiver, events, rows = world
    receiver.plan("test-inbox", ["400", "success"])
    service.process([events[0]])
    assert service.process([events[0]])["attempts"] == 0
    assert len(receiver.records()["calls"]) == 1
    assert rows("notification_deliveries")[0]["failed_at"] is not None


def test_max_attempts_and_bounded_exponential_backoff(world):
    service, receiver, events, rows = world
    receiver.plan("test-inbox", ["503"])
    for i in range(3):
        result = service.process([events[0]])
        delivery = rows("notification_deliveries")[0]
        assert delivery["attempt_count"] == i + 1
        if i < 2:
            attempt = rows("notification_attempts")[-1]
            assert (delivery["next_attempt_at"] - attempt["completed_at"]).total_seconds() == 2 ** i
            wait_due(service)
    assert result["notifications_permanent_failure"] == 1
    assert delivery["terminal_reason"] == "attempts_exhausted" and delivery["next_attempt_at"] is None
    assert service.process([events[0]])["attempts"] == 0 and len(receiver.records()["calls"]) == 3
    assert Policy(base_delay=2, max_delay=3).delay(10) == 3


@pytest.mark.parametrize("stage", ["after_create", "before_generation_commit", "before_claim_commit"])
def test_transaction_rollback_before_provider(world, stage):
    service, receiver, events, rows = world

    def fail(where):
        if where == stage:
            raise RuntimeError("injected")

    service.failure_hook = fail
    with pytest.raises(RuntimeError, match="injected"):
        service.process([events[0]])
    assert receiver.records()["calls"] == [] and rows("notification_attempts") == []
    if stage != "before_claim_commit":
        assert rows("notification_deliveries") == []
    service.failure_hook = None
    assert service.process([events[0]])["notifications_delivered"] == 1


@pytest.mark.parametrize("stage", ["after_claim_commit", "after_provider", "before_result_commit"])
def test_crash_stale_claim_recovery_and_acceptance_dedup(world, stage):
    service, receiver, events, rows = world

    def crash(where):
        if where == stage:
            raise RuntimeError("process interrupted")

    service.failure_hook = crash
    with pytest.raises(RuntimeError):
        service.process([events[0]])
    assert rows("notification_deliveries")[0]["status"] == "SENDING"
    service.failure_hook = None
    wait_due(service)
    assert service.process([events[0]])["claims_recovered"] == 1
    assert rows("notification_attempts")[0]["outcome"] == "UNKNOWN"
    wait_due(service)
    assert service.process([events[0]])["notifications_delivered"] == 1
    assert len(receiver.records()["receipts"]) == 1
    assert len(receiver.records()["calls"]) == (1 if stage == "after_claim_commit" else 2)


def test_accepted_response_lost_uses_same_provider_key(world):
    service, receiver, events, rows = world
    receiver.plan("test-inbox", ["accepted_timeout", "success"])
    assert service.process([events[0]])["notifications_retryable_failure"] == 1
    assert len(receiver.records()["receipts"]) == 1
    wait_due(service)
    assert service.process([events[0]])["notifications_delivered"] == 1
    assert receiver.records()["calls"][-1]["mode"] == "deduplicated"
    assert len(receiver.records()["receipts"]) == 1 and len(rows("notification_attempts")) == 2


def test_stale_worker_result_is_fenced(world):
    from notifications.provider import DeliveryResult
    service, _, events, rows = world
    service.generate([events[0]])
    old = service.claim()
    wait_due(service)
    assert service.claim()["recovered"]
    wait_due(service)
    new = service.claim()
    assert service.finish(old, DeliveryResult("DELIVERED", 200, "controlled-" + old["notification_id"]), 1) == "STALE_RESULT"
    assert rows("notification_deliveries")[0]["claim_token"] == new["claim_token"]


def test_last_orphaned_claim_exhausted_unknown(world):
    service, _, events, rows = world
    service.policy = replace(service.policy, max_attempts=1)
    service.configuration["policy"]["max_attempts"] = 1
    service.generate([events[0]])
    service.claim()
    wait_due(service)
    assert service.claim()["status"] == "PERMANENT_FAILURE"
    assert rows("notification_deliveries")[0]["terminal_reason"] == "attempts_exhausted_outcome_unknown"
    assert rows("notification_attempts")[0]["outcome"] == "UNKNOWN"


def test_restart_separate_process_delivers_pending(world):
    service, receiver, events, rows = world
    service.generate([events[0]])
    # The direct-construction child program below reads exactly these three
    # variables; the packaged CLI's provider/lease/backoff variables are
    # deliberately not set here because this test does not exercise CLI or
    # environment parsing.
    env = {**os.environ, "NOTIFICATION_ENDPOINT": receiver.endpoint,
           "NOTIFICATION_DESTINATION": "test-inbox", "NOTIFICATION_TIMEOUT_SECONDS": "0.2"}
    # The packaged CLI entry point hard-codes the alerts_serving source and has
    # no source override, so the restarted worker over the self-seeded source
    # is launched by constructing the service directly with the parent's
    # policy, provider route, source schema and target schema, in a fresh
    # process with a fresh engine over the persisted pending row. This verifies
    # separate-process recovery through direct service construction; packaged
    # CLI configuration is covered separately by
    # test_cli_invalid_provider_config_never_logs_secret, not by this test.
    child = ("from notifications.model import Policy; "
             "from notifications.provider import ControlledHTTPProvider; "
             "from notifications.service import NotificationService; "
             "from storage.postgres_redis_serving import get_postgres_engine; "
             "import json, os; "
             "provider = ControlledHTTPProvider(os.environ['NOTIFICATION_ENDPOINT'], "
             "os.environ['NOTIFICATION_DESTINATION'], float(os.environ['NOTIFICATION_TIMEOUT_SECONDS'])); "
             f"service = NotificationService(get_postgres_engine(), provider, source={service.source!r}, "
             f"schema={service.schema!r}, policy=Policy(base_delay=1, max_delay=2, lease_seconds=1)); "
             "print(json.dumps(service.process(generate=False)))")
    command = [sys.executable, "-c", child]
    proc = subprocess.run(command, capture_output=True, text=True, env=env, timeout=20)
    assert proc.returncode == 0, f"child rc={proc.returncode} stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert decode_child_stdout(proc)["notifications_delivered"] == 1
    retry = subprocess.run(command, capture_output=True, text=True, env=env, timeout=20)
    assert retry.returncode == 0, f"retry rc={retry.returncode} stdout={retry.stdout!r} stderr={retry.stderr!r}"
    assert decode_child_stdout(retry)["attempts"] == 0
    assert len(receiver.records()["calls"]) == len(rows("notification_attempts")) == 1


def test_concurrent_generation_and_workers(world):
    service, receiver, events, rows = world
    barrier = threading.Barrier(2)

    def worker():
        engine = get_postgres_engine()
        try:
            other = NotificationService(engine, service.provider, source=service.source, schema=service.schema,
                                        policy=service.policy)
            barrier.wait(timeout=10)
            return other.process([events[0]])
        finally:
            engine.dispose()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: worker(), range(2)))
    assert sum(r["notifications_created"] for r in results) == sum(r["notifications_delivered"] for r in results) == 1
    assert len(rows("notification_deliveries")) == len(rows("notification_attempts")) == len(receiver.records()["calls"]) == 1


def test_bounded_batch_progress_and_new_destination(world):
    service, receiver, _, rows = world
    service.batch_size = 1
    assert service.process()["notifications_created"] == 1
    assert service.process()["notifications_created"] == 1
    assert service.process()["notifications_created"] == 0
    provider = ControlledHTTPProvider(receiver.endpoint, "second-inbox", timeout=0.1)
    other = NotificationService(service.engine, provider, source=service.source, schema=service.schema,
                                policy=service.policy)
    assert other.process()["notifications_created"] == 2
    assert len({r["notification_id"] for r in rows("notification_deliveries")}) == 4


def test_missing_invalid_event_and_config_conflict(world):
    service, _, events, rows = world
    with pytest.raises(NotificationError, match="does not exist"):
        service.generate([events[0], "0" * 64])
    assert rows("notification_deliveries") == []
    with pytest.raises(ValueError):
        service.generate(["not-an-id"])
    service.generate([events[0]])
    service.configuration["policy"]["base_delay"] = 999
    with pytest.raises(NotificationError, match="configuration"):
        service.generate([events[0]])


def test_postgres_outage_never_calls_provider(world, monkeypatch):
    service, receiver, _, _ = world
    engine = create_engine("postgresql+psycopg2://127.0.0.1:1/unavailable", connect_args={"connect_timeout": 1})
    try:
        # Keeps the default source=alerts_serving on purpose: the engine is
        # unreachable, and the constructor performs no I/O, so the first
        # transaction fails before any schema-level read; no alerts_serving
        # row is ever consulted on this path.
        broken = NotificationService(engine, service.provider, schema=service.schema)
        with pytest.raises(NotificationError, match="PostgreSQL"):
            broken.process()
        assert receiver.records()["calls"] == []
    finally:
        engine.dispose()


def test_no_redis_or_m7_evaluation_or_legacy_router(world, monkeypatch):
    import redis
    import alerts.evaluator
    service, _, _, _ = world
    monkeypatch.setattr(redis.Redis, "execute_command", lambda *a, **k: pytest.fail("Redis used"))
    monkeypatch.setattr(alerts.evaluator, "evaluate", lambda *a, **k: pytest.fail("M7 reevaluated"))
    assert service.process()["notifications_delivered"] == 2
    assert "alerts.alert_router" not in sys.modules


@pytest.mark.parametrize("endpoint,destination", [("https://example.com/notify", "inbox"),
    ("http://user:SECRET@127.0.0.1:1234/notify", "inbox"), ("http://127.0.0.1:1234/notify?token=SECRET", "inbox"),
    ("http://127.0.0.1:1234/notify", "private@example.com"), ("", "")],
    ids=["external-host", "embedded-credentials", "query-token", "private-address", "empty-config"])
def test_configuration_error_redaction(endpoint, destination):
    with pytest.raises(ValueError) as error:
        ControlledHTTPProvider(endpoint, destination)
    assert "SECRET" not in str(error.value) and "private@" not in str(error.value)


def test_unexpected_provider_error_sanitized(world, monkeypatch):
    service, _, events, rows = world

    def bad(_):
        raise RuntimeError("token=SECRET_PRIVATE_CREDENTIAL")

    monkeypatch.setattr(service.provider, "send", bad)
    assert service.process([events[0]])["notifications_retryable_failure"] == 1
    assert "SECRET" not in json.dumps(rows("notification_attempts"), default=str)


def test_http_call_has_no_open_sql_transaction(world, monkeypatch):
    service, _, events, _ = world
    original = service.provider.send

    def check(payload):
        assert service.engine.pool.checkedout() == 0
        return original(payload)

    monkeypatch.setattr(service.provider, "send", check)
    assert service.process([events[0]])["notifications_delivered"] == 1


def test_provider_connection_outage_then_recovery(world, tmp_path, monkeypatch):
    service, _, events, rows = world
    with running_receiver(tmp_path / "offline.sqlite") as receiver:
        port = receiver.server_port
        provider = ControlledHTTPProvider(receiver.endpoint, "offline-inbox", timeout=0.2)
    offline = NotificationService(service.engine, provider, source=service.source, schema=service.schema,
                                  policy=service.policy)
    # Connecting to a freshly closed loopback listener is platform-specific:
    # Linux surfaces a connection refusal (httpx.ConnectError) but Windows
    # surfaces httpx.ConnectTimeout, so the outage is driven by injecting the
    # deterministic ConnectError the "network" classification is defined for,
    # instead of trusting the OS. Once the outage flag is cleared the real
    # transport is restored before the receiver restarts, so recovery still
    # performs a live loopback HTTP exchange and durable receipt.
    original_send = httpx.Client.send
    outage = {"refusing": True}

    def refuse_during_outage(self, request, **kwargs):
        if outage["refusing"]:
            raise httpx.ConnectError("controlled connection refused", request=request)
        return original_send(self, request, **kwargs)

    monkeypatch.setattr(httpx.Client, "send", refuse_during_outage)
    assert offline.process([events[0]])["notifications_retryable_failure"] == 1
    assert rows("notification_attempts")[0]["error_class"] == "network"
    outage["refusing"] = False
    with running_receiver(tmp_path / "offline.sqlite", port) as receiver:
        wait_due(offline)
        result = offline.process([events[0]])
        assert result["notifications_delivered"] == 1, f"result={result}"
        assert len(receiver.records()["receipts"]) == 1


def test_cli_invalid_provider_config_never_logs_secret(world):
    service, _, _, _ = world
    env = {**os.environ, "NOTIFICATION_PROVIDER": "controlled_http",
           "NOTIFICATION_ENDPOINT": "http://user:SECRET_CLI_CANARY@127.0.0.1:1234/notify",
           "NOTIFICATION_DESTINATION": "inbox"}
    proc = subprocess.run([sys.executable, "-m", "notifications.service", "--schema", service.schema],
                          capture_output=True, text=True, env=env, timeout=20)
    assert proc.returncode == 1 and "SECRET_CLI_CANARY" not in proc.stdout + proc.stderr
    assert "M8 notification processing failed" in proc.stderr


def test_currency_provenance_mapping_from_persisted_m7_fixture(world):
    # This test seeds its own additional USD/INR M7 fixture through the frozen
    # evaluator; M8 itself has no evaluator dependency and never reads analytical tables.
    from alerts.evaluator import evaluate
    from scripts.milestone7_cases import publish, revenue, rule
    service, receiver, _, _ = world
    suffix = uuid.uuid4().hex[:12]
    source, alerts, target = "m7_source_" + suffix, "m7_m8_" + suffix, "m8_currency_" + suffix
    try:
        publish(service.engine, source, 1, [revenue("-0.00000001"),
                revenue("-999999999999999999999999999999.99999999", currency="USD")])
        evaluate([rule(), rule("usd-test", currency="USD")], engine=service.engine, source_schema=source, alert_schema=alerts)
        before = source_integrity(service.engine, alerts)
        other = NotificationService(service.engine, service.provider, source=alerts, schema=target, policy=service.policy)
        assert other.process()["notifications_delivered"] == 2
        payloads = {json.loads(r["payload"])["notification"]["currency"]: json.loads(r["payload"])["notification"]
                    for r in receiver.records()["receipts"]}
        assert payloads["USD"]["metric_value"] == "-999999999999999999999999999999.99999999"
        assert "USD -999999999999999999999999999999.99999999" in payloads["USD"]["message"]
        assert payloads["INR"]["notification_id"] != payloads["USD"]["notification_id"]
        assert before == source_integrity(service.engine, alerts)
    finally:
        drop_generated_schemas(service.engine, (target, alerts, source))
