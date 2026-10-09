"""M7 business-contract tests with real PostgreSQL persistence and isolated sources."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import uuid

import pytest
from sqlalchemy import create_engine, text

from alerts import evaluator
from alerts.definitions import Rule, condition, load_rules
from scripts.milestone7_cases import product, publish, revenue, rule
from storage.postgres_redis_serving import get_postgres_engine, read_postgres_snapshot


@pytest.fixture()
def world():
    engine = get_postgres_engine()
    suffix = uuid.uuid4().hex[:12]
    source, target = "m7_source_" + suffix, "m7_test_" + suffix
    publish(engine, source, 1)

    def run(rules=None, **kwargs):
        return evaluator.evaluate(rules or [rule()], engine=engine, source_schema=source, alert_schema=target, **kwargs)

    def rows(table):
        with engine.connect() as conn:
            return [dict(r) for r in conn.execute(text(f'SELECT * FROM "{target}".{table} ORDER BY 1')).mappings()]

    yield engine, source, target, run, rows
    with engine.begin() as conn:
        # Disposable generated fixtures only, never retained production source/history.
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{target}" CASCADE'))
        conn.execute(text(f'DROP SCHEMA "{source}" CASCADE'))
    engine.dispose()


@pytest.mark.parametrize("comparison,value,expected", [("lt", "10", False), ("lte", "10", True),
    ("gt", "10", False), ("gte", "10", True), ("lt", "9.99999999", True), ("gt", "10.00000001", True)])
def test_threshold_boundaries_exact_decimal(world, comparison, value, expected):
    engine, source, _, run, rows = world
    publish(engine, source, 2, [revenue(value)])
    result = run([rule(comparison=comparison)])
    assert result["counts"]["FIRED"] == int(expected)
    assert rows("alert_evaluations")[0]["qualifies"] is expected


def test_currency_and_product_identity(world):
    engine, source, _, run, rows = world
    products = [product(), product("20", currency="USD"), product("20", name="Other"), product("4", minute=5)]
    publish(engine, source, 2, [revenue(), revenue("20", currency="USD")], products)
    result = run([rule(), rule("usd-test", currency="USD"),
                  rule("product-inr", kind="product_threshold", metric="revenue",
                       dimensions={"product_id": "product-1", "product_name": "Product A"}),
                  rule("product-other", kind="product_threshold", metric="units_sold", threshold="1", comparison="gt",
                       dimensions={"product_id": "product-1", "product_name": "Other"})])
    assert result["counts"]["FIRED"] == 2 and result["counts"]["OK"] == 2
    evaluations = {r["rule_id"]: r for r in rows("alert_evaluations")}
    assert evaluations["usd-test"]["currency"] == "USD" and not evaluations["usd-test"]["qualifies"]
    assert evaluations["product-inr"]["window_start"].minute == 5
    assert evaluations["product-other"]["evidence"]["current_row"]["product_name"] == "Other"


def test_two_qualifying_currencies_and_negative_full_precision(world):
    engine, source, _, run, rows = world
    publish(engine, source, 2, [revenue("-0.00000001"),
                               revenue("-999999999999999999999999999999.99999999", currency="USD")])
    rules = [rule(threshold="0"), rule("usd-test", currency="USD", threshold="0")]
    assert run(rules)["counts"]["FIRED"] == 2
    records = {r["currency"]: r for r in rows("alert_evaluations")}
    assert records["USD"]["evidence"]["metric_value"] == "-999999999999999999999999999999.99999999"
    assert records["INR"]["scope_id"] != records["USD"]["scope_id"]
    assert run(rules)["counts"]["NOOP"] == 2
    assert len(rows("alert_events")) == 2


@pytest.mark.parametrize("previous,current,minute,outcome", [("100", "69.99999999", 0, "FIRED"),
    ("100", "70", 0, "OK"), ("0", "-1", 0, "NO_BASELINE"), ("-100", "-200", 0, "NO_BASELINE"),
    ("100", "1", -1, "NO_BASELINE")])
def test_revenue_drop_adjacent_comparable_window(world, previous, current, minute, outcome):
    engine, source, _, run, rows = world
    publish(engine, source, 2, [revenue(previous, minute=minute), revenue(current)])
    result = run([rule(kind="revenue_drop", threshold="0.7")])
    assert result["outcomes"][0]["outcome"] == outcome
    if outcome == "FIRED":
        assert rows("alert_evaluations")[0]["evidence"]["previous_row"]["net_revenue"] == "100.00000000"


def test_idempotency_identity_and_provenance(world):
    engine, source, target, run, rows = world
    snapshot_before = read_postgres_snapshot(engine, source)
    first = run()
    ids = [r["evaluation_id"] for r in rows("alert_evaluations")]
    second = run()
    assert second["counts"]["NOOP"] == 1 and len(rows("alert_events")) == 1
    assert ids == [r["evaluation_id"] for r in rows("alert_evaluations")]
    assert first["source"] == snapshot_before["metadata"] == rows("source_snapshots")[0]["provenance"]
    assert read_postgres_snapshot(engine, source) == snapshot_before
    assert evaluator.read_results(engine, target)["events"][0]["provenance"] == first["source"]


def test_cooldown_expiry_recovery_and_new_version_window(world, monkeypatch):
    engine, source, _, run, rows = world
    now = [datetime(2026, 10, 4, 12, tzinfo=timezone.utc)]
    monkeypatch.setattr(evaluator, "_database_now", lambda conn: now[0])
    assert run()["counts"]["FIRED"] == 1
    publish(engine, source, 2, [revenue("4", minute=2)])
    now[0] += timedelta(seconds=59)
    assert run()["counts"]["SUPPRESSED"] == 1  # new window/version does not bypass
    publish(engine, source, 3, [revenue("3", minute=3)])
    now[0] += timedelta(seconds=1)
    assert run()["counts"]["FIRED"] == 1  # exact deadline
    publish(engine, source, 4, [revenue("10", minute=4)])
    assert run()["counts"]["RECOVERED"] == 1
    publish(engine, source, 5, [revenue("2", minute=5)])
    assert run()["counts"]["SUPPRESSED"] == 1  # recovery doesn't erase deadline
    now[0] += timedelta(days=1)
    assert run()["status"] == "NOOP"  # same immutable evaluation never refires on a timer
    assert len(rows("alert_events")) == 2 and len(rows("alert_evaluations")) == 5
    assert rows("alert_state")[0]["status"] == "ACTIVE"


def test_restart_new_process_persists_cooldown(world):
    _, source, target, run, rows = world
    first = run()
    command = [sys.executable, "-c",
               "import json; from alerts.evaluator import evaluate; from scripts.milestone7_cases import rule; "
               f"print(json.dumps(evaluate([rule()],source_schema='{source}',alert_schema='{target}')))" ]
    result = subprocess.run(command, capture_output=True, text=True, env=os.environ.copy(), timeout=30)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "NOOP"
    assert len(rows("alert_events")) == 1
    assert rows("alert_state")[0]["last_fired_at"].isoformat() == first["evaluated_at"]


@pytest.mark.parametrize("stage", ["after_rule", "before_commit"])
def test_failure_rolls_back_all_alert_business_tables(world, stage):
    _, _, _, run, rows = world

    def fail(where):
        if where == stage:
            raise RuntimeError("injected transaction failure")

    with pytest.raises(RuntimeError, match="injected"):
        run(failure_hook=fail)
    for table in ("source_snapshots", "alert_definitions", "alert_state", "alert_evaluations", "alert_events"):
        assert rows(table) == []
    assert run()["counts"]["FIRED"] == 1


@pytest.mark.parametrize("corrupt", ["status", "digest", "history", "stale", "version", "money"])
def test_invalid_uncommitted_stale_source_rejected(world, corrupt):
    engine, source, _, run, rows = world
    publish(engine, source, 2)
    run()
    before = rows("alert_events")
    with engine.begin() as conn:
        if corrupt == "status":
            conn.execute(text(f'DELETE FROM "{source}".serving_state'))
        elif corrupt == "digest":
            conn.execute(text(f'UPDATE "{source}".serving_state SET snapshot_digest=:v'), {"v": "0" * 64})
        elif corrupt == "history":
            conn.execute(text(f'DELETE FROM "{source}".serving_history WHERE gold_version=2'))
        elif corrupt == "stale":
            # v1/v2 have the same rows; restore ALL old provenance in isolated fixture only.
            conn.execute(text(f'''UPDATE "{source}".serving_state s SET
                (gold_version,manifest_path,manifest_digest,snapshot_identity,served_at)=
                (SELECT gold_version,manifest_path,manifest_digest,snapshot_identity,served_at
                 FROM "{source}".serving_history WHERE gold_version=1)'''))
        elif corrupt == "version":
            conn.execute(text(f'UPDATE "{source}".serving_state SET gold_version=3'))
        else:
            conn.execute(text(f'UPDATE "{source}".revenue_by_minute SET net_revenue=999'))
    with pytest.raises(evaluator.AlertEvaluationError):
        run()
    assert rows("alert_events") == before and len(rows("alert_evaluations")) == 1


def test_empty_missing_and_disabled_not_fabricated_recovery(world):
    engine, source, _, run, rows = world
    run()
    publish(engine, source, 2, [], [])
    assert run()["counts"]["NO_DATA"] == 1
    assert rows("alert_state")[0]["status"] == "UNKNOWN"
    assert rows("alert_state")[0]["last_fired_at"] is not None
    assert run([rule("disabled", enabled=False)])["counts"]["DISABLED"] == 1
    assert len(rows("alert_events")) == 1


def test_postgres_unavailable_explicit():
    engine = create_engine("postgresql+psycopg2://127.0.0.1:1/unavailable", connect_args={"connect_timeout": 1})
    try:
        with pytest.raises(evaluator.AlertEvaluationError, match="PostgreSQL unavailable"):
            evaluator.evaluate([rule()], engine=engine, alert_schema="m7_unavailable")
    finally:
        engine.dispose()


def test_redis_and_notification_modules_not_used(world, monkeypatch):
    import redis
    _, _, _, run, _ = world
    monkeypatch.setattr(redis.Redis, "execute_command", lambda *a, **kw: pytest.fail("Redis must not be called"))
    assert run()["counts"]["FIRED"] == 1
    assert "alerts.alert_router" not in sys.modules


def test_concurrent_evaluators_emit_once(world):
    _, source, target, _, rows = world
    barrier = threading.Barrier(2)

    def execute():
        barrier.wait(timeout=10)
        return evaluator.evaluate([rule()], source_schema=source, alert_schema=target)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: execute(), range(2)))
    assert sorted(r["status"] for r in results) == ["EVALUATED", "NOOP"]
    assert len(rows("alert_events")) == len(rows("alert_state")) == len(rows("alert_evaluations")) == 1


def test_m4_commit_cannot_interleave_alert_source(world):
    engine, source, _, run, rows = world
    held, release = threading.Event(), threading.Event()

    def hook(stage):
        if stage == "before_commit":
            held.set()
            assert release.wait(timeout=10)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run, failure_hook=hook)
        assert held.wait(timeout=10)
        with engine.begin() as conn:
            acquired = conn.execute(text("SELECT pg_try_advisory_xact_lock(hashtext(:key))"),
                                    {"key": f"oip.gold-serving.{source}"}).scalar_one()
            assert not acquired
        release.set()
        assert future.result(timeout=10)["source"]["gold_version"] == 1
    assert rows("source_snapshots")[0]["gold_version"] == 1


@pytest.mark.parametrize("changes", [{"threshold": 1.2}, {"currency": "*"}, {"cooldown_seconds": 0},
    {"comparison": "DROP TABLE"}, {"threshold": "NaN"}, {"threshold": "0.000000001"},
    {"kind": "ml"}, {"dimensions": {"region": "test", "currency": "USD"}}, {"severity": "urgent"}])
def test_invalid_configuration_rejected(changes):
    with pytest.raises(ValueError):
        rule(**changes)


def test_immutable_definition_expected_topic_and_clock_guard(world, monkeypatch):
    _, _, _, run, rows = world
    run()
    with pytest.raises(evaluator.AlertEvaluationError, match="immutable"):
        run([replace(rule(), threshold="11")])
    with pytest.raises(evaluator.InvalidAlertSource):
        run(expected_topic="wrong-topic")
    assert len(rows("alert_events")) == 1
    now = datetime(2000, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(evaluator, "_database_now", lambda c: now)
    engine, source, _, _, _ = world
    publish(engine, source, 2)
    with pytest.raises(evaluator.AlertEvaluationError, match="clock"):
        run()


def test_example_rules_and_pure_evaluation():
    path = Path(__file__).resolve().parents[2] / "alerts" / "rules.example.json"
    rules = load_rules(str(path))
    assert len(rules) == 4 and {r.currency for r in rules} == {"INR", "USD"}
    assert all(isinstance(r, Rule) for r in rules)
    assert condition(rule(), {"revenue_by_minute": []})["reason"] == "NO_DATA"
