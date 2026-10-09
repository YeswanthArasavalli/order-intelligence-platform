"""M9 bounded metrics, fail-open hooks, protocol, health and source-integrity tests."""

from contextlib import contextmanager
import json
import math
from pathlib import Path
import threading
import time

import httpx
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families
import pytest

from observability.catalog import DQ_RULES, SPECS, Metrics
from observability.metrics_exporter import Collector, MetricsServer, Probes
from observability import telemetry
from scripts.milestone9_baseline import fingerprint


def sample(metrics, name, labels=None):
    return metrics.registry.get_sample_value(name, labels or {})


def batch(updates=None, seq=1, component="postgres"):
    return {"session": "a" * 32, "sequence": seq, "components": [component], "stopped": False, "dropped": 0,
            "updates": updates or [{"name": "oip_operation_started_total", "labels": {"operation": "postgres_commit"}, "value": 1}]}


@contextmanager
def server():
    collector = Collector()
    http = MetricsServer(("127.0.0.1", 0), collector)
    worker = threading.Thread(target=http.serve_forever, daemon=True)
    worker.start()
    try:
        yield collector, f"http://127.0.0.1:{http.server_port}"
    finally:
        http.shutdown()
        http.server_close()
        worker.join(timeout=3)


@pytest.fixture()
def instrumented(monkeypatch):
    with server() as pair:
        if telemetry._client:
            telemetry._client.close()
        telemetry._client = None
        monkeypatch.setenv("OIP_TELEMETRY_URL", pair[1] + "/observe")
        yield pair
        if telemetry._client:
            telemetry._client.close()
        telemetry._client = None


def test_registration_types_exposition_and_bounded_labels():
    metrics = Metrics()
    families = list(text_string_to_metric_families(generate_latest(metrics.registry).decode()))
    assert len(families) >= len(SPECS)
    forbidden = {"order_id", "user_id", "product_id", "event_id", "evaluation_id", "notification_id",
                 "alert_event_id", "destination", "sql", "error", "url", "uuid", "hash", "timestamp"}
    for name, spec in SPECS.items():
        assert name.startswith("oip_") and spec.help and spec.source and spec.kind != "summary"
        assert not forbidden.intersection(spec.labels)
        if spec.kind == "counter":
            assert name.endswith("_total")
        if spec.kind == "histogram":
            assert name.endswith("_seconds")
        assert all(0 < len(values) <= 32 for values in spec.labels.values())
    assert sum(len(f.samples) for f in families) < 3000


def test_dq_label_names_match_frozen_rules():
    from data_quality.validators import RULE_NAMES
    assert DQ_RULES == RULE_NAMES and len(DQ_RULES) == 21


def test_counter_histogram_and_no_double_count():
    collector = Collector()
    collector.ingest(batch())
    collector.ingest(batch())
    assert sample(collector.metrics, "oip_operation_started_total", {"operation": "postgres_commit"}) == 1
    assert sample(collector.metrics, "oip_telemetry_batches_total", {"result": "duplicate"}) == 1
    collector.ingest(batch([{"name": "oip_operation_duration_seconds", "labels": {"operation": "postgres_commit"}, "value": 0.2}], seq=2))
    assert sample(collector.metrics, "oip_operation_duration_seconds_count", {"operation": "postgres_commit"}) == 1
    assert sample(collector.metrics, "oip_operation_duration_seconds_sum", {"operation": "postgres_commit"}) == 0.2


@pytest.mark.parametrize("change", [
    {"name": "arbitrary_metric"}, {"labels": {"notification_id": "private"}},
    {"labels": {"operation": "SELECT password"}}, {"value": float("nan")}, {"value": -1},
    {"value": "secret"}, {"name": "oip_dependency_up", "labels": {"component": "postgres"}},
], ids=["unknown-name", "forbidden-label", "unbounded-value", "nan", "negative", "text", "probe-spoof"])
def test_invalid_batch_rejected_atomically(change):
    collector = Collector()
    data = batch()
    invalid = {**data["updates"][0], **change}
    data["updates"].append(invalid)
    with pytest.raises(ValueError):
        collector.ingest(data)
    assert sample(collector.metrics, "oip_operation_started_total", {"operation": "postgres_commit"}) == 0
    assert sample(collector.metrics, "oip_telemetry_batches_total", {"result": "rejected"}) == 1


def test_heartbeat_clean_idle_failure_and_expiry():
    collector = Collector(heartbeat_timeout=1)
    assert sample(collector.metrics, "oip_application_state", {"component": "postgres"}) == -1
    collector.ingest(batch())
    assert sample(collector.metrics, "oip_application_running_processes", {"component": "postgres"}) == 1
    error = batch([{"name": "oip_operation_calls_total", "labels": {"operation": "postgres_commit", "result": "failure"}, "value": 1}], seq=2)
    collector.ingest(error)
    assert sample(collector.metrics, "oip_application_state", {"component": "postgres"}) == 2
    collector.sessions["a" * 32]["time"] -= 2
    collector.heartbeat()
    assert sample(collector.metrics, "oip_application_state", {"component": "postgres"}) == 0
    stopped = batch(seq=3)
    stopped["stopped"] = True
    collector.ingest(stopped)
    assert sample(collector.metrics, "oip_application_state", {"component": "postgres"}) == -1


def test_receiver_session_bound():
    collector = Collector()
    for i in range(256):
        data = batch()
        data["session"] = f"{i:032x}"
        collector.ingest(data)
    data = batch()
    data["session"] = "f" * 32
    with pytest.raises(ValueError):
        collector.ingest(data)
    assert len(collector.sessions) == 256


def test_metrics_and_health_http_no_dependency_io():
    with server() as (collector, endpoint):
        with httpx.Client(trust_env=False) as client:
            assert client.get(endpoint + "/healthz").json() == {"status": "UP"}
            assert client.get(endpoint + "/readyz").status_code == 503
            collector.collected = True
            assert client.get(endpoint + "/readyz").json()["status"] == "DEGRADED"
            collector.ready = True
            assert client.get(endpoint + "/readyz").status_code == 200
            assert client.post(endpoint + "/observe", json=batch()).status_code == 204
            response = client.get(endpoint + "/metrics")
            assert response.status_code == 200 and "oip_operation_started_total" in response.text
            assert client.post(endpoint + "/observe", json={"payload": "PRIVATE"}).status_code == 400
            assert "PRIVATE" not in client.get(endpoint + "/metrics").text


def test_hooks_preserve_values_exceptions_and_observe(instrumented):
    collector, _ = instrumented
    expected = {"status": "NOOP"}

    @telemetry.observed("postgres_commit")
    def success():
        return expected

    error = ValueError("PRIVATE_EXCEPTION_NOT_A_LABEL")

    @telemetry.observed("postgres_commit")
    def failure():
        raise error

    assert success() is expected
    with pytest.raises(ValueError) as caught:
        failure()
    assert caught.value is error
    telemetry.flush()
    assert sample(collector.metrics, "oip_operation_calls_total", {"operation": "postgres_commit", "result": "noop"}) == 1
    assert sample(collector.metrics, "oip_operation_calls_total", {"operation": "postgres_commit", "result": "failure"}) == 1
    assert "PRIVATE_EXCEPTION" not in generate_latest(collector.metrics.registry).decode()


def test_exporter_unavailable_does_not_fail_business(monkeypatch):
    monkeypatch.setenv("OIP_TELEMETRY_URL", "http://127.0.0.1:1/observe")
    telemetry._client = None
    try:
        @telemetry.observed("postgres_commit")
        def business():
            return "unchanged"
        start = time.monotonic()
        assert business() == "unchanged" and time.monotonic() - start < 0.5
        telemetry.flush()
        assert telemetry._client.dropped > 0
    finally:
        telemetry._client.close()
        telemetry._client = None


def test_disabled_hooks_do_no_io(monkeypatch):
    monkeypatch.delenv("OIP_TELEMETRY_URL", raising=False)
    monkeypatch.setattr(telemetry, "Client", lambda *a: pytest.fail("disabled client constructed"))
    assert telemetry.observed("gold_commit")(lambda: 7)() == 7
    telemetry.record("oip_producer_events_total", component="producer", result="enqueued")


def test_queue_bounded_and_invalid_destination_fails_open(monkeypatch):
    monkeypatch.setenv("OIP_TELEMETRY_URL", "https://user:PLACEHOLDER@example.invalid/observe")
    telemetry._client = None
    assert telemetry.observed("producer_publish")(lambda: "ok")() == "ok"
    assert telemetry._client is None


def test_result_mapping_uses_existing_counts(instrumented):
    collector, _ = instrumented
    telemetry.silver({"input_records": 8, "passed_records": 6, "failed_records": 2,
                      "silver_records": 4, "dlq_records": 2, "replay_records": 2,
                      "failure_breakdown_json": '{"invalid_amount":2,"invalid_currency":1}'})
    telemetry.returned("notifications_generate", {"notifications_created": 2, "notifications_suppressed_duplicate": 1}, (), {})
    telemetry.returned("notifications_claim", {"recovered": True, "status": "PERMANENT_FAILURE"}, (), {})
    telemetry.flush()
    assert sample(collector.metrics, "oip_dq_records_total", {"result": "evaluated"}) == 8
    assert sample(collector.metrics, "oip_silver_records_total", {"route": "unique"}) == 6
    assert sample(collector.metrics, "oip_notification_requests_total", {"result": "created"}) == 2
    assert sample(collector.metrics, "oip_notification_attempt_results_total", {"result": "UNKNOWN"}) == 1
    assert sample(collector.metrics, "oip_notification_exhausted_total") == 1


def test_metadata_empty_invalid_and_unavailable_distinct():
    probes = Probes.__new__(Probes)
    probes.metrics, probes.meta = Metrics(), {}
    probes.metadata("postgres", None)
    assert sample(probes.metrics, "oip_snapshot_present", {"component": "postgres"}) == 0
    assert sample(probes.metrics, "oip_snapshot_rows", {"component": "postgres", "table": "revenue"}) == 0
    metadata = {"gold_version": 1, "revenue_rows": 0, "product_rows": 0, "served_at": "2026-10-04T00:00:00+00:00",
                "serving_status": "SERVED", "snapshot_digest": "a" * 64, "source_topic": "test", "source_root": "s3a://gold/test",
                "manifest_digest": "b" * 64, "snapshot_identity": "c" * 64}
    probes.metadata("postgres", metadata)
    assert sample(probes.metrics, "oip_snapshot_present", {"component": "postgres"}) == 1
    assert sample(probes.metrics, "oip_snapshot_rows", {"component": "postgres", "table": "revenue"}) == 0
    with pytest.raises(ValueError):
        probes.metadata("postgres", {**metadata, "gold_version": -1})
    assert sample(probes.metrics, "oip_snapshot_metadata_valid", {"component": "postgres"}) == 0
    probes.unknown("postgres")
    assert math.isnan(sample(probes.metrics, "oip_snapshot_rows", {"component": "postgres", "table": "revenue"}))


def test_restart_resets_observed_counters_not_business_state():
    before = Collector()
    before.ingest(batch())
    after = Collector()
    assert sample(before.metrics, "oip_operation_started_total", {"operation": "postgres_commit"}) == 1
    assert sample(after.metrics, "oip_operation_started_total", {"operation": "postgres_commit"}) == 0
    assert math.isnan(sample(after.metrics, "oip_snapshot_version", {"component": "gold"}))


def test_probe_does_not_publish_false_down_before_result(monkeypatch):
    collector = Collector()
    collector.ready = True
    collector.metrics.child("oip_dependency_up", component="postgres").set(1)
    probes = Probes.__new__(Probes)
    probes.collector, probes.metrics = collector, collector.metrics
    probes.meta, probes.reachable = {}, {}

    def postgres():
        assert sample(collector.metrics, "oip_dependency_up", {"component": "postgres"}) == 1
        raise ConnectionError("simulated actual failure")

    def subsequent_probe():
        assert not collector.ready

    monkeypatch.setattr(probes, "postgres", postgres)
    monkeypatch.setattr(probes, "redis", subsequent_probe)
    monkeypatch.setattr(probes, "kafka", subsequent_probe)
    monkeypatch.setattr(probes, "http", lambda endpoint: subsequent_probe())
    probes.collect()
    assert not collector.ready
    assert sample(collector.metrics, "oip_dependency_up", {"component": "postgres"}) == 0
    assert sample(collector.metrics, "oip_dependency_state", {"component": "postgres"}) == 0


def test_dashboard_provisioning_and_queries_reference_real_metrics():
    root = Path(__file__).resolve().parents[2] / "observability" / "grafana" / "dashboards"
    dashboards = [json.loads(p.read_text(encoding="utf-8")) for p in root.glob("*.json")]
    assert {d["uid"] for d in dashboards} == {"oip-platform", "oip-pipeline", "oip-serving-delivery"}
    for dashboard in dashboards:
        assert 1 <= len(dashboard["panels"]) <= 12
        for panel in dashboard["panels"]:
            assert panel["datasource"]["uid"] == "prometheus-oip" and panel["targets"]
            assert "revenue_per_minute_inr" not in str(panel)


def test_only_instrumentation_changed_frozen_business_ast():
    root = Path(__file__).resolve().parents[2]
    baselines = sorted((root / "milestone9_evidence" / "20261004a").glob("baseline-*.json"))
    if not baselines:
        # The M9 baseline bundle is private runtime evidence, intentionally excluded
        # from this public repository; the integrity check only applies when present.
        pytest.skip("milestone9_evidence baseline bundle is not shipped publicly")
    baseline = json.loads(baselines[0].read_text())["result"]["files"]
    current = fingerprint()
    assert set(current) == set(baseline)
    changed = []
    for path, original in baseline.items():
        if original["sha256"] != current[path]["sha256"]:
            assert "business_ast" in original and original["business_ast"] == current[path]["business_ast"], path
            changed.append(path)
    assert len(changed) == 12
