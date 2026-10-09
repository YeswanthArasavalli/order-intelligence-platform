"""Fail-open, opt-in lifecycle telemetry. No business I/O or prometheus dependency."""

import atexit
from collections import deque
from datetime import datetime
from functools import wraps
import json
import os
import threading
import time
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, Request, build_opener
import uuid

# The client deliberately has no imports of business modules or prometheus_client.
_client = None
_lock = threading.Lock()


class Client:
    def __init__(self, endpoint):
        parsed = urlsplit(endpoint)
        if parsed.scheme != "http" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path != "/observe":
            raise ValueError("Invalid telemetry endpoint")
        self.endpoint = endpoint
        self.session = uuid.uuid4().hex
        self.sequence = 0
        self.queue = deque()
        self.components = set()
        self.lock = threading.Lock()
        self.send_lock = threading.Lock()
        self.wake = threading.Event()
        self.closed = False
        self.dropped = 0
        self.thread = threading.Thread(target=self.run, daemon=True, name="oip-telemetry")
        self.thread.start()
        atexit.register(self.close)

    def add(self, name, labels, value, component):
        with self.lock:
            self.components.add(component)
            if len(self.queue) >= 2048:
                self.dropped += 1
            else:
                self.queue.append({"name": name, "labels": labels, "value": value})
        self.wake.set()

    def send(self, stopped=False):
        if not self.send_lock.acquire(timeout=0.35):
            return
        try:
            with self.lock:
                updates = [self.queue.popleft() for _ in range(min(len(self.queue), 256))]
                self.sequence += 1
                dropped, self.dropped = self.dropped, 0
                data = {"session": self.session, "sequence": self.sequence, "components": sorted(self.components),
                        "stopped": stopped, "dropped": dropped, "updates": updates}
            try:
                request = Request(self.endpoint, json.dumps(data, allow_nan=False).encode(), {"Content-Type": "application/json"})
                with build_opener(ProxyHandler({})).open(request, timeout=0.25) as response:
                    if response.status != 204:
                        raise OSError("Telemetry rejected")
            except Exception:
                with self.lock:
                    self.dropped += len(updates) + dropped
                # No retry of an ambiguously accepted batch; never log request/exception.
        finally:
            self.send_lock.release()

    def run(self):
        heartbeat = time.monotonic()
        while not self.closed:
            self.wake.wait(0.25)
            self.wake.clear()
            if self.queue or time.monotonic() - heartbeat >= 3:
                self.send()
                heartbeat = time.monotonic()
            time.sleep(0.02)

    def flush(self):
        deadline = time.monotonic() + 1
        while self.queue and time.monotonic() < deadline:
            self.send()
        self.send()  # Barrier for a batch already removed by the sender thread.

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.wake.set()
        self.thread.join(timeout=0.4)
        self.flush()
        self.send(stopped=True)


def record(name, value=1, *, component, **labels):
    global _client
    try:
        endpoint = os.getenv("OIP_TELEMETRY_URL")
        if not endpoint:
            return
        if name in {"oip_snapshot_version", "oip_snapshot_rows", "oip_snapshot_timestamp_seconds"}:
            labels["component"] = component
        with _lock:
            if _client is None:
                _client = Client(endpoint)
        _client.add(name, labels, value, component)
    except Exception:
        pass  # Instrumentation never replaces a business result or exception.


def flush():
    try:
        if _client:
            _client.flush()
    except Exception:
        pass


def observed(operation):
    # Labels are literal at each instrumentation point; exporter validates catalog.
    component = {"schema_resolve": "bronze", "dlq_write": "silver", "provider_send": "provider"}.get(operation, operation.split("_")[0])

    def decorate(function):
        @wraps(function)
        def call(*args, **kwargs):
            if not os.getenv("OIP_TELEMETRY_URL"):
                return function(*args, **kwargs)
            start = time.monotonic()
            record("oip_operation_started_total", component=component, operation=operation)
            try:
                result = function(*args, **kwargs)
            except BaseException as exc:
                record("oip_operation_calls_total", component=component, operation=operation, result="failure")
                record("oip_operation_last_failure_timestamp_seconds", time.time(), component=component, operation=operation)
                record("oip_operation_duration_seconds", time.monotonic() - start, component=component, operation=operation)
                category = "other"
                chain = exc
                for _ in range(4):
                    if chain is None:
                        break
                    names = {c.__name__ for c in type(chain).__mro__}
                    if names & {"SQLAlchemyError", "ConnectionError", "TimeoutError", "RedisError", "HTTPError"}:
                        category = "dependency"
                        break
                    if names & {"ValueError", "InvalidSnapshot", "InvalidGoldSnapshotError", "InvalidAlertSource"}:
                        category = "validation"
                    chain = chain.__cause__
                record("oip_operation_failures_total", component=component, operation=operation, category=category)
                if operation == "schema_resolve":
                    record("oip_decode_failures_total", component="bronze", stage="schema")
                if operation == "dashboard_load":
                    record("oip_dashboard_loads_total", component="dashboard", source="failure", redis_status=getattr(exc, "redis_status", "unavailable"))
                raise
            status = result.get("status", "") if isinstance(result, dict) else ""
            outcome = "noop" if status in {"NOOP", "NOOP_MIGRATED"} else "empty" if status == "NO_COMMITTED_MANIFEST" or operation in {"redis_read", "notifications_claim"} and result is None else "success"
            record("oip_operation_calls_total", component=component, operation=operation, result=outcome)
            record("oip_operation_last_success_timestamp_seconds", time.time(), component=component, operation=operation)
            record("oip_operation_duration_seconds", time.monotonic() - start, component=component, operation=operation)
            try:
                returned(operation, result, args, kwargs)
            except Exception:
                pass
            return result
        return call
    return decorate


def returned(operation, result, args, kwargs):
    if operation == "dashboard_load":
        record("oip_dashboard_loads_total", component="dashboard", source="redis" if result.source == "Redis cache" else "postgres", redis_status=result.redis_status)
    elif operation == "gold_commit":
        gold(result)
    elif operation == "alerts_evaluate":
        for name, value in result["counts"].items():
            record("oip_alert_evaluations_total", value, component="alerts", result=name)
        rules = args[0] if args else kwargs["rules"]
        record("oip_alert_rules", len(rules), component="alerts", state="configured")
        record("oip_alert_rules", sum(r.enabled for r in rules), component="alerts", state="enabled")
        record("oip_alert_source_version", result["source"]["gold_version"], component="alerts")
    elif operation == "notifications_generate":
        record("oip_notification_requests_total", result["notifications_created"], component="notifications", result="created")
        record("oip_notification_requests_total", result["notifications_suppressed_duplicate"], component="notifications", result="duplicate")
    elif operation == "notifications_claim":
        category = "empty" if result is None else "recovered" if result.get("recovered") else "claimed"
        record("oip_notification_claims_total", component="notifications", result=category)
        if category == "claimed" and result["attempt_number"] > 1:
            record("oip_notification_retries_total", component="notifications")
        if category == "recovered":
            record("oip_notification_attempt_results_total", component="notifications", result="UNKNOWN")
            record("oip_notification_delivery_results_total", component="notifications", result=result["status"])
            if result["status"] == "PERMANENT_FAILURE":
                record("oip_notification_exhausted_total", component="notifications")
    elif operation == "notifications_finish":
        record("oip_notification_delivery_results_total", component="notifications", result=result)
        if result != "STALE_RESULT":
            delivery = args[2] if len(args) > 2 else kwargs["result"]
            record("oip_notification_attempt_results_total", component="notifications", result=delivery.outcome)
            if result == "PERMANENT_FAILURE" and delivery.outcome == "RETRYABLE_FAILURE":
                record("oip_notification_exhausted_total", component="notifications")
    elif operation == "provider_send":
        record("oip_provider_calls_total", component="provider", provider="controlled_http", channel="http", result=result.outcome)


def producer_delivery(error, message):
    try:
        record("oip_producer_events_total", component="producer", result="delivery_error" if error else "delivered")
        latency = message.latency()
        if latency is not None:
            record("oip_operation_duration_seconds", latency, component="producer", operation="producer_delivery")
    except Exception:
        pass


def producer_queued(event_type):
    record("oip_producer_events_total", component="producer", result="enqueued")
    allowed = {"ORDER_CREATED", "ORDER_PAID", "ORDER_SHIPPED", "ORDER_DELIVERED", "ORDER_CANCELLED", "ORDER_REFUNDED"}
    record("oip_producer_enqueued_events_total", component="producer", event_type=event_type if isinstance(event_type, str) and event_type in allowed else "unknown")


def silver(summary):
    try:
        for result, key in (("evaluated", "input_records"), ("passed", "passed_records"), ("failed", "failed_records")):
            record("oip_dq_records_total", summary[key], component="silver", result=result)
        for route, key in (("accepted", "silver_records"), ("rejected", "dlq_records"), ("replay", "replay_records")):
            record("oip_silver_records_total", summary[key], component="silver", route=route)
        record("oip_silver_records_total", summary["silver_records"] + summary["dlq_records"], component="silver", route="unique")
        for name, count in json.loads(summary["failure_breakdown_json"]).items():
            record("oip_dq_rule_failures_total", count, component="silver", rule=name)
        record("oip_pipeline_batches_total", component="silver", stage="silver", result="written")
    except Exception:
        pass


def gold(manifest):
    try:
        record("oip_gold_events_total", manifest["input_records"], component="gold", result="input")
        record("oip_gold_events_total", manifest["new_events"], component="gold", result="new")
        record("oip_snapshot_version", manifest["version"], component="gold")
        record("oip_snapshot_timestamp_seconds", datetime.fromisoformat(manifest["committed_at"]).timestamp(), component="gold")
        for table, key in (("revenue", "revenue_by_minute"), ("product", "product_rankings")):
            record("oip_snapshot_rows", manifest["tables"][key]["rows"], component="gold", table=table)
        record("oip_pipeline_batches_total", component="gold", stage="gold", result="written")
    except Exception:
        pass
