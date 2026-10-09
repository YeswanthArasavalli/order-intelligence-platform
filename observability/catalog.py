"""Fixed operational metric/label allowlist; no identifiers, money or business rules."""

from dataclasses import dataclass
from itertools import product
import math

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

OPERATIONS = {
    "producer_publish": "producer", "producer_delivery": "producer",
    "bronze_write": "bronze", "schema_resolve": "bronze",
    "silver_write": "silver", "dlq_write": "silver",
    "gold_batch": "gold", "gold_commit": "gold",
    "postgres_serve": "postgres", "postgres_commit": "postgres",
    "redis_refresh": "redis", "redis_read": "redis",
    "dashboard_load": "dashboard", "alerts_evaluate": "alerts",
    "notifications_generate": "notifications", "notifications_claim": "notifications",
    "notifications_finish": "notifications", "notifications_process": "notifications",
    "provider_send": "provider",
}
COMPONENTS = tuple(sorted(set(OPERATIONS.values())))
DEPENDENCIES = ("postgres", "redis", "kafka", "schema_registry", "minio", "dashboard")
OUTCOMES = ("DELIVERED", "RETRYABLE_FAILURE", "PERMANENT_FAILURE", "UNKNOWN")
DQ_RULES = (
    "null_order_id", "null_user_id", "null_product_id", "invalid_product_name",
    "invalid_amount", "null_timestamp", "malformed_timestamp", "invalid_timestamp",
    "future_timestamp", "old_timestamp", "invalid_region", "invalid_product_id",
    "invalid_event_type", "schema_drift", "invalid_currency", "invalid_payment_method",
    "invalid_status", "invalid_event_metadata", "invalid_schema_version",
    "invalid_provenance", "event_ts_mismatch",
)
BUCKETS = (0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60, 300)


@dataclass(frozen=True)
class Spec:
    kind: str
    help: str
    labels: dict
    source: str
    client: bool = True


SPECS = {
    "oip_operation_started_total": Spec("counter", "Observed operation starts; not unique business events", {"operation": tuple(OPERATIONS)}, "Lifecycle entry"),
    "oip_operation_calls_total": Spec("counter", "Completed operation invocations by result", {"operation": tuple(OPERATIONS), "result": ("success", "failure", "noop", "empty")}, "Lifecycle return/exception"),
    "oip_operation_duration_seconds": Spec("histogram", "Elapsed invocation time; do not sum nested operation layers", {"operation": tuple(OPERATIONS)}, "Monotonic operation clock"),
    "oip_operation_last_success_timestamp_seconds": Spec("gauge", "Last observed successful/noop/empty completion time", {"operation": tuple(OPERATIONS)}, "Lifecycle completion"),
    "oip_operation_last_failure_timestamp_seconds": Spec("gauge", "Last observed raised operation failure time", {"operation": tuple(OPERATIONS)}, "Lifecycle exception"),
    "oip_operation_failures_total": Spec("counter", "Raised operation failures by bounded error category", {"operation": tuple(OPERATIONS), "category": ("dependency", "validation", "other")}, "Exception class category, never text"),
    "oip_producer_events_total": Spec("counter", "Actual producer enqueue/delivery/error observations", {"result": ("enqueued", "delivered", "enqueue_error", "delivery_error")}, "Producer produce/callback"),
    "oip_producer_enqueued_events_total": Spec("counter", "Enqueued events by fixed type; not delivery acknowledgements", {"event_type": ("ORDER_CREATED", "ORDER_PAID", "ORDER_SHIPPED", "ORDER_DELIVERED", "ORDER_CANCELLED", "ORDER_REFUNDED", "unknown")}, "Producer enqueue"),
    "oip_bronze_records_total": Spec("counter", "Existing Bronze batch counts at receive/write completion", {"result": ("received", "written")}, "Existing batch count"),
    "oip_decode_failures_total": Spec("counter", "Decoder/schema invocation failures; Spark recomputation may repeat", {"stage": ("record", "schema")}, "Existing decoder exception boundary"),
    "oip_pipeline_batches_total": Spec("counter", "Observed pipeline branch completions", {"stage": ("bronze", "silver", "gold"), "result": ("written", "noop", "empty", "recovered")}, "Existing sink branches"),
    "oip_dq_records_total": Spec("counter", "Existing completed DQ summary counts including its replay semantics", {"result": ("evaluated", "passed", "failed")}, "M2 completed summary"),
    "oip_dq_rule_failures_total": Spec("counter", "Existing DQ rule failure occurrences; a row can fail multiple rules", {"rule": DQ_RULES}, "M2 failure_breakdown_json"),
    "oip_silver_records_total": Spec("counter", "Existing route counts; unique means accepted plus rejected non-replays", {"route": ("accepted", "rejected", "replay", "unique")}, "M2 completed summary"),
    "oip_dlq_records_written_total": Spec("counter", "Physical records in successful DLQ writes; delivery remains at-least-once", {}, "After existing Kafka DLQ write"),
    "oip_gold_events_total": Spec("counter", "Existing committed Gold input/new counts and replay branch count", {"result": ("input", "new", "replay")}, "M3 manifest/result"),
    "oip_snapshot_version": Spec("gauge", "Last observed Gold publication or current polled serving metadata version", {"component": ("gold", "postgres", "redis")}, "Manifest result / singleton / Redis metadata"),
    "oip_snapshot_rows": Spec("gauge", "Metadata row counts, not recomputed business metrics", {"component": ("gold", "postgres", "redis"), "table": ("revenue", "product")}, "Existing snapshot metadata"),
    "oip_snapshot_timestamp_seconds": Spec("gauge", "Gold commit or PostgreSQL served_at; Redis copies source served_at", {"component": ("gold", "postgres", "redis")}, "Existing metadata timestamp"),
    "oip_snapshot_present": Spec("gauge", "1 snapshot exists, 0 no committed snapshot, NaN probe unavailable", {"component": ("postgres", "redis")}, "Bounded metadata probe", False),
    "oip_snapshot_metadata_valid": Spec("gauge", "1 basic metadata valid, 0 absent/invalid, NaN unavailable; not full digest validation", {"component": ("postgres", "redis")}, "Bounded metadata probe", False),
    "oip_cache_matches_postgres": Spec("gauge", "1 cached metadata identity/version matches SQL, 0 differs, NaN uncheckable", {}, "Metadata comparison only", False),
    "oip_dashboard_loads_total": Spec("counter", "Read-service loads, not Streamlit network requests", {"source": ("redis", "postgres", "failure"), "redis_status": ("valid", "missing", "invalid", "unavailable")}, "M6 load return/exception"),
    "oip_alert_evaluations_total": Spec("counter", "Per-rule results of observed M7 invocations; NOOP is not a new firing", {"result": ("FIRED", "SUPPRESSED", "NOOP", "OK", "RECOVERED", "NO_DATA", "NO_BASELINE", "DISABLED")}, "M7 returned counts"),
    "oip_alert_rules": Spec("gauge", "Rule configuration of last observed invocation, not a historical definition count", {"state": ("configured", "enabled")}, "M7 supplied configuration"),
    "oip_alert_source_version": Spec("gauge", "Source version returned by last observed M7 invocation", {}, "M7 provenance"),
    "oip_notification_requests_total": Spec("counter", "Created logical requests versus existing-request observations", {"result": ("created", "duplicate")}, "M8 committed generation result"),
    "oip_notification_claims_total": Spec("counter", "Claim transactions including no-work and expired-claim recovery", {"result": ("claimed", "empty", "recovered")}, "M8 committed claim result"),
    "oip_notification_attempt_results_total": Spec("counter", "Recorded attempt outcomes; UNKNOWN is an expired claim", {"result": OUTCOMES}, "M8 committed finish/recovery"),
    "oip_notification_delivery_results_total": Spec("counter", "Parent delivery transitions or fenced stale results; not unique requests", {"result": (*OUTCOMES[:3], "STALE_RESULT")}, "M8 committed result/recovery"),
    "oip_notification_retries_total": Spec("counter", "Claims whose persisted attempt number exceeds one", {}, "M8 claim result"),
    "oip_notification_exhausted_total": Spec("counter", "Observed retry exhaustion transitions", {}, "M8 finish/recovery result"),
    "oip_provider_calls_total": Spec("counter", "Actual controlled-provider calls; DELIVERED counts acknowledgements including idempotent repeats", {"provider": ("controlled_http",), "channel": ("http",), "result": OUTCOMES[:3]}, "M8 send result"),
    "oip_provider_acceptances_total": Spec("counter", "New durable controlled-receiver acceptances, not repeated acknowledgements", {"provider": ("controlled_http",), "channel": ("http",)}, "Controlled receiver after receipt commit"),
    "oip_dependency_up": Spec("gauge", "Reachability/protocol probe: 1 up, 0 down, -1 not yet probed", {"component": DEPENDENCIES}, "Periodic dependency probe", False),
    "oip_dependency_state": Spec("gauge", "-1 unknown, 0 down, 1 up, 2 reachable but serving metadata absent/invalid", {"component": DEPENDENCIES}, "Probe and metadata availability", False),
    "oip_dependency_probe_duration_seconds": Spec("histogram", "Dependency probe duration; not business request latency", {"component": DEPENDENCIES}, "Probe monotonic clock", False),
    "oip_dependency_failures_total": Spec("counter", "Failed dependency probe attempts", {"component": DEPENDENCIES}, "Probe outcome", False),
    "oip_application_running_processes": Spec("gauge", "Fresh observed process sessions; no process IDs exposed", {"component": COMPONENTS}, "Bounded heartbeat sessions", False),
    "oip_application_state": Spec("gauge", "-1 unknown/clean idle, 0 lost heartbeat, 1 live, 2 live with last operation failure", {"component": COMPONENTS}, "Heartbeat and operation results", False),
    "oip_telemetry_last_received_timestamp_seconds": Spec("gauge", "Last received application telemetry by component", {"component": COMPONENTS}, "Collector receive time", False),
    "oip_telemetry_batches_total": Spec("counter", "Telemetry ingestion batches by acceptance result", {"result": ("accepted", "rejected", "duplicate")}, "Collector protocol", False),
    "oip_telemetry_dropped_updates_total": Spec("counter", "Client-reported dropped updates; abrupt-process losses may be unreported", {}, "Next successful client report", False),
    "oip_exporter_collection_timestamp_seconds": Spec("gauge", "Last completed dependency collection cycle", {}, "Collector clock", False),
    "oip_exporter_start_timestamp_seconds": Spec("gauge", "Exporter epoch; operational counters reset on restart", {}, "Exporter startup", False),
}


class Metrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.values = {}
        for name, spec in SPECS.items():
            cls = {"counter": Counter, "gauge": Gauge, "histogram": Histogram}[spec.kind]
            args = {"buckets": BUCKETS} if spec.kind == "histogram" else {}
            metric = cls(name, spec.help, tuple(spec.labels), registry=self.registry, **args)
            self.values[name] = metric
            for values in product(*spec.labels.values()):
                child = metric.labels(*values) if spec.labels else metric
                if spec.kind == "gauge":
                    child.set(float("nan"))

    @staticmethod
    def validate(update, client=True):
        if not isinstance(update, dict) or set(update) != {"name", "labels", "value"}:
            raise ValueError("Invalid metric update")
        spec = SPECS.get(update["name"])
        labels, value = update["labels"], update["value"]
        if spec is None or client and not spec.client or not isinstance(labels, dict) or set(labels) != set(spec.labels):
            raise ValueError("Metric/labels not allowed")
        if any(not isinstance(v, str) or v not in spec.labels[k] for k, v in labels.items()):
            raise ValueError("Label value not allowed")
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or value > 1e15:
            raise ValueError("Metric value not allowed")
        return spec

    def apply(self, update):
        spec = self.validate(update)
        child = self.child(update["name"], **update["labels"])
        getattr(child, {"counter": "inc", "gauge": "set", "histogram": "observe"}[spec.kind])(update["value"])

    def child(self, name, **labels):
        metric = self.values[name]
        return metric.labels(**labels) if SPECS[name].labels else metric
