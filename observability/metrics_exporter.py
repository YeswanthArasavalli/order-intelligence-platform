"""M9 bounded lifecycle collector and cached metrics/health endpoints.

No business calculations, table scans, Kafka consumption or data-lake reads.
"""

from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import re
import threading
import time

import httpx
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
import redis
from sqlalchemy import URL, create_engine, text

from observability.catalog import COMPONENTS, DEPENDENCIES, OPERATIONS, Metrics


class Collector:
    def __init__(self, metrics=None, heartbeat_timeout=15):
        self.metrics = metrics or Metrics()
        self.lock = threading.Lock()
        self.sessions = {}
        self.last_failure = {c: False for c in COMPONENTS}
        self.ever_seen = set()
        self.expired = set()
        self.heartbeat_timeout = heartbeat_timeout
        self.ready = False
        self.collected = False
        self.metrics.child("oip_exporter_start_timestamp_seconds").set(time.time())
        for component in DEPENDENCIES:
            self.metrics.child("oip_dependency_up", component=component).set(-1)
            self.metrics.child("oip_dependency_state", component=component).set(-1)
        self.heartbeat()

    def ingest(self, data):
        try:
            if (not isinstance(data, dict) or set(data) != {"session", "sequence", "components", "stopped", "dropped", "updates"}
                    or not isinstance(data["session"], str) or not re.fullmatch(r"[0-9a-f]{32}", data["session"])
                    or type(data["sequence"]) is not int or not 1 <= data["sequence"] <= 2**53
                    or type(data["stopped"]) is not bool or type(data["dropped"]) is not int or not 0 <= data["dropped"] <= 1e12
                    or not isinstance(data["components"], list) or not 1 <= len(data["components"]) <= len(COMPONENTS)
                    or any(c not in COMPONENTS for c in data["components"])
                    or not isinstance(data["updates"], list) or len(data["updates"]) > 256):
                raise ValueError("Invalid telemetry envelope")
            for update in data["updates"]:
                self.metrics.validate(update)
            with self.lock:
                now = time.time()
                self._prune(now)
                previous = self.sessions.get(data["session"])
                if previous and data["sequence"] <= previous["sequence"]:
                    self.metrics.child("oip_telemetry_batches_total", result="duplicate").inc()
                    return
                if previous is None and len(self.sessions) >= 256:
                    raise ValueError("Bounded session capacity reached")
                for update in data["updates"]:
                    self.metrics.apply(update)
                    if update["name"] == "oip_operation_calls_total":
                        self.last_failure[OPERATIONS[update["labels"]["operation"]]] = update["labels"]["result"] == "failure"
                self.sessions[data["session"]] = {"sequence": data["sequence"], "time": now,
                    "components": tuple(data["components"]), "stopped": data["stopped"]}
                self.ever_seen.update(data["components"])
                for component in data["components"]:
                    self.expired.discard(component)
                    self.metrics.child("oip_telemetry_last_received_timestamp_seconds", component=component).set(now)
                self.metrics.child("oip_telemetry_dropped_updates_total").inc(data["dropped"])
                self.metrics.child("oip_telemetry_batches_total", result="accepted").inc()
                self._heartbeat(now)
        except (ValueError, TypeError, KeyError):
            self.metrics.child("oip_telemetry_batches_total", result="rejected").inc()
            raise ValueError("Telemetry rejected") from None

    def _prune(self, now):
        for key, value in list(self.sessions.items()):
            if now - value["time"] > self.heartbeat_timeout and not value["stopped"]:
                self.expired.update(value["components"])
            if now - value["time"] > 600:
                del self.sessions[key]

    def _heartbeat(self, now):
        self._prune(now)
        for component in COMPONENTS:
            live = sum(component in v["components"] and not v["stopped"] and now - v["time"] <= self.heartbeat_timeout
                       for v in self.sessions.values())
            state = (2 if self.last_failure[component] else 1) if live else 0 if component in self.expired else -1
            self.metrics.child("oip_application_running_processes", component=component).set(live)
            self.metrics.child("oip_application_state", component=component).set(state)

    def heartbeat(self):
        with self.lock:
            self._heartbeat(time.time())


class Probes:
    def __init__(self, collector):
        self.collector, self.metrics = collector, collector.metrics
        password = os.environ["POSTGRES_PASSWORD"]
        self.engine = create_engine(URL.create("postgresql+psycopg2", username=os.getenv("POSTGRES_USER", "oip_user"),
            password=password, host=os.getenv("POSTGRES_HOST", "postgres"), port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "order_intelligence")), pool_pre_ping=True, pool_size=1, max_overflow=0,
            hide_parameters=True, connect_args={"connect_timeout": 1, "options": "-c statement_timeout=1000"})
        self.cache = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")), username=os.getenv("REDIS_USERNAME") or None,
            password=os.getenv("REDIS_PASSWORD") or None, socket_timeout=1, socket_connect_timeout=1,
            retry_on_timeout=False, decode_responses=True)
        self.namespace = os.getenv("REDIS_GOLD_NAMESPACE", "oip:m5:gold-serving")
        if not re.fullmatch(r"oip:m5:[a-z0-9][a-z0-9_-]{0,79}", self.namespace):
            raise ValueError("Invalid configured namespace")
        self.meta = {}
        self.reachable = {}

    def unknown(self, component):
        self.meta[component] = None
        for metric in ("oip_snapshot_version", "oip_snapshot_timestamp_seconds", "oip_snapshot_present", "oip_snapshot_metadata_valid"):
            self.metrics.child(metric, component=component).set(float("nan"))
        for table in ("revenue", "product"):
            self.metrics.child("oip_snapshot_rows", component=component, table=table).set(float("nan"))

    def metadata(self, component, meta):
        if meta is None:
            self.meta[component] = None
            for metric in ("oip_snapshot_present", "oip_snapshot_metadata_valid", "oip_snapshot_version", "oip_snapshot_timestamp_seconds"):
                self.metrics.child(metric, component=component).set(0)
            for table in ("revenue", "product"):
                self.metrics.child("oip_snapshot_rows", component=component, table=table).set(0)
            return
        self.metrics.child("oip_snapshot_present", component=component).set(1)
        try:
            if meta["serving_status"] != "SERVED" or type(meta["gold_version"]) is not int or meta["gold_version"] < 1:
                raise ValueError("Invalid serving metadata")
            if any(type(meta[k]) is not int or meta[k] < 0 for k in ("revenue_rows", "product_rows")):
                raise ValueError("Invalid serving counts")
            stamp = meta["served_at"]
            if isinstance(stamp, str):
                stamp = datetime.fromisoformat(stamp)
            if stamp.tzinfo is None or not re.fullmatch(r"[0-9a-f]{64}", meta["snapshot_digest"]):
                raise ValueError("Invalid metadata identity")
            if any(not isinstance(meta[k], str) or not meta[k] for k in
                   ("source_topic", "source_root", "manifest_digest", "snapshot_identity")):
                raise ValueError("Missing metadata identity")
            self.metrics.child("oip_snapshot_version", component=component).set(meta["gold_version"])
            self.metrics.child("oip_snapshot_timestamp_seconds", component=component).set(stamp.timestamp())
            for table, key in (("revenue", "revenue_rows"), ("product", "product_rows")):
                self.metrics.child("oip_snapshot_rows", component=component, table=table).set(meta[key])
            self.metrics.child("oip_snapshot_metadata_valid", component=component).set(1)
            self.meta[component] = meta
        except (ValueError, TypeError, KeyError, AttributeError):
            self.unknown(component)
            self.metrics.child("oip_snapshot_present", component=component).set(1)
            self.metrics.child("oip_snapshot_metadata_valid", component=component).set(0)
            raise ValueError("Metadata invalid") from None

    def postgres(self):
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(text("SET TRANSACTION READ ONLY"))
                conn.execute(text("SELECT 1"))
                self.reachable["postgres"] = True
                row = conn.execute(text("SELECT gold_version,revenue_rows,product_rows,served_at,serving_status,snapshot_digest,"
                                        "source_topic,source_root,manifest_digest,snapshot_identity "
                                        "FROM gold_serving.serving_state WHERE singleton_id=1")).mappings().first()
                self.metadata("postgres", dict(row) if row else None)

    def redis(self):
        self.cache.ping()
        self.reachable["redis"] = True
        # Atomic metadata/field-count observation, constant number of commands.
        with self.cache.pipeline(transaction=True) as pipe:
            pipe.hget(self.namespace + ":current", "@metadata")
            pipe.hlen(self.namespace + ":current")
            raw, size = pipe.execute()
        if raw is None:
            if size:
                raise ValueError("Missing cache metadata")
            self.metadata("redis", None)
            return
        if len(raw) > 16384:
            raise ValueError("Oversized cache metadata")
        meta = json.loads(raw)
        if size != 1 + meta["revenue_rows"] + meta["product_rows"]:
            raise ValueError("Incomplete cache metadata")
        self.metadata("redis", meta)

    @staticmethod
    def kafka():
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                             "socket.timeout.ms": 1500, "log_level": 0})
        description = admin.describe_cluster(request_timeout=2).result(timeout=3)
        if not description.nodes:
            raise ValueError("No broker metadata")

    @staticmethod
    def http(endpoint):
        with httpx.Client(timeout=1, trust_env=False, follow_redirects=False) as client:
            response = client.get(endpoint)
            if response.status_code != 200:
                raise ValueError("Health request failed")

    def collect(self):
        endpoints = {
            "schema_registry": os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081").rstrip("/") + "/config",
            "minio": os.getenv("MINIO_ENDPOINT", "http://minio:9000").rstrip("/") + "/minio/health/ready",
            "dashboard": os.getenv("OIP_DASHBOARD_HEALTH_URL", "http://oip-modernization-verify:8501/_stcore/health"),
        }
        healthy = True
        for component in DEPENDENCIES:
            start = time.monotonic()
            self.reachable[component] = False
            try:
                if component in endpoints:
                    self.http(endpoints[component])
                else:
                    getattr(self, component)()
                up = 1
                state = 1
                if component in {"postgres", "redis"} and self.meta.get(component) is None:
                    state, healthy = 2, False
            except Exception:
                self.collector.ready = False
                up = int(self.reachable[component])
                state, healthy = (2 if up else 0), False
                self.metrics.child("oip_dependency_failures_total", component=component).inc()
                if component in {"postgres", "redis"}:
                    self.unknown(component)
                    if up:
                        self.metrics.child("oip_snapshot_metadata_valid", component=component).set(0)
            self.metrics.child("oip_dependency_up", component=component).set(up)
            self.metrics.child("oip_dependency_state", component=component).set(state)
            self.metrics.child("oip_dependency_probe_duration_seconds", component=component).observe(time.monotonic() - start)
        pg, cache = self.meta.get("postgres"), self.meta.get("redis")
        match = int(all(pg[k] == cache[k] for k in ("gold_version", "snapshot_digest", "source_topic", "source_root",
                                                  "manifest_digest", "snapshot_identity"))) if pg and cache else float("nan")
        self.metrics.child("oip_cache_matches_postgres").set(match)
        self.metrics.child("oip_exporter_collection_timestamp_seconds").set(time.time())
        self.collector.ready, self.collector.collected = healthy, True

    def close(self):
        self.cache.close()
        self.engine.dispose()


class MetricsServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, collector):
        self.collector = collector
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def respond(self, code, body, kind="application/json"):
        try:
            self.send_response(code)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        collector = self.server.collector
        if self.path == "/metrics":
            collector.heartbeat()
            return self.respond(200, generate_latest(collector.metrics.registry), CONTENT_TYPE_LATEST)
        if self.path == "/healthz":
            return self.respond(200, b'{"status":"UP"}')
        if self.path == "/readyz":
            status = "UP" if collector.ready else "DEGRADED" if collector.collected else "UNKNOWN"
            return self.respond(200 if collector.ready else 503, json.dumps({"status": status}).encode())
        return self.respond(404, b'{}')

    def do_POST(self):
        if self.path != "/observe":
            return self.respond(404, b'{}')
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 65536:
                raise ValueError("Invalid length")
            self.connection.settimeout(1)
            self.server.collector.ingest(json.loads(self.rfile.read(length)))
            return self.respond(204, b"")
        except (ValueError, TypeError, OSError):
            return self.respond(400, b'{"error":"invalid telemetry"}')


def main():
    collector = Collector()
    probes = Probes(collector)
    interval = int(os.getenv("METRICS_COLLECTION_INTERVAL_SECONDS", "15"))
    if not 1 <= interval <= 300:
        raise ValueError("Collection interval must be 1–300 seconds")
    stopped = threading.Event()

    def poll():
        while not stopped.is_set():
            probes.collect()
            collector.heartbeat()
            stopped.wait(interval)

    worker = threading.Thread(target=poll, daemon=True)
    worker.start()
    server = MetricsServer((os.getenv("METRICS_BIND_ADDRESS", "0.0.0.0"), int(os.getenv("METRICS_EXPORTER_PORT", "8000"))), collector)
    try:
        server.serve_forever()
    finally:
        stopped.set()
        server.server_close()
        worker.join(timeout=15)
        probes.close()


if __name__ == "__main__":
    main()
