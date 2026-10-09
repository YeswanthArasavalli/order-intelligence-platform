# M9 operational observability contract

## Assessment and architecture

Inspected 2026-10-04. Existing prometheus-client 0.21.0, Prometheus v2.54.1,
Grafana 11.3.0 and datasource provisioning: KEEP. Existing exporter: UPDATE NOW
because it recomputes money from obsolete gold.*, reads legacy alerts/DQ, and
reports consumer offsets from the wrong group. Stub scrape jobs: UPDATE NOW;
their exporters do not exist. Empty dashboard provisioning: UPDATE NOW. No
unrelated dependency upgrade or business-contract change is planned.

Application lifecycle hooks → bounded best-effort asynchronous HTTP telemetry →
one Prometheus-compatible exporter → Prometheus → three Grafana dashboards.
The exporter also performs fixed periodic read-only dependency/metadata probes.
No business tables, lake history, Kafka history or notification history are scanned.
No revenue/DQ/alert/notification decisions are recomputed. Prometheus is optional
to every business operation.

## Telemetry reliability and bounds

Instrumentation is opt-in using OIP_TELEMETRY_URL. An unset value performs no I/O.
Hooks preserve function returns/exceptions and do not change business transactions.
The client has a bounded queue, batches updates on a daemon thread and uses bounded
network timeouts. Failed batches are not retried, preventing ambiguous double
increments. A best-effort shutdown flush is bounded. An exporter outage, queue
overflow or abrupt process death can lose operational observations; durable
business ledgers remain authoritative. Reported transport drops and last contact
make this limitation visible. These are observed-operation counters, not audited
business totals or backfilled historical counts.

Receiver sessions/sequence numbers are internal, capped and expiring; never metric
labels. Duplicate batches in a live session are ignored. Application heartbeats
are distinct from operation success. Completed one-shot jobs become UNKNOWN/idle,
not falsely DOWN. Expired live heartbeats indicate DOWN; latest operation failure
can indicate DEGRADED. No scheduler or distributed process supervisor is added.

Prometheus counters/histograms reset when the exporter restarts. PromQL rate/increase
handles reset boundaries. Current PostgreSQL/Redis metadata gauges repopulate on
the next poll; lifecycle-only Gold gauges remain unknown until observed again.
Operation labels identify distinct layers; never sum nested operation durations
or interpret an invocation counter as unique business events.

One exporter aggregates its opted-in processes without topic/database labels.
Gold publication and M7 configuration gauges describe the last observed invocation,
which can be an isolated verification fixture. PostgreSQL/Redis metadata probes
instead describe the configured default serving source. Equal version numbers
alone do not establish that these sources are identical.

## Safety and cardinality

Metrics and label values are allowlisted in catalog.py. No topic/partition,
currency, rule ID, destination, URL, SQL, exception, user/order/product/event ID,
UUID, hash or timestamp is a label. DQ labels are the existing 21 rule names,
checked against the frozen validator list in tests; their logic is not copied.
Only counts, durations, versions and timestamps are values; no monetary values.
Invalid telemetry is rejected atomically. Metrics endpoints expose no business
payload or credentials. Prometheus-generated instance/job labels refer only to
three fixed internal targets.

Scrapes do no dependency I/O. A background collector uses SELECT 1 plus one M4
singleton serving_state row; Redis PING/HGET metadata/HLEN; Kafka cluster metadata;
Schema Registry, MinIO and Streamlit health requests. Timeouts bound each probe.
Redis metadata validity is not a full snapshot-digest validation; validation
success/failure metrics come from the real M5/M6 read paths. Cached version/age
does not assert current PostgreSQL availability.

Dependency reachability, metadata validity, data presence, application activity,
exporter health and Prometheus target up are separate signals. Failed metadata
probes expose unknown values, not fake zero business counts. A valid empty source
has zero row gauges and snapshot_present=1 with a working exporter and dependency;
an absent committed snapshot instead has snapshot_present=0.

## Scope and metric sources

Producer: actual enqueue/error and delivery callbacks/latency. Bronze: existing
batch count and write completion; decoder/schema-resolution invocation failures
(Spark retries/recomputation may repeat invocations). DQ/Silver: existing committed
summary and its rule breakdown; actual DLQ writes are physical at-least-once writes.
Gold: existing commit result/version/table counts/time and replay/empty branches.
M4/M5: existing return statuses, exceptions and elapsed calls. Dashboard: read-service
loads/source selection, not an invented Streamlit HTTP request count. M7: returned
evaluation/outcome counts and supplied enabled-rule count. M8: committed generate,
claim and finish returns, provider outcomes/latency and actual controlled-receiver
acceptances. Logical requests and attempts/acknowledgements are separate metrics.

No fabricated Kafka consumer-group lag: Spark uses checkpoint offsets, not the
legacy exporter's invented consumer group. Broker connectivity, enqueue/delivery
and Bronze input activity are the verified Kafka-flow signals. Unavailable producer
internal retry counts and infrastructure process restart counts are not invented.

This is bounded local operational certification, not HA/distributed scale,
long-retention, tracing, centralized logging or browser-pixel certification.

## Configuration and operator interpretation

Compose exposes exporter, Prometheus and Grafana on loopback ports 8000, 9090 and
3000 by default. Applications explicitly set `OIP_TELEMETRY_URL` to an HTTP URL
ending in `/observe`: `http://metrics-exporter:8000/observe` on the Compose network,
or `http://127.0.0.1:8000/observe` for host processes. Set it in each process to be
observed; it is not enabled globally by starting the exporter.

The exporter polls `gold_serving.serving_state` in the configured `POSTGRES_DB`
and the `REDIS_GOLD_NAMESPACE` active hash. `METRICS_COLLECTION_INTERVAL_SECONDS`
defaults to 15 (allowed 1–300); this is a delay after each bounded probe cycle.
`OIP_DASHBOARD_HEALTH_URL` must identify the actual running Streamlit service.
Its default is the retained local verification container, not a provisioned
production application. For host-run Streamlit on Docker Desktop, use
`http://host.docker.internal:8501/_stcore/health` with Streamlit listening on a
container-reachable interface.

`/healthz` is exporter liveness; `/readyz` is dependency/metadata readiness and
returns 503 while UNKNOWN or DEGRADED. It does not gate the business applications.
`/metrics` remains available during dependency failures. Prometheus target `up`
means the exporter can be scraped, not that every application is running.
The Platform Overview exposes both dependency states and application contact.
Zero counters with no telemetry contact are not proof that no business work exists.
Cleanly completed one-shot jobs are UNKNOWN/IDLE; lost heartbeat is LOST CONTACT,
not proof that a remote process has terminated.

The 45 allowlisted OIP families initialize 972 exposition samples, including
histogram buckets and client-library creation timestamps. Histogram boundaries
are 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60, 300 seconds plus infinity.
The client queue caps at 2048 updates, each batch at 256 updates; the receiver caps
bodies at 65536 bytes and live protocol sessions at 256, expiring them after 600s.
Heartbeats run every 3s and expire after 15s. Prometheus scrapes every 5s with a
3s timeout and a 3000-sample limit for the OIP target. The static Prometheus YAML
is authoritative; the legacy scrape-interval environment variable is not consumed.

Prometheus/Grafana self-scrapes retain only the fixed process start/CPU/RSS and
metrics-handler families listed in the configuration, plus Prometheus-generated
scrape health. No business alert conditions or provider-delivery decisions are
encoded as Prometheus alerting rules. See `milestone9_observability_report.md`
for acceptance evidence and retained failure investigations.
