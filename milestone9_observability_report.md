# Milestone 9 — Observability

## 1. Final Status

**PASS** — verified against real local services on 2026-10-04.

M9 adds bounded operational instrumentation, a working metrics exporter,
Prometheus collection and three provisioned Grafana dashboards. The complete
suite passed **202 tests** (179 frozen tests plus 23 M9 tests); the final focused
M9 run passed **23 tests**. All M1–M8 runtime regressions passed. The final audit
found **63 baseline-covered files byte-identical**, **12 minimally instrumented
files with unchanged normalized business AST**, and unchanged retained M4/M7/M8
business data hashes. Certification is for the local operational scope below.

Primary evidence directory: `milestone9_evidence/20261004a/`. Artifact stems in
this report refer to paired `.json` command/result records and `.log` output in
that directory unless another path is given. Failed attempts are retained.

## 2. Scope

Observe existing producer, Bronze, DQ/Silver/DLQ, Gold, PostgreSQL serving, Redis
cache, dashboard read service, M7 alert evaluation and M8 delivery lifecycles.
Expose reachability, metadata state, activity, latency, failures and process
contact without recalculating business results. PostgreSQL remains authoritative
for serving, alerts and notification state; Redis remains a disposable cache.

The added code does not change event contracts, DQ rules, replay identity,
currency/Decimal arithmetic, Gold manifests, SQL publication transactions, cache
publication, M7 conditions/cooldowns or M8 identity/lease/retry/provider policy.
M8 still has exactly one controlled HTTP provider. External provider delivery
is not certified in this environment.

There is no M10 implementation, business analytics extension, tracing/logging
stack, provider migration, distributed scheduler, cloud/Kubernetes, CI/CD or
load/retention project in this milestone.

## 3. Current-World Assessment

Inspected the exporter, scrape/provisioning configuration, Compose, dependencies,
environment template, entrypoints, frozen lifecycle code, contracts, tests,
verification scripts, reports and roadmap before instrumentation.

| Existing item | Decision | Reason |
|---|---|---|
| prometheus-client 0.21.0, Prometheus v2.54.1, Grafana 11.3.0 | KEEP | Existing project pins support the required local interfaces. |
| Grafana datasource `prometheus-oip` | KEEP | Existing provisioned datasource can query the verified Prometheus service. |
| Legacy exporter | UPDATE NOW | It recomputed monetary values from obsolete `gold.*`, read legacy DQ/alert tables and assumed the wrong Kafka consumer group. |
| Placeholder scrape jobs | UPDATE NOW | Referenced exporters/endpoints were not implemented. |
| Dashboard provisioning | UPDATE NOW | Provisioning existed without the required operational dashboard JSON. |
| Frozen business implementations and contracts | KEEP with minimal observation hooks | Real lifecycle outcomes already provide the required measurements. |
| Unrelated dependencies and legacy consumers | KEEP | No M9 requirement warrants a migration. |

Baseline `baseline-1791128639822519664` records 75 source/contract/SQL/test/verifier
file fingerprints and retained business state. The default PostgreSQL snapshot
is Gold version 3, topic `order-events-m3-20261003b`, 18 revenue rows and 13 product
rows, digest `26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231`.
Default M7 has two firing events and four evaluations; default M8 has two
delivered requests and two attempts. These retained defaults are unchanged.

## 4. Architecture

```text
Existing lifecycle result/callback/exception
    → opt-in bounded asynchronous telemetry client
    → metrics-exporter /observe → allowlisted in-memory metric registry

Fixed read-only dependency/metadata probes → same registry
    → /metrics → Prometheus → Grafana (three provisioned dashboards)
```

`observability/telemetry.py` uses the Python standard library. An unset
`OIP_TELEMETRY_URL` performs no telemetry I/O. Enabled hooks preserve original
returns and exceptions; network delivery runs on a daemon thread. Queue capacity
is 2048 updates, batch size at most 256, HTTP timeout 0.25s, with a bounded
best-effort shutdown flush. Ambiguously accepted batches are not retried.

The exporter validates complete batches before updating metrics. Internal
session/sequence identifiers suppress live-session duplicate batches without
becoming labels. At most 256 sessions are retained, expiring after 600s. Request
bodies are capped at 65536 bytes. Heartbeats run every 3s; fresh contact expires
after 15s. Operational observations can be lost; they are not a durable ledger.

Scrapes read cached metrics only. The background probe loop waits 15s after each
cycle by default, configurable from 1–300s. It performs:

- PostgreSQL: read-only transaction, `SELECT 1` and one indexed singleton
  `gold_serving.serving_state` metadata row; connect timeout 1s and statement
  timeout 1000ms. No analytical/history scans.
- Redis: PING and a transactional metadata HGET plus HLEN on one active hash;
  metadata limited to 16384 bytes. No key scan or full snapshot fetch.
- Kafka: bounded `AdminClient.describe_cluster`, not a consumer/history read.
- Schema Registry: GET `/config` (one global configuration), avoiding a subject
  enumeration as the registry grows.
- MinIO: `/minio/health/ready`; Streamlit: `/_stcore/health`, with bounded HTTP
  timeouts. The Streamlit URL must name the actual running application.

Only existing counts, manifests, return statuses and callback timings are
observed. No additional Spark actions or Gold/revenue computation were added.
PostgreSQL/Redis probes read metadata, not business rows. Full-data hashing in the
baseline/audit is a one-off verification activity, never scrape behavior.

## 5. Metrics Inventory

There are **45 OIP metric families** and **972 initialized exposition samples**,
including histogram buckets, sums/counts and client-library creation timestamps.
The exact allowlisted label domains and HELP text are in
`observability/catalog.py` and the audit artifact. `—` means no application label;
Prometheus adds fixed `job`/`instance` target labels.

| Metric | Type | Labels | Source | Meaning |
|---|---|---|---|---|
| `oip_operation_started_total` | Counter | operation | Lifecycle entry | Observed operation starts, not unique business events. |
| `oip_operation_calls_total` | Counter | operation, result | Return/exception boundary | Completed invocations: success, failure, noop or empty. |
| `oip_operation_duration_seconds` | Histogram | operation | Monotonic call clock / producer delivery callback | Elapsed operation latency; nested layers must not be summed. |
| `oip_operation_last_success_timestamp_seconds` | Gauge | operation | Completion clock | Last success/noop/empty completion. |
| `oip_operation_last_failure_timestamp_seconds` | Gauge | operation | Exception clock | Last raised operation failure. |
| `oip_operation_failures_total` | Counter | operation, category | Exception class category | Raised failures categorized as dependency, validation or other; no exception text. |
| `oip_producer_events_total` | Counter | result | Actual enqueue and callback | Enqueued, delivered, enqueue-error and delivery-error observations. |
| `oip_producer_enqueued_events_total` | Counter | event_type | Successful enqueue | Six fixed lifecycle types or unknown; not delivery acknowledgements. |
| `oip_bronze_records_total` | Counter | result | Existing batch count | Received and successfully written batch records. |
| `oip_decode_failures_total` | Counter | stage | Existing decoder/schema exception path | Record/schema invocation failures; Spark recomputation may repeat them. |
| `oip_pipeline_batches_total` | Counter | stage, result | Existing sink branches | Bronze/Silver/Gold written, noop, empty or recovered branch completions. |
| `oip_dq_records_total` | Counter | result | M2 completed summary | Existing evaluated/passed/failed counts with frozen replay semantics. |
| `oip_dq_rule_failures_total` | Counter | rule | Existing `failure_breakdown_json` | Occurrences of the 21 frozen DQ rules; one row can fail several rules. |
| `oip_silver_records_total` | Counter | route | M2 completed summary | Accepted, rejected, replay and accepted-plus-rejected unique counts. |
| `oip_dlq_records_written_total` | Counter | — | Successful existing Kafka DLQ write | Physical write count; DLQ delivery remains at-least-once. |
| `oip_gold_events_total` | Counter | result | Existing manifest/result/replay branch | Input, new and replay event observations. |
| `oip_snapshot_version` | Gauge | component | Gold manifest / SQL singleton / cache metadata | Last observed Gold publication or current polled serving version. |
| `oip_snapshot_rows` | Gauge | component, table | Existing snapshot metadata | Revenue/product row counts, without recomputing business values. |
| `oip_snapshot_timestamp_seconds` | Gauge | component | Commit or `served_at` metadata | Gold publication time or SQL source serving time; Redis copies SQL time. |
| `oip_snapshot_present` | Gauge | component | SQL/Redis metadata probe | 1 committed snapshot exists, 0 none, NaN unavailable. |
| `oip_snapshot_metadata_valid` | Gauge | component | Basic metadata validation | 1 valid, 0 absent/invalid, NaN unavailable; not full digest validation. |
| `oip_cache_matches_postgres` | Gauge | — | Metadata comparison | Identity/version metadata match, mismatch or NaN when uncheckable. |
| `oip_dashboard_loads_total` | Counter | source, redis_status | M6 read-service return/exception | Redis/PostgreSQL/failure loads and valid/missing/invalid/unavailable cache status. |
| `oip_alert_evaluations_total` | Counter | result | M7 returned counts | FIRED, SUPPRESSED, NOOP, OK, RECOVERED, NO_DATA, NO_BASELINE, DISABLED. |
| `oip_alert_rules` | Gauge | state | Supplied M7 configuration | Configured/enabled rules in the last observed invocation. |
| `oip_alert_source_version` | Gauge | — | Returned M7 provenance | Last observed evaluation's Gold version. |
| `oip_notification_requests_total` | Counter | result | Committed M8 generation result | Created logical requests versus existing-request observations. |
| `oip_notification_claims_total` | Counter | result | Committed M8 claim result | Claimed, empty and expired-claim recovery transactions. |
| `oip_notification_attempt_results_total` | Counter | result | Committed M8 finish/recovery | DELIVERED, RETRYABLE_FAILURE, PERMANENT_FAILURE and expired-claim UNKNOWN. |
| `oip_notification_delivery_results_total` | Counter | result | M8 result/recovery | Parent delivery transitions or fenced STALE_RESULT; not unique requests. |
| `oip_notification_retries_total` | Counter | — | Persisted claimed attempt number | Claims whose attempt number exceeds one. |
| `oip_notification_exhausted_total` | Counter | — | M8 finish/recovery result | Observed transitions to retry exhaustion. |
| `oip_provider_calls_total` | Counter | provider, channel, result | Actual M8 send result | Controlled HTTP call outcomes; delivered acknowledgements can include idempotent repeats. |
| `oip_provider_acceptances_total` | Counter | provider, channel | Controlled receiver after receipt commit | New durable receiver acceptances, not repeated acknowledgements. |
| `oip_dependency_up` | Gauge | component | Periodic protocol probe | 1 reachable, 0 down, -1 not yet probed. |
| `oip_dependency_state` | Gauge | component | Probe plus metadata state | -1 unknown, 0 down, 1 up, 2 reachable with absent/invalid serving metadata. |
| `oip_dependency_probe_duration_seconds` | Histogram | component | Probe monotonic clock | Dependency probe latency, separate from business latency. |
| `oip_dependency_failures_total` | Counter | component | Probe outcome | Failed dependency probe attempts. |
| `oip_application_running_processes` | Gauge | component | Bounded heartbeat sessions | Fresh observed processes; no process IDs exposed. |
| `oip_application_state` | Gauge | component | Heartbeat and operation result | -1 unknown/clean idle, 0 lost heartbeat, 1 live, 2 live with last operation failure. |
| `oip_telemetry_last_received_timestamp_seconds` | Gauge | component | Collector receive clock | Last received contact for each fixed application role. |
| `oip_telemetry_batches_total` | Counter | result | Ingestion protocol | Accepted, rejected and duplicate batches. |
| `oip_telemetry_dropped_updates_total` | Counter | — | Next successful client report | Reported dropped updates; abrupt-process losses can be unreported. |
| `oip_exporter_collection_timestamp_seconds` | Gauge | — | Collector clock | Last completed dependency collection cycle. |
| `oip_exporter_start_timestamp_seconds` | Gauge | — | Exporter startup | Exporter epoch, for uptime and counter-reset interpretation. |

Histogram boundaries are 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60 and 300
seconds, plus infinity. Standard `_bucket`/`_sum`/`_count` derivatives and `_created`
gauges are library-generated; only fixed `le` bucket values are added.

Infrastructure self-scrapes additionally retain the following standard families
where exposed; they are separate from the 45 OIP definitions:

| Metric | Type | Labels | Source | Meaning |
|---|---|---|---|---|
| `up` | Gauge | job, instance | Prometheus scrape engine | Target scrape success, not application/business readiness. |
| `scrape_duration_seconds` | Gauge | job, instance | Prometheus scrape engine | Most recent scrape duration. |
| `scrape_samples_scraped` | Gauge | job, instance | Prometheus scrape engine | Samples received before relabeling. |
| `scrape_samples_post_metric_relabeling` | Gauge | job, instance | Prometheus scrape engine | Retained samples after relabeling. |
| `scrape_series_added` | Gauge | job, instance | Prometheus scrape engine | Newly observed scrape series. |
| `process_start_time_seconds` | Gauge | job, instance | Prometheus/Grafana process collectors | Process start epoch. |
| `process_cpu_seconds_total` | Counter | job, instance | Prometheus/Grafana process collectors | Process CPU time. |
| `process_resident_memory_bytes` | Gauge | job, instance | Prometheus/Grafana process collectors | Process resident memory. |
| `promhttp_metric_handler_requests_total` | Counter | code, job, instance | Native metrics HTTP handler | Metrics-handler response codes. |

## 6. Cardinality Controls

Application labels are enums, not business identifiers. There are 19 fixed
operation slots, ten application roles, six probed dependencies, 21 DQ rule names,
two table roles and one provider/channel pair. Result domains are finite per
family. No topic, partition, currency, rule ID, order/customer/product/event ID,
destination, URL, SQL, exception text, UUID, hash or timestamp is a metric label.
Values are counts, durations, versions and timestamps, never money or payloads.

Client updates must have exactly the registered metric and label keys, allowlisted
label values and finite nonnegative numeric values no larger than 1e15. Invalid
batches are rejected atomically. Probe-only metrics cannot be set by clients.
Live-session bounds are independent of exported time-series cardinality.

All series are initialized from the catalog, and the live exposition audit
checked every sample/name/label against it: 972 samples below the configured 3000
sample limit. Prometheus/Grafana native scrapes are filtered to the four process/
handler families above rather than retaining arbitrary upstream query labels.

One exporter aggregates all its instrumented processes. In particular, the last
observed Gold/M7 gauges can reflect controlled verification fixtures; the
PostgreSQL/Redis probes describe configured default serving metadata. Version
equality alone is not a cross-source identity check. Historical business totals
are not backfilled into counters.

## 7. Prometheus

Version **v2.54.1**, existing data volume and 15-day retention setting retained.
Host listener is loopback `127.0.0.1:9090` by default. The static configuration is
authoritative: scrape interval 5s, timeout 3s, evaluation interval 15s.
`PROMETHEUS_SCRAPE_INTERVAL` is a documented legacy variable, not YAML templating.

| Job | Actual endpoint | Verification |
|---|---|---|
| oip | `http://metrics-exporter:8000/metrics` | UP; 972 allowlisted samples; sample limit 3000. |
| prometheus | `http://localhost:9090/metrics` inside Prometheus | UP; native allowlist applied. |
| grafana | `http://grafana:3000/metrics` | UP; native allowlist applied. |

`promtool check config /etc/prometheus/prometheus.yml` passed. The real readiness,
targets and query APIs passed, including dashboard PromQL expressions. Final
live stack evidence: `stack-1791136251580428228`; final configuration check:
`promtool-1791136270299113326`. Earlier actual activity values remain in the
pre-restart activity capture; resets do not rewrite those observations.

There are no fabricated exporter targets or Kafka consumer-group lag metrics.
Spark checkpoint offsets are not treated as offsets of the legacy consumer group.
No business alert or provider-delivery decision is encoded in Prometheus rules.

## 8. Grafana

Version **11.3.0**, loopback port 3000, existing environment-provided admin
credentials and datasource UID **`prometheus-oip`** retained. Dashboard JSON is
mounted read-only at `/var/lib/grafana/dashboards` with file provisioning.

| Dashboard / UID | Panels | Operator questions |
|---|---:|---|
| Order Intelligence — Platform Overview / `oip-platform` | 12 | Scrape/dependency health, failures and latency, snapshot presence/age, telemetry loss, Python process contact and last contact. |
| Order Intelligence — Data Pipeline / `oip-pipeline` | 8 | Producer delivery, Bronze/DQ/Silver/DLQ/Gold flow, rule failures, replay/empty branches and stage performance. |
| Order Intelligence — Serving & Delivery / `oip-serving-delivery` | 9 | SQL/cache state and latency, dashboard source/fallback, M7 outcomes, M8 requests/attempts/retries and provider acknowledgements/acceptances. |

The real `/api/health`, datasource health, datasource-proxy PromQL and dashboard
APIs succeeded. All three dashboards report `provisioned=true`; every panel's
PromQL executed successfully. The verifier compares expected panel counts/titles
and explicit status mappings with the provisioned API objects. Dependency values
map to UP/DOWN/UNKNOWN/DEGRADED; application contact maps to LIVE/LOST CONTACT/
UNKNOWN-IDLE/LAST OP FAILED. Latest dashboard/source-label verification is in
`stack-1791136251580428228`; `stack-1791135430990748355` also records the final
12/8/9 panel layout and real values before the last exporter rebuild.

This verifies provisioning, data access and query execution. Browser layout,
pixels and interactive UX were not separately validated.

## 9. Health / Readiness

- `/healthz`: exporter HTTP liveness, 200 while serving.
- `/readyz`: 503 UNKNOWN before collection, 503 DEGRADED on dependency/metadata
  problems, 200 UP after a healthy cycle. This is not a business execution gate.
- `/metrics`: remains available with cached operational state during dependency
  failure; scrapes cause no dependency I/O.
- `up{job="oip"}`: exporter scrape reachability only.
- Dependency reachability is separate from serving metadata validity; a reachable
  store with absent/invalid metadata is DEGRADED, not an unavailable server.
- A valid empty committed snapshot has row count zero and `snapshot_present=1`.
  No committed snapshot has `snapshot_present=0`. Failed metadata probes expose
  unknown values rather than fake zero business totals.
- Fresh heartbeats show live processes; cleanly completed one-shot jobs become
  UNKNOWN/IDLE. LOST CONTACT indicates expired contact, not proof of process death.
- SQL/cache timestamp age is publication age. Cache metadata matching is not full
  M5/M6 digest validation and does not claim that PostgreSQL is currently reachable.

The default dashboard probe names the retained verification container. Operators
must point `OIP_DASHBOARD_HEALTH_URL` at their actual Streamlit deployment; README
documents a Docker Desktop host-process example.

## 10. Failure Testing

| Scenario | Actual result | Evidence stem |
|---|---|---|
| PostgreSQL stopped | Dependency down, readiness 503; M6 reads valid cache; M8 fails explicitly. | `failure-activity-postgres-down-1791132296061410135`, `failure-postgres-down-1791132311194948313` |
| PostgreSQL restored | Metadata/readiness and all scrape targets recover. | `failure-stack-1791132326847805477` |
| Redis stopped | Dependency down; M6 falls back to PostgreSQL and reports that source. | `failure-activity-redis-down-1791132335810633578`, `failure-redis-down-1791132346823797410` |
| Redis restored | Probe/metadata/scrape recovery. | `failure-stack-1791132361853480154` |
| Exporter stopped | Prometheus sees `up=0`; instrumented M6/M7 calls continue despite telemetry loss. | `failure-target-down-1791132379314661378`, `failure-activity-independence-1791132387485154042` |
| Exporter restarted | Counters reset as documented; SQL metadata version 3 repopulates; target UP. | `failure-restart-1791132394783780766` |
| Prometheus stopped | M6/M7 calls continue and the real M3 second phase commits successfully. | `failure-activity-independence-1791132396699519995`, `pipeline-with-oip-prometheus-down-1791132461788514648` |
| Grafana stopped | M6/M7 calls continue and the real M3 third phase commits successfully. | `failure-activity-independence-1791132476718505272`, `pipeline-with-oip-grafana-down-1791132526078594706` |
| Prometheus/Grafana restored | Targets, datasource and provisioned dashboard query checks pass. | `failure-stack-1791132474012335678`, `failure-stack-1791132533567962366` |
| Application process restart | Two separate M7 CLI processes each return four NOOPs; metric rises 6→14, business result unchanged. | `application-restart-1791132151946827345` |
| No data | Isolated real SQL/cache source distinguishes absent commit from committed empty; metrics HTTP 200 and ready UP for valid empty. | `no-data-1791131728452984879` |

Failure orchestration restores stopped dependencies in `finally` and waits for
health before subsequent checks. Frozen M6 regression additionally tested both
stores down; frozen M7/M8 regressions independently exercised their outages.

The first SQL-outage check exposed a new M9 readiness race:
`failure-postgres-down-1791132162574690492`. The collector temporarily published
dependency-down before the probe completed while readiness still reflected the
previous cycle. The fix removed that premature zero and clears readiness on an
actual exception. A regression test was added without weakening the assertion;
the image was rebuilt and the entire outage sequence passed. Both failed and
corrected artifacts are retained. The stack verifier also waits for fresh
post-recovery probe samples rather than accepting an older scrape.

## 11. Runtime Evidence

Real Docker services were used: Kafka, Schema Registry, MinIO, PostgreSQL 16.14,
Redis, the retained Spark verification container, the new exporter, Prometheus
and Grafana. Streamlit ran inside the verification container. Workloads were
controlled fixtures, not production customer traffic.

| Activity | Evidence stem |
|---|---|
| Existing-version exporter build / initial stack startup | `build-1791129680292772637`, `stack-start-1791129783815854055` |
| M4/M5 serving and M6 Redis read through frozen APIs | `serving-1791131665758748487` |
| Isolated M7 fire then repeat NOOP | `alerts-1791131667343397738` |
| Controlled M7 cooldown suppression/recovery | `controlled-m7-1791131680278531577` |
| Isolated M8 retry, successful delivery, duplicate generation and expired-claim recovery | `notifications-1791131673004514407` |
| Real producer/Avro/Bronze, M2 and M3 activity | `pipeline-m1-1791131771379409569`, `pipeline-m2-1791131811887358855`, `pipeline-m3-1791131885279148983` |
| PromQL confirms lifecycle metric changes | `activity-1791131920976007508` |
| Final bounded Schema Registry probe rebuild and real stack check | `rebuild-bounded-schema-probe-1791136237467224050`, `restart-bounded-schema-probe-1791136249407224178`, `stack-1791136251580428228` |
| Final source/file integrity audit | `audit-1791136272664836882` |

Selected actual PromQL values at 16:38:40 UTC, before intentional exporter resets:

| Observation | Value |
|---|---:|
| Producer delivered / Bronze written | 60 / 60 |
| DQ evaluated / Silver accepted / physical DLQ written | 58 / 35 / 21 |
| Gold new events | 24 |
| Successful PostgreSQL commit / Redis refresh invocations | 7 / 2 |
| Dashboard valid Redis load | 1 |
| M7 FIRED / SUPPRESSED observations | 4 / 1 |
| M8 logical requests created | 2 |
| M8 attempt DELIVERED / RETRYABLE_FAILURE / UNKNOWN | 2 / 1 / 1 |
| New controlled provider acceptances | 2 |
| Provider-send duration observations | 3 |

These aggregate several fixtures and operation layers; they are not a single
unique-event conservation equation. Metric snapshots after exporter restart
correctly contain reset counters, not reconstructed historical totals.

The initial M1 activity command used a topic prefix rejected by the unchanged
frozen verifier (`pipeline-m1-1791131669752730851`). The M9 harness was corrected
to `order-events-m1-m9observe-20261004a`; no frozen assertion was changed.

## 12. Regression Results

Every row below is **PASS** based on real unchanged frozen verifier execution.
The 21 M9 `regression-*.json` wrappers all returned zero. Nested runtime artifacts
record the underlying assertions, states and expected failure outcomes.

| Milestone | Result | Coverage and evidence |
|---|---|---|
| M1 | PASS | Real producer/Avro/Schema Registry/Bronze; `regression-m1-1791132856650548176`. |
| M2 | PASS | Initial/restart/recovery with DQ, routes and replay; `regression-m2-initial-1791132918662548319`, `regression-m2-restart-1791132994859913097`, `regression-m2-recovery-1791133049080576459`. |
| M3 | PASS | Initial/second/third/replay/recovery; `regression-initial-1791133098435515522`, `regression-second-1791133135555286477`, `regression-third-1791133176218013858`, `regression-replay-1791133219829036390`, `regression-recovery-1791133258509305142`. |
| M4 | PASS | Initial/retry/transaction failure/old/corrupt source; `regression-m4-initial-1791133679264268825`, `regression-m4-retry-1791133695328841257`, `regression-m4-failure-1791133711776731066`, `regression-m4-old-1791133723856300364`, `regression-m4-corrupt-1791133738787376032`. |
| M5 | PASS | Serve/repeat/rebuild/real dependency outages; `regression-m5-serve-1791133739801251294`, `regression-m5-serve-1791133740682644388`, `regression-m5-rebuild-1791133741615143586`, `regression-m5-outages-1791133775141474141`. |
| M6 | PASS | Default source plus healthy/missing/corrupt/cache rebuild/Redis down/SQL down/both down/recovery; `regression-m6-production-1791133779410022555`, `regression-m6-runtime-1791133851818108802`. |
| M7 | PASS | Production-shaped evaluation, restart NOOP, controlled cooldown, Redis independence, explicit SQL failure and recovery; `regression-m7-1791133888230791164`. |
| M8 | PASS | Frozen inspect/runtime/outages/recovery/integrity in `milestone8_evidence/m920261004a/`; runtime `runtime-1791133909625979008`, final integrity `integrity-1791133944630597459`. |

M1/M2/M3 use fresh topics `order-events-m1-m9-20261004a`,
`order-events-m2-m9-20261004a` and `order-events-m3-m9-20261004a`, with detailed
evidence under `milestone3_evidence/<topic>/`. M4 uses that fresh M3 topic and
schema `gold_m4_m9reg20261004a`. M5 uses namespace
`oip:m5:verify-m9reg-20261004a`; M6/M7 runtime evidence uses run `m920261004a`.
Fresh fixtures avoid repeatedly multiplying retained physical Silver history;
they use the same frozen oracles and preserve the default serving source.

M8 runs against fresh database `m9_m8_20261004a` with an immutable copy of the
retained M7 source schema, and telemetry enabled. The frozen verifier is unchanged.
Its runtime includes actual process exit 73 after receiver acceptance, recovery,
concurrent workers, retries, permanent outcomes and idempotent receipts. Final
isolated histories contain 11 logical requests, 17 attempts, 9 delivered and 2
permanently failed requests; the receiver records 9 receipts and 16 HTTP requests.
Both dependency-outage captures and both recovery captures returned zero. The
retained default M8 database/receipt history was not repurposed for these scenarios.

## 13. Test Results

| Check | Result | Evidence stem |
|---|---|---|
| Complete Spark-aware unit suite | 202 passed, 1 warning, 150.13s | `full-tests-1791133594764052231` |
| Final focused M9 tests | 23 passed, 7.40s | `tests-1791136263029084613` |
| Final live metrics/Grafana API verification | PASS | `stack-1791136251580428228` |
| Ruff on M9 code | PASS | `ruff-1791136263914385165` |
| Python compilation of M9 and instrumented packages | PASS | `compile-1791136264168463021` |
| Host dependency check | PASS | `host-pip-1791136267518974937` |
| New exporter dependency check | PASS | `exporter-pip-1791136269622273808` |
| Retained verification-container dependency check | Existing failure: pygobject requires missing pycairo | `verification-pip-1791136268755770028` |
| Compose configuration | PASS | `compose-final-1791136269881830179` |
| Prometheus configuration | PASS | `promtool-1791136270299113326` |
| Final retained business data / file scope audit | PASS | `audit-1791136272664836882` |

M9 tests cover registration, types/HELP, finite domains, atomic rejection,
counter/histogram updates, duplicate batch handling, session bounds, real HTTP
exposition, heartbeat/idle/lost/failure states, readiness, empty/invalid/unavailable
metadata, counter resets, disabled/failing telemetry, preserved returns/exceptions,
existing-result mapping, dashboard configuration and normalized frozen AST.

A targeted scan of the captured M9 JSON/logs found no credential-bearing database
URLs, authorization headers or password/API-key/access-token assignments. Runtime
metric inspection independently verified the allowlisted labels.

Initial focused runs exposed a missing prometheus-client installation in the
retained verification container, then missing PySpark imports under plain Python.
The existing pin was installed and the M9 runner switched to the existing
Spark-aware test runner. Artifacts `tests-1791129620157244397`,
`install-existing-prometheus-pin-1791129703336588484` and
`tests-1791129714783204994` retain the failures/correction. Assertions were kept.

The first full suite, `full-tests-1791133025652631826`, had **1 failed / 201 passed**
while Spark regressions also ran. Frozen M8
`test_max_attempts_and_bounded_exponential_backoff` expected `attempts_exhausted`
but got `attempts_exhausted_outcome_unknown`. Its controlled HTTP timeout is 0.1s;
telemetry was disabled for this test invocation. The receiver ledger records
three 503 requests, but the exact client transport exception was not preserved
after fixture teardown. Contention is consistent with the evidence, not a proven
low-level diagnosis.

Remaining regressions were paused; the unchanged test passed sequentially, then
the complete suite passed sequentially. Evidence:
`m8-unchanged-retry-test-sequential-1791133374727632739`,
`m8-receiver-ledger-diagnosis-1791133440491667115` and the successful full suite
above. An initial diagnostic glob matched nothing and is also retained as
`m8-failure-diagnosis-1791133367547995261`. M8 code, assertions and timeouts were
not adjusted to mask the failure. Subsequent frozen M8 runtime regression with
telemetry enabled passed. Final dashboard/probe-configuration refinements were
additionally checked by the final 23-test run and live API verification.

## 14. Dependency Review

Root `requirements.txt` was not changed. The new exporter image has its own
small requirements file using existing project pins:

| Dependency | Verified version | Decision |
|---|---|---|
| prometheus-client | 0.21.0 | KEEP; install existing pin in retained verification container. |
| SQLAlchemy | 2.0.36 | KEEP. |
| psycopg2-binary | 2.9.10 | KEEP. |
| redis | 5.2.0 | KEEP. |
| httpx | 0.27.2 | KEEP. |
| confluent-kafka | 2.15.1 | KEEP; AdminClient connectivity probe. |
| Prometheus | v2.54.1 | KEEP existing image version. |
| Grafana | 11.3.0 | KEEP existing image version. |
| Exporter Python | 3.11.17 | Actual built runtime from `python:3.11-slim-bookworm`. |

Version evidence: `runtime-versions-1791134712119254610`,
`prometheus-version-1791134712413702972`, `grafana-version-1791134713655853227`.
The Python base tag is not digest-pinned; the measured version describes this
build, not a promise about future rebuilds. The existing verification-container
`pygobject 3.42.1 requires pycairo` issue remains outside the M9 image; host and
new exporter dependency checks pass. No unrelated upgrade was introduced.

## 15. Files Changed

**Added:**

- `observability/CONTRACT.md`, `observability/catalog.py`, `observability/telemetry.py`.
- `observability/Dockerfile`, `observability/requirements.txt`.
- `observability/grafana/dashboards/platform.json`.
- `observability/grafana/dashboards/pipeline.json`.
- `observability/grafana/dashboards/serving-delivery.json`.
- `scripts/milestone9_baseline.py`, `scripts/milestone9_activity.py`.
- `scripts/capture_milestone9_check.py`, `scripts/verify_milestone9.py`.
- `tests/unit/test_observability.py`.
- `milestone9_observability_report.md` and append-only M9/regression evidence.

**Updated observability/configuration/documentation:**

- `observability/metrics_exporter.py`: replace obsolete business-query exporter
  with cached operational metrics, bounded probes and health/readiness.
- `observability/prometheus/prometheus.yml`: real targets, fixed intervals,
  cardinality/retained-family limits.
- `observability/grafana/provisioning/dashboards/dashboards.yml`: provision the
  separate read-only dashboard directory. Existing datasource remains in place.
- `docker-compose.yml`: additive exporter image/service, dashboard mount and
  loopback metrics/Prometheus/Grafana host listeners.
- `.env.example`: opt-in telemetry, exporter/probe settings and accurate static
  scrape-interval documentation; no active `.env` change.
- `README.md`: M9 operation, URLs, configuration and troubleshooting.
- `PHASE_STATUS.md`: additive M9 acceptance status; historical milestone claims
  retained.

The 12 instrumented files are itemized in section 16. No frozen verifier/test,
business SQL, root requirements or business entrypoint was rewritten for M9.

## 16. Frozen Files

The baseline covers 75 files under producer, Kafka, streaming, DQ, storage,
dashboard, alerts, notifications, scripts and tests. Final audit
`audit-1791136272664836882` confirms 63 byte-identical files and these 12 justified
instrumentation-only changes:

| Frozen file modified | Minimal change / justification |
|---|---|
| `producer/kafka_publisher.py` | Observe enqueue/callback outcomes and publish latency at actual lifecycle boundaries. |
| `streaming/bronze_writer.py` | Observe existing batch counts, completed writes and decode/schema exception paths. |
| `streaming/silver_transformer.py` | Observe existing committed DQ summary, route/replay/recovery branches and successful DLQ writes. |
| `streaming/gold_aggregator.py` | Observe batch duration and existing empty/noop/replay branches. |
| `storage/gold_store.py` | Observe commit result/metadata after the existing publication function returns. |
| `storage/gold_postgres_serving.py` | Decorate existing serve/commit operations for result, failure and duration. |
| `storage/postgres_redis_serving.py` | Decorate existing refresh/read operations without changing atomic cache logic. |
| `dashboard/read_service.py` | Observe the existing load result/source or exception; dashboard UI/read semantics unchanged. |
| `alerts/evaluator.py` | Observe returned evaluation counts/provenance and supplied rule configuration. |
| `notifications/service.py` | Observe committed generation/claim/finish/process outcomes without moving transactions or provider calls. |
| `notifications/provider.py` | Observe existing send result/latency; no provider or response-policy change. |
| `scripts/milestone8_receiver.py` | Observe a new controlled receipt only after its durable commit; dedup/response behavior retained. |

Normalized AST comparison removes the known telemetry import, decorators and
standalone hooks (including an observation-only receiver condition). The
remaining business AST is identical for all 12. This structural check complements
the unchanged frozen assertions and real runtime regressions; it is not a claim
that those files remain byte-identical.

Retained default data also matches the pre-M9 baseline exactly: four M4 tables,
five M7 tables and three M8 tables. Selected independent hashes:

- Revenue 18 rows: `0d37f7c1fce0ddeef03cd24e77c1c31c46bb1464f9bee48cc54c6b709bfdc384`.
- Product 13 rows: `e7b797eafc6c9036ad2d5057605a9fcf04f9a6348a45b4d098ed31cdba47d3dd`.
- M7 firing events 2: `187b4a9f7a87550f29b9eed48e660ca08859ed751903901e85db339de9c597d2`.
- Default M8 deliveries 2: `80410e033bb7bab57b71db9b0b363dc69550ee07eae80f7d99fc7024558f8252`.
- Default M8 attempts 2: `125a17262aea185cd20cebdc65f253a11cf3403f56a6ad347b51e741e3f1f2a4`.

The baseline and final audit contain every compared table hash, including state,
history, definitions, source binding and evaluations.

## 17. Known Limitations

1. Best-effort asynchronous telemetry can lose observations during overflow,
   outage or abrupt death. Some drops cannot be reported. No exactly-once or
   historical business-total claim is made.
2. Exporter restarts reset counters/histograms. Metadata gauges repopulate;
   lifecycle-only gauges remain unknown until observed. PromQL rate/increase
   handles resets but does not reconstruct lost observations.
3. No topic/database labels are exported; last-invocation gauges can reflect
   isolated fixtures. Do not infer source equivalence from matching versions.
4. Metadata-only probes do not redo full snapshot validation. Publication age,
   live dependency reachability and application contact are distinct signals.
5. No fabricated Spark group lag, hidden producer-internal retry counts or
   infrastructure process-restart counter. No distributed process supervisor.
6. The controlled HTTP provider is the sole M8 transport; external provider
   delivery remains uncertified. Metric collection changes no delivery policy.
7. The timing-sensitive M8 test failed under concurrent verification; the exact
   transport exception is unavailable. Unchanged sequential test/suite and real
   M8 regression passed. This is not load/performance certification.
8. Retained verification-container pip check still reports missing pycairo for
   pygobject. New exporter and host dependency checks pass.
9. Grafana provisioning/API/query behavior is verified; browser pixels, HA,
   distributed scale, long retention and production access control are not.
   Local endpoints use loopback host bindings and the existing Docker network.
10. Applications must opt into telemetry and the dashboard probe URL must match
    the actual deployment. Zero unobserved activity is not proof of no business work.

## 18. Certification

**M9 STATUS: PASS.** Implementation, real metric changes, Prometheus target health,
Grafana provisioning/query execution, empty-source interpretation, outage and
restart behavior, all eight frozen milestone regressions and retained-state
integrity are verified in the referenced evidence. Failed attempts and their
corrections remain available; test expectations were not weakened.

**Frozen after verification at 2026-10-04 17:58:47 UTC.** Cleanup stopped only
`oip-modernization-verify`, `oip-metrics-exporter`, `oip-prometheus`, `oip-grafana`,
`oip-schema-registry`, `oip-kafka`, `oip-minio`, `oip-redis` and `oip-postgres`.
All nine inspect as `exited` with `Running=false`. Containers, volumes, SQL/lake
state, receiver SQLite receipts and verification evidence are retained. The
Streamlit/receiver processes inside the verification container stopped with it;
unrelated Supabase services were left untouched. The documented URLs therefore
require restarting the relevant services before use.

Cleanup evidence: `freeze-before-1791136713991253924`,
`freeze-stop-1791136727919616470`, `freeze-state-1791136728149884576` and
`freeze-after-1791136728433224327`.

The current repository roadmap uses historical phase numbers and does not
confirm an M10 milestone. No M10 designation or implementation is asserted.
