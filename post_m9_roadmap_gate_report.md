# Post-M9 Roadmap & Architecture Gate

## 1. Executive Verdict

**POST-M9 ROADMAP GATE: PASS.** Discovery and planning are complete; no future
milestone has been implemented. Audit date: **2026-10-05**.

**M10 is not defined by the repository.** The certified milestone chain has no
designated successor after M9. The historical phase roadmap separately lists
**Phase 12 — Dockerization** followed by **Phase 13 — CI/CD**. These are genuine
repository plans, but their phase numbers are not milestone numbers, their
historical specifications are incomplete, and neither has been established as M10.

**NEXT RECOMMENDED MILESTONE: Reproducible Local Runtime Packaging & Launch
Verification.** **This is an architectural recommendation, not an existing
repository M10.** It is a bounded interpretation of the historical packaging
direction and M1's reproducibility follow-up, not the entire productionization
backlog. Section 12 defines a planning-only scope.

**Functional architecture: COMPLETE at the frozen local certification scope.**
All 14 stages in the requested original flow are represented by M1–M9. Invocation
and verification across those boundaries are demonstrated; a self-starting,
continuously scheduled production deployment is not. Notification certification
uses the controlled HTTP provider, not an external messaging service. Broader
historical feature aspirations, including statistical baselines and additional
Gold datasets, are not implied by this completeness verdict.

**Final gate: WAIT — ROADMAP DECISION REQUIRED.** The owner must adopt or decline
the proposed descriptive scope, choose the supported local execution environment
and artifact-availability policy, and explicitly authorize any future packaging
work. Stopping after M9 is also a defensible choice for a local demonstrator.

## 2. M1–M9 Frozen Status

| Milestone | Status | Core Capability |
|---|---|---|
| M1 — Technology & Compatibility Review | PASS / FROZEN | Synthetic producer, Confluent-framed Avro, Kafka/Registry compatibility and real Spark/Bronze runtime. |
| M2 — Bronze → DQ → Silver → DLQ | PASS / FROZEN | 21 explicit DQ rules, durable outcomes, source-coordinate replay and bounded recovery. |
| M3 — Silver → Gold | PASS / FROZEN | Currency-separated Decimal/event-time metrics, unique source ledger and immutable committed snapshots. |
| M4 — Gold → PostgreSQL | PASS / FROZEN | Manifest-pinned, transactional complete-snapshot serving and provenance. |
| M5 — PostgreSQL → Redis | PASS / FROZEN | Atomic full-snapshot cache publication, validation, idempotency and rebuild. |
| M6 — Dashboard | PASS / FROZEN | Streamlit currency/provenance-aware display, valid Redis preference and authoritative SQL fallback. |
| M7 — Alerts | PASS / FROZEN | One-shot explicit business rules, deterministic evaluations/firings and SQL cooldown/state/history. |
| M8 — Notifications | PASS / FROZEN | Deterministic persisted requests, fenced leased attempts, bounded retries and real controlled HTTP acceptance. |
| M9 — Observability | PASS / FROZEN | 45 bounded OIP metric families, dependency/metadata probes, Prometheus and three Grafana dashboards. |

These statuses come from the frozen reports and retained evidence; this audit did
not rerun acceptance tests or extend their certification. M9's complete suite
records **202 passing cases**, including 23 M9 cases, and passing M1–M8 runtime
regressions. The existing qualifications remain in force.

## 3. Roadmap Evidence

Classification used throughout this report:

- **A — EXPLICIT ROADMAP ITEM:** an explicitly sequenced repository phase/next
  milestone. Historical numbering and current certification are kept distinct.
- **B — IMPLIED FUTURE WORK:** a recorded limitation, deferred improvement or
  future enhancement, without a post-M9 next-milestone designation.
- **C — ARCHITECTURAL RECOMMENDATION:** a proposal made by this audit. Supporting
  repository context does not turn a new scoped proposal into an existing plan.

Each future item has one classification. Implemented capabilities and items not
mentioned by the repository are identified separately; an absence is not a plan.

| Source | Exact Reference | What It Says |
|---|---|---|
| `README.md` | “What this project is”, lines 13–17 | Replicates an e-commerce Kafka/Spark medallion analytics pipeline and is built phase by phase. Establishes local production-style engineering intent, not a SaaS contract. |
| `README.md` | “Verified pipeline status”, lines 19–27 | “The historical roadmap below is not a runtime acceptance report.” Current contracts supersede older ten-rule/business-key descriptions. |
| `README.md` | “Build roadmap”, lines 575–579 | Phase 9 Dashboard; Phase 10 “Alerting system — Rule engine + Z-score/EWMA, Slack”; Phase 11 Observability; Phase 12 “Dockerization — Dockerfile per service, prod compose”; Phase 13 “CI/CD — GitHub Actions: lint → test → build → deploy”. **A:** the remaining historical ordered phases are 12 then 13. |
| `PHASE_STATUS.md` | Milestone 9 paragraph, lines 44–54 | “The historical phase numbering below does not establish an M10 milestone; no M10 implementation is included.” |
| `PHASE_STATUS.md` | Historical table/“Notes”, lines 69–76 | Phase 12 Dockerization and Phase 13 CI/CD remain “See notes”; calls for Dockerfiles per service and GitHub Actions workflows. Also describes infrastructure Compose plus direct Python execution. **A**, not a formal modern milestone specification. |
| `.github/workflows/ci.yml` | Line 1 and job `placeholder`, lines 5–10 | “CI pipeline stub — Phase 13 will complete this”; only checkout and an echo. Workflow existence is not functioning CI/CD. **A:** historical Phase 13 corroborated. |
| `current_system_report.md` | §39, “Step 1 — Establish the authoritative target architecture”, lines 2189–2196 | Earlier auditor asks whether runtime remains direct local processes or becomes fully Compose-orchestrated. **B:** recorded design decision, not a post-M9 mandate. |
| `current_system_report.md` | §41 “Phase Verification Plan”, lines 2503–2504 | Phase 12: build/start applications, “All services run under Compose”, “No direct manual process required”; Phase 13: a real Actions run with lint/tests/build. **A:** historical acceptance direction, without current source/identity/provider details. |
| `milestone1_current_real_world_review_report.md` | §9 “Prioritized follow-ups”, lines 338–342 | Supported Kafka/Registry line, Python packaging, storage distribution, aligned Spark/Hadoop modernization, application image and transitive lock. **B:** explicit follow-ups, not named next milestone. |
| `milestone2_bronze_dq_silver_dlq_report.md` | §14 “Remaining issues”/“FUTURE MODERNIZATION”, lines 374–402 | Single writer, local history index, no business event ID, policy governance and lifecycle/storage follow-ups. **B.** |
| `streaming/GOLD_CONTRACT.md` | “Persistence and replay”, lines 118–119; “Deferred architecture”, lines 121–128 | Local full-ledger/snapshot design; retention/compaction/cleanup need later scaling work; extra metric families not included. **B.** Serving adoption was subsequently completed by M4–M9. |
| `milestone5_postgres_redis_report.md` | §28 “Remaining limitations”, lines 525–536 | One-shot refresh, bounded memory, namespace writer constraint, no scheduler/daemon, Redis not a business backup. **B.** |
| `milestone6_dashboard_report.md` | §26 “Remaining limitations”, lines 547–559 | Source-age qualification, full-snapshot scope, no cache-refresh scheduler and no browser screenshot suite. **B.** |
| `milestone7_alerts_report.md` | §19 “Known limitations and operational risks”, lines 699–713 | No scheduling/distributed evaluator, retention/purge, auth redesign, cloud deployment or CI/CD. **B.** Earlier delivery/metrics exclusions were addressed by M8/M9. |
| `milestone8_notifications_report.md` | §20 “Known limitations”, lines 640–655 | Controlled-only delivery, provider idempotency caveats, invocation-driven retries, no repair UI, retention or scale claim. **B.** Observability was subsequently addressed by M9. |
| `milestone9_observability_report.md` | §17 “Known Limitations”, lines 527–550 | Best-effort telemetry, resets, bounded probes/contact, controlled provider, timing-sensitive test qualification, pycairo, no browser/HA/scale/long-retention/access-control certification. **B.** |
| `milestone9_observability_report.md` | §18 “Certification”, lines 573–574 | “The current repository roadmap uses historical phase numbers and does not confirm an M10 milestone.” No designation is asserted. |
| `README.md` | “Future enhancements”, lines 624–629 | Iceberg, OpenMetadata, Great Expectations, Kubernetes/KEDA, ML anomaly detection and Avro v2. **B:** options, not the next milestone. |
| `current_system_report.md` | §39 “Step 16 — Security hardening”, lines 2346–2355 | Earlier auditor recommends credentials, auth, TLS, least privilege and secrets changes after correctness, for shared use. **B:** recorded recommendation, not a current security-hardening milestone. |

Inspection covered root documentation; both M1 reports and M2–M9 reports;
Gold/DQ/serving/alerts/notifications/observability contracts; Compose and exporter
Dockerfile; dependency/environment templates; Makefile and dispatcher; source
comments; verification/capture scripts; tests; CI configuration; and retained
evidence. Searches included M10/Milestone 10, Phase 10/11, next/after/post-M9,
roadmap/TODO/FIXME and operational, deployment, security, governance and scaling
terms. No project TODO list or TODO/FIXME comment established a successor.

There is **no `docs/`, `architecture/` or standalone roadmap/design directory** in
this checkout. The README link to `docs/architecture.md` has no target. Root
reports and module contracts are the actual design sources. Evidence lives in
`milestone2_evidence/` through `milestone9_evidence/`, rather than a generic
`evidence/` directory. Searches of that evidence found no independent M10 plan.
The `.remember/` harness directories supplied no roadmap. Third-party virtualenv
source, caches and incidental strings such as timezone “M10” are not project plans.

**Authority rule:** frozen scoped reports/contracts and real evidence outrank the
older diagram, generic COMPLETE/Pending tables and the pre-M1 forensic report.
The old report remains evidence of original intent and recommendations; its
pre-repair “no tests/no dashboards/broken Avro” findings are not current gaps.

## 4. M1–M9 Next-Milestone Chain

| From | Stated next / equivalent | Exact source | Interpretation |
|---|---|---|---|
| M1 | Bronze → Data Quality → DLQ → Silver | `milestone1_producer_kafka_schema_spark_bronze_report.md`, §10, lines 608–622 | Direction later realized as M2. The newer M1 technology review §9 lists lifecycle follow-ups but no alternative numbered successor. |
| M2 | “Milestone 3: Silver → Gold — lifecycle-aware, replay-safe aggregations.” | `milestone2_bronze_dq_silver_dlq_report.md`, §16, lines 420–425 | Explicit M2 → M3. |
| M3 | “Milestone 4 — committed Gold → PostgreSQL serving with currency-aware identities, decimal-safe schema and idempotent complete-snapshot upserts.” | `milestone3_silver_gold_report.md`, §27, lines 638–644 | Explicit M3 → M4; M4 ultimately uses transactional complete replacement. |
| M4 | **No explicit next-milestone designation in the report.** | `milestone4_gold_postgres_report.md`, §§27–29, lines 331–357 | Lists downstream adoption needs, not “M5 next”. Actual progression to M5 is established by the later M5 report, not an invented M4 quotation. |
| M5 | “Milestone 6 — Currency-aware dashboard consumption of the verified M5 Redis snapshot, with authoritative M4 PostgreSQL fallback.” | `milestone5_postgres_redis_report.md`, §32, lines 596–601 | Explicit M5 → M6. |
| M6 | “Milestone 7 — Currency-aware alert consumption of authoritative M4 PostgreSQL analytical data, with explicit source-version and cooldown semantics.” | `milestone6_dashboard_report.md`, §28, lines 596–601 | Explicit M6 → M7. |
| M7 | “M8 — Notifications.” | `milestone7_alerts_report.md`, §21, lines 753–755 | Explicit M7 → M8. |
| M8 | “M9 — Observability.” | `milestone8_notifications_report.md`, §22, lines 685–687 | Explicit M8 → M9. |
| M9 | No confirmed M10 or designated next milestone | `milestone9_observability_report.md`, §18, lines 573–574 | Chain stops at M9. |

The historical implementation progression is M1 → M2 → M3 → M4 → M5 → M6 → M7 →
M8 → M9. The report-to-report *explicit next-reference* chain has a documentary
gap at M4 and no successor after M9. Neither gap authorizes numbering a new M10.

## 5. Is M10 Defined?

**NO. M10 is not defined by the repository.**

Search results contain negative statements in M9/PHASE_STATUS, not a positive
M10 specification. “Phase 10” in legacy alert modules is the old alerting phase,
already covered by the scoped M7/M8 path; it is not a post-M9 milestone. “Phase 11”
in `main.py` and requirements is historical observability numbering.

Historical Phase 12 does exist. Its exact designation is **Dockerization**, its
README deliverables are **“Dockerfile per service, prod compose”**, and the older
audit's acceptance direction is applications running under Compose without
manual processes. Historical Phase 13 is **CI/CD**, with lint/test/build/deploy
aspirations and a placeholder workflow.

Neither historical item specifies all of the following for the frozen world:
the supported OS/runtime, image/dependency artifact provenance, one-shot M4/M5/M7/M8
invocation ordering and cadence, checkpoint/writer ownership, controlled-receiver
network namespace and ledger lifetime, failure/recovery evidence, exclusions,
M1–M9 regressions or PASS/PARTIAL/BLOCKED rules. Dependencies on packaging before
CI are implied by sequence, not formally enumerated. Thus a future direction is
documented, but there is **no implementation-ready repository M10** and no exact
current milestone designation to announce under Outcome A.

## 6. Current Platform Capability Map

| Capability | Current implementation / verification | Qualification |
|---|---|---|
| Synthetic events / Avro / Kafka / Registry | Real generator/publisher, registered schema and binary decode, M1 runtime/regressions | Locally verified; region-keyed producer, dev single-node broker and v1 schema. Not a real customer ingestion SLA. |
| Bronze / DQ / Silver / DLQ | Structured raw envelope, 21 rules, durable MinIO audit, replay/recovery, M2 | Locally verified correctness. Bronze append/checkpoint crash atomicity and DLQ at-least-once delivery remain explicit limits. |
| Gold business metrics | Two Decimal/currency/event-time tables and source ledger, committed full snapshots, M3 | Correct within additive event-flow contract; single writer/full-history recomputation. Not reconciled settlement accounting. |
| PostgreSQL serving | Atomic complete replacement, bounded validation, state/history, M4 | Production-capable transaction/idempotency patterns; no enterprise deployment/HA/backup certification. |
| Redis cache | One validated atomic snapshot hash, WATCH conflicts, reconstruction from SQL, M5 | Rebuildable cache, not authority or backup; row/byte bounded; can lag source. |
| Streamlit dashboard | Actual AppTest/read-service/HTTP checks, M6 | Locally verified analytical display; source provenance is visible. No browser-pixel or authenticated product-UI certification. |
| Business alerts | Explicit rules, SQL-persisted cooldown/history, coherent source locks, M7 | Restart/concurrency-safe within serialized local invocation scope; no continuously scheduled/distributed evaluator. |
| Notifications | Persistent claims/results/retries and real loopback HTTP/SQLite receipts, M8 | Engine is locally verified; concrete transport is deliberately CONTROLLED/TEST. External/human delivery is not certified. |
| Observability | Live scrape/query/provisioning/failure checks, 45 families and 29 Grafana panels, M9 | Optional operational observer; lossy telemetry, resettable counters, metadata-only probes and local single-instance services. |
| Runtime packaging / automation | Infrastructure Compose, exporter image, manual CLI/Streamlit/Spark paths and retained verification container | Partial packaging, not an all-application deployment. No scheduled serving/evaluation/retry progression. Historical Phase 12 is deferred. |
| Tests / evidence | Eight substantive test modules under `tests/unit/`, capture/verifier scripts and retained real-service evidence | Tests include real SQL/HTTP/Spark despite directory naming. `integration/` and `e2e/` have initializer files only; no additional packaged CI suite. |
| Legacy extras | Old SQL/cache writers, statistical engines and Slack/PagerDuty router remain isolated | Historical/prototype paths, not active certified M7/M8 or authoritative serving. Their presence does not certify those features. |

“Production-capable” describes selected design mechanisms, not a blanket
production-ready system. The defensible overall description is a **locally
verified, production-style end-to-end data-platform demonstrator**. README's
marketing “production-grade” header is not deployment certification.

Original intent evidence: README §“What this project is” and the historical
forensic report §§1–4 describe synthetic e-commerce data, operational/business
analytics, local Docker and medallion processing. They do not specify tenant
isolation, subscription/user management, SaaS APIs, managed-cloud infrastructure
or a research experiment. A portfolio/educational use is consistent with this
demonstrator, but “portfolio” is an inference, not an explicit repository label.

## 7. Remaining Known Limitations

The table evaluates the requested M9 limitations against the actual report.
It does not turn exclusions into defects or automatic milestones.

| Limitation | Actual source / interpretation | Roadmap disposition |
|---|---|---|
| Best-effort telemetry | M9 §17.1 / telemetry contract: intentional fail-open bounded delivery; observations can be lost | **B — known limitation**, no reliable/durable telemetry project designated. |
| Exporter counter resets | M9 §17.2: in-memory counters reset; rates/increases handle boundaries, not lost data | **B — known limitation**, normal documented counter lifecycle, not a persistence defect. |
| Metadata-only probes | M9 §17.4: do not replace real M5/M6 validation | **B — intentional scope**, no request to scan business data from metrics. |
| No distributed process supervisor | M9 §17.5 / observability contract | **B — explicitly outside scope**; historical Dockerization is adjacent but does not require a distributed scheduler. |
| No distributed-scale certification | M9 §17.7/9 and M3/M5/M7/M8 limits | **B — known qualification**; Kubernetes/KEDA is a separate optional enhancement. |
| No HA Prometheus/Grafana | M9 §17.9 and single-instance Compose | **B — outside certification**, no repository HA-observability milestone. |
| No long-retention certification | M9 §17.9; existing 15-day Prometheus configuration | **B — outside certification**, not absence of all retention configuration. |
| No browser-pixel validation | M9 §17.9; M6 AppTest/HTTP qualification | **B — known test boundary**, no designated screenshot project. |
| External provider delivery not certified | M9 §17.6, M8 §20 and controlled-provider contract | **B — intentionally qualified transport**; Slack is a historical intention, not current delivery proof. |
| Timing-sensitive M8 concurrent test | M9 §13 and §17.7: initial failure, preserved diagnostics, unchanged sequential pass | **B — known qualification**, exact client exception unavailable; no proven concurrency semantic defect or permission to weaken tests. |
| Existing pycairo dependency issue | M9 §14/§17.8 and actual pip-check artifact | **B — environment qualification**; unrelated verification-image package issue, not a demonstrated exporter failure. |
| No deployment automation | M9 §2 exclusion; partial Compose/image packaging exists | **A — historical Phase 12**, no current post-M9 scope/authorization. |
| No functional CI/CD | M9 §2 exclusion; workflow only checkout/echo | **A — historical Phase 13**; distinguish existing stub from functioning automation. |
| No cloud/Kubernetes deployment | M9 §2 exclusion; README Future enhancements names Kubernetes/KEDA | Kubernetes/KEDA: **B — optional enhancement**. Managed-cloud deployment requirements are **not mentioned**. |
| No security-hardening milestone | M9 §17.9 excludes production access-control certification; old audit §39 Step 16 | **B — recorded shared-environment recommendation**, no numbered/current security milestone. |
| No authoritative-history retention/purge | M3, M7 §19 and M8 §20 exclusions; not an enumerated M9 §17 project | **B — deferred/known limit**; Kafka and Prometheus retention settings already exist. |
| No scheduler | M5/M6/M7/M8 one-shot limitations and M9 contract | **B — deliberate local execution boundary**; invocation cadence is an operator/design decision. |

The M9 report does not literally enumerate every deployment/scheduler/retention
item in its Known Limitations section. Some are scope exclusions or inherited
M3–M8 constraints. Those distinctions matter.

Earlier downstream exclusions have been resolved selectively: M2's Prometheus
wiring is now represented in M9 observations; M7's notifications in M8; M8's
observability in M9. A separate PostgreSQL DQ audit sink, additional Gold metrics
and statistical baselines were not silently added.

## 8. Remaining Architectural Gaps

Status vocabulary: **already implemented**, **explicitly deferred**, **known
limitation**, **not mentioned**, **recommended future work**. A/B/C tags apply to
future items, not to completed capabilities. Recommendations below are options
with stated prerequisites, not new mandatory requirements.

| Area / item | Status | Evidence / boundary / classification |
|---|---|---|
| Reliability — restart recovery | Already implemented | Checkpoints/source replay, manifests, SQL idempotency, Redis rebuild, M7 cooldown and M8 lease/fencing recovery; bounded failure paths tested. |
| Reliability — durability | Already implemented, with known limits | Named volumes, committed manifests and SQL history; receiver SQLite ledger. Bronze/DLQ cross-system atomicity and provider ledger loss remain **B** limitations. |
| Reliability — failure recovery | Already implemented | M4 rollback, explicit partial-output guards, cache fallback/rebuild, M8 expired-claim recovery and M9 outage restoration. No universal hard-kill guarantee. |
| Reliability — HA | Known limitation | RF1 Kafka, single-node MinIO/SQL/Redis/metrics stack; M9 excludes HA. **B**, not a local functional defect. |
| Reliability — backup/restore | Not mentioned as an operational contract | No RPO/RTO, scheduled backup policy or restore certification found. M9's pg_dump/psql copies test sources, not disaster recovery. Optional restore-drill proposal is **C**. |
| Scalability — throughput | Known limitation | M2 audit scans, M3 full-ledger recomputation, M4 driver collection and M5 memory bounds. **B**; no TPS target/SLO. |
| Scalability — concurrency | Already implemented within bounds | M4/M7 locks, M5 WATCH and M8 SKIP LOCKED/fencing; tests prove selected overlaps, not general scale. |
| Scalability — partition scaling | Already implemented static configuration; dynamic qualification absent | Topic admin defines 8 order/4 payment partitions, RF1. Kubernetes/KEDA autoscaling is **B** future enhancement; partition growth behavior is unqualified. |
| Scalability — multiple writers | Explicitly deferred / known limitation | One M2 writer per output and M3 writer per root. M4 serialization is not distributed ownership. **B**. |
| Scalability — distributed evaluators | Explicitly deferred | M7 serializes one result schema and needs two SQL connections per invocation. **B**. |
| Scalability — notification throughput | Known limitation | Bounded batch 100 default/1000 max, safe concurrent claims, no sustained-rate test. **B**. |
| Operations — scheduling | Explicitly deferred | M4/M5 serving, M7 evaluation and M8 retry progress require invocations; dashboard polling does not refresh cache. **B**. |
| Operations — deployment | Explicitly deferred beyond existing infrastructure/exporter packaging | Historical **A Phase 12**; broader acceptance unfulfilled. Narrow local packaging proposal is a separate **C** item. |
| Operations — CI/CD | Explicitly deferred | **A Phase 13**, placeholder workflow, no real checks/build/deploy results. |
| Operations — supervision | Already implemented for infrastructure only | Compose `restart: unless-stopped`; no platform application supervisor. Absence of distributed supervision is **B** known scope. |
| Operations — retention | Partly implemented / explicitly deferred | Kafka topic retention and Prometheus duration exist; no lake/orphan/M7/M8 purge policy. Safe later lake/history cleanup is **B**. |
| Operations — log aggregation | Not implemented; known scope exclusion | structlog/Compose logs exist; M9 explicitly excludes centralized logging/tracing (**B**), no named replacement project. |
| Security — authentication | Partly implemented | SQL/MinIO/Grafana credentials and optional Redis ACL client fields exist. No application-user authentication; old audit shared-use hardening is **B**. |
| Security — authorization | Not mentioned as an application model | No tenant/user/RBAC design. Least-privilege infrastructure use is an old-audit **B** recommendation, not implemented application authorization. |
| Security — secrets management | Already implemented environment sourcing; shared-use hardening deferred | Active paths sanitize errors/use env; no managed secret store. Moving secrets beyond dotenv for shared use is **B** (old audit Step 16). |
| Security — network exposure | Already implemented local bridge and partial loopback binding | M9 ports bind loopback; core infrastructure port mappings lack explicit loopback binding. Restricted shared exposure is **B** recommendation, not a certified shared deployment. |
| Security — TLS | Not implemented on examined local paths | Kafka PLAINTEXT, HTTP Registry/MinIO/telemetry/controlled receiver, S3A SSL disabled. TLS “where appropriate” is **B** from old audit, conditional on shared deployment. |
| Security — audit/security controls | Business audit already implemented; security controls not mentioned | DQ/provenance/evaluation/attempt records are not login/access audit, threat modeling or compliance certification. No invented requirement. |
| Product — external notification providers | Explicitly deferred / known certification limit | Active M8 controlled provider only; legacy Slack/PagerDuty code isolated. **B**. |
| Product — user-facing alert management | Explicitly outside scope | Rules JSON and `read_results()` backend exist; no rule/repair/notification UI. **B**, not necessary for CLI demonstrator completion. |
| Product — configuration management | Already implemented local env/CLI/JSON | Pydantic legacy settings and explicit active-module configuration; immutable persisted rule/route policy. Central dynamic configuration service is **not mentioned**. |
| Product — APIs | Existing service APIs/Python boundaries implemented; product API deferred/absent | Registry/metrics/provider HTTP is not a customer REST API. M7 contract excludes HTTP API (**B**); no SaaS API specification. |
| Data platform — schema governance | Partly implemented / future enhancement | Registered Avro v1/BACKWARD plus strict version checks; Avro v2/BACKWARD_TRANSITIVE and reference-policy governance are **B**. |
| Data platform — retention | Explicitly deferred beyond Kafka/Prometheus | M3 retained snapshots/orphans and M7/M8 history; reference/replay/FK dependencies prevent blind expiration. **B**. |
| Data platform — archival | Raw archive already implemented; tiering not mentioned | Bronze raw archive and retained immutable Gold versions exist. No cold-tier or archive restore SLA. |
| Data platform — lineage | Provenance already implemented; catalog lineage enhancement deferred | Source coordinates, manifests/digests and alert/notification links exist. OpenMetadata enterprise/catalog lineage is **B** optional enhancement. |
| Data platform — data catalog | Future enhancement | README names OpenMetadata (**B**); no deployment or dataset ownership workflow. |
| Data platform — transactional lake / DQ reporting frameworks | Future enhancement | Iceberg and Great Expectations are **B** options; current Parquet/DQ correctness does not require replacing them. |
| Performance — load/stress testing | Known limitation | M3/M8/M9 explicitly exclude such certification. **B**; target workload needed before a scoped test project. |
| Performance — latency benchmarking | Basic latency observations already implemented; benchmark unqualified | M9 histograms/runtime durations are not controlled benchmark/SLO evidence. **B** scale/performance limitation. |
| Performance — capacity planning | Not mentioned with requirements | No capacity model, workload envelope or resource budget found. Optional workload qualification is **C**, not an assumed SLA. |
| Cloud/production — Kubernetes | Explicit future enhancement | README Kubernetes/KEDA (**B**), no manifests/operating model/certification. Optional, not required. |
| Cloud/production — managed Kafka | Not mentioned | No provider, contract, budget or migration design. Not adopted as a recommendation here. |
| Cloud/production — managed PostgreSQL | Not mentioned | No managed-service requirement or deployment artifacts. |
| Cloud/production — cloud object storage | Portability rationale only | README “Why MinIO instead of S3?” mentions reuse of S3 clients; no verified cloud deployment/credentials/policy design. **B** portability direction, not delivered cloud operation. |
| Cloud/production — HA deployment | Known limitation | No topology/failover/RPO/SLO requirement; local services are single instances. **B** qualification, not automatic next work. |

Additional frozen semantic limits: amount originates as double; no stable upstream
business event ID exists; cancellation/refund may overlap; product names are part
of identity. **B** future contract/governance work is recorded in M2/M3, but any
change would require separate versioned business decisions and cannot be folded
into packaging or treated as an unapproved bug fix.

## 9. Explicitly Deferred Work

**Still relevant after M9:**

- **A:** historical Phase 12 application Dockerization and Phase 13 real CI/CD.
- **B:** supported Kafka/Registry migration, Python runtime packaging/lifecycle,
  storage-distribution decision and coordinated Spark/Hadoop/SDK modernization
  (M1 §9, repeated later). Their recorded upstream lifecycle findings were not
  revalidated against external websites during this repository-only audit.
- **B:** transactional lake/scalable identity index, stable business-event/schema
  evolution, governed DQ age/currency references, retention/compaction/orphan cleanup.
- **B:** unattended invocation/scheduling, distributed evaluation and capacity
  qualification; local one-shot paths intentionally exclude them.
- **B:** external-provider certification, operator terminal-delivery repair,
  notification/alert management UI and multi-channel routing.
- **B:** extra Gold heatmap/funnel/baseline/unique-count families, statistical or ML
  anomaly detection and settled-accounting contract evolution.
- **B:** optional Iceberg, OpenMetadata, Great Expectations, Kubernetes/KEDA and Avro
  v2 enhancements from README. These are separate possible projects.
- **B:** shared-environment security/access-control hardening, browser validation,
  centralized logging/tracing and HA/long-retention certification limits.

**Do not carry old exclusions forward incorrectly:** authoritative serving/cache/
dashboard adoption was achieved by M4–M6; persisted alerts by M7; notification
engine by M8; operational metrics/Grafana by M9. Legacy unqualified code remains,
but is not the active certified route. The original forensic audit's broken
primary pipeline, empty tests and missing Grafana dashboards are superseded.

**Not a deferred requirement:** managed-cloud services, multi-region SaaS,
Airflow, dbt, Kafka Connect, Terraform, ArgoCD, service mesh or mandatory
OpenTelemetry. No project requirement establishing those technologies was found.

## 10. Candidate Future Milestones

Rankings assume the evidence-supported goal of a shareable **local demonstrator**,
not an unprovided enterprise/customer deployment. HIGH effort/risk is a cost,
not an argument to choose a project. Value/urgency change if real users, exposure,
SLAs or workload are specified. Candidates are not an approved sequential roadmap.

| Candidate | Source | Value | Technical Importance | Risk | Effort |
|---|---|---|---|---|---|
| 1. Reproducible Local Runtime Packaging & Launch Verification | **C** — this audit's narrowed proposal; motivated by M1 §9.5 and historical Phase 12 | HIGH | HIGH | MEDIUM | MEDIUM |
| 2. Python/Spark runtime packaging lifecycle | **B** — M1 §9.2, later environment limits | MEDIUM | HIGH | MEDIUM | MEDIUM |
| 3. Storage distribution/artifact availability decision | **B** — M1 §9.3 and fresh-pull qualification | HIGH | HIGH | HIGH | HIGH |
| 4. Supported Kafka/Registry paired migration | **B** — M1 §9.1 / M2 §13 | MEDIUM | HIGH | HIGH | HIGH |
| 5. Historical Phase 12 — Dockerization, full stated breadth | **A** — README Phase 12 / old audit §41 | HIGH | HIGH | HIGH | HIGH |
| 6. Historical Phase 13 — CI/CD | **A** — README/PHASE_STATUS / workflow stub | HIGH | HIGH | MEDIUM | HIGH |
| 7. Controlled Local One-shot Scheduling | **C** — proposed response to M5/M7/M8 invocation limits, not a named repository plan | MEDIUM | MEDIUM | MEDIUM | MEDIUM |
| 8. Isolated PostgreSQL Backup/Restore Drill | **C** — recovery extension inferred here; no repository backup contract | MEDIUM | HIGH | MEDIUM | MEDIUM |
| 9. Shared-environment Access/Secrets/Network Hardening | **B** — old audit §39 Step 16; M9 exclusions | MEDIUM | HIGH | HIGH | HIGH |
| 10. Bounded Workload/Concurrency Qualification | **C** — proposed measurement project addressing recorded scale limits | MEDIUM | HIGH | MEDIUM | MEDIUM |
| 11. Gold Snapshot Retention/Compaction Design | **B** — Gold contract lines 118–119 / M3 §25 | MEDIUM | HIGH | HIGH | HIGH |
| 12. Avro v2 / DQ Reference-policy Governance | **B** — README enhancements / M2 §14 | MEDIUM | MEDIUM | HIGH | HIGH |
| 13. One External Notification Provider Certification | **B** — original Slack direction and M8 future-provider qualification | MEDIUM | MEDIUM | HIGH | MEDIUM |
| 14. Browser Presentation Qualification | **B** — M6/M9 explicit browser test boundaries | LOW | LOW | LOW | MEDIUM |
| 15. Business-contract / Additional Metrics Evolution | **B** — M3 §25–26 / historical extra metrics | MEDIUM | MEDIUM | HIGH | HIGH |
| 16. Data Catalog / Lineage (OpenMetadata) | **B** — README Future enhancements | LOW | MEDIUM | MEDIUM | HIGH |
| 17. DQ Reporting/Expectation Store (Great Expectations) | **B** — README Future enhancements | LOW | MEDIUM | MEDIUM | MEDIUM |
| 18. Transactional Lake (Iceberg) | **B** — README Future enhancements / M2 modernization | LOW | MEDIUM | HIGH | HIGH |
| 19. Kubernetes/KEDA Deployment | **B** — README Future enhancements | LOW | LOW | HIGH | HIGH |
| 20. ML/Baseline Training | **B** — README Future enhancements / M7 exclusions | LOW | LOW | HIGH | HIGH |
| 21. HA Observability / Long-retention Qualification | **B** — M9 outside-certification boundary, no designated project | LOW | LOW | HIGH | HIGH |

The supplemental scores make dependency urgency and frozen-boundary impact
explicit. “Impact” is prospective integration/semantic risk, not a change made by
this audit. Candidate 1 is recommended as an additive packaging layer; it does
not require modifying frozen business code.

| Rank | Dependency Urgency | Impact on frozen M1–M9 | Reason / prerequisite |
|---:|---|---|---|
| 1 | HIGH | LOW | Removes dependence on manually provisioned verification containers; enables meaningful portable automation. |
| 2 | MEDIUM | MEDIUM | Maintained runtime matters if support is a goal; changing driver/worker Python requires separate certification. |
| 3 | MEDIUM | HIGH | Artifact availability can block clean setup; choose distribution/archive policy before any data migration. |
| 4 | MEDIUM | HIGH | Recorded lifecycle reason; needs persisted KRaft/schema compatibility and rollback design. |
| 5 | MEDIUM | MEDIUM | Real historical direction, but “prod compose” is broader and less specified than the recommended bounded subset. |
| 6 | MEDIUM | LOW | Real automated checks need a reproducible runtime and an actual Git remote/runner; workspace is not a Git repository. Deployment target is unspecified. |
| 7 | MEDIUM | LOW | Useful only if unattended freshness/retry progression is desired; needs cadence/ownership decisions. |
| 8 | LOW | LOW | Valuable for durable operational use; RPO/RTO and receiver-ledger boundaries need definition first. |
| 9 | LOW | MEDIUM | Becomes HIGH urgency before shared/untrusted deployment, which is not the current local target. |
| 10 | LOW | LOW | Needs workload, rate, resource budget and latency targets; observations alone do not establish capacity. |
| 11 | LOW | HIGH | Deletion/compaction touches replay/manifests/provenance; define protected versions and recovery first. |
| 12 | LOW | HIGH | Version/rule changes alter admission and downstream contracts; needs business policy owner. |
| 13 | LOW | MEDIUM | Needs provider/account/destination, acceptance semantics and idempotency design; external delivery is not necessary for local certification. |
| 14 | LOW | NONE | Presentation-only evidence can be additive, with no pixel guarantee required by current scope. |
| 15 | LOW | HIGH | Requires actual business definitions; frozen event-flow semantics cannot be relabeled as settlement. |
| 16 | LOW | LOW | Existing provenance suffices locally; ownership/catalog demand is unspecified. |
| 17 | LOW | MEDIUM | Current DQ is verified; adding a second authority would need governance, not framework adoption for its own sake. |
| 18 | LOW | HIGH | Storage architecture migration must be justified by scale/transactions, not novelty. |
| 19 | LOW | HIGH | No cluster/managed-service need, budget or workload; least justified next step for local intent. |
| 20 | LOW | HIGH | Requires baseline/training/metric contracts and data, not a pipeline completion task. |
| 21 | LOW | MEDIUM | Availability/history requirements are absent; current local single-instance design is intentional. |

Ranking 1 ahead of lifecycle migrations does not claim lifecycle concerns are
resolved. If clean acquisition of existing artifacts is impossible, candidate 1
is BLOCKED until a separate artifact/distribution or modernization decision is
authorized. It must not smuggle upgrades into packaging to get a PASS.

## 11. Recommended Next Step

**Classification: ARCHITECTURAL RECOMMENDATION (C).**

**NEXT RECOMMENDED MILESTONE: Reproducible Local Runtime Packaging & Launch
Verification.**

**This is an architectural recommendation, not an existing repository M10.**

Reasons for this bounded recommendation:

1. It directly follows the original local execution intent and historical Phase
   12 direction, rather than introducing another business feature.
2. M1 already asks for an application image/transitive reproducibility, and
   runtime evidence depends on a retained, manually populated verification
   container with cached connectors. Functional results should be reproducible
   without that specific container.
3. Only the exporter currently has a project Dockerfile; core applications still
   require separate CLI/Spark/Streamlit commands. Packaging can wrap these proven
   interfaces without changing their semantics or inventing scheduling.
4. A demonstrated local runtime is a more useful prerequisite for historical
   Phase 13 automation than a placeholder workflow or an arbitrary cloud stack.

Before starting, obtain a decision on: **(a)** whether a local demonstrator is
still the target, **(b)** the supported OS/Docker/Spark invocation path, **(c)**
approved frozen artifact acquisition/preservation, and **(d)** adoption of this
bounded scope with explicit file-change permissions. This audit assigns no number
and does not authorize implementation. Full Phase 12 or Phase 13 may be selected
later under their exact historical names, but require a current design gate.

## 12. Proposed Scope

### Proposed Milestone

**Reproducible Local Runtime Packaging & Launch Verification — unnumbered,
planning only.** No proposed file or service in this section was created.

#### Objective

Recreate and run the existing frozen local application interfaces from declared
artifacts/configuration in a fresh isolated runtime, without relying on the
retained `oip-modernization-verify` container or undocumented package installs.
Prove the declared launch procedure and restart behavior with existing oracles.

#### Why now

The 14-stage functional architecture is represented and certified. The next
defensible local benefit is reproducibility, not new analytics or an enterprise
deployment. Packaging precedes meaningful CI automation and resolves a recorded
M1/historical Phase 12 concern.

#### Current-state dependency

M1–M9 remain frozen authorities. Existing single-writer/manifest/advisory-lock/
cache/notification contracts must be honored. Existing direct version pins and
Spark 3.5.9/Scala 2.12/Hadoop 3.3.4/SDK 1.12.262 coordinates are inputs, not a
license to migrate. Current support limitations remain separately disclosed.

#### Inputs

- Frozen source/contracts/verifiers/tests and M9 certification evidence.
- Chosen local OS/Docker workflow and bounded resource envelope.
- Approved image/base/dependency artifact provenance and acquisition plan;
  explicit availability of old MinIO artifacts or a separately approved decision.
- Non-secret configuration contract; secrets injected at execution, never baked
  into images/locks/evidence.
- Explicit reviewed rule file and controlled-provider destination/endpoint/ledger.
- Isolated topic/lake roots, SQL database/schema and Redis namespace plan that
  satisfies existing CLI source-schema restrictions.

#### Outputs

- Additive local runtime image recipe(s), resolved artifact/version/checksum
  inventory and packaging-specific launch/stop instructions.
- A bounded manual stage-launch wrapper or additive deployment override using
  existing application modules. Invocation order is explicit, not a scheduler.
- Real build/fresh-runtime/start/restart/failure/regression evidence and an
  independently reviewable scoped report.
- No change to business contracts, no promise of supported enterprise deployment.

#### Architecture

```text
Frozen application source + approved artifacts + runtime configuration
    → additive local packaging layer
    → existing producer / Spark / serving / dashboard / alerts / notifications
    → existing authoritative stores and optional M9 observation path

Existing frozen verification oracles
    → isolated fresh local runtime
    → append-only evidence → regression/integrity review → scoped freeze
```

Use appropriate Spark and non-Spark packaging roles rather than arbitrarily one
Dockerfile per Python module. Driver/executor Python versions must agree.
M4 requires Spark; M5/M7/M8 are bounded one-shot invocations. Their correct order
must be documented, without changing them into daemons.

The controlled M8 endpoint is loopback-only: processor and receiver must share
the permitted network namespace. Do not substitute a Docker service hostname to
bypass frozen endpoint validation. Preserve the receiver ledger across runtime
recreation. Dashboard health must refer to the actual packaged Streamlit process,
not the old verification-container name. Metrics remain optional consumers.

#### Files likely affected

Prospective **new**, additive artifacts only, for example under a chosen
`deployment/local/` directory: Spark/non-Spark image recipes, artifact manifests
or packaging locks, local Compose override or launch script, and a bounded runbook.
Prospective new packaging-verification scripts/tests and a separate report/evidence
directory may also be needed. These paths are examples, not existing files.

Do not replace `docker-compose.yml`, `requirements.txt`, existing Dockerfile,
Makefile or dispatcher under this scope. If implementation cannot use additive
packaging without changing a frozen interface, stop for an explicit design/scope
decision. Historical Phase 13's workflow is not edited as part of this proposal.

#### Frozen files that must remain untouched

All existing M1–M9 production modules, schemas, contracts, tests, verifiers,
reports/evidence, dependency files, base Compose and Grafana/Prometheus
configuration. Existing README/PHASE_STATUS and active `.env` remain unchanged;
new runbook/configuration descriptions can live with the additive packaging.
No business-data reset, main-selector cleanup or legacy-module removal.

#### Runtime dependencies

Existing Docker/Compose, Python/Spark/Java/Scala tuple, Kafka/Registry, S3-compatible
local storage, PostgreSQL, Redis, Streamlit, M9 exporter/Prometheus/Grafana and the
same controlled receiver. No new orchestrator, cloud service or provider required.
Image registries/package/Maven artifact access must be verifiable. A sealed,
checksummed archive is acceptable only if explicitly adopted as the artifact
source; undocumented warm caches are not a reproducibility result.

#### Acceptance criteria

1. A declared clean runtime with no retained verification container or undeclared
   packages builds/starts from the approved artifact source. Record complete
   resolved versions/checksums; direct pins alone are not a transitive lock.
2. Existing core pins remain unchanged. A packaging lock may record transitive
   resolutions only with runtime verification and no silent incompatibility.
3. Existing producer/Avro/Bronze/Silver/DLQ/Gold and downstream interfaces run
   through the documented manual launch procedure on isolated fixtures. Use
   exact configured source bindings and full Decimal/currency/provenance equality.
4. Dashboard startup/read behavior, persisted M7 outcomes, M8 controlled receipts
   and live M9 scrape/provisioned queries work from the fresh packaging.
5. Stage ownership, checkpoints, durable storage/receiver-ledger mounts, ports,
   rule configuration and stop/restart behavior are explicit and reproducible.
6. Restart/retry of completed work preserves the existing NOOP/dedup semantics;
   no extra writer is launched on a single-writer root.
7. All frozen tests/verifiers appropriate to M1–M9 pass without weakening oracles.
   The retained timing-sensitive test is executed sequentially as qualified;
   no load/scale claim follows from that pass.
8. Final hash/scope audit proves frozen files and retained default data unchanged;
   all failures/corrections are retained and cleanup preserves durable state.
9. Packaging does not claim current support, external-provider delivery, continuous
   freshness or HA. Any unresolved dependency check is classified explicitly.

#### Failure scenarios

- Image/package/Maven artifacts unavailable, or build attempts undeclared cache use.
- Wrong internal/host endpoints, malformed configuration or missing injected secret.
- Spark driver/worker mismatch or unavailable connector JARs.
- No committed snapshot, invalid snapshot and unavailable SQL/cache dependency.
- Process restart with the same checkpoint versus accidental competing writer.
- Redis loss followed by documented rebuild; existing SQL fallback behavior.
- M8 receiver interruption/restart with retained ledger, pending lease recovery
  via a subsequent explicit invocation, and lost-ledger qualification.
- Exporter/Prometheus/Grafana absence does not block business operations.
- Stop/start preserves source/schema bindings and does not delete volumes/history.

#### Regression requirements

Reuse frozen M1–M9 test/oracle expectations and fresh isolated run labels. Do not
replay fixture-creation phases into terminal/reused test schemas. Execute the
complete suite and real per-milestone runtime regression checks sequentially where
shared test resources require it. Preserve all current financial, replay,
transaction, fallback, cooldown, identity, retry, provider and telemetry limits.

#### Explicit exclusions

Dependency/support-line upgrades; production-code/refactoring changes; scheduling
or daemon conversion; distributed supervision; CI/CD implementation; cloud or
Kubernetes; HA/failover; security redesign; backup/retention projects; load or stress
certification; browser-pixel automation; external providers; business metrics,
schema/event identity or accounting changes. Do not repair pycairo by changing the
retained environment merely to report a clean check.

#### Evidence required

Artifact source/version/digest inventory; clean build logs and exit codes; fresh
runtime process/version inventory; explicit non-secret launch commands; source
coordinates/manifest/SQL/cache equality; dashboard HTTP/rendered read evidence;
M7 persisted state and M8 attempts/receiver receipts; M9 scrape/Grafana query
evidence; restart/failure captures; complete test and regression results; before/
after frozen hashes/default-state checks; final retained-volume shutdown status.

#### PASS / PARTIAL / BLOCKED rules

- **PASS:** every declared clean-runtime launch, required failure/restart,
  regression and integrity criterion passes with reproducible artifacts and honest
  inherited qualifications; no frozen semantic edits.
- **PARTIAL:** bounded packaging works for a clearly stated subset but one or more
  required entrypoints, fresh acquisition or runtime checks remain unverified;
  no full reproducibility/production claim.
- **BLOCKED:** required artifacts cannot be legally/practically acquired under the
  chosen plan; runtime target is undecided; satisfying the scope requires frozen
  semantic changes or forbidden upgrades; a regression fails unresolved; or
  safe isolated verification/durable ledger preservation cannot be established.

Apply **INSPECT → FIX within the approved additive scope → RUN → VERIFY →
EVIDENCE → FREEZE → NEXT MILESTONE**. Freeze only after scoped PASS. This proposal
is not that implementation or certification.

## 13. Functional Completeness Assessment

**Yes: all 14 components of the requested original local architecture are
represented by M1–M9.**

| Original stage | Frozen coverage | Completeness qualification |
|---|---|---|
| 1. Synthetic e-commerce events | M1 producer/generator | Synthetic inputs, not production commerce traffic. |
| 2. Avro | M1 | Registered/framed schema and real decode. |
| 3. Kafka | M1, M2 | Real broker/producer/consumption/DLQ; development RF1. |
| 4. Schema Registry | M1 | Actual schema resolution; only implemented version contract certified. |
| 5. Spark Structured Streaming | M1–M3 | Real structured-streaming bounded drains/restarts; manual/container runtime. |
| 6. MinIO Bronze | M1/M2 | Structured raw archive and malformed envelope. |
| 7. Silver + DQ | M2 | 21 rules, replay identity, durable outcomes and DLQ. |
| 8. Gold business metrics | M3 | Two certified currency-aware metric families, not all old aspirational outputs. |
| 9. PostgreSQL | M4 | Authoritative committed snapshot serving; M7/M8 business ledgers. |
| 10. Redis | M5 | Disposable full-snapshot cache. |
| 11. Streamlit dashboard | M6 | Validated source/fallback, exact monetary display and provenance. |
| 12. Alerts | M7 | Explicit one-shot business rules; no unqualified statistical baseline engine. |
| 13. Notifications | M8 | Real controlled HTTP/receipt acceptance, not external messaging. |
| 14. Prometheus/Grafana observability | M9 | Real bounded metrics, three dashboards, API/query/outage verification. |

**Functional completeness does not mean one autonomous always-on deployment.**
The runtime/verifier evidence covers connected stage boundaries and persisted
results; it does not prove a single clean-machine unattended run of all 14 stages
under one managed topology. M4/M5/M7/M8 advance when explicitly invoked.

The wider historical README mentions heatmaps, funnel, Z-score/EWMA/ML, Slack
and other features not in the frozen certification. If “complete” is redefined
to include every historical feature, that broader product is **PARTIAL** and
requires explicit new business scope. That is a different claim from the requested
14-stage local architecture, which is **COMPLETE within its certified contracts**.

## 14. Production Hardening vs Functional Completion

| Dimension | Post-M9 finding | Implication |
|---|---|---|
| Functional completeness | COMPLETE at the bounded local/controlled scope | No automatic need for another business capability milestone. |
| Production hardening | PARTIAL / not globally certified | Good transaction/idempotency/provenance patterns exist; deployment model, support policy, backup, exposure and operating ownership need target-specific decisions. |
| Cloud deployment | NOT IMPLEMENTED / no required target | S3-compatible design and optional Kubernetes language are not managed-cloud deployment proof. |
| Scale certification | NOT CERTIFIED | Small real fixtures/selected concurrency and M9 latency metrics do not establish throughput, capacity, load or HA. |

The most evidence-supported original purpose is a local production-style
end-to-end analytics demonstration with synthetic events, not a specified SaaS
or cloud service. Reproducibility can improve its practical usability without
making cloud, distributed scaling, authentication or external providers mandatory.
If the intended audience changes to actual users/shared production, revisit
security, lifecycle, backup and reliability priority under explicit requirements.

## 15. What MUST NOT Be Done Yet

- Do not create an M10 designation, implementation directory, schema or service.
- Do not reinterpret historical Phase 10 as M10 or silently renumber Phase 12/13.
- Do not alter any frozen M1–M9 implementation, test, contract, report/evidence,
  dependency, Compose/configuration or existing documentation during this gate.
- Do not remove legacy modules, fix Makefile/dispatcher paths or rewrite the old
  diagram as incidental cleanup. The absent `scripts.verify` dispatcher target
  and Phase-1 Make verification are not the certified milestone oracle.
- Do not weaken the M8 retry test, hide its first failure, claim the pycairo issue
  is fixed, or treat local counter resets/metadata probes as business defects.
- Do not deploy old Slack/PagerDuty code as if it were active M8.
- Do not add authentication, Kubernetes, managed cloud, OpenTelemetry or an
  orchestrator simply because they are common platform technologies.
- Do not combine packaging, security, HA, retention, upgrades, provider migration,
  CI and ML into a giant “productionization” milestone.
- Do not start runtimes, purge history, regenerate cluster identity or change
  endpoint/policy bindings to facilitate planning. Only this report is produced.

## 16. Frozen-State Integrity

**M1–M9 integrity: PASS for this audit. Code changes: NONE.**

Before inspection and after report creation, a read-only SHA-256 comparison
fingerprinted **1,058 pre-existing project files** (12,974,577 bytes), including
hidden project configuration and **898 evidence files**. It used a deterministic
sorted list of `(relative path, file SHA-256)` pairs, compact JSON serialization,
then SHA-256 of that list. New/deleted/renamed files in this scope affect the hash.
The sole excluded new report is `post_m9_roadmap_gate_report.md`.

| Verified group | Existing files | Unchanged before/after aggregate SHA-256 |
|---|---:|---|
| All pre-existing project files | 1058 | `ae5106295787b5782b6cc25c837fe445944a4b576c3b953e14067dead2291c9b` |
| Application/scripts/contracts/config within module directories + main | 89 | `b922e49e577097d15812acf619eb4267649682441247ce88bc5af3dece4feb90` |
| Tests | 11 | `a457e5dbc66a86b901507e3bcd94daddc567ee4a6ae3b46d1aad18dd8ea68008` |
| Root/exporter dependency manifests | 2 | `9e4aae48d5bde421e523b5893798ffe025530f89e7a829b6763f16ca88fdd8e7` |
| Compose/Dockerfile/CI/Makefile/active env/template | 6 | `b75af8102d98435d1cc13d3a088723f5a9a701c1ab3118063064d0b9985a9003` |
| Milestone reports (both M1 reports plus M2–M9) | 10 | `df4c141d1e79a26ad8063a86c5fe85bc6a1b312f4fca5ab04ad866f1021ec1a2` |
| Retained milestone evidence | 898 | `a4ef2619b2122d3590643e688244ce91926db7f0915c484bdd9bca75ae5c23a7` |

Groups overlap and do not sum to the total. Excluded directory names: `.venv`,
`.git`, `__pycache__`, `.pytest_cache`, `.ruff_cache` (generated tooling/dependencies,
not project roadmap). No installations, test runs, imports of application modules
or cache-generating compilation were performed. Python checksum execution used
`-B`. Active `.env` was hashed without disclosing contents. There is no Git
repository here, so integrity is based on content/path hashes rather than git diff.

No Docker start/stop/rebuild, dependency update or database/lake/cache mutation
was performed. One read-only targeted `docker.exe inspect --format` attempt
failed because the Docker Desktop Linux engine pipe was unavailable. Therefore
this audit makes **no fresh live-container or database-state assertion**. The last
retained M9 shutdown evidence records all nine OIP containers stopped; the last
M9 data audit records unchanged M4/M7/M8 source hashes. This gate read that
evidence without claiming to recertify it.

Retained anchors inspected:

- `milestone9_evidence/20261004a/audit-1791136272664836882.json`:
  return code 0, business data/AST unchanged, 63 byte-identical files and 12
  instrumentation-only differences against the pre-M9 baseline.
- `milestone9_evidence/20261004a/full-tests-1791133594764052231.log`:
  202 passed, one recorded Authlib warning.
- `milestone9_evidence/20261004a/freeze-state-1791136728149884576.log`:
  nine OIP containers recorded `exited false`.

**Only new/modified artifact in this audit:** `post_m9_roadmap_gate_report.md`.
All existing production code, tests, dependency files, Docker/CI configuration,
M1–M9 reports/contracts/evidence and existing README/PHASE_STATUS remain untouched.

## 17. Final Gate

**WAIT — ROADMAP DECISION REQUIRED.**

The discovery gate itself is **PASS**: repository plans are identified, numbering
is not invented, the local functional architecture is assessed and frozen-state
integrity is preserved.

- **Repository-defined next milestone:** none after M9 in the certified chain.
  Historical ordered backlog: **Phase 12 — Dockerization**, then **Phase 13 — CI/CD**.
- **M10 defined:** **NO**.
- **Recommended next milestone:** **Reproducible Local Runtime Packaging & Launch
  Verification**, unnumbered and planning-only.
- **Classification:** **ARCHITECTURAL RECOMMENDATION**, not repository-defined M10.
- **Functional architecture:** **COMPLETE within the 14-stage local certification**.
- **Implementation permission:** not granted by this report; adopt the scope and
  runtime/artifact decisions explicitly before any future work.

If no packaging/operational deployment outcome is wanted, stop at the already
certified M9 state. If it is wanted, use the bounded proposal rather than an
invented M10 or an all-encompassing productionization project. **Stop after this gate.**
