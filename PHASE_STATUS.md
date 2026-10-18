# Phase Status

> Historical implementation inventory, not proof of runtime completion. Runtime
> acceptance is recorded in `milestone1_current_real_world_review_report.md` and
> `milestone2_bronze_dq_silver_dlq_report.md`. DQ/Silver semantics are defined in
> `data_quality/CONTRACT.md`; downstream “COMPLETE” labels below remain unaudited
> by Milestone 2.

Milestone 3 acceptance and its scoped Gold implementation are recorded in
`milestone3_silver_gold_report.md`; `streaming/GOLD_CONTRACT.md` defines current
event-time, money, lifecycle and snapshot semantics. Historical PostgreSQL/cache/
dashboard completion labels are not Milestone 3 acceptance claims.

Milestone 4 acceptance and its scoped authoritative PostgreSQL serving
implementation are recorded in `milestone4_gold_postgres_report.md`;
`storage/POSTGRES_SERVING_CONTRACT.md` defines the additive schema, complete
snapshot replacement, idempotency and rollback semantics. Legacy PostgreSQL,
Redis, dashboard, alert and observability consumers remain outside the M4 scope.

Milestone 5 acceptance is recorded in `milestone5_postgres_redis_report.md`.
`storage/REDIS_SERVING_CONTRACT.md` defines the PostgreSQL-only, Decimal-safe,
currency-aware Redis snapshot cache and its atomic refresh/rebuild behavior.
Dashboard, alert and observability consumer migration remains outside M5.

Milestone 6 dashboard acceptance is recorded in `milestone6_dashboard_report.md`:
M5 Redis preferred, authoritative `gold_serving` fallback, explicit currency and
source/version context, Decimal-safe tables and tested unavailable/empty states.
The historical status table below is not acceptance evidence for other consumers.

Milestone 7 is **PASS and frozen** (2026-10-04): 137 passing tests, including
40 alert tests, plus real-service verification and M1–M6 runtime regressions.
Acceptance and retained evidence are recorded in `milestone7_alerts_report.md`.
`alerts/CONTRACT.md` defines authoritative gold_serving reads, currency-aware
rules, deterministic identity, PostgreSQL cooldown/state/history and restart
semantics. The alert entrypoint is one-shot; notification delivery is outside M7.

Milestone 8 is **PASS and frozen** (2026-10-04): 179 passing tests, including
42 notification tests, real PostgreSQL/controlled HTTP verification and M1–M7
runtime regressions. See `milestone8_notifications_report.md` and
`notifications/CONTRACT.md`. External provider delivery is **not certified in this
environment**; the single provider is a controlled local HTTP transport with durable
receipts. M7 source hashes remain unchanged.

Milestone 9 is **PASS and frozen** (2026-10-04): 202 passing tests (179 frozen tests plus
23 observability tests) executed inside the Linux verification container with the
local serving stack reachable — historical, environment-specific evidence, not a
native result of any single host — plus real Prometheus scrapes, three provisioned
Grafana dashboards, failure/restart verification and passing M1–M8 runtime
regressions. In this public repository the self-contained tier now collects 221
tests, run via `python main.py verify`; the other 116 service-dependent tests
require a running PostgreSQL/Redis stack, and the M9 baseline-integrity test
skips because its private `milestone9_evidence/` bundle is not published; 74
of those service-dependent tests (the M4/M5/M7 suites) run in the GitHub
Actions `service-integration` job against disposable PostgreSQL 16/Redis 7
service-container state, while the 42 M8 notification tests remain outside
CI until their retained certified-fixture dependency is removed. The
`tests/` directory collects 337 tests overall (221 self-contained + 116
service-dependent), and the current recorded `python main.py verify` run is 217
passed, 4 skipped, 0 failed across the 221-tier tests. An earlier count of 91
self-contained tests (recorded when the verification dispatcher was added
2026-10-09) is superseded and must not be read as the current inventory. See
README 'Testing and verification' for the authoritative, current figures.
See `milestone9_observability_report.md` and `observability/CONTRACT.md`. The
45 bounded operational metric families observe existing lifecycles and read-only
metadata. Twelve files have minimal instrumentation with unchanged business AST;
63 other baseline-covered files and retained M4/M7/M8 data hashes are unchanged.
Async telemetry is best-effort; external provider delivery and browser-pixel
validation remain uncertified. The historical phase numbering below does not
establish an M10 milestone; no M10 implementation is included.
Verification containers were stopped at 17:58:47 UTC, retaining data and evidence.

| Phase | Name                       | Status       |
|-------|----------------------------|--------------|
| 1     | Project Foundation         | ✅ COMPLETE  |
| 2     | Order Event Generator      | ✅ COMPLETE  |
| 3     | Kafka Infrastructure       | ✅ COMPLETE  |
| 4     | Spark + Bronze Layer       | ✅ COMPLETE  |
| 5     | Data Validation Layer      | ✅ COMPLETE  |
| 6     | Silver Layer               | ✅ COMPLETE  |
| 7     | Gold Layer                 | ✅ COMPLETE  |
| 8     | PostgreSQL Analytics Store | ✅ COMPLETE  |
| 9     | Dashboard                  | ✅ COMPLETE  |
| 10    | Alerting System            | ✅ COMPLETE  |
| 11    | Observability              | ✅ COMPLETE  |
| 12    | Dockerization              | ⬜ See notes |
| 13    | CI/CD                      | ⬜ See notes |

## Notes
Phase 12 (Dockerization) and Phase 13 (CI/CD) are partially shipped, not
absent: `docker-compose.yml` provides the full infrastructure stack with
healthchecks, `deployment/local/Dockerfile` and `observability/Dockerfile`
cover the packaged local runtime and the metrics exporter, and
`.github/workflows/ci.yml` runs two jobs — `unit-tests` (the self-contained
suites via `python main.py verify`) and `service-integration` (the 74
reproducible service-dependent M4/M5/M7 tests on ephemeral PostgreSQL/Redis
service containers). The remaining work for full closure is: per-service
Dockerfiles for the streaming jobs themselves, any build/deploy automation
(currently deliberately out of scope), and CI coverage of the 42 M8
notification tests, which is blocked until that suite stops depending on the
unpublished retained certified M7 fixture. The project is fully runnable
in the current state using docker-compose.yml for infrastructure and
running Python services directly (python main.py <service>).
