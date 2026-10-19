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
skips because its private `milestone9_evidence/` bundle is not published; the
GitHub Actions `service-integration` job is configured for all 116 in the
local workflow, and its first published run executed the earlier
74-test (M4/M5/M7) version against disposable PostgreSQL 16/Redis 7
service-container state (a later authorized run of the full 116-test job is
recorded below); that first published run
[39106581230](https://github.com/YeswanthArasavalli/order-intelligence-platform/actions/runs/39106581230)
(2026-10-18, branch `ci/service-integration`, commit
`e4096f79bbd6472b46b383bd11dde43c693dcc5c`), reported completed/success with
both the `unit-tests` and `service-integration` jobs successful, the
collection gate confirming exactly 74 collected tests, and the pytest
command reported as exiting successfully; the verbatim pytest pass/skip
breakdown was not verified because GitHub's raw Actions logs require
authentication — no "74 passed, 0 skipped" claim is made — while the 42 M8
notification tests have since been made self-seeding in code (the fixture now
publishes an isolated `m7_source_*` snapshot, runs the frozen M7 evaluator into
an isolated `m7_m8_*` alert schema and binds the service to it via its supported
`source` parameter and a unique `m8_test_*` target, and its best-effort
teardown drops each of its own three generated schemas in its own separate
transaction) and are added to the `service-integration`
job's 116-test collection/execution commands. That expanded 116-test job has
since had its first successful authorized remote CI run: run
[39140812500](https://github.com/YeswanthArasavalli/order-intelligence-platform/actions/runs/39140812500)
(commit `adaa488`, branch `ci/service-integration`) reports overall success; an
earlier authorized 116-test attempt, run
[39133404000](https://github.com/YeswanthArasavalli/order-intelligence-platform/actions/runs/39133404000)
on commit `b54bc6b`, failed during its pytest step and was superseded by the
successful run. The collection gate's `116 tests collected` and the successful
pytest step are reported from the authenticated run logs, not the
unauthenticated public page (which exposes only run identity, commit, branch,
overall success and job status), and no verbatim "116 passed" console line is
claimed here. Separately, the 42 M8 tests and the full four-suite combination
had been executed locally against a freshly provisioned disposable PostgreSQL
16.15 / Redis 7.4.11 pair (loopback-bound ports, least-privilege non-superuser
test login): the focused outage-and-recovery test passed (1 passed), the full
M8 suite passed (42 passed), and the four-suite regression passed 116/116 (0
failed, 0 skipped, 0 xfailed, 0 errors; pytest exit code 0) with exactly 116
collected. Generated
test schemas were dropped by fixture teardown and the disposable Redis
keyspace was empty afterward; the temporary resources were removed by exact
name. Local evidence logs are retained under the gitignored
`logs/phase2h_evidence/` directory. This CI success proves only the executed
116 service-dependent suites on disposable CI services — not a complete live
Kafka/Spark-to-dashboard end-to-end deployment.
The `tests/` directory collects 337 tests overall (221 self-contained + 116
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
suites via `python main.py verify`) and `service-integration` (the
reproducible service-dependent M4/M5/M7 suites — and, since the M8
self-seeding change, the M4/M5/M7/M8 suites — on ephemeral PostgreSQL/Redis
service containers). The remaining work for full closure is: per-service
Dockerfiles for the streaming jobs themselves and any build/deploy automation
(both currently deliberately out of scope); the authorized CI run of the
expanded 116-test `service-integration` job has since completed successfully
(run
[39140812500](https://github.com/YeswanthArasavalli/order-intelligence-platform/actions/runs/39140812500),
commit `adaa488`; superseding an earlier failed pytest-step attempt, run
[39133404000](https://github.com/YeswanthArasavalli/order-intelligence-platform/actions/runs/39133404000)
on commit `b54bc6b`). The M8 fixture seeds its own
source and its generated events no longer depend on retained
`alerts_serving` rows (a source-inspection property of the test code, not a
behavioral one), and the 42 M8 tests have since been executed and passed in a
verified local disposable environment (evidence under
`logs/phase2h_evidence/`), so a local M8 pass is claimed on that evidence;
the full 116-test job has since been executed in CI and reported overall
success (`39140812500` on commit `adaa488`; its per-step collection/pytest
figures are drawn from the authenticated run logs, not the public page), with
CI success claimed only for those executed suites. The `service-integration` job's
first published run —
[39106581230](https://github.com/YeswanthArasavalli/order-intelligence-platform/actions/runs/39106581230)
(2026-10-18, branch `ci/service-integration`, commit
`e4096f79bbd6472b46b383bd11dde43c693dcc5c`) — is reported completed with
overall success (both jobs successful; collection gate confirmed exactly 74
collected tests; pytest exited successfully per the existing run report; the
verbatim pass/skip breakdown was not verified from the available log
evidence). It evidences only the 74 M4/M5/M7 suites on disposable CI service
containers — not production readiness or full end-to-end pipeline coverage.
The M8 self-seeding and 116-test CI integration described above is
implemented and has since been executed locally: in a verified local
disposable environment the four suites collected exactly 116 tests and passed
116 (0 failed, 0 skipped, 0 xfailed, 0 errors; pytest exit code 0), with gitignored
logs under `logs/phase2h_evidence/`; the job has since been executed in CI and
reported overall success (`39140812500` on commit `adaa488`; per-step figures
from the authenticated run logs). CI success here is claimed only for the
executed suites, not a full live end-to-end deployment.
The project is fully
runnable in the current state using docker-compose.yml for infrastructure and
running Python services directly (python main.py <service>).
