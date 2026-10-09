# Reproducible Local Runtime Packaging & Launch Verification

**Workstream:** PHASE C — Reproducible Local Runtime Packaging & Launch Verification  
**Run:** `packc20261005a`  
**Platform:** Linux/amd64 under WSL2/Docker Desktop 29.6.2  
**Verdict:** **PASS — bounded local Linux/amd64 packaging and launch verification completed**

## 1. Scope

This workstream built and exercised an additive local packaging layer for the
already-frozen Complete Order Intelligence Platform. It covered a clean nested
containerd-backed Docker Engine, exact artifact acquisition, offline application
image builds, startup/readiness, the M1–M9 runtime path, frozen regression
oracles, failure/recovery cases, stop/restart, and retained-resource integrity.

The certification scope is one local Linux/amd64 host. It does not claim
production deployment, multi-host independence, high availability, all
architectures, load capacity, browser-pixel equivalence, or external-provider
delivery.

## 2. Frozen inputs

The authoritative prerequisite was `artifact_reverification_gate_report.md`.
The controlled MinIO input was verified before use:

| Input | Value |
|---|---|
| Archive | `minio_frozen_linux_amd64_sealed_20261005.tar` |
| Archive size | `80105984` bytes |
| Archive SHA256 | `58800abbac143c831a53c00fadbe9fe0af30914c83835fd52bc7a7c487fe355a` |
| Server reference | `minio/minio:RELEASE.2024-11-07T00-52-20Z` |
| Server governing index | `sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` |
| Client reference | `minio/mc:RELEASE.2024-11-05T11-29-45Z` |
| Client governing index | `sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` |

The frozen runtime tuple was preserved: Spark/PySpark 3.5.9, Scala 2.12.18,
Hadoop 3.3.4, hadoop-aws 3.3.4, AWS SDK bundle 1.12.262, Spark Kafka and
Avro connectors 3.5.9, PostgreSQL JDBC 42.7.3, Spark Java 17.0.19+10,
application Python 3.11.9, Spark driver/workers Python 3.10.12, and exporter
Python 3.11.17. No frozen application module, contract, test, root Compose file,
root requirements file, active `.env`, or prior report was modified.

## 3. Packaging architecture

The additive package is under `deployment/local/`. `runtime.py` creates the
privileged isolated engine `oip-packaging-phase-c-engine` with data volume
`oip-packaging-phase-c-data`; no host Docker socket, source Docker store, OIP
volume, retained verifier container, or retained application container is
mounted into it. The inner Compose project is `oip-packaged`, with a separately
verified cold namespace `oip-packaged-final`.

The package contains three built roles:

- `oip-local-core:phase-c`: application Python 3.11.9, dashboard, producer,
  M4/M5/M7/M8 and frozen tests.
- `oip-local-spark:phase-c`: exact Spark/PySpark base, Python 3.10.12 for both
  driver and workers, and the vendored 18-JAR closure.
- `oip-local-exporter:phase-c`: exporter Python 3.11.17 and its locked closure.

Infrastructure uses exact digest references for Kafka, Registry, PostgreSQL,
Redis, Prometheus and Grafana. MinIO and mc are loaded only from the verified
archive and use `pull_policy: never`. The bridge network is explicitly named
`oip-packaged_default`; application/data/evidence volumes are named. The two
empty image-directory anonymous volumes created during the first Compose model
were retired, verified empty and left unattached; final containers use explicit
`kafka-secrets` and `registry-secrets` mounts.

M8 keeps the required loopback endpoint
`http://127.0.0.1:18088/notify` inside the receiver-owned network namespace.
The receiver ledger is persisted in the named receiver volume. M9 is optional
for business operation but enabled for the full certification run.

## 4. Artifact inventory

`deployment/local/artifact_inventory.json` is the machine-readable inventory.
It records 12 exact image references, source/tag/platform/purpose/verification,
actual descriptors, and the MinIO index/manifest/config/executable identity
distinctions. The image plan is in `deployment/local/images.json`.

The retained package contains:

- 83 core wheel distributions, 86 Spark-role wheel distributions and 15
  exporter-role distributions, each represented in a SHA256 lock and verified
  after offline installation with `pip check`.
- 18 distinct Maven/JAR closure artifacts for the five direct frozen
  coordinates and their transitives. Copied classpath and Ivy-cache copies are
  separately hashed and inventoried.
- Frozen `assets/source.tar`, root/exporter requirement inputs, configuration
  material, wrapper scripts, and required M9 baseline fixture.
- Explicit archive size/hash and exact MinIO server/client executable hashes.

The final inventory records the primary packaged topic, isolated M8 database
and run label, project/network, persistent volumes, retired empty anonymous
directories and installed package closure equality. No credential values are
included in the inventory.

## 5. Files created

Additive source/configuration files created under `deployment/local/` were:

- `Dockerfile`, `compose.yaml`, `.dockerignore`, `images.json`;
- `resolve.py`, `preflight.py`, `entrypoints.py`, `oracle_adapter.py`,
  `mismatch_worker.sh`;
- `runtime.py`, `metadata.py`, `audit.py`, `RUNBOOK.md`;
- generated `root-input.txt`, `exporter-input.txt`, `state.json`,
  `artifact_inventory.json`, `assets/`, and `evidence/packc20261005a/`.

The evidence directory contains timestamped command JSON/log pairs, passing and
expected-failure output, M8 SQLite backups, stopped-container state, final
integrity data and checksum material. `packaging_phase_c_report.md` is the
required report created at the workspace root.

## 6. Existing files modified

No existing frozen application, contract, test, dependency, root orchestration,
configuration or prior evidence/report file was modified. All implementation
changes are additive packaging files under `deployment/local/` plus this report.
The packaging source tar is a copied archive of frozen source; it does not alter
the source tree.

The final integrity manifest reports zero changes to the 1,069 pre-existing
workspace files, including zero non-harness changes. No `.env` content was
copied to evidence or printed. The two retained OIP container mount-array
differences in the raw JSON comparison were ordering only; destination/source
canonicalization produced exact retained state equality.

## 7. Build process

`prepare` performed the following real operations in the initially empty inner
engine:

1. Checked the MinIO archive byte count and SHA256.
2. Loaded MinIO and mc from that archive and checked governing descriptor, image
   identity, Linux/amd64 platform, version, commit and executable hash.
3. Pulled all other declared base/infrastructure images by exact digest into the
   new engine.
4. Resolved the complete Python wheel closures and the 18-JAR closure. A later
   locked re-resolution used `--no-index --find-links` and `--require-hashes`.

`build` used three `--no-cache --network none` image builds. Every built image
passed `pip check`. The installed-closure inventory then compared installed
distribution names/versions against each full lock: core 83, Spark 86 and
exporter 15.

Actual tuple evidence is in
`actual-frozen-tuple-workers-1791222354759139324.log`:

```text
Spark/PySpark 3.5.9; driver 3.10.12; workers 3.10.12, 3.10.12;
Scala 2.12.18; Hadoop 3.3.4; AWS SDK 1.12.262;
JDBC 42.7.3; Java 17.0.19+10
```

## 8. Clean-runtime creation

The first inner daemon reported zero images and zero containers before archive
load. The archive checksum, image load and exact MinIO/mc identity checks all
passed. Its application images were then built offline and run without the
retained verifier.

A second final cold namespace, `oip-packaged-final`, was checked separately.
It began with zero project containers and zero project volumes. Before launch,
the check verified no business topics, no Registry subjects, no serving/alert
schemas, zero Redis keys and zero objects in the four buckets. It then launched
the pipeline and passed the dashboard, M7, M8 and M9 oracles. This independently
demonstrated that the package did not require the first warm packaged namespace.

## 9. Startup sequence

The implemented and exercised order was:

1. Start Kafka, Schema Registry, MinIO, PostgreSQL and Redis with health/readiness
   checks, not merely container-running checks.
2. Run mc bucket initialization after MinIO readiness for `bronze`, `silver`,
   `gold` and `checkpoints`.
3. Publish a real Avro producer fixture to a dedicated topic.
4. Run sequential available-now Bronze, Silver/DQ/DLQ and Gold drains with
   dedicated checkpoint paths.
5. Run frozen M3 initial/second/third fixture phases and obtain committed Gold.
6. Invoke M4 against the committed manifest, then M5 from authoritative
   PostgreSQL to Redis.
7. Start the actual Streamlit dashboard and check `/_stcore/health`.
8. Run one-shot M7 evaluation.
9. Start the durable receiver, then run one-shot M8 notification processing.
10. Start exporter, Prometheus and Grafana and run the unchanged M9 host oracle
    through an additive endpoint/launch adapter.

## 10. Runtime evidence

The launch evidence shows successful producer fixture, bounded pipeline,
committed Gold, M4, M5, dashboard readiness, M7, receiver, M8 receipts and M9
stack. Representative evidence files are:

- `packaged-producer-fixture-*.log`
- `packaged-bronze-silver-gold-*.log`
- `launch-m3-initial-*.log`, `launch-m3-second-*.log`,
  `launch-m3-third-*.log`
- `packaged-m4-*.log`, `packaged-m5-*.log`, `packaged-dashboard-ready-*.log`
- `packaged-m7-*.log`, `packaged-m8-*.log`,
  `packaged-provider-receipts-*.log`
- `m9-stack-*.log`, `m9-activity-*.log`, and controlled M9 activity logs.

The main serving fixture produced Gold version 3 with 18 revenue rows and 13
product rows. M4 restart returned a NOOP for the same committed snapshot,
proving restart did not reaggregate or create a new business result.

## 11. M1-M9 regression evidence

The frozen regression sequence passed as follows:

| Area | Real checks passed |
|---|---|
| M1 | Fresh wire/schema/Bronze delivery and checkpoint restart identity |
| M2 | Initial, restart and recovery DQ/Silver/DLQ behavior |
| M3 | Replay, replay and recovery committed Gold behavior, plus initial/second/third launch fixture |
| M4 | Initial, retry, failure, stale/old and corrupt snapshot cases |
| M5 | Serve, verify and rebuild equality cases |
| M6 | Production, healthy, missing, corrupt and rebuild Redis/PG fallback cases |
| M7 | Production, restart and controlled cooldown/state cases |
| M8 | Fresh two-event baseline, runtime, Redis-down and integrity checks |
| M9 | Fresh activity, process restart, controlled serving/alerts/notifications/no-data and stack checks |

The final complete frozen suite ran all eight `tests/unit/test_*.py` files in one
session: **202 passed, 1 existing warning**. The passing evidence is
`frozen-full-suite-http-before-spark-1791221352765747612.log`. No assertion or
timeout was rewritten. An earlier directory-order attempt recorded 201 passed
and one timing failure; the final ordered run passed all 202 cases and both
records are retained honestly.

## 12. Failure/recovery evidence

All required failure classes were executed with explicit diagnostics:

1. Missing archive and wrong archive SHA256/size were rejected before load.
2. A nonexistent PostgreSQL endpoint produced a bounded nonzero one-shot job.
3. A real Python 3.11.9 worker in the Spark role was initialized and rejected
   by the 3.10.12 driver/worker preflight.
4. Removing one of the 18 connector artifacts produced an explicit missing
   connector error naming the JAR.
5. PostgreSQL down was checked through M5/M7/M8 and M9 failure paths.
6. Redis down, PostgreSQL rebuild, Redis rebuild and dashboard fallback were
   checked with the business path still operating where the contracts require.
7. Receiver interruption produced namespace-unavailable failure, then
   force-recreation with the same named ledger delivered/reconciled correctly.
8. Prometheus and Grafana unavailable cases caused the M9 stack oracle to fail
   explicitly while business and alert operations continued.
9. Exporter unavailable was checked as an optional observability outage.
10. M8 PostgreSQL/Redis and receiver recovery cases passed with durable ledgers.

Representative diagnostic files include
`driver-worker-mismatch-*.log`, `missing-connector-*.log`,
`receiver-namespace-unavailable-*.log`, `m9-stack-*.log` with expected exit 1,
and the `*-restore`, `*-rebuild` and `m8-restored` logs. Failure output contains
no real credentials or provider secrets.

## 13. Restart evidence

The packaged engine and all application services were stopped, then restarted
from the declared state. The restart deliberately began with PostgreSQL only,
waited for infrastructure readiness and bucket initialization, recreated
application/observability services against named volumes/network, and passed:

- M4 NOOP against the existing committed Gold snapshot;
- M5 refresh/equality;
- M7 NOOP behavior;
- M8 zero-new-attempt repeat with the same durable receipts;
- dashboard production health and M9 stack checks.

The outer engine itself was stopped and resumed before this partial-startup
test. A business-only `--no-observability` restart was also exercised. Final
stop captured all new container state, persistent volume names, service logs and
timestamped SQLite backups before stopping the outer engine.

## 14. Dashboard evidence

The packaged dashboard is the actual `streamlit run dashboard/app.py` process.
The production M6 oracle passed the real Streamlit AppTest and HTTP health URL;
the configured health path is `/_stcore/health` and is exposed through the
declared `OIP_DASHBOARD_HEALTH_URL`. The dashboard served Redis data when valid,
fell back to authoritative PostgreSQL when Redis was missing/corrupt/unavailable,
and preserved provenance and dashboard row equality.

The final cold namespace independently passed the production dashboard oracle.
No fake health endpoint or replacement dashboard was added.

## 15. M7 evidence

M7 ran as the frozen one-shot evaluator against PostgreSQL serving data and the
existing example rules. The packaged launch produced two firing events and the
expected four-rule evaluation state. Production, restart, controlled, Redis-down,
PostgreSQL-down and fresh suppression activity passed. The M8 isolated fixture
used two freshly created events in the fresh packaged database, satisfying the
full M8 test precondition without using retained alert rows.

## 16. M8 evidence

M8 used the frozen notification service and the frozen controlled receiver. The
endpoint remained the required loopback URL, and the processor remained a
one-shot job. Launch delivered two requests and the independent receipt oracle
verified two durable receipts. The isolated M8 run passed runtime, retry,
idempotency, receiver interruption/recovery and source-integrity checks.

The full M8 oracle’s first run exposed a real timing sensitivity while unrelated
Spark activity was concurrent. That run is retained as a failed diagnostic. A
fresh database/run (`order_intelligence_m8_packc20261005ab`) was then created,
with two fresh M7 events, and the unchanged oracle passed. Durable backups record
the packaged receiver and isolated M8 ledgers without exposing payload secrets.

## 17. M9 evidence

The optional observability stack passed when enabled:

- exporter health/readiness and exposition;
- Prometheus readiness, three required scrape jobs and UP targets;
- actual PromQL queries and metric-label allow-list;
- Grafana health, Prometheus datasource UID `prometheus-oip`, proxy query and
  provisioned dashboard content/panel counts.

Fresh Gold, Redis/dashboard, alert and controlled notification activity was
generated before the M9 activity oracle. The frozen application-restart adapter
changed only historical host/container launch wiring; it did not change oracle
assertions. Prometheus, Grafana and exporter outage behavior was separately
verified as fail-open for core operations where required.

## 18. Integrity/hash evidence

`deployment/local/evidence/packc20261005a/final-integrity.json` records:

- 1,069 pre-existing files and 93,303,760 pre-existing bytes;
- baseline aggregate SHA256
  `9db731054bb2477c61849bba7aaf1502ca5f3a3da503745900938e7afbba86de`;
- zero frozen existing-file changes and zero unexpected additions;
- retained OIP container exact-state equality and all nine retained containers
  stopped;
- new packaging engine stopped;
- exact archive hash and size;
- named-volume/ledger evidence and generated artifact hashes.

The authoritative prior artifact gate recorded the retained Docker image
inventory fingerprint `c0c4d4fed5b4abd180a4c981fa817d0a3a6772b86b7f665d16d7a440588c5bfe`
and retained-state fingerprint
`f1f2f7305080a496f1128a8b6fff1d486f9162fc17e031fec6ed6886debc51fa` before
Phase C. The package’s final live audit retained exact OIP IDs, images, states,
restart counts and mounts; only Docker inspect array ordering differed and was
canonicalized.

The final daemon was intentionally stopped before a later optional Docker image
baseline helper could be rerun; Docker Desktop’s Linux endpoint then became
unavailable. The report therefore does not claim a new live image comparison at
that later point. The retained local image baseline and prior authoritative
fingerprint remain preserved evidence. The offline final rehash records this
qualification rather than presenting it as a live Docker check.

## 19. Limitations

- Certification is Linux/amd64 on a local WSL2/Docker Desktop host and requires
  a privileged nested Docker Engine with sufficient local resources.
- Initial exact-digest base/infrastructure acquisition requires registry/network
  access. Application image builds and packaged Python installation are offline
  from retained, hash-locked artifacts. This is not a complete air-gapped
  registry mirror.
- MinIO/mc are certified from the sealed Linux/amd64 archive only; other
  architecture payloads and legacy image-store index semantics are outside the
  scope.
- Observability is optional for core business operation; the full M9 result
  requires the optional stack enabled.
- The final report certifies functional contracts and frozen oracles, not load,
  stress, HA, DR, backup/retention, security, browser pixels or multi-host use.
- The Docker Desktop endpoint became unavailable only after all final runtime
  stop operations and live integrity checks had completed. The retained evidence
  records the endpoint qualification and no post-stop resource mutation was
  attempted.

## 20. Explicit exclusions

No Kubernetes, cloud deployment, HA, distributed production orchestration,
CI/CD, scheduling conversion, autoscaling, security redesign, external
notification provider, browser-pixel certification, load/stress certification,
ML, FX, business metric, schema, accounting, backup/retention redesign or
technology upgrade was implemented. No successor milestone label was created.

## 21. Final verdict

**PASS for the bounded objective: Reproducible Local Runtime Packaging & Launch
Verification on Linux/amd64.**

The clean runtime was created from declared artifacts, the exact sealed MinIO
archive was checksum-verified and loaded, all three application images were
actually built offline, the complete M1–M9 path launched, the actual dashboard,
M7, M8 and M9 worked, 202 frozen tests passed, required failures and recoveries
were exercised, cold-start independence was demonstrated, stop/restart passed,
and retained OIP resources and frozen files were preserved.

This PASS is limited to the scope in this report. It is not a production-ready,
multi-host, all-architecture, HA, security, scale, DR or external-provider
certification. All newly created runtime containers and the isolated engines are
stopped; evidence and declared persistent volumes are preserved. Phase C is
complete and execution stops here.
