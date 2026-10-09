# Phase C local Linux/amd64 runbook

This additive package runs the frozen M1–M9 implementation inside a separate
containerd-backed Docker Engine. Its certification project is `oip-packaged`;
the outer engine is `oip-packaging-phase-c-engine`. The sealed MinIO archive,
exact image references, retained wheelhouses, complete SHA256 locks and JAR
inventory are declared inputs. `artifact_inventory.json` records their actual
identities and verification. See the root `packaging_phase_c_report.md` for the
certified results and recorded unsuccessful attempts.
The final cold-start re-verification used a second entirely empty business
namespace, `oip-packaged-final`, using these same declared built images, its own
network and ten new named volumes. It began with zero namespace containers/
volumes and independently verified empty topics, Registry subjects, SQL serving/
alert schemas, Redis keys and four buckets before launch. Both projects and the
outer engine are stopped at certification freeze.

## Prerequisites and layout

- Host Python 3 (standard library only), Docker CLI and a reachable Linux/amd64
  Docker Engine that supports a privileged nested engine. This run used WSL2 /
  Docker Desktop29.6.2, 12CPUs and approximately7.6GiB Docker memory.
- Exact official image acquisition and PyPI/Maven acquisition need networking.
  All application image builds use `--network none`; all Python installs use
  vendored wheels and `--require-hashes`. No host virtual environment is used.
- The already sealed archive must be at the project root:
  `minio_frozen_linux_amd64_sealed_20261005.tar`, 80105984bytes,
  SHA256 `58800abbac143c831a53c00fadbe9fe0af30914c83835fd52bc7a7c487fe355a`.
  MinIO/mc cannot be substituted or fetched implicitly: Compose uses
  `pull_policy: never` for both. Their image IDs in this containerd store are
  governing index identities, not OCI config digests.
- `assets/source.tar` is the exact frozen source/test/config bundle, including
  the required pre-M9 baseline fixture. Root/exporter requirement inputs are
  copied byte-for-byte. `assets/{core,spark,exporter}/requirements.lock` fixes
  every wheel version/hash, including a declared packaging-only pip installer.
  Spark/PySpark comes from the exact Spark base, not a second pip installation.
- The active root `.env` is neither copied nor consumed. `compose.yaml` contains
  clearly labeled local example credentials only. Local credentials/config
  are deployment inputs, not alterations of financial or alert rules.
- From WSL, `runtime.py` converts host `/mnt/<drive>/...` paths for
  Windows `docker.exe cp`; inner paths remain Linux paths.

## Fresh certification sequence

Execute from the project root in a fresh Docker context with the reserved
engine/data-volume names absent and no `deployment/local/state.json`. The
existing certified instance is retained/stopped; use restart below for it.
`prepare` refuses existing state, engine or engine volume and checks that the
new daemon has zero images/containers. Do not reset a retained instance to make
it appear fresh.

```bash
python3 -B deployment/local/runtime.py prepare
python3 -B deployment/local/runtime.py build
python3 -B deployment/local/runtime.py start
python3 -B deployment/local/runtime.py launch
python3 -B deployment/local/runtime.py regression
python3 -B deployment/local/runtime.py failures
python3 -B deployment/local/runtime.py extra_outages
python3 -B deployment/local/runtime.py restart
python3 -B deployment/local/metadata.py
python3 -B deployment/local/runtime.py stop
```

`all` runs prepare/build/start/launch/regression/failures/restart/stop; the
stepwise sequence additionally demonstrates M6 PostgreSQL-down cached UI and
records enriched inventory. Acquisition uses the existing complete hash locks
when present; Maven resolution must match the frozen18-JAR closure. Thus a
later resolver cannot silently update transitive versions. Every command gets
a timestamped JSON command/exit record and a corresponding diagnostic log.
Timeout exit124 also retains partial diagnostics. Unexpected exits stop that
phase; inspect evidence before resuming.

The frozen full suite is all eight `tests/unit/test_*.py` files,202cases, in one
pytest session. Tight100ms M8 HTTP tests precede Spark fixtures; optional M9 is
quiesced, test telemetry opted out, then restored. The runner asserts file-set
coverage. No test/assertion/timeout is patched. Earlier directory-order runs
had201passes/1timing failure; these logs remain. Runtime oracle fixtures are
non-reentrant: do not recreate existing topics, override baselines, or rerun
fresh-schema creation against already exercised fixtures.

## Explicit sequencing and semantics

1. `start`: broker API readiness, Registry HTTP readiness, MinIO readiness,
   PostgreSQL and Redis readiness, then idempotent mc creation of
   `bronze`, `silver`, `gold`, `checkpoints`.
2. `launch`: a real generator/KafkaPublisher Avro fixture, then bounded
   `availableNow` Bronze→Silver/DQ/DLQ→Gold drains using frozen callbacks.
   Frozen M3initial/second/third creates the controlled two-currency Gold
   serving fixture at `order-events-m3-packc20261005a`.
3. M4 API wrapper reads committed manifest-pinned Gold; M5 CLI publishes the
   authoritative PostgreSQL snapshot into Redis.
4. Actual Streamlit starts only after serving. M7 evaluates example rules in
   one shot; the receiver starts; M8 processes persisted M7 events in one shot.
5. Certification enables optional M9 and waits for the unchanged stack oracle,
   three UP targets, datasource proxy, dashboard content/PromQL and readiness.

One-shot jobs are run explicitly and exit. Long-running services are
Kafka/Registry/MinIO/PostgreSQL/Redis/dashboard/receiver and optional
exporter/Prometheus/Grafana. There is no scheduler or new business loop.

The production-path local database is `order_intelligence`; source schemas
`gold_serving`/`alerts_serving`, Redisnamespace `oip:m5:gold-serving`, and
notification target `m8_packaged` all reside solely in the new daemon's volumes.
The bridge network is explicitly named `oip-packaged_default`. Eight data/
evidence volumes and two empty Confluent image-directory volumes are named in
Compose. Two earlier image-generated anonymous directory volumes remain
unattached in the isolated engine as historical evidence; final services use
the explicit named mounts, not those anonymous directories.
Dedicated M1/M2/M3 topics/checkpoints and controlled oracle schemas are separate.
The independent M8 fixture uses newly created
`order_intelligence_m8_packc20261005ab`, fresh frozen M4/M7 invocations, two new
events and its own ledger/baseline. It does not copy retained OIP SQL state.

## Stop and restart the retained packaged instance

```bash
python3 -B deployment/local/runtime.py restart
python3 -B deployment/local/runtime.py stop
```

Restart resumes the outer daemon if stopped, deliberately starts only
PostgreSQL, converges infrastructure readiness/buckets, restores apps/optional
M9, and verifies M4 NOOP, M5 equality, M7 four NOOPs, M8 zero new attempts,
unchanged two durable receipts, dashboard data and the M9 stack. It does not
regenerate fixture topics or reaggregate Gold.
Infrastructure/application/observability containers are explicitly recreated
against their named volumes/network, preventing stale Docker endpoint IDs from
surviving a network-model change. To select the secondary retained instance:

```bash
OIP_LOCAL_PROJECT=oip-packaged-final python3 -B deployment/local/runtime.py restart
OIP_LOCAL_PROJECT=oip-packaged-final python3 -B deployment/local/runtime.py stop
```

Business-only launch/restart is supported and was verified:

```bash
python3 -B deployment/local/runtime.py restart --no-observability
python3 -B deployment/local/runtime.py stop
```

This leaves exporter/Prometheus/Grafana stopped and sets application telemetry
empty. `launch --no-observability` is the equivalent option for an unexercised
fresh fixture. Full M9 certification phases require observability.

Stop preserves all declared volumes, captures every new container's selected
state, asserts none running, retains service diagnostics, creates uniquely
timestamped consistent SQLite backups, then stops the outer daemon. An already
stopped engine is handled explicitly. Evidence snapshots are appended rather
than overwriting previous stopped-state/ledger captures.
Thin receiver/exporter entrypoints translate SIGTERM into the frozen servers'
existing close/finally paths; their final stopped exit codes must be0, rather
than a Docker timeout/SIGKILL. Kafka/Registry use their image-provided JVM
shutdown handling. This is packaging lifecycle wiring, not an HTTP/API change.

## Endpoints while running

Outer host publications are loopback-only:

| Service | Host URL |
|---|---|
| Streamlit | `http://127.0.0.1:18501` |
| Exporter | `http://127.0.0.1:18000/metrics` |
| Prometheus | `http://127.0.0.1:19090` |
| Grafana | `http://127.0.0.1:13000` |

Infrastructure has no direct outer host publication. M8 always uses
`http://127.0.0.1:18088/notify` inside the receiver-owned namespace;
notifications uses `network_mode: service:receiver`. The SQLite ledger lives
in the explicit `oip-packaged_receiver-ledger` volume. A stopped namespace
owner explicitly prevents job startup; receiver force-recreation with that
volume preserves receipts. Frozen M8 oracles separately prove real connection
refusal, due-time retry, acceptance ambiguity, fencing and same-ledger recovery.

## Evidence and integrity

- `evidence/packc20261005a/`: timestamped commands/logs, expected-failure
  records, all passing and unsuccessful frozen oracles, stopped-runtime JSON,
  consistent receiver ledger backups, frozen-input baseline, final integrity
  manifest and `SHA256SUMS`.
- `artifact_inventory.json`:12declared image references/actual descriptors,
  tags/sources/platforms/purposes, three installed Python closures,18distinct
  JARs plus explicitly inventoried vendored copies, every build artifact hash,
  runtime tuple, archive/MinIO identity distinctions and oracle fixture names.
- `audit.py` compares all1,069original path/size/SHA256 records, nine retained
  OIP IDs/images/states/restart counts/mounts, authorized new paths, archive and
  final stopped engine. The imported original baseline contains hashes and
  selected resource state only, not `.env` contents or credential dumps.
  All38retained outer image records are compared as well; only the newly
  acquired official isolation-engine image is additional. Docker's unordered
  mount-list display is canonicalized by destination/source; every mount field
  and the complete state/health/timestamp record remain strict comparisons.

After a clean stop:

```bash
python3 -B deployment/local/audit.py
sha256sum -c deployment/local/evidence/packc20261005a/SHA256SUMS
```

The audit and checksum manifest exclude their own files to avoid circular
hashes. Autonomous `.remember` changes are reported separately; application,
contracts, tests, requirements and prior reports must have zero changes.
Certification is local Linux/amd64 functional packaging, not browser-pixel,
production/HA/security/load or multi-host/all-platform certification.
