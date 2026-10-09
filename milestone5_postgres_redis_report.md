# Milestone 5 — Authoritative PostgreSQL → Redis Serving

## 1. Executive result

**PASS — 2026-10-04.** Implemented a deterministic, reconstructable Redis cache
of the two authoritative M4 PostgreSQL analytical tables. PostgreSQL remains the
source of truth. The retained M4 source was read without changing its tables,
serving state, or history. Redis deletion, rebuild, service outages, retries,
partial-load failure, and overlapping refreshes were verified.

Final retained source and cache:

| Property | Actual result |
|---|---|
| PostgreSQL verification schema | `gold_m4_final` |
| Default deployment schema | `gold_serving` |
| Source topic | `order-events-m3-20261003b` |
| Gold version | `3` |
| Redis verification namespace | `oip:m5:verify-20261004a` |
| Default deployment namespace | `oip:m5:gold-serving` |
| Revenue rows | 18: 17 INR, 1 USD |
| Product rows | 13: 12 INR, 1 USD |
| Active top-level Redis keys | 1 hash |
| Active hash fields | 32: 18 revenue + 13 product + metadata |
| TTL | `-1` (persistent, no expiry) |
| PostgreSQL / Redis independent digest | `26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231` |

## 2. PASS / PARTIAL / BLOCKED

**PASS.** All requested critical criteria passed within this bounded serving
contract: PostgreSQL-only source; no Gold recomputation; Decimal/currency/product
identity preservation; idempotency; new/removed/empty row replacement; rebuild;
Redis/PostgreSQL failure behavior; partial-refresh protection; namespace isolation;
independent equality; actual container verification; automated tests; and M1–M4
regressions. No requirements upgrade or unrelated downstream implementation was
made. Cache loss does not lose PostgreSQL business state.

The full relevant suite contains **60 passing tests**, including **24 M5 tests**.
One existing Authlib deprecation warning remains. The unrelated pre-existing
container `pygobject`/`pycairo` dependency issue remains (section 26).

## 3. Current-world inspection

Inspected the Redis client/builders/helpers, relevant dashboard and alert call
sites, metrics exporter Redis references, Redis settings and Compose service,
M4 source implementation/contract/DDL, existing tests, Redis README/status
references, and installed Redis/PostgreSQL runtime. Searches for the Redis helper
names found no active analytical writer calling the legacy builders from M4.

| Question | Finding |
|---|---|
| A. What Redis stored | Actual `docker.exe exec oip-redis redis-cli --scan` returned no keys at start. Legacy code defines KPI/leaderboard/alert values. |
| B. Consumers | Dashboard calls `get_summary`; imports but does not call leaderboard helper. Alert engines use cooldown `is_alert_active` / `set_alert_active`. Metrics exporter reads PostgreSQL, not Redis analytics. Prometheus config references a Redis exporter target. |
| C. Keys | `kpi:revenue:1min:<region>:<start>`, `kpi:summary:latest`, `leaderboard:products:5min:<start>`, `alert:active:<type>:<region>`; heatmap/list key builders also exist. |
| D. Serialization | JSON; revenue builder converts money to float, recomputes net and rounds to two places; timestamp uses `str`. |
| E. TTL | Revenue 600s, leaderboard 300s, summary/dashboard 60s, alert cooldown 900s. |
| F. Invalidation | TTL-based; no complete-snapshot replacement; empty input is ignored. |
| G. Structures | Per-window/region revenue, global summary, top-ten product list, alert cooldown payload. |
| H. Builder source | Caller-supplied rows; builders do not themselves query PostgreSQL or Gold. Comments describing post-Spark/cache-aside population exceed actual wiring. |
| I. M4 conflicts | No currency/window-end identity, product identity collapsed into one window list, float conversion/cross-currency summary, missing source-version metadata, nontransactional revenue pipeline. Product JSON also lacks a Decimal-aware encoder. |
| J. Safe reuse | Existing installed Redis/SQL client libraries, environment conventions, M4 schema/identity/digest contract. Preserve legacy helper interfaces for existing consumers; do not reuse their monetary builders for M5. |

No pre-existing Redis tests were found. The old `Makefile` has a global flush
target; it was neither invoked nor used for M5 refresh/verification.

## 4. Existing Redis architecture

`storage/redis_client.py` is a lazy singleton redis-py client, DB 0, decoded
responses, five-second connection/socket timeouts. Its revenue builder uses
`pipeline(transaction=False)`; that is batching, not atomic snapshot publication.
Summary reads catch errors and return `None`. Alert cooldowns are operational
keys with TTLs and remain a separate legacy contract.

Compose uses `redis:7-alpine`, a 256 MB maximum, `allkeys-lru`, AOF with
`appendfsync everysec`, and RDB saves. Actual server: **Redis 7.4.9**. This eviction
policy motivated a **single hash per active analytical snapshot**: metadata and
rows cannot be evicted independently. No Redis server setting was changed.

## 5. M4 PostgreSQL source contract

M5 reads `serving_state`, `revenue_by_minute`, and `product_rankings` using a single
`REPEATABLE READ` transaction with `SET TRANSACTION READ ONLY`. It does not call
M4 schema creation, Gold readers, Parquet, Kafka or Spark. `serving_history` is
read by the independent verification harness to prove it remains unchanged;
it is not an input to the cache builder.

The state must be `SERVED`, contract `1.0`, with valid version/digest metadata.
Row counts, full identities, UTC timestamps, integer counts, Decimal values and
the recomputed logical digest must agree with the state record. A missing,
unavailable or corrupt source fails before cache publication.

Runtime SQL inspection confirmed timestamp-with-time-zone dimensions, text
identifiers, BIGINT counts and NUMERIC(38,8) financial columns in `gold_m4_final`.
Its original `served_at` remained `2026-10-04T04:07:52.628308+00:00`.

Relevant read-only SQL checks executed through `docker.exe exec oip-postgres
psql -U oip_user -d order_intelligence -c ...` included:

```sql
SELECT table_name, column_name, data_type, numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_schema='gold_m4_final'
  AND table_name IN ('revenue_by_minute','product_rankings')
ORDER BY table_name, ordinal_position;
SELECT source_topic, gold_version, snapshot_digest, revenue_rows, product_rows, served_at
FROM gold_m4_final.serving_state;
SELECT currency, count(*) FROM gold_m4_final.product_rankings GROUP BY currency;
```

Source provenance copied from PostgreSQL (never dereferenced by M5):

```text
manifest_path: s3a://gold/order-events-m3-20261003b/_commits/00000000000000000003.json
manifest_digest: 999d54805e0eef990514cfaa34101a88bcdee239e73c584be2424db6e7d5e424
snapshot_identity: ee26b6e28a8a3d5a8f1b52970af4dcfed5accac0b530c675818a1016d533420f
snapshot_digest: 26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231
```

## 6. Redis data/key contract

Full specification: [storage/REDIS_SERVING_CONTRACT.md](storage/REDIS_SERVING_CONTRACT.md).
Production API/CLI: `storage.postgres_redis_serving.serve_postgres_snapshot()` /
`python -m storage.postgres_redis_serving` with existing environment loaded.

```text
<namespace>:current             HASH: active complete snapshot
<namespace>:stage:<UUID hex>    HASH: private attempt

Hash fields:
@metadata
revenue:<b64(start)>:<b64(end)>:<b64(region)>:<currency>
product:<b64(start)>:<b64(end)>:<b64(product_id)>:<b64(product_name)>:<currency>
```

`b64` is URL-safe Base64 of UTF-8, without padding; empty dimension uses `_`.
Namespace grammar is `oip:m5:[a-z0-9][a-z0-9_-]{0,79}`. This prevents nested
namespace/wildcard escapes; encoded dimensions cannot alter key structure.

Values are JSON with sorted field names, compact separators, ASCII escaping and
NaN disabled. Revenue and product JSON have exactly their respective M4 columns.
Counts are integers, money fixed-eight-place strings, timestamps UTC strings
with six fractional digits and `+00:00`. Nullable average is JSON `null`.

No KPI summary was added: revenue/product snapshots and metadata meet the current
verified data scope. Heatmaps, funnels, baselines and new metrics are not served.

## 7. Currency handling

Currency is an explicit dimension in every financial row identity, both in the
hash field and JSON value. It must match `[A-Z]{3}`. INR and USD remain distinct
even when every other dimension matches. There is no FX conversion or
cross-currency total. Actual M4 runtime data has both currencies in both tables;
synthetic PostgreSQL fixtures additionally test same-window collisions.

## 8. Decimal handling

PostgreSQL drivers must supply Decimal, not floats. Validation uses precision 80
and rejects nonfinite values, excess scale/range and negative non-net metrics.
JSON preserves eight fractional places exactly; readers return monetary strings
for callers to convert to Decimal when needed. Net/average are copied as stored.

Tests cover positive maximum `999999999999999999999999999999.99999999`, full
negative net range, `-30.00000001`, nullable average and exact UTC microseconds.
The independent verifier parses monetary text to Decimal and compares all values.

## 9. Version metadata

`@metadata` contains `cache_contract_version`, `source_database`, `source_schema`,
all M4 source provenance/version/digest fields, counts, `served_at`, and
`serving_status`. Active state is `SERVED`; incomplete staging is `STAGING`.
`served_at` is copied from PostgreSQL rather than fabricated on refresh.

Runtime metadata identifies `order_intelligence.gold_m4_final`, source topic
`order-events-m3-20261003b`, Gold version 3, contract 1.0 and the digest above.
Metadata and rows become active together; no separate state/pointer can outlive
an evicted set of analytical rows.

## 10. Idempotency

Same version alone does not imply NOOP. The complete expected hash, including all
row fields and metadata, must match existing Redis bytes. A watched read then
confirms no concurrent refresh changed it. Retry and separate-process restart
return `NOOP` with identical logical metadata and values.

A missing/corrupt cache row with intact source metadata triggers full repair.
An older PostgreSQL version is rejected while a newer active Redis version exists.
Different source identity or conflicting same-version metadata is rejected.
Malformed metadata requires explicit cache invalidation before rebuild.

## 11. Complete refresh semantics

Each accepted refresh copies the entire source snapshot. The tested transition
`A,B,C → A,B,D` removes C, preserves A/B and introduces D. Product rows removed
from PostgreSQL disappear in the same publication. A newer empty committed source
produces an active hash containing only metadata. No delta/upsert accumulation or
independent ranking/metric computation occurs.

## 12. Atomicity / partial-refresh protection

The writer WATCHes the current key, reads/validates PostgreSQL, builds a private
UUID staging hash in chunks of 500 fields with `STAGING` metadata, and verifies
all staged fields. It then marks the complete staged metadata `SERVED`, WATCHes
that key and verifies it again. One mutating `RENAME(stage,current)` inside
watched MULTI/EXEC replaces the active hash atomically.

The reader uses one HGETALL for rows plus metadata and validates keys/counts/digest.
Readers never use staging keys. Tests interrupt a 504-field load after 500 fields:
staging still says `STAGING`, and the previous complete active hash is unchanged.
After recovery, all 504 fields are published. Other failure hooks cover after
staging and immediately before publication.

Overlapping-refresh tests interleave a version 3 publication while version 2 is
staged; WATCH aborts the older attempt. Stage eviction/deletion before publication
also aborts without replacing the old cache. Only this attempt's staging key is
cleaned up. This is optimistic conflict detection, not a distributed lease/lock.

## 13. Rebuild behavior

`delete_cache(client, namespace)` deletes only the exact active hash. The runtime
verifier deletes `oip:m5:verify-20261004a:current`, checks it is absent, rebuilds
from PostgreSQL, and independently verifies equality. PostgreSQL rows, state and
history compare unchanged before and after. Repeated final rebuild produced the
same version 3 digest and 32 hash fields.

Redis is not a backup. Deleting it also deletes its local prior-version guard;
the current authoritative PostgreSQL state then defines the rebuild.

## 14. Redis failure behavior

Actual container outage commands were executed:

```bash
docker.exe stop oip-redis
# separate serving verification while Redis is down
docker.exe start oip-redis
```

The serving attempt reported failure, PostgreSQL remained unchanged, and recovery
verification succeeded with the same 18/13 rows and digest. AOF retained the old
complete cache, so recovery was an idempotent NOOP. Explicit cache-loss recovery
was separately proved by deletion/rebuild.

If a response is lost after RENAME, completion is ambiguous to the caller. Retry
checks complete contents. There is no cross-system transaction or false guarantee
that every transport exception means publication did not happen.

## 15. PostgreSQL failure behavior

Actual `docker.exe stop oip-postgres` followed by serving produced a clear failure.
Redis bytes remained unchanged and still identified their existing version.
`docker.exe start oip-postgres` restored the source; retry and independent equality
passed. No fabricated refresh, timestamp change or alternate Gold read occurs.

The cache may remain readable during a PostgreSQL outage, labeled with its last
successfully copied version. No guarantee of current PostgreSQL freshness is made
while PostgreSQL cannot be checked.

## 16. TTL policy

M5 uses no TTL; final `TTL(current)` was `-1`. Legacy TTL variables remain scoped
to legacy helpers. Persistent analytical caching is appropriate because the source
has explicit committed versions. Complete refresh removes absent rows; TTL is not
used as version control. Redis memory pressure can evict the whole active hash,
which becomes a miss requiring rebuild; PostgreSQL remains correct.

## 17. Namespace policy

All M5 keys begin with the validated `oip:m5:<slug>:` prefix. Production delete
operations target the exact current key or a generated attempt UUID. No global
flush was used. Tests assert unrelated sibling-namespace keys survive serving and
deletion, and reject wildcard/path/nested-namespace input.

Successful RENAME removes the old active hash and the staging name. Failure
cleanup deletes only the owned stage. Process kill/unavailable cleanup may leave
inactive stage hashes; with workers stopped they can be removed via exact
namespace-qualified SCAN/DEL maintenance. No automated sweep can delete another
running worker's stage. Ordinary completed runtime refreshes left one key.

## 18. Automated test matrix

All tests in `tests/unit/test_redis_serving.py` use actual PostgreSQL/Redis and
newly generated synthetic M4-format schemas/namespaces; teardown removes only
those disposable fixtures. Historical M1–M4 data is not used for destructive tests.

| Required case | Automated evidence / expectation |
|---|---|
| 1 Initial serving | `test_initial_and_final_independent_equality`: source equals cache |
| 2 Same-version retry | `test_same_version_noop_preserves_values_metadata_and_ttl`: exact bytes and NOOP |
| 3 New version | `test_new_version_replaces_removed_rows`: version 2 complete state |
| 4 Removed row | Same test: C absent after A/B/D replacement |
| 5 Multiple currencies | `test_currencies_decimal_negative_net_and_product_identity`: same-window INR/USD |
| 6 Decimal precision | Same test plus full-negative-range test: exact NUMERIC(38,8) bounds |
| 7 Negative net | `-30.00000001` and full negative range preserved |
| 8 Product identity | Same ID with distinct name, currency and window: four distinct entries |
| 9 Delete/rebuild | `test_cache_deletion_rebuild_leaves_postgres_untouched` |
| 10 Redis unavailable | `test_redis_unavailable_keeps_postgres`; actual stopped-container test also passed |
| 11 PostgreSQL unavailable | `test_postgres_unavailable_no_fake_refresh`; actual stopped-container test also passed |
| 12 Partial refresh | Three failure stages plus `test_failure_midway_through_multichunk_load` |
| 13 Process restart | `test_restart_process_is_idempotent`: new Python process, NOOP |
| 14 Old source version | `test_old_postgres_version_rejected`: no Redis rollback |
| 15 Corrupt source | Digest mismatch, negative gross, NaN and negative count rejected |
| 16 Empty snapshot | `test_empty_committed_snapshot`: metadata only, zero rows |
| 17 Namespace isolation | `test_namespace_isolation_and_key_encoding` |
| 18 Final equality | Independent `verify()` used across successful transitions |

Additional cases cover cache-row repair, competing publication, staging eviction,
same-version metadata conflict, row/byte resource limits, nullable average and
microsecond timezone normalization. Parameterization yields 24 M5 tests.

## 19. Automated test results

Final source revision results:

```text
M5 focused run:       24 passed in 3.62s (before final STAGING status refinement)
Final full suite:     60 passed, 1 warning in 77.75s
Breakdown:           26 M2/M3 + 10 M4 + 24 M5
Ruff:                All checks passed
Compile/import:      passed
Compose validation:  passed
```

Final full suite includes the STAGING refinement and assertion. An earlier
process-restart test exposed an unnecessary optional dotenv import in the CLI;
the CLI now uses the supplied environment directly. It passes in a fresh process.

Exact commands run (workspace root):

```bash
python3 -m scripts.capture_milestone5_check --step tests --namespace oip:m5:verify-20261004a
python3 -m scripts.capture_milestone5_check --step full-tests --namespace oip:m5:verify-20261004a
".venv/Scripts/python.exe" -m ruff check storage/postgres_redis_serving.py scripts/verify_milestone5.py scripts/capture_milestone5_check.py tests/unit/test_redis_serving.py
".venv/Scripts/python.exe" -m compileall -q storage/postgres_redis_serving.py scripts/verify_milestone5.py scripts/capture_milestone5_check.py tests/unit/test_redis_serving.py
docker.exe compose config --quiet
docker.exe exec -e PYTHONPATH=/opt/oip oip-modernization-verify python3 -c 'import storage.postgres_redis_serving; import scripts.verify_milestone5; print("M5 imports passed without Spark or Gold dependencies")'
```

The full-suite wrapper invokes Spark's actual Python/JVM through
`/opt/spark/bin/spark-submit /opt/oip/scripts/run_spark_tests.py tests/unit`.
No unit tests were skipped. Final result evidence:
`milestone5_evidence/oip-m5-verify-20261004a/full-tests-1791091745306837998.{json,log}`.
It ran from `2026-10-04T05:27:44.642516+00:00` to
`2026-10-04T05:29:05.306705+00:00` (wrapper timestamps).

## 20. Real runtime results

Source: retained M4 PostgreSQL 16.14 schema `gold_m4_final`.
Target: actual Redis 7.4.9 container, DB 0, isolated M5 namespace.
Application: redis-py 5.2.0, Python 3.10.12 verification container.

```bash
python3 -m scripts.capture_milestone5_check --step serve --namespace oip:m5:verify-20261004a
python3 -m scripts.capture_milestone5_check --step serve --namespace oip:m5:verify-20261004a
python3 -m scripts.capture_milestone5_check --step rebuild --namespace oip:m5:verify-20261004a
python3 -m scripts.capture_milestone5_check --step outages --namespace oip:m5:verify-20261004a
python3 -m scripts.capture_milestone5_check --step dependencies --namespace oip:m5:verify-20261004a
```

The capture wrapper's default schema is explicitly a verification default,
`gold_m4_final`. Production module and independent verifier default to `gold_serving`.
Each JSON records exact command, return code, start/end UTC timestamps and result;
paired logs record stdout/stderr. No password arguments are recorded.

Evidence directory: `milestone5_evidence/oip-m5-verify-20261004a/`.

| Evidence stem | Result |
|---|---|
| `serve-1791090902648635238` | Initial SERVED at 2026-10-04 05:15:02 UTC |
| `serve-1791090903535840969` | Separate-process NOOP |
| `rebuild-1791090904444920600` | Explicit deletion/rebuild, PostgreSQL unchanged |
| `redis-down-1791090947521021338` | Redis stopped, failure reported, PostgreSQL unchanged |
| `serve-1791090954305578233` | Redis recovery, equality and NOOP |
| `postgres-down-1791090959827918959` | PostgreSQL stopped, Redis unchanged |
| `serve-1791090969382867164` | PostgreSQL recovery, equality and NOOP |
| `rebuild-1791091746147220756` | Final source revision: deletion/rebuild SERVED |
| `serve-1791091746991489348` | Final source revision: retry NOOP at `2026-10-04T05:29:06.933089+00:00` |

For outage orchestration, each service is restarted in `finally` and its Docker
healthcheck awaited. No container volume or historical schema was reset.

## 21. Independent PostgreSQL-vs-Redis equality verification

`scripts/verify_milestone5.py` reads PostgreSQL with psycopg2 in a read-only
repeatable-read transaction and Redis with HGETALL. Its own table-column lists,
Base64 dimension decoder, Decimal parsing, UTC normalization, row sorting and
SHA-256 computation are independent of the writer's helpers. It compares all
columns, full identities, uniqueness, currencies, counts, metadata and digests.
Matching row counts alone are never accepted as equality.

```text
PostgreSQL digest = Redis digest = serving_state.snapshot_digest
= 26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231
PostgreSQL rows = Redis rows = 18 revenue / 13 product
Redis keys = 1; hash fields = 32; TTL = -1
```

Representative actual rows (all amounts shown as exact JSON strings):

| Table | UTC window | Identity | Metrics |
|---|---|---|---|
| Revenue | 2026-10-03 14:50–14:51 | north-india / INR | orders 1, gross `100.00000000`, net `100.00000000` |
| Revenue | 2026-10-03 14:55–14:56 | north-india / INR | orders 1, refund count 1, refund `30.00000000`, net `70.00000000` |
| Revenue | 2026-10-03 16:10–16:11 | north-india / USD | orders 1, gross/net/average `20.00000000` |
| Product | 2026-10-03 14:50–14:55 | prod-3001 / Case 1 / INR | units 1, revenue `100.00000000` |
| Product | 2026-10-03 14:55–15:00 | prod-3002 / Case 2 / INR | units 1, revenue `100.00000000` |

The evidence contains complete representative arrays and exact timestamp strings.
M5 verifies that M4 metadata and `serving_history` remain identical across runtime
serving, rebuild and Redis failure. There is no PostgreSQL mutation in M5.

## 22. M1 regression

**PASS** — fresh topic `order-events-m1-m5-20261004a`, 4 producer/Bronze records,
actual publisher/registry/wire/Bronze checks.

```bash
python3 -m scripts.capture_milestone3_check --step m1 --topic order-events-m1-m5-20261004a
```

Evidence: `milestone3_evidence/order-events-m1-m5-20261004a/m1-1791091092251188991.json`.

## 23. M2 regression

**PASS** — fresh topic `order-events-m2-m5-20261004a`.

| Phase | Bronze | Silver | DLQ | Audit |
|---|---:|---:|---:|---:|
| initial | 32 | 11 | 19 | 32 |
| restart/fresh checkpoint | 35 | 12 | 19 | 70 |
| recovery | isolated source fixture | 1 | 1 | recovery guards passed |

```bash
python3 -m scripts.capture_milestone3_check --step m2-initial --topic order-events-m2-m5-20261004a
python3 -m scripts.capture_milestone3_check --step m2-restart --topic order-events-m2-m5-20261004a
python3 -m scripts.capture_milestone3_check --step m2-recovery --topic order-events-m2-m5-20261004a
```

Evidence under `milestone3_evidence/order-events-m2-m5-20261004a/`:
`m2-initial-1791091126820924602`, `m2-restart-1791091168249859395`,
`m2-recovery-1791091190534220790` (each JSON/log).

## 24. M3 regression

**PASS** — replay of `order-events-m3-20261003b`: 35 Bronze, 2 DLQ, 33 unique Gold
events, 18 revenue rows, 13 product rows, committed version 3 unchanged. Deliberate
duplicate Silver replay increased physical Silver rows to 1056; Gold deduplication
preserved the authoritative 33 events.

M3's differently normalized legacy oracle digest stayed
`8263508ee79077227be47a2a550111b4b65e45daf01e28bed149eae5c0e57579`.
The M4/M5 canonical digest remained `26ec1b51…2a231`.

```bash
python3 -m scripts.capture_milestone3_check --step replay --topic order-events-m3-20261003b
```

Evidence: `milestone3_evidence/order-events-m3-20261003b/replay-1791091236453105792.json`.

## 25. M4 regression

**PASS** — fresh isolated schema `gold_m4_m5reg20261004` used the existing M4
verifier without modifying `gold_m4_final`. Initial v1→v3, retry, failure rollback,
old-version rejection and corrupt/uncommitted-source handling all passed.
Final counts 18/13 and digest `26ec1b51…2a231` matched Gold independently.

```bash
python3 -m scripts.capture_milestone4_check --phase initial --topic order-events-m3-20261003b --schema gold_m4_m5reg20261004
python3 -m scripts.capture_milestone4_check --phase retry --topic order-events-m3-20261003b --schema gold_m4_m5reg20261004
python3 -m scripts.capture_milestone4_check --phase failure --topic order-events-m3-20261003b --schema gold_m4_m5reg20261004
python3 -m scripts.capture_milestone4_check --phase old --topic order-events-m3-20261003b --schema gold_m4_m5reg20261004
python3 -m scripts.capture_milestone4_check --phase corrupt --topic order-events-m3-20261003b --schema gold_m4_m5reg20261004
```

Evidence under `milestone4_evidence/order-events-m3-20261003b/`:
`initial-1791091251255346225`, `retry-1791091268289122654`,
`failure-1791091278335324114`, `old-1791091291026066022`,
`corrupt-1791091303552836636` (each JSON/log).

## 26. Dependency review

| Component | Actual/retained version | Classification |
|---|---|---|
| redis-py | 5.2.0 host and verification container | KEEP |
| Redis server | 7.4.9 from existing `7-alpine` image | KEEP |
| SQLAlchemy | 2.0.36 | KEEP |
| psycopg2 | 2.9.10 | KEEP |
| PostgreSQL | 16.14 | KEEP |
| Spark/PySpark, Kafka/Avro connectors | 3.5.9 | NO CHANGE |
| Hadoop / AWS SDK | 3.3.4 / 1.12.262 | NO CHANGE |
| fastavro / confluent-kafka | retained verified 1.12.2 / 2.15.1 | NO CHANGE |
| Java / Scala | Java 17.0.19 runtime / Scala 2.12 connectors | NO CHANGE |

No UPDATE NOW was needed; no speculative upgrade was scheduled. `requirements.txt`
was unchanged. Redis was initially absent from the minimal verification container;
installed the **already pinned** dependency with:

```bash
docker.exe exec oip-modernization-verify python3 -m pip install redis==5.2.0
```

This added redis 5.2.0 and its Python-3.10 transitive async-timeout 5.0.1, not a
requirements version upgrade. Runtime Python is 3.10.12; host venv uses the
existing selected interpreter. M5 imports without Spark on the runtime path.

Host `.venv/Scripts/python.exe -m pip check`: **No broken requirements found**.
Container `python3 -m pip check`: existing unrelated
`pygobject 3.42.1 requires pycairo, which is not installed`. M5 does not use it.
The full suite's existing Authlib/httpx deprecation warning is not an M5 failure.

Version capture evidence: `python-versions-1791091049658818755`,
`redis-version-1791091049941500719`, `postgres-version-1791091050129475663`.

## 27. Downstream compatibility findings

Dashboard summary assumes a currency-free platform total and INR display; product
queries use legacy SQL. The dashboard imports legacy read helpers and does not
use M5. No alias bridges M5 data to those incompatible shapes. Alerts retain their
existing cooldown keys and legacy analytics. Metrics exporter has no Redis
analytics reader and still queries legacy PostgreSQL. No consumers were silently
switched or claimed compatible. M5 offers a validated read primitive for later
explicit adoption.

## 28. Remaining limitations

- One-shot API/CLI, not a new daemon, scheduler or automatic M4 callback.
- PostgreSQL may advance after the consistent read. Redis represents its recorded
  source version until another successful refresh; no distributed transaction.
- Bounded in-memory design: 100000 rows/table and 16 MiB serialized field/value
  bytes by default. Python/Redis memory overhead is additional. HGETALL and RENAME
  are appropriate to this scope, not a large-scale throughput claim.
- One scheduled writer per namespace recommended. WATCH safely detects tested
  overlaps; no distributed lock or Redis Cluster claim.
- Redis contents are not authoritative or durable business backups. Missing cache
  requires rebuild; explicit invalidation loses the cache's prior-version guard.
- Malformed/conflicting metadata fails closed and needs explicit invalidation.
- An abrupt kill may leave inactive stage data for namespace-scoped maintenance.

## 29. Known risks

Memory pressure under existing `allkeys-lru` can evict a whole active/staged hash.
This affects availability, not PostgreSQL correctness or partial-state claims.
Consumers must use the full-snapshot read primitive or equivalent atomic read;
multiple individual field reads across refresh boundaries are not coherent.
Cached version metadata is provenance, not a freshness SLA during source outages.
Connection loss after publication is resolved by retry. Mutable Docker tags
remain an existing reproducibility consideration; installed versions are recorded.

Secrets are sourced only from environment configuration. M5 supports optional
Redis username/password without embedding them in code or evidence. SQL schema
and Redis namespace inputs are validated. No credential-bearing DSNs are printed.

## 30. Files changed

New implementation and verification files:

- `storage/postgres_redis_serving.py`
- `storage/REDIS_SERVING_CONTRACT.md`
- `scripts/verify_milestone5.py`
- `scripts/capture_milestone5_check.py`
- `tests/unit/test_redis_serving.py`
- `milestone5_postgres_redis_report.md`

Scoped documentation/configuration additions:

- `.env.example` — optional M5 namespace/source/bounds/auth configuration.
- `README.md` — current M5 entrypoint/contract, distinction from legacy keys.
- `PHASE_STATUS.md` — scoped M5 acceptance reference.

Append-only evidence was added to `milestone5_evidence/` and the existing
M3/M4 evidence directories for regressions. No commits were made; the workspace
has no Git repository.

## 31. Files intentionally unchanged

`storage/redis_client.py` legacy builders/readers/alert cooldowns;
`storage/gold_postgres_serving.py`, `storage/gold_store.py`, M4 DDL and source
contract; existing M1–M4 tests/verifiers; producer, Bronze, DQ, Silver and Gold
production code; dashboard, alerts and observability; `docker-compose.yml`,
`requirements.txt`, active `.env`, and Makefile. No future analytics, deployment or
CI/CD feature was introduced.

All named volumes and historical fixtures were preserved. Newly generated unit
test schemas/namespaces alone were removed in teardown. Verification used existing
containers plus retained named volumes, and did not reset Kafka/MinIO/PostgreSQL.

After verification, only the OIP services started for this run were stopped:

```bash
docker.exe stop oip-modernization-verify oip-schema-registry oip-kafka oip-minio oip-redis oip-postgres
docker.exe ps -a --filter name=oip --format '{{.Names}}\t{{.Status}}'
```

All six were confirmed stopped. Containers, volumes, M4 source data and the Redis
AOF-backed verification cache were retained; no volume deletion was performed.

## 32. Exact SINGLE next milestone

**Milestone 6 — Currency-aware dashboard consumption of the verified M5 Redis
snapshot, with authoritative M4 PostgreSQL fallback.** Define explicit cache
miss/staleness behavior and preserve Decimal/currency/product identities in that
consumer. This is the only proposed next milestone and was not implemented here.
