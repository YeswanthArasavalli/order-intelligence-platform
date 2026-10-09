# MILESTONE 2 — BRONZE → DQ → SILVER → DLQ

## 1. Result

**PASS** — verified on 2026-10-03 with real Kafka, Schema Registry, Spark and MinIO.

Valid events reached Silver; invalid/malformed events reached Kafka DLQ with
reasons; source replay, checkpoint restart, repeated lifecycle events and sink
recovery were exercised. Automated tests and a fresh Milestone 1 regression passed.
No existing data was reset or deleted.

Report: `milestone2_bronze_dq_silver_dlq_report.md`.
Evidence: `milestone2_evidence/` (full command output and machine-readable results).

## 2. Executive summary

- Replaced nine inconsistent validation flags with **21 explicit, documented checks**.
- Corrected refund validation, product matching, missing-field/type handling and
  separate timestamp classifications. Preserved valid zero-value unpaid cancellations.
- Added currency, payment method, status, metadata and schema-version validation.
- Preserved malformed Avro values in an additive Bronze envelope so they can reach
  DQ/DLQ. Normal order-event transport remains Confluent Avro.
- Replaced `(order_id, event_type)` deduplication with durable Kafka-coordinate
  replay detection. Repeated payments/refunds at different offsets remain distinct.
- Persisted per-record outcomes and batch summaries in MinIO; added explicit DLQ
  provenance, deterministic Silver batch paths and restart/recovery checks.
- Final primary fixture: **31 Kafka events → 35 Bronze rows including intentional
  replay copies → 12 Silver events + 19 DLQ events**. The audit contains 70 rows
  after a deliberate complete fresh-checkpoint replay.
- Fresh Milestone 1 regression: **4 delivered → 4 Bronze rows**, Avro schema ID 1.

## 3. Files inspected

Complete relevant implementations and configurations were inspected before edits:

- `streaming/bronze_writer.py`, `streaming/silver_transformer.py`,
  `streaming/spark_session.py`.
- `data_quality/validators.py`, package layout, existing empty test packages.
- `producer/order_generator.py`, `producer/kafka_publisher.py`,
  `producer/schemas/order_event.avsc`.
- `kafka/topic_config.py`, `utils/config.py`, `utils/logger.py`, `main.py`.
- `requirements.txt`, `.env.example`, relevant non-secret `.env` settings,
  Kafka/Registry/MinIO and initialization sections of `docker-compose.yml`.
- `README.md`, `PHASE_STATUS.md`, `Makefile`, DQ table definition in
  `scripts/postgres/init.sql`, historical system/Milestone 1 reports.
- `scripts/verify_milestone1_bronze.py`; targeted downstream timestamp/metadata
  references, without downstream implementation changes.

Runtime inspection also confirmed the existing DLQ configuration, all four MinIO
buckets, initially empty Silver bucket, and the actual 16-row modernization Bronze
schema. That schema had partition/offset but **no stored topic or decode envelope**.
The original dataset was re-read successfully with the new explicit schema.

## 4. Files modified

| File | Change | Reason |
|---|---|---|
| `streaming/bronze_writer.py` | Nullable decode envelope, raw base64, topic/schema/Kafka timestamp metadata, guarded exact timestamp conversion, failOnDataLoss=true | Route corrupt records without losing provenance; preserve valid Avro behavior |
| `data_quality/validators.py` | 21 null-safe rules, complete reason collection, explicit schema handling and ingestion-relative timestamp policy | Correct and auditable routing |
| `data_quality/currency_codes.py` — new | Pinned 178-code ISO 4217 reference | Validate codes without inventing an INR-only business restriction or adding a dependency |
| `streaming/silver_transformer.py` | Explicit Bronze reader, durable audit/replay index, deterministic batch writes, recovery guards, explicit DLQ envelope | Preserve events and make restart behavior testable |
| `data_quality/CONTRACT.md` — new | Rule, timestamp, Silver, identity, delivery and recovery contracts | Define guarantees and limitations |
| `.env.example` | Document source/DLQ topics and Silver versus Gold watermark behavior | Make actual configuration discoverable |
| `README.md`, `PHASE_STATUS.md` | Point to verified reports and supersede historical DQ/status claims | Distinguish implementation inventory from runtime acceptance |
| `tests/unit/test_milestone2.py` — new | Real Spark tests across required cases and edge conditions | Verify actual functions |
| `scripts/milestone2_cases.py` — new | Shared controlled fixtures | Reproducible unit/runtime cases |
| `scripts/run_spark_tests.py` — new | Pytest runner under spark-submit | Use the verified Linux JVM/Python environment |
| `scripts/verify_milestone2.py` — new | Live initial/restart/recovery/legacy verification | Prove routing and durability behavior |
| `scripts/capture_milestone2_check.py` — new | Capture exact commands, logs and JSON evidence | Preserve independently inspectable results |
| `milestone2_evidence/**` — new | Runtime results, logs, environment checks | Evidence for this report |
| This report — new | Required sixteen-section milestone record | Record completion and follow-ups |

No application dependency declaration changed.

## 5. DQ audit

| Previous finding | Current state | Action | Evidence |
|---|---|---|---|
| Documentation says 10 rules; only 9 flags | **FIXED** | Authoritative contract and RULE_NAMES enumerate 21; historical claims marked superseded | Contract, validator, tests |
| Future timestamps folded into invalid timestamp | **FIXED** | Distinct null/malformed/invalid/future/old classifications | Runtime offset 4: future_timestamp; unit boundary tests |
| Product regex missing end anchor | **FIXED** | Entire-string match, also rejecting final newline | `prod-123abc` at offset 2 → DLQ |
| Refunds bypass non-positive amount rejection | **FIXED** | Require finite positive refund amount | Negative refund at offset 12 → invalid_amount |
| Narrow schema-drift detection | **FIXED** for implemented schema | Detect missing required fields, wrong physical types and decode errors; retain raw malformed payload | Unit schema tests; runtime offsets 27–29 |
| No currency validation | **FIXED** | Pinned active ISO 4217 codes | ZZZ → DLQ; USD and INR pass |
| No payment-method validation | **FIXED** | Existing six generator values or optional null | Offset 6 fails; offset 15 passes |
| No status validation | **FIXED** | Status must match lifecycle type | Offset 7 → invalid_status |
| No event_metadata validation | **FIXED** | Null or strict JSON object; arbitrary object keys retained | Runtime malformed JSON; unit non-object/NaN cases |
| DQ results not persisted | **PARTIALLY FIXED** relative to old architecture | Persisted per-record and per-batch in MinIO; PostgreSQL/metrics wiring deferred | Durable `batches/` and `runs/` read-back |
| Inconsistent zero-input quality | **FIXED** | 0%, matching existing SQL definition | Unit test and real empty-callback summary |

Additional defects found and addressed:

- Valid unpaid cancellations with amount zero were rejected. Generator-grounded
  exception now passes; no general zero-amount exemption was added.
- NaN/infinite amounts could pass. Both now reach DLQ.
- Malformed Avro previously stopped ingestion before DQ. Record failures now
  retain raw bytes and a bounded code; Registry outages still fail for retry.
- Business-key dedup removed legitimate repeated stages and did not work across
  batches. Durable source-coordinate replay handling replaces it.
- Silver assumed existing Bronze files for schema inference and hardcoded the
  source topic. Explicit schema/topic configuration and empty-source startup pass.
- Silver overwrote Bronze batch provenance. `_batch_id` is preserved and
  `_silver_batch_id` is separate.
- DLQ lacked source topic/raw failure evidence; batch DQ results were discarded.
  Both are now persisted explicitly.
- Processing-time age checks could invalidate a delayed Silver replay. Age now
  uses immutable Bronze ingestion time where available.

The previous 24-hour arrival-age policy is **INTENTIONALLY RETAINED**, with a
distinct `old_timestamp` reason and durable quarantine. Unknown optional JSON
keys and product IDs outside the sample catalog are deliberately not rejected.

## 6. Final DQ contract

All rules have severity **ERROR**. Every applicable failure is captured in
`_failure_reason` and individual flags; booleans never remain SQL null.
New passing events route to Silver; new failing events route to DLQ. A repeated
source identity is recorded as REPLAY. Both DQ decisions and routing are audited.

| Rule / reason | Inputs | Valid condition; otherwise invalid |
|---|---|---|
| `null_order_id` | order_id | Non-null, nonblank string |
| `null_user_id` | user_id | Non-null, nonblank string |
| `null_product_id` | product_id | Non-null, nonblank string |
| `invalid_product_name` | product_name | Non-null, nonblank string |
| `invalid_amount` | amount, event_type, metadata | Finite positive double; zero only for CANCELLED with JSON boolean was_paid=false |
| `null_timestamp` | timestamp | Present |
| `malformed_timestamp` | timestamp physical type | Long epoch-ms type; no numeric-string coercion |
| `invalid_timestamp` | timestamp | Greater than zero and no later than UTC 9999-12-31T23:59:59.999 |
| `future_timestamp` | timestamp, ingestion time | No more than 60 seconds ahead, inclusive |
| `old_timestamp` | timestamp, ingestion time | No more than 24 hours behind, inclusive |
| `invalid_region` | region | One of the five existing generator regions |
| `invalid_product_id` | product_id | Complete `prod-[0-9]+` match |
| `invalid_event_type` | event_type | One of the six Avro lifecycle symbols |
| `schema_drift` | all fields, physical types, decode status | Expected types, required values present, successful record decode |
| `invalid_currency` | currency | Exact uppercase active ISO 4217 code from pinned 2026-09-17 list |
| `invalid_payment_method` | payment_method | Null or UPI/Credit Card/Debit Card/Net Banking/Wallet/COD |
| `invalid_status` | status, event_type | Matching CREATED/PAID/SHIPPED/DELIVERED/CANCELLED/REFUNDED |
| `invalid_event_metadata` | event_metadata | Null or strict JSON object |
| `invalid_schema_version` | schema_version | Exactly `1.0`, the implemented contract |
| `invalid_provenance` | source topic, partition, offset | Nonblank topic; present, nonnegative partition and offset |
| `event_ts_mismatch` | timestamp, event_ts | Exact epoch-ms equivalence for representable timestamps |

UTC is enforced in the Spark session. Missing/wrong-type/out-of-range timestamps
do not also receive future/old reasons. Standalone validation without ingestion
time uses the supplied clock or processing time. An event arriving one hour late
passed; one arriving two days late reached DLQ. There is no Silver watermark.

Rules do not invent order/user ID formats, catalog membership, required payment
IDs, monetary rounding, currency conversion, or lifecycle ordering constraints.
Malformed Avro schema/type failures are classified at the decode boundary and
retain raw bytes even when business fields cannot be extracted.

## 7. Silver contract

- Business fields keep their Avro types: strings, `amount: double`,
  `timestamp: bigint`, and derived `event_ts: timestamp` in UTC.
- Only `payment_method` and `event_metadata` may be null in passing business data.
  Optional schema/transport metadata may be null for legacy Bronze files.
- No trimming, case folding, rounding or conversion changes business values.
- Retains source topic/partition/offset, Kafka timestamp/schema ID where available,
  Bronze ingestion time/file/batch, event identity/fingerprint, DQ decision/time/
  contract version, quality flag, cleaned time and separate Silver batch ID.
- Valid rows have `_dq_passed=true`, `_quality_passed=true`, empty failure reason
  and `_route=SILVER`. Individual failure flags and raw/original payload copies
  remain in the audit/DLQ rather than Silver.
- Partitions: deterministic `silver_batch` string, integer year/month/day,
  string event_type_lower and region. Existing hour remains an integer column.
- Full actual read-back schema is saved in `restart.json`.

Layout:

```text
s3a://silver/<topic>/silver_batch=<key>/year=/month=/day=/event_type_lower=/region=/
s3a://silver/_dq/<topic>/batches/batch_key=<key>/
s3a://silver/_dq/<topic>/runs/batch_key=<key>/
```

The extra Silver batch partition supports write-once retries. Downstream Gold
must be validated against this actual schema/layout in the next milestone.

## 8. Deduplication

**Identity: `topic:partition:offset`.** The Avro contract has no business event ID.
Payload fingerprinting detects conflicting content at the same source coordinates;
it does not collapse identical content at different offsets.

| Scenario | Verified behavior |
|---|---|
| Same source repeated within one batch | One destination event; additional occurrence audited as REPLAY |
| Same source in a later Bronze file/batch | REPLAY; no additional Silver/DLQ event |
| Restart with same checkpoint and no new input | Zero input; counts unchanged |
| New source offset, same order and event type | Preserved as another event |
| Two identical refund payloads at different offsets | Both preserved |
| Entire Bronze reread with fresh checkpoint | 35 REPLAY audit rows; no new Silver/DLQ event |
| Conflicting payloads at identical coordinates | Explicit failure, not arbitrary selection |
| Late event inside retained age limit | Preserved; no watermark eviction |

Completed MinIO audit batches provide the cross-batch identity index. Batch keys
derive from checkpoint location, source topic and batch ID. Evaluated decisions
are staged before output; deterministic Silver paths are reused after successful
commit. An unfinished batch blocks a different checkpoint until recovered.

This requires **one writer per topic/output pair**. History scans are not a
scalable transactional index. Republishing a business event to another offset
cannot safely be distinguished from a legitimate repeat without an upstream ID.

## 9. DLQ

Default topic remains `order-events-dlq`; actual existing configuration was
confirmed: one partition, replication one, gzip, delete policy, 30-day retention.
Controlled verification used `order-events-m2-20261003a-dlq` with matching settings.

The diagnostic envelope is JSON, as in the original DLQ design; source events
remain Avro. Fields include event identity/key, available order/type, event time,
all failure reasons, ERROR severity, DQ version, original decoded event JSON,
raw base64 for decode failures, decode code, source topic/partition/offset/schema
ID, Bronze file, ingestion/check times and Silver batch ID.

Kafka keys, infrastructure settings and exception stack traces are excluded.
Original application metadata is retained for diagnosis; routine batch logs emit
counts rather than payloads. No additional personal data is introduced.

Writes use `acks=all` and idempotent producer retries. Delivery is **at-least-once**:
a crash after Kafka accepts records but before the completion summary can cause
re-emission. Consumers can deduplicate by event_id. Kafka and Parquet are not one
transaction. Successful normal replay/restart tests produced no duplicate DLQ IDs.

## 10. Runtime verification

Primary source: **`order-events-m2-20261003a`**, partition **0** throughout.
Evidence: `milestone2_evidence/order-events-m2-20261003a/`.
Each decision includes the exact Bronze file and source coordinates; `dlq` entries
also contain destination partition/offset. `silver_events` records accepted IDs.

| Required case | Source offset / identifier | Observed result |
|---|---|---|
| Valid event | 0, `m2-valid` | Bronze → DQ pass → Silver |
| Invalid amount | 1, `m2-amount` | DLQ: invalid_amount |
| Invalid product | 2, `m2-product` | DLQ: invalid_product_id |
| Invalid timestamp | 3, `m2-timestamp` | DLQ: invalid_timestamp |
| Future timestamp | 4, `m2-future` | DLQ: future_timestamp, distinct from invalid_timestamp |
| Invalid currency | 5, `m2-currency` | DLQ: invalid_currency |
| Invalid payment method | 6, `m2-payment` | DLQ: invalid_payment_method |
| Invalid status | 7, `m2-status` | DLQ: invalid_status |
| Malformed/schema-invalid | 27–29 | Bronze raw/error envelope → schema_drift → DLQ |
| Duplicate/replay | Extra Bronze copies of offsets 0/1; later full reread | REPLAY audit; destination counts unchanged |
| Valid lifecycle | 21–26, `m2-lifecycle` | CREATED, PAID, SHIPPED, DELIVERED and both REFUNDED events preserved |

Additional live cases: malformed metadata, excessively old and one-hour-late
timestamps, unpaid zero cancellation, negative refund, NaN, infinity, optional
nulls, USD/international, unknown schema version, blank user and invalid region/name.

| Stage | Bronze rows | Silver rows | DLQ rows | Audit rows |
|---|---:|---:|---:|---:|
| Initial 30 Kafka events plus two intentional Bronze replay copies | 32 | 11 | 19 | 32 |
| Separate-process restart, no new input | 32 | 11 | 19 | 32 |
| Two more source replay copies plus one new Kafka offset | 35 | 12 | 19 | 35 |
| Fresh-checkpoint complete Bronze replay | 35 | 12 | 19 | 70 |

Initial DQ totals were 12 pass / 20 fail including replayed input; routing totals
were 11 Silver / 19 DLQ / 2 replay. Run quality was 37.5%. DQ counts and new-event
routing counts intentionally measure different things; no input disappeared.

Sample equivalence: offset 0 timestamp `1791040128954` =
`2026-10-03 15:08:48.954 UTC`, unchanged in Silver.

Checkpoint metadata, source logs, offset logs and commit logs were verified at:

```text
s3a://checkpoints/order-events-m2-20261003a/bronze/
s3a://checkpoints/order-events-m2-20261003a/silver/
s3a://checkpoints/order-events-m2-20261003a-fresh/silver/
```

Separate recovery fixture passed: injected failure after Silver commit, successful
retry with **one Silver / one DLQ**, completed-batch no-op, unfinished-batch checkpoint
guard, partial-output guard, empty-source startup and zero-input quality summary.
Legacy verification read all **16 original modernization Bronze rows**, supplied
the missing topic from the configured source, and passed DQ/timestamp assertions.

Reproduction commands (use a new unique topic for a new initial run):

```bash
python3 scripts/capture_milestone2_check.py --step initial --topic order-events-m2-<new-run>
python3 scripts/capture_milestone2_check.py --step restart --topic order-events-m2-<new-run>
python3 scripts/capture_milestone2_check.py --step recovery --topic order-events-m2-<new-run>
```

The capture script preserves exact Docker/spark-submit arguments. It uses the
retained `oip-modernization-verify` Linux Spark container and existing infrastructure.
No mocked result was used to establish live routing PASS.

## 11. Automated tests

**7 pytest tests passed**, executing real Spark 3.5.9 transformations in Linux;
27 shared controlled fixtures plus null/type/boundary/replay/decode edge cases.

- Actual DQ decisions and actual Silver/DLQ transformation functions.
- Required valid/amount/product/timestamp/future/currency/payment/status cases.
- Malformed Avro/schema handling and infrastructure-error propagation.
- Source replay, repeated business events and conflicting-payload rejection.
- Timestamp boundaries, immutable ingestion-relative age and event_ts equivalence.
- Optional metadata, invalid JSON objects/constants and unpaid cancellation.
- Zero-input quality and invalid count arguments.

```bash
python3 scripts/capture_milestone2_check.py --step tests --topic order-events-m2-20261003a
```

Saved result: **7 passed, 1 warning in 22.24s** (`tests.log` / `tests.json`).
The warning is the existing Authlib HTTPX deprecation.
Live scripts additionally assert end-to-end routing, checkpoints and recovery.
Targeted Ruff and Compose configuration validation passed. Host imports and
`pip check` passed; the container-wide package caveat is recorded below.

## 12. Milestone 1 regression

**PASS — fresh producer-to-Bronze runtime after implementation changes.**

- Topic: `order-events-m1-regression-20261003m2b`.
- Actual generator constructors and `KafkaPublisher`: **4 delivered, 0 failed**.
- Subject version 1, schema ID 1, wire header **`0000000001`**; registered schema
  equals `producer/schemas/order_event.avsc`.
- Spark drained partition 0 offsets 0–3 into **4 structured Bronze rows**.
- Required fields/provenance were non-null and source coordinates unique.
- UTC epoch-ms/event_ts equivalence and checkpoint files passed.
- Actual runtime: Spark 3.5.9, Python 3.10.12, Java 17.0.19, Hadoop 3.3.4,
  AWS SDK 1.12.262.

Evidence: `milestone2_evidence/order-events-m1-regression-20261003m2b/regression.json`
and `.log`; wire/producer checks are recorded in `environment_checks.md`.
Spark progress reported 12 input rows because the Bronze callback performs
multiple actions; Kafka offsets and Parquet read-back establish the true count 4.

## 13. Current-real-world review

| Component | Current version | Decision | Reason |
|---|---|---|---|
| Host / Spark-container Python | 3.11.9 / 3.10.12 | **UPDATE LATER** | Runtime works; supported newer patch/image migration remains lifecycle work |
| PySpark / Spark | 3.5.9 | **KEEP** | Real DQ, Parquet, recovery and streaming checks pass |
| Kafka Spark connector | spark-sql-kafka-0-10_2.12:3.5.9 | **KEEP** | Exact Spark/Scala alignment; both source and DLQ sink verified |
| Spark Avro package | spark-avro_2.12:3.5.9 | **KEEP** | Preserve aligned baseline; Python fastavro handles Confluent decoding |
| fastavro | 1.12.2 | **KEEP** | Normal and malformed binary paths verified |
| confluent-kafka | 2.15.1 | **KEEP** | Actual publisher, admin and read-back consumers pass |
| Authlib / HTTPX / Requests | 1.8.0 / 0.27.2 / 2.34.2 | **KEEP** | Registry imports/registration work; known deprecation is nonfatal |
| Pydantic / pydantic-settings | 2.9.2 / 2.6.1 | **NO CHANGE REQUIRED** | Existing config models import; Spark expressions implement DQ |
| Validation/DLQ framework | PySpark + stdlib JSON | **NO CHANGE REQUIRED** | No new library required; existing Kafka connector writes DLQ |
| Hadoop AWS / AWS SDK | 3.3.4 / 1.12.262 | **KEEP** | Previously aligned POM pair; actual S3A reads/writes pass |
| Java / Scala | 17.0.19 / 2.12 | **KEEP** | Verified Spark-compatible runtime |
| Kafka / Schema Registry images | Confluent 7.7.12 paired | **UPDATE LATER** | Preserve working pair; 7.7 support lifecycle migration remains deferred |
| MinIO server / client | RELEASE.2024-11-07T00-52-20Z / RELEASE.2024-11-05T11-29-45Z | **UPDATE LATER** | Existing storage works; maintenance/distribution migration remains deferred |
| pytest / Faker / structlog | 8.3.3 / 31.0.0 / 24.4.0 | **KEEP** | Existing declared pins used successfully |

No **UPDATE NOW** decision was necessary and no versions were upgraded solely
because newer versions exist. The Linux verification container was populated with
already-declared test/producer dependencies; requirements remained unchanged.

Sources: the completed Milestone 1 modernization review, current official
[Spark 3.5.9 Kafka guide](https://spark.apache.org/docs/3.5.9/structured-streaming-kafka-integration.html)
(including at-least-once sink semantics), and
[SIX ISO 4217 List One](https://www.six-group.com/dam/download/financial-information/data-center/iso-currrency/lists/list-one.xml),
published 2026-09-17 and fetched 2026-10-03. Lifecycle caveats from the prior review
remain applicable; no lifecycle migration was implemented here.

## 14. Remaining issues

### BLOCKING

None for the verified local Milestone 2 acceptance scope.

### NON-BLOCKING

- Kafka DLQ remains at-least-once across sink/completion crash windows; there is
  no cross-system transaction. Partial Parquet outputs fail closed and require
  explicit recovery rather than automatic deletion/overwrite.
- One writer per topic/output is required. Audit-history scans and small files
  need a scalable storage/index approach before high-volume operation.
- No upstream stable event ID exists. New-offset business re-publication remains
  distinguishable only as another source event. Topic recreation/cluster migration
  requires a new identity namespace/output; coordinates are scoped to this cluster.
- Bronze append/checkpoint crash atomicity is unchanged; Silver handles repeated
  source coordinates, but Bronze itself is not claimed exactly-once.
- A full container `pip check` reports the existing OS package `pygobject 3.42.1`
  lacks `pycairo`. Host project `pip check` is clean; DQ/Spark/Kafka tests pass.
  This container-wide dependency check is not represented as clean.
- Registry-auth/network failure retry is distinguished in code and unit-tested;
  no live Registry outage or hard process kill during a Parquet task was performed.
- The 24-hour ingestion-age rule and pinned currency list need governed policy/
  reference-data updates over time. Historical/withdrawn currency codes are outside
  the active-code contract; optional metadata semantics beyond JSON are not inferred.
- DQ PostgreSQL/Prometheus integration remains deferred. Current MinIO audit is
  the authoritative persisted milestone evidence.
- Producer-side Avro serialization rejects schema-invalid dictionaries before
  Kafka; those publisher failures are distinct from records already in Bronze.
  The controlled malformed-wire fixtures deliberately exercised the latter path.

### FUTURE MODERNIZATION

Supported Python/container patches, Confluent major/support-line migration,
MinIO maintenance/distribution migration, transactional lake storage, scalable
identity indexing, and an upstream business event ID remain follow-up work.

## 15. Files/data intentionally NOT changed

- Producer generator/publisher implementation and authoritative Avro schema.
- `requirements.txt`, active `.env`, cluster identity, Compose infrastructure,
  Spark dependency factory and topic-admin implementation.
- Existing Kafka topics/subjects and historical Bronze/checkpoint contents.
- Gold, PostgreSQL writers/schema, Redis, dashboard, alerts, observability,
  CI/CD, security configuration, Kafka major version and MinIO deployment.
- No bucket, topic, volume, database table or retained data was deleted/reset.

Only new controlled topics/subjects, Bronze/Silver/audit prefixes, checkpoints,
evidence files and the scoped code/documentation above were added or changed.
Recovery fixtures and the deliberately incomplete test prefix are retained.
The four services started for verification were stopped afterward, restoring
their initial state while preserving containers and volumes.

## 16. Next milestone

**Milestone 3: Silver → Gold — lifecycle-aware, replay-safe aggregations.**

Validate revenue semantics, event-time windows and multi-batch accumulation
against the verified Silver schema. Implement only that immediate milestone next.
