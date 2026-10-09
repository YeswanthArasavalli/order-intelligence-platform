# Milestone 1 — Current technology and compatibility review

**Review date:** 2026-10-03  
**Scope:** Producer → Kafka → Schema Registry → Spark → Bronze  
**Result:** **PASS for the updated local milestone runtime.** The version review and bounded changes are complete. Lifecycle migrations remain recommendations, not completed upgrades.

## 1. Executive conclusion

The Avro architecture is still appropriate. The producer emits Confluent-framed Avro; Bronze resolves its schema ID through Schema Registry, decodes with `fastavro`, and writes structured Parquet through S3A. There is no reason to replace this with JSON.

Applied changes:

| Component | Before | After |
|---|---|---|
| Project `.venv` | Python 3.14.6, only pip installed | Recreated with installed Windows Python **3.11.9**; full `requirements.txt` installed |
| Confluent Python client | `2.5.3` | `confluent-kafka[schemaregistry]==2.15.1` |
| Python Avro implementation | `fastavro==1.9.7` | `fastavro==1.12.2` |
| Schema Registry import dependency | Absent | `authlib==1.8.0` |
| PySpark | `3.5.3` | **`3.5.9`** |
| Spark Kafka/Avro artifacts | `_2.12:3.5.3` | **`_2.12:3.5.9`** |
| Kafka and Schema Registry images | Confluent `7.7.1` | Confluent **`7.7.12`**, together |
| Requests | `2.32.3` | `2.34.2` |
| AWS Java SDK bundle | Override `1.12.367` | **`1.12.262`**, matching Hadoop 3.3.4's published dependency |
| Repeatable verification | Ad hoc, long-running Spark invocation | `scripts/verify_milestone1_bronze.py`, bounded drain and assertions |

Final verification proved another **8 delivered events → 8 new Bronze rows**, yielding **16 cumulative rows** in the dedicated smoke topic's Bronze prefix. A subsequent restart with the same checkpoint read no new input and retained exactly 16 unique Kafka partition/offset pairs.

The stack is a **verified development baseline**, not an entirely current production baseline:

- Confluent 7.7's Community/Standard support ended on **2026-07-26**. Applying 7.7.12 does not extend that lifecycle. Confluent **7.9.10** is a candidate for a separately verified Kafka 3.x migration; the current major line is **8.3.2**. [S5]
- MinIO's community repository now says it is **no longer maintained**, and describes source-only distribution with unmaintained legacy binaries. Its old community documentation URL now serves AIStor documentation. A storage distribution/migration decision is needed rather than inventing a newer interchangeable Docker tag. [S11–S12]
- The selected official Spark image contains Python **3.10.12**. It works with this milestone, but Python.org lists the 3.10 line as end-of-life on **2026-10-01**. A maintained Python 3.11-based Spark image is a follow-up packaging task. The host producer uses Python 3.11.9, not that container interpreter. [S1]

## 2. Method and evidence boundaries

Reviewed the complete dependency file, Compose configuration and environment version variables, producer, Avro schema, Bronze decoder, Spark factory, and previous milestone report. Checked official release/support documentation, PyPI metadata, Docker Hub tag metadata, and Maven POMs. Then installed and exercised the actual application against local Docker Desktop infrastructure.

Version facts below are observations as of the review date. A package index's latest version is not proof that a complete deployment using it works. Compatibility statements distinguish documented constraints from exercised paths.

`requirements.txt` pins direct dependencies but is **not a complete transitive lockfile**. Its comment now states that accurately. The full installation passed `pip check`; that check alone does not prove application behavior. The separate runtime checks provide that evidence for this milestone.

## 3. Technology classification

Categories:

- **CURRENT AND COMPATIBLE**: current release or current maintenance release of the selected line, with applicable compatibility evidence.
- **OUTDATED BUT STILL ACCEPTABLE**: retained for this bounded local milestone; not a blanket support or security endorsement.
- **OUTDATED AND SHOULD BE UPDATED**: a concrete maintenance/lifecycle reason supports an update; the action column distinguishes applied changes from follow-ups.
- **INCOMPATIBLE / UNSAFE**: unsuitable environment or unsupported version combination, including combinations that can happen to pass a small smoke test.
- **LATEST VERSION SHOULD NOT BE USED**: not a drop-in replacement for this selected stack; this does not mean the upstream release itself is defective.

| Technology / configuration | Classification | Current-world comparison and decision |
|---|---|---|
| Original Python 3.14.6 `.venv` with 2024 pins | **INCOMPATIBLE / UNSAFE** | Missing application packages; older binary pins fell back to source builds during the dry-run. Spark 3.5's broad `Python >=3.8` metadata is not evidence of testing every future Python release. Recreated the pip-only environment on installed 3.11.9. |
| Selected host Python 3.11.9 | **OUTDATED AND SHOULD BE UPDATED** | Compatible and exercised, but Python.org lists 3.11.17 as the current 3.11 release. The 3.11 line has security support through October 2027. Current 3.11 patch provisioning is a follow-up; no global Python installation was replaced. [S1] |
| Latest Python 3.14.8 for this Spark 3.5 stack | **LATEST VERSION SHOULD NOT BE USED** | Python.org lists 3.14.8 as latest stable and 3.15 as pre-release. This review does not establish Spark 3.5 worker compatibility on 3.14. Keep the selected 3.11 host environment. [S1–S2] |
| Spark image Python 3.10.12 | **OUTDATED AND SHOULD BE UPDATED** | Runtime pass, but the Python 3.10 lifecycle has ended. The official image tag does not imply every bundled component remains supported. [S1] |
| Java 17.0.19 | **CURRENT AND COMPATIBLE** | Supported LTS major for Spark 3.5 and Confluent 7.7; observed in Spark execution. This is a major-version compatibility classification, not a claim that 17.0.19 is the newest patch. Spark supports Java 8/11/17; the old “Java 11+” comment was too broad. [S2, S5] |
| `confluent-kafka` 2.5.3 | **OUTDATED AND SHOULD BE UPDATED** | Updated to current 2.15.1 with declared Schema Registry extras. Broker wire compatibility is not tied to matching the broker's release number. Imports, metadata, serialization, delivery and decoding passed. [S6] |
| `confluent-kafka[schemaregistry]` 2.15.1 | **CURRENT AND COMPATIBLE** | Current PyPI release; producer works with Confluent 7.7.12 in this test. Newer optional Schema Registry association lookup returned 404, followed by successful normal subject registration. [S6] |
| `fastavro` 1.9.7 | **OUTDATED AND SHOULD BE UPDATED** | Updated within the 1.x line to 1.12.2; current library supports Python 3.9–3.14. Tested `timestamp-millis`, schemaless decoding, and worker execution. [S7] |
| `fastavro` 1.12.2 / Authlib 1.8.0 / Requests 2.34.2 | **CURRENT AND COMPATIBLE** | Current PyPI versions checked. Authlib was required by the new Schema Registry client imports. Requests stays within the HTTP dependency's 2.x line. [S6–S8] |
| Apache Avro format / `.avsc` contract | **CURRENT AND COMPATIBLE** | Apache's download page lists Avro 1.12.2. Avro the format, Apache's libraries, and `fastavro` are distinct version domains. No schema change was needed; registered schema equality and the five-byte header were verified. [S9] |
| Spark/PySpark 3.5.3 | **OUTDATED AND SHOULD BE UPDATED** | Apache explicitly recommends 3.5 users update to maintenance release 3.5.9 for correctness/security fixes. Updated PySpark, Kafka artifact, Avro artifact and verification image together. [S3] |
| Spark/PySpark 3.5.9 + Scala 2.12 artifacts | **CURRENT AND COMPATIBLE** | Current maintenance release of the chosen 3.5 line, not latest Spark overall. Actual JVM execution, Python workers, Kafka, S3A, Parquet and restart passed. [S2–S4] |
| Latest Spark 4.2.0 / 4.x connector artifacts | **LATEST VERSION SHOULD NOT BE USED** | A major migration requires matching Scala artifacts, Java/Python requirements, SQL behavior and Hadoop/storage dependencies. Do not attach 4.x artifacts to a 3.5 runtime. [S2–S4] |
| Kafka broker, Confluent 7.7.1 → 7.7.12 (Kafka 3.7 line) | **OUTDATED AND SHOULD BE UPDATED** | Latest 7.7 patch applied as a bounded improvement. The line still needs a supported-line migration. Apache Kafka's highest supported line listed is 4.3.1; that is not the version of this Confluent image. [S5, S10] |
| Schema Registry 7.7.1 → 7.7.12 | **OUTDATED AND SHOULD BE UPDATED** | Patched with the broker; same lifecycle follow-up applies. Existing schema ID 1 remained usable. [S5] |
| Confluent 8.3.2 / Kafka 4.x drop-in change | **LATEST VERSION SHOULD NOT BE USED** | Not selected for an in-place tag-only change to persisted KRaft data. Requires upgrade-path, metadata and client verification beyond this patch-level test. [S5, S10] |
| Hadoop client + `hadoop-aws` 3.3.4 | **OUTDATED BUT STILL ACCEPTABLE** | Spark 3.5.9's published parent POM specifies 3.3.4; observed at runtime. Keep the versions identical. Hadoop 3.5.0 is current upstream, but replacing only `hadoop-aws` is unsafe. [S13–S15] |
| AWS SDK override 1.12.367 with Hadoop 3.3.4 | **INCOMPATIBLE / UNSAFE** | It passed the initial write, but differs from Hadoop's declared 1.12.262 and its published guidance. Corrected to the declared version and verified a fresh write/read/restart. “Unsafe” here concerns the unsupported pairing, not an observed runtime failure. [S14–S15] |
| AWS SDK 1.12.262 with Hadoop 3.3.4 | **OUTDATED BUT STILL ACCEPTABLE** | Selected for dependency alignment in the legacy local stack, not as a modern SDK target. A future SDK-major update belongs with the complete Hadoop/Spark storage stack. |
| Latest Hadoop 3.5.0 / newer AWS SDK as standalone replacements | **LATEST VERSION SHOULD NOT BE USED** | Not compatible by mere version proximity. Hadoop 3.4+ also uses the AWS SDK v2 family. Review and replace the whole dependency set together. [S13–S15] |
| MinIO server `RELEASE.2024-11-07T00-52-20Z` | **OUTDATED AND SHOULD BE UPDATED** | Retained as the working local store. Current upstream maintenance/distribution changes prevent treating this as a simple latest-tag upgrade. New distribution and existing-volume migration remain follow-ups. [S11–S12] |
| MinIO `mc` `RELEASE.2024-11-05T11-29-45Z` | **OUTDATED BUT STILL ACCEPTABLE** | Existing init image exited 0 against the retained server. Current `minio/mc` Docker Hub tag query returned 404; no latest image claim is made. |
| Docker Engine 29.6.2 / Compose 5.3.1 | **CURRENT AND COMPATIBLE** | Local versions observed. Compose documentation supports v2/v5 and the Compose Specification. Configuration validation and required service startup passed. No tooling upgrade was justified by the milestone. [S16] |
| Faker 31.0.0 / structlog 24.4.0 / HTTPX 0.27.2 | **OUTDATED BUT STILL ACCEPTABLE** | Retained; generation, logging and Registry HTTP requests work. HTTPX meets the new client's `>=0.26` requirement. Current versions observed are 40.40.0 / 26.1.0 / 0.28.1 respectively. |
| boto3/botocore 1.35.62 | **OUTDATED BUT STILL ACCEPTABLE** | Retained as an aligned pair. They do not implement Spark's S3A connector; updating them would not modernize the Java storage chain. Current PyPI versions observed: 1.43.108 each. |

## 4. Compatibility-chain review

### Python ↔ Kafka client

The new client has a Windows CPython 3.11 wheel and installed without native `librdkafka` compilation. The host environment imported the publisher, generator, topic admin, Spark factory and Bronze decoder. The first producer attempt revealed `ModuleNotFoundError: authlib`; declaring the Schema Registry extra and `authlib==1.8.0` fixed it.

Authlib emits a deprecation warning about its HTTPX integration. The actual synchronous Registry operations passed. This warning is recorded rather than treated as an application failure or used to trigger an unrelated HTTP framework migration.

### Python client ↔ Kafka broker

The producer used `localhost:9092`, received usable external listener metadata and got successful delivery callbacks. Existing `order-events` still has **8 partitions**. The dedicated `order-events-modernization` topic has **1 partition** and finished at end offset **16**.

Confluent client version 2.15.1, Confluent Platform version 7.7.12, Kafka's 3.7 product line and Spark's transitive Kafka Java client 3.4.1 are different version schemes. Equal numbers are not required across them. Spark's connector exercises the broker protocol independently of the Python producer.

### Broker ↔ Schema Registry ↔ Avro

Both service images resolve to 7.7.12. Kafka and Schema Registry health checks passed. Registry uses `kafka:9092`; Spark workers use `http://schema-registry:8081`.

Verified:

```text
subject: order-events-modernization-value
subject version: 1
schema ID: 1
wire header (hex): 0000000001
registered schema == JSON-parsed producer/schemas/order_event.avsc
```

The first byte is magic byte 0; the next four bytes are schema ID 1. A host consumer and the real Spark decoder resolved and decoded the payload. The same schema text reused existing ID 1 rather than creating a new schema definition. The new subject is specific to the smoke topic.

### PySpark ↔ Java ↔ Spark artifacts

Selected artifact coordinates:

```text
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9
org.apache.spark:spark-avro_2.12:3.5.9
org.apache.hadoop:hadoop-aws:3.3.4
com.amazonaws:aws-java-sdk-bundle:1.12.262
org.postgresql:postgresql:42.7.3
```

`SPARK_VERSION` now contains the entire `3.5.9` version instead of composing a hard-coded `.3` suffix. The Spark parent POM reports Scala 2.12.18, Hadoop 3.3.4 and Apache Avro 1.11.5. Spark's own Avro dependency is not independently replaced with Apache's newest Avro library.

The verification image is:

```text
apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu
sha256:2442368ef4a5ca60e81604cf48d22d847344d009237cd21a1943b060278898bb
```

It ran Spark 3.5.9, Java 17.0.19 and Python 3.10.12. Driver and Python workers share that image interpreter; the Windows producer's different Python minor version is not a cross-worker mismatch. `fastavro==1.12.2` and `structlog==24.4.0` were explicitly installed inside the container. The Windows `.venv` does not supply Linux container dependencies.

Direct Windows Spark execution remains subject to the previous missing `HADOOP_HOME`/`winutils.exe` blocker; this review did not install those components or claim that path now passes. Spark runtime verification was container-based.

### Spark/Hadoop/S3A ↔ MinIO

Hadoop 3.3.4's published POM specifies AWS SDK 1.12.262. The final JVM reported exactly those versions and Maven resolved the final package set with **zero evicted modules**. This corrected the initial 1.12.367 override, which had evicted the declared SDK.

The existing S3A configuration works for the retained local HTTP MinIO server: explicit endpoint, path-style addressing and the simple credentials provider. New Parquet writes, partition discovery, read-back, checkpoint offset writes and checkpoint commit writes all succeeded. No S3A connector classes or storage architecture were replaced.

### Docker images ↔ Compose

`docker.exe compose config --quiet` passed after correcting an indentation error introduced during the first image-tag edit. The shared `KAFKA_VERSION` is 7.7.12 in `.env`, `.env.example`, and both Compose fallbacks. Existing internal/external listener mappings and KRaft cluster identity were retained.

The initial Spark mount attempt used a WSL path with the Windows Docker CLI and did not expose the source file. Using `<repository-root>:/opt/oip` resolved it. The final verification container mounted source read-only.

Docker Hub queries for `minio/minio` and `minio/mc` returned HTTP 404 during review. Existing cached images ran successfully; a fresh pull on a clean machine was not proven. That is a reproducibility limitation, not evidence that the cached runtime failed.

## 5. Runtime verification evidence

### Sequence

| Step | Observation |
|---|---|
| Install entire requirements file into project `.venv` | PASS, Windows Python 3.11.9 |
| `pip check` | `No broken requirements found.` |
| Milestone module imports | `MILESTONE_IMPORTS_PASS` |
| Ruff on changed Spark factory and new verification script | `All checks passed!` |
| Compose validation | PASS |
| Start Kafka, Schema Registry, MinIO, MinIO init | Kafka/Registry healthy; MinIO live; init exit 0 |
| Existing-topic producer smoke | `order-events`: 8 attempts, 8 delivered, 0 failed |
| Dedicated-topic producer smoke | `order-events-modernization`: 8 attempts, 8 delivered, 0 failed |
| First Spark write (initial SDK override) | Batch 0 wrote 8 rows; checkpoint commit 0 present |
| Align SDK, restart with bounded verifier | Existing 8 rows read back; no new input |
| Publish after alignment | 8 more attempts, 8 delivered, 0 failed |
| Final-chain new write | Batch 1 wrote 8 rows; cumulative Bronze count 16 |
| Final-chain restart without new production | 0 input rows; count remained 16; no duplicate partition/offset pairs |
| Schema and actual wire inspection | `WIRE_AND_SCHEMA_PASS` |

The generator emits several lifecycle events per order, so a two-second run does not guarantee an invariant event count. Here each successful producer invocation reported 8 events.

Final bounded verification returned exit code **0** and:

```json
{
  "spark": "3.5.9",
  "python": "3.10.12",
  "java": "17.0.19",
  "hadoop": "3.3.4",
  "aws_sdk": "1.12.262",
  "topic": "order-events-modernization",
  "bronze_path": "s3a://bronze/order-events-modernization/",
  "count": 16,
  "checkpoint": "s3a://checkpoints/modernization-review/bronze/",
  "input_rows_this_run": 0
}
```

The verifier asserts:

- Exact expected cumulative row count.
- Presence and non-null values for `order_id`, `event_type`, `amount`, `timestamp`, `event_ts`, `_kafka_offset`, `_kafka_partition`, and `region`.
- Unique Kafka partition/offset pairs within this single-topic prefix.
- `unix_millis(event_ts) == timestamp` for every row and Spark session timezone UTC.
- Initial checkpoint metadata, source offsets, offset log and commit exist.
- Streaming query terminates within 120 seconds after startup.

Representative row, independently matching the directly decoded Kafka value:

```text
order_id:          ord-50856067
event_type:        ORDER_CREATED
amount:            400.21
timestamp:         1791035389489
event_ts:          2026-10-03 13:49:49.489
_kafka_partition:  0
_kafka_offset:     0
region:            north-india
```

Observed read-back schema:

```text
struct<order_id:string,event_type:string,user_id:string,product_id:string,
product_name:string,amount:double,currency:string,payment_method:string,
status:string,event_metadata:string,timestamp:bigint,schema_version:string,
event_ts:timestamp,_kafka_offset:bigint,_kafka_partition:int,
_ingested_at:timestamp,_batch_id:bigint,year:int,month:int,day:int,hour:int,
region:string>
```

Partition columns were inferred as integers on read-back even though the writer constructs strings; the event and Kafka metadata contracts were verified.

Checkpoint objects observed:

```text
modernization-review/bronze/metadata
modernization-review/bronze/sources/0/0
modernization-review/bronze/offsets/0
modernization-review/bronze/commits/0
modernization-review/bronze/offsets/1
modernization-review/bronze/commits/1
```

First batch objects included Parquet files under both `region=north-india` and `region=south-india` in `bronze/order-events-modernization/year=2026/month=10/day=03/hour=13/`.

### Interpretation of metrics and test limits

The final-chain write's Spark progress reported **24 `numInputRows`**, while the callback counted **8 rows**, Kafka advanced from offset 8 to 16, and Bronze grew from 8 to 16. The existing `foreachBatch` performs multiple actions without persistence (`isEmpty`, `count`, write), which can cause repeated reads to be counted in Spark's progress metrics. The report uses delivery counts, source offsets and read-back counts for event cardinality, not that inflated metric.

Successful committed-checkpoint restart is not a proof of exactly-once writes during a crash between the append and checkpoint commit. The existing append-based `foreachBatch` sink has not gained batch-id deduplication. Failure-injection, load, schema-evolution, malformed-event/DLQ and downstream tests are not part of this smoke result.

The first auxiliary host decoder check failed because a WSL environment assignment did not reach the Windows Python process; its default Registry Docker hostname was not host-resolvable. Explicitly setting `os.environ['SCHEMA_REGISTRY_URL']='http://localhost:8081'` inside the Windows process fixed the check. Native PowerShell environment settings below avoid that ambiguity.

## 6. Re-run instructions

### Host environment and producer (PowerShell, project root)

The populated project environment is `.venv\Scripts\python.exe`. To create an environment on a new checkout, select Python 3.11 explicitly:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pip check
docker.exe compose up -d kafka schema-registry minio minio-init
$env:KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
$env:SCHEMA_REGISTRY_URL = 'http://localhost:8081'
.\.venv\Scripts\python.exe main.py producer --tps 1 --duration 2 --error-rate 0
```

The last command publishes to the normal `order-events` topic. For the dedicated verification topic used here, add `--topic order-events-modernization`. Additional production changes the expected cumulative smoke count.

### Recheck the retained 16-row smoke result (PowerShell)

The stopped container `oip-modernization-verify` is retained with its installed Python packages and cached Maven downloads. It was created with internal service endpoints from `.env`, `PYTHONPATH=/opt/oip`, `SPARK_MASTER=local[2]`, `KAFKA_TOPIC_ORDERS=order-events-modernization`, and `SPARK_CHECKPOINT_DIR=s3a://checkpoints/modernization-review`.

```powershell
docker.exe compose up -d kafka schema-registry minio
docker.exe start oip-modernization-verify
$packages = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.apache.spark:spark-avro_2.12:3.5.9,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3'
docker.exe exec oip-modernization-verify /opt/spark/bin/spark-submit --packages $packages --conf spark.jars.ivy=/tmp/.ivy2 /opt/oip/scripts/verify_milestone1_bronze.py --expected-count 16
docker.exe stop oip-modernization-verify
docker.exe compose stop schema-registry kafka minio
```

Use the same checkpoint when rechecking the same output prefix. A new checkpoint with `startingOffsets=earliest` and the same output prefix would replay data into this append-only sink.

### Recreate only the verification container if needed (PowerShell)

Use this when the named verification container does not exist. The smoke topic and buckets from this review must still exist, or be prepared separately with new matching smoke locations and expected counts.

```powershell
docker.exe run -d --name oip-modernization-verify --network oip-network --user root --env-file .env -v "${PWD}:/opt/oip:ro" -e PYTHONPATH=/opt/oip -e KAFKA_TOPIC_ORDERS=order-events-modernization -e SPARK_CHECKPOINT_DIR=s3a://checkpoints/modernization-review -e 'SPARK_MASTER=local[2]' --entrypoint sleep apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu infinity
docker.exe exec oip-modernization-verify python3 -m pip install fastavro==1.12.2 structlog==24.4.0
```

Explicit `--packages` is required at submission for the tested Spark container path; setting builder properties after a submit-time JVM has launched is not a substitute for loading the connector JARs. The first download can take several minutes. The verifier uses `availableNow=True` and exits, avoiding reliance on a host timeout to stop a streaming container.

## 7. File and environment change ledger

| File / environment | Change |
|---|---|
| `requirements.txt` | Five changed/added package entries: Confluent client+extra, fastavro, Authlib, PySpark, Requests; accurate Python/Java/locking comments |
| `streaming/spark_session.py` | Full Spark version constant, matching connector versions, Hadoop-compatible SDK alignment |
| `docker-compose.yml` | Matching 7.7.12 defaults for Kafka and Registry |
| `.env.example` | Documented Confluent version 7.7.12 |
| `.env` | Active Confluent version 7.7.12; credentials and cluster ID retained |
| `scripts/verify_milestone1_bronze.py` | New bounded integration verifier using actual Bronze source/sink functions |
| This report | Release comparisons, classification, test evidence, limitations and rerun commands |
| Project `.venv` | Replaced the inspected pip-only 3.14.6 environment with a populated 3.11.9 environment |

The producer, schema and Bronze decoding implementation from Milestone 1 remain Avro-based. No Silver, Gold, PostgreSQL, Redis, Dashboard, Alerts, Prometheus, Grafana, CI/CD or production-security implementation was changed. Their packages were installed as part of the existing complete requirements file; those applications were not runtime-certified by that installation.

At completion the started milestone infrastructure and verification container were **stopped**, returning the project infrastructure to its initial stopped state. Kafka topics, schemas, MinIO buckets, named data volumes and PostgreSQL data were not deleted. Test artifacts are retained. The successful normal-topic producer smoke also added 8 records to `order-events`; only the dedicated smoke topic was consumed by this review's new Spark verification.

## 8. Retained Python dependency inventory

These entries were inspected in the complete requirements file and retained. Latest PyPI observations are informational; downstream compatibility was not evaluated merely because a newer release exists.

| Dependency | Retained pin | Latest observed |
|---|---|---|
| boto3 / botocore | 1.35.62 / 1.35.62 | 1.43.108 / 1.43.108 |
| psycopg2-binary | 2.9.10 | 2.9.13 |
| SQLAlchemy | 2.0.36 | 2.1.3 |
| redis | 5.2.0 | 8.1.0 |
| Streamlit | 1.40.1 | 1.65.0 |
| Plotly | 5.24.1 | 7.1.0 |
| pandas | 2.2.3 | 3.0.6 |
| Faker | 31.0.0 | 40.40.0 |
| prometheus-client | 0.21.0 | 0.26.0 |
| HTTPX | 0.27.2 | 0.28.1 |
| tenacity | 9.0.0 | 9.1.4 |
| python-dotenv | 1.0.1 | 1.2.4 |
| pydantic | 2.9.2 | 2.13.5 |
| pydantic-settings | 2.6.1 | 2.15.0 |
| structlog | 24.4.0 | 26.1.0 |
| pytest | 8.3.3 | 9.1.1 |
| pytest-cov | 6.0.0 | 7.1.0 |
| pytest-asyncio | 0.24.0 | 1.4.0 |
| pytest-mock | 3.14.0 | 3.16.0 |
| Ruff | 0.7.4 | 0.16.10 |
| Black | 24.10.0 | 26.5.1 |

Retained ancillary image declarations: `postgres:16-alpine`, `redis:7-alpine`, `prom/prometheus:v2.54.1`, `grafana/grafana:11.3.0`, and `provectuslabs/kafka-ui:v0.7.2`. Mutable tags such as `16-alpine` do not identify an exact installed patch. Those services were not upgraded or started for this review.

## 9. Prioritized follow-ups

1. **Supported Kafka/Registry line:** evaluate migration from 7.7.12 to a supported paired release. Confluent 7.9.10 is a Kafka 3.x candidate with Community/Standard support listed through 2027-02-19; 8.3.2 is a separate Kafka 4.x migration. Validate the persisted KRaft upgrade path and repeat milestone tests before selecting it. The 7.7 patch performed here is interim maintenance.
2. **Python packaging:** provision a current Python 3.11 patch for the host and a maintained Python 3.11 runtime for Spark. Preserve matching Spark driver/worker Python minors. Existing 3.11.9/3.10.12 results are the measured environments, not claims of latest-patch compliance.
3. **Storage distribution:** select a maintained MinIO/AIStor or other compatible S3 distribution and validate existing-data migration and artifact availability. The existing community image is a local compatibility fixture, not an actively maintained distribution recommendation.
4. **Spark/Hadoop modernization:** keep the verified 3.5.9 + Hadoop 3.3.4 + SDK 1.12.262 tuple together until a separately tested whole-stack migration replaces it. Updating only one storage JAR is not sufficient.
5. **Reproducibility:** a dedicated application image and transitive dependency lock would make the verified environment easier to reproduce. The retained verification container and commands document the current execution path but are not a production deployment definition.

## 10. Official sources consulted

- **S1 — Python releases/lifecycle:** https://www.python.org/downloads/
- **S2 — Spark 3.5.9 runtime requirements and PyPI metadata:** https://spark.apache.org/docs/3.5.9/ and https://pypi.org/project/pyspark/3.5.9/
- **S3 — Spark 3.5.9 maintenance recommendation:** https://spark.apache.org/releases/spark-release-3-5-9.html
- **S4 — Exact Spark Kafka artifact and deployment requirements:** https://spark.apache.org/docs/3.5.9/structured-streaming-kafka-integration.html
- **S5 — Confluent versions, latest patches, Kafka mapping and support dates:** https://docs.confluent.io/platform/current/installation/versions-interoperability.html and https://docs.confluent.io/platform/7.7/release-notes.html
- **S6 — Confluent Python client, wheels, extras and broker compatibility:** https://pypi.org/project/confluent-kafka/ and https://pypi.org/pypi/confluent-kafka/json
- **S7 — fastavro supported interpreters and features:** https://pypi.org/project/fastavro/
- **S8 — Current HTTP/import dependency metadata:** https://pypi.org/pypi/requests/json and https://pypi.org/pypi/authlib/json
- **S9 — Apache Avro current release:** https://avro.apache.org/project/download/ (the attempted `/releases/` and `/download/` paths returned 404)
- **S10 — Apache Kafka supported/archived releases:** https://kafka.apache.org/community/downloads/
- **S11 — MinIO upstream maintenance/distribution notice:** https://raw.githubusercontent.com/minio/minio/master/README.md
- **S12 — MinIO documentation endpoint now serving AIStor:** https://docs.min.io/community/minio-object-store/
- **S13 — Hadoop current releases:** https://hadoop.apache.org/release/
- **S14 — Hadoop 3.3.4 classpath and SDK compatibility guidance:** https://hadoop.apache.org/docs/r3.3.4/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html
- **S15 — Published dependency metadata:** https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-project/3.3.4/hadoop-project-3.3.4.pom and https://repo.maven.apache.org/maven2/org/apache/spark/spark-parent_2.12/3.5.9/spark-parent_2.12-3.5.9.pom
- **S16 — Supported Compose CLI generations and Compose Specification:** https://docs.docker.com/compose/intro/history/
- **S17 — Image tag availability:** Docker Hub API at `https://hub.docker.com/v2/repositories/confluentinc/cp-kafka/tags`, `confluentinc/cp-schema-registry/tags`, and `apache/spark/tags`; actual image pulls and container execution verified the selected tags.
- **S18 — Remaining Python version inventory:** PyPI JSON API `https://pypi.org/pypi/<package>/json` for each package in section 8.

**Final determination:** the updated Producer → Kafka → Schema Registry → Spark → Bronze path is verified, with Avro preserved and a repeatable bounded smoke check. Lifecycle and packaging follow-ups are explicitly documented rather than hidden behind the successful runtime result.
