# MILESTONE 1 — PRODUCER → KAFKA → SCHEMA REGISTRY → SPARK → BRONZE

## 1. Result

**PASS**

A real synthetic order event was successfully traced through:

```text
ORDER GENERATED
    ↓
CONFLUENT AVRO SERIALIZED
    ↓
SCHEMA REGISTRY
    ↓
KAFKA order-events
    ↓
SPARK STRUCTURED STREAMING
    ↓
SCHEMA REGISTRY-BACKED AVRO DECODING
    ↓
MINIO BRONZE PARQUET
```

The OIP infrastructure was stopped after verification. Kafka, MinIO, PostgreSQL, Redis, and other volumes were preserved.

---

## 2. Files inspected

Relevant files inspected:

- `producer/order_generator.py`
- `producer/kafka_publisher.py`
- `producer/schemas/order_event.avsc`
- `kafka/topic_config.py`
- `streaming/bronze_writer.py`
- `streaming/spark_session.py`
- `docker-compose.yml`
- `main.py`
- `requirements.txt`
- `.env`
- `.env.example`
- `utils/config.py`
- `utils/logger.py`
- `scripts/generate_cluster_id.sh`
- `scripts/wait_for_services.sh`
- Existing Kafka, Schema Registry, MinIO, and Spark references throughout the repository

Searches covered:

- `AvroSerializer`
- `AvroDeserializer`
- Schema Registry URLs and subjects
- `order-events`
- Kafka bootstrap servers
- `from_json`
- `from_avro`
- Spark Kafka source
- MinIO/S3A configuration
- Spark checkpoints
- `order_id`
- `event_type`
- Kafka topic creation
- Main CLI dispatch

---

## 3. Files modified

### `streaming/bronze_writer.py`

Replaced the incorrect JSON decoding path with a Confluent Avro decoder that:

- Validates the Confluent magic byte.
- Extracts the embedded schema ID.
- Fetches the corresponding schema from Schema Registry.
- Decodes the Avro payload with `fastavro.schemaless_reader`.
- Normalizes `timestamp-millis` into the existing Bronze epoch-millisecond representation.
- Raises errors instead of silently producing all-null records.

### `streaming/spark_session.py`

Added:

```python
spark.sql.session.timeZone = UTC
```

This ensures Spark timestamp interpretation and Bronze partitioning use UTC consistently.

### `docker-compose.yml`

Corrected Kafka internal/external listeners:

- Internal Docker clients use `kafka:9092`.
- Host clients use `localhost:9092`.
- Host port `9092` maps to Kafka’s external listener on container port `29092`.

This fixes the previous runtime failure where host clients received the advertised address `kafka:9092`, which was not resolvable from Windows.

### `kafka/topic_config.py`

Made `bootstrap_servers` optional for `create_topics()`.

It now defaults to:

```text
KAFKA_BOOTSTRAP_SERVERS
```

or:

```text
localhost:9092
```

This allows the existing `main.py topics` dispatcher to work without changing unrelated topic functionality.

### `main.py`

Removed the dispatcher service selector from `sys.argv` before invoking the selected service.

This fixes:

```text
python main.py producer --tps 1
```

Previously, the producer received the literal argument `producer` and rejected it through `argparse`.

### `requirements.txt`

Added the missing direct dependency:

```text
requests==2.32.3
```

The existing Confluent Schema Registry client imports `requests`, but the dependency was not declared.

---

## 4. Root cause

The original Bronze reader performed:

```python
F.col("value").cast("string")
F.from_json(...)
```

The producer performs:

```python
AvroSerializer(...)
```

Confluent Avro values are not plain JSON. Their wire format is:

```text
1 byte magic byte
4 byte schema ID
Avro binary payload
```

The original Spark job treated all of those bytes as a JSON string. Therefore:

- The Avro payload was not decoded.
- Spark could not recover the order event fields.
- Bronze would receive null or invalid structured fields.
- Downstream DQ and Silver processing would reject or misinterpret the data.

The producer also exposed an independent Kafka connectivity problem:

```text
Producer bootstrap: localhost:9092
Kafka advertised address: kafka:9092
```

The Windows host could connect initially but then could not resolve the advertised internal Docker hostname.

There were also two entry-point issues:

- `main.py topics` called `create_topics()` without its required argument.
- `main.py producer ...` passed the dispatcher selector into the producer’s argument parser.

---

## 5. Solution

The Bronze decoder now handles the actual Confluent Avro format.

For each Kafka value:

1. Verify the value has at least five bytes.
2. Verify the magic byte is `0`.
3. Extract the schema ID from bytes 1–4.
4. Request the schema from:

   ```text
   http://schema-registry:8081/schemas/ids/<schema-id>
   ```

5. Parse the returned schema with `fastavro`.
6. Decode the bytes after the five-byte Confluent header.
7. Convert the decoded record into the existing Spark `ORDER_EVENT_SCHEMA`.
8. Normalize the Avro timestamp to epoch milliseconds.
9. Pass the structured record to the existing Bronze writer.

The decoder intentionally raises on:

- Null Kafka values
- Short Confluent headers
- Invalid magic bytes
- Schema Registry failures
- Avro decoding failures
- Unsupported timestamp values

This prevents malformed Avro values from silently becoming all-null Bronze records.

The producer remains Avro-based. JSON was not introduced.

---

## 6. Runtime verification

### Infrastructure

Docker Compose was validated and started without deleting volumes.

Verified services:

- Kafka: healthy
- Schema Registry: healthy
- Kafka UI: healthy
- MinIO: reachable
- PostgreSQL: healthy
- Redis: healthy
- Prometheus: healthy
- Grafana: healthy

### Kafka topic

The corrected topic creation path was executed through:

```text
python main.py topics
```

Result:

```text
order-events created
payment-events created
order-events-dlq created
alerts-output created
```

`order-events` verification:

```text
PartitionCount: 8
ReplicationFactor: 1
```

Runtime Kafka offset state showed:

```text
order-events:4:6
```

The six test records were in partition `4`, with offsets `0` through `5`.

### Schema Registry

The producer registered:

```text
Subject: order-events-value
Version: 1
Schema ID: 1
```

The registered schema matched:

```text
producer/schemas/order_event.avsc
```

The exact subject is consistent with Confluent TopicNameStrategy:

```text
<topic>-value
```

### Producer

Command executed:

```text
python main.py producer --tps 1 --duration 2 --error-rate 0
```

Result:

```text
total_events: 6
failed: 0
```

The producer successfully:

- Connected to Kafka through the corrected external listener.
- Connected to Schema Registry.
- Registered the Avro schema.
- Serialized six events.
- Published six events.
- Received six successful delivery callbacks.

### Spark

Spark 3.5.3 was executed in an isolated Apache Spark 3.5.3 container on the existing `oip-network`.

The Spark job:

- Connected to Kafka using `kafka:9092`.
- Connected to Schema Registry using `schema-registry:8081`.
- Connected to MinIO using `minio:9000`.
- Loaded the Kafka connector.
- Started a Structured Streaming query.
- Consumed six Kafka records.
- Successfully decoded all six Confluent Avro records.
- Wrote Bronze batch `0`.

Spark log evidence:

```text
bronze_streaming_query_started
batch_id: 0
record_count: 6
bronze_batch_written
path: s3a://bronze/order-events/
```

### Bronze path

MinIO contained:

```text
bronze/order-events/
year=2026/month=10/day=03/hour=11/region=north-india/
part-00000-b6df2909-660b-4dc7-8993-d1f16c0c4f90.c000.snappy.parquet
```

Object size:

```text
5.8 KiB
```

### Bronze record verification

Bronze read-back returned:

```text
BRONZE_COUNT 6
```

Bronze schema:

```text
struct<
  order_id:string,
  event_type:string,
  user_id:string,
  product_id:string,
  product_name:string,
  region:string,
  amount:double,
  currency:string,
  payment_method:string,
  status:string,
  event_metadata:string,
  timestamp:bigint,
  event_ts:timestamp,
  _kafka_offset:bigint,
  _kafka_partition:int,
  _ingested_at:timestamp,
  _batch_id:bigint,
  year:string,
  month:string,
  day:string,
  hour:string
>
```

Representative decoded Bronze records:

```json
{
  "order_id": "ord-00e0dc93",
  "event_type": "ORDER_CREATED",
  "amount": 2410.03,
  "timestamp": 1791027201272,
  "event_ts": "2026-10-03T11:33:21.272Z",
  "_kafka_offset": 0,
  "_kafka_partition": 4,
  "region": "north-india"
}
```

```json
{
  "order_id": "ord-00e0dc93",
  "event_type": "ORDER_PAID",
  "amount": 2289.53,
  "timestamp": 1791027201473,
  "event_ts": "2026-10-03T11:33:21.473Z",
  "_kafka_offset": 1,
  "_kafka_partition": 4,
  "region": "north-india"
}
```

```json
{
  "order_id": "ord-e9b2b21a",
  "event_type": "ORDER_CANCELLED",
  "amount": 444.14,
  "timestamp": 1791027202474,
  "event_ts": "2026-10-03T11:33:22.474Z",
  "_kafka_offset": 5,
  "_kafka_partition": 4,
  "region": "north-india"
}
```

This proves:

- `order_id` is present.
- `event_type` is present.
- `amount` is present.
- Timestamp decoding is correct.
- UTC conversion is correct.
- Kafka offset and partition metadata are retained.
- The data is structured Parquet, not JSON text.

### Checkpoint

MinIO contained checkpoint state:

```text
s3a://checkpoints/milestone1-docker/bronze/metadata
s3a://checkpoints/milestone1-docker/bronze/offsets/0
s3a://checkpoints/milestone1-docker/bronze/sources/0/0
s3a://checkpoints/milestone1-docker/bronze/commits/0
```

### JSON failure verification

The modified Bronze implementation contains:

```text
FROM_JSON_PRESENT False
```

No JSON parsing was used.

There were no Bronze query errors related to:

- `from_json`
- `AnalysisException`
- `PythonException`
- Avro deserialization
- Schema Registry lookup

---

## 7. Tests/checks executed

| Check | Result |
|---|---|
| Python syntax check for modified files | PASS |
| Docker Compose config validation | PASS |
| Kafka listener correction | PASS |
| Kafka broker health | PASS |
| Schema Registry health | PASS |
| Kafka topic creation through `main.py topics` | PASS |
| `order-events` topic partition verification | PASS |
| Schema Registry subject registration | PASS |
| Producer import and startup | PASS |
| Producer Avro serialization | PASS |
| Producer delivery callbacks | PASS |
| Producer delivered count | PASS: 6 |
| Producer failed count | PASS: 0 |
| Spark 3.5.3 startup in Docker | PASS |
| Spark Kafka connector loading | PASS |
| Spark Schema Registry HTTP access | PASS |
| Spark Confluent Avro header decoding | PASS |
| Spark Kafka consumption | PASS |
| Bronze batch creation | PASS |
| Bronze record count | PASS: 6 |
| Bronze structured schema read-back | PASS |
| Bronze order/event/amount fields | PASS |
| Bronze Kafka metadata | PASS |
| Bronze MinIO object | PASS |
| Bronze checkpoint | PASS |
| Windows direct Spark startup | BLOCKED by missing `HADOOP_HOME`/`winutils.exe` |
| Silver/Gold/PostgreSQL/Redis/Dashboard/Alerts | NOT RUN, intentionally out of scope |

No destructive commands were executed.

No Kafka data, MinIO data, PostgreSQL data, or Docker volumes were deleted.

---

## 8. Remaining issues

### BLOCKING

None for the verified Spark-container execution path.

The requested milestone has been completed and verified using:

```text
Windows Docker Desktop infrastructure
+
Apache Spark 3.5.3 container
+
actual project source
+
actual Kafka
+
actual Schema Registry
+
actual MinIO
```

### NON-BLOCKING

1. Direct Windows PySpark execution still fails before application startup because Hadoop requires:

   ```text
   HADOOP_HOME
   hadoop.home.dir
   winutils.exe
   ```

   The Spark container path works and was used for the actual milestone verification.

2. The project has no dedicated Spark Compose service or application Dockerfile.

3. The Spark container verification required passing the Spark package coordinates explicitly to `spark-submit`.

4. The producer still uses `region` as its Kafka key rather than `order_id`.

5. Producer serialization exceptions are still logged/swallowed rather than propagated.

6. Producer `total_events` still counts attempts rather than only successful deliveries.

7. Kafka remains a single-node development configuration with replication factor 1.

8. The existing `main.py verify` and alert entry points remain unrelated broken paths.

9. Malformed-event handling was not advanced into DLQ/Silver processing because that belongs to a later milestone.

10. The generated Kafka topic set includes all existing configured topics, not only `order-events`.

---

## 9. Scope discipline

The following areas were intentionally left untouched:

- Silver
- Gold
- PostgreSQL writers and schemas
- Redis
- Dashboard
- Alerts
- Prometheus
- Grafana
- CI/CD
- Data-quality rules
- Production security hardening
- Downstream aggregation semantics

Only the following boundary-supporting files were modified:

- `streaming/bronze_writer.py`
- `streaming/spark_session.py`
- `docker-compose.yml`
- `kafka/topic_config.py`
- `main.py`
- `requirements.txt`

No Docker volumes were deleted.

No PostgreSQL reset was run.

No Kafka topic deletion was run.

No MinIO bucket deletion was run.

---

## 10. Next milestone

The next immediate milestone should be:

```text
BRONZE
    →
DATA QUALITY
    →
DLQ
    →
SILVER
```

That milestone should validate the six structured Bronze records produced here, implement DQ result persistence, verify malformed-event routing, and prove Silver deduplication without changing Gold, PostgreSQL serving, Redis, dashboard, alerting, or observability yet.
