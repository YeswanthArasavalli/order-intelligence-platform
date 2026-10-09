# ORDER INTELLIGENCE PLATFORM
# COMPLETE FORENSIC AUDIT REPORT

**Audit type:** Read-only forensic audit  
**Audit date:** 2026-10-03  
**Repository:** `<repository-root>`  
**Environment observed:** Windows 11/Docker Desktop, WSL2, Python 3.11–3.14 installations, Java 17  
**Code changes made:** None during the audit  
**Persistent data destroyed:** None  
**OIP containers:** Started temporarily for verification, then stopped. Volumes were preserved.

---

## 1. Executive Summary

This repository contains the beginnings of a local real-time order analytics platform.

The intended system is:

1. Generate e-commerce order lifecycle events.
2. Serialize them with Avro.
3. Publish them to Kafka.
4. Register the Avro schema in Schema Registry.
5. Process events with Spark Structured Streaming.
6. Store raw data in MinIO Bronze.
7. Validate and deduplicate into Silver.
8. Aggregate business metrics into Gold.
9. Persist Gold metrics into PostgreSQL.
10. Cache dashboard results in Redis.
11. Display KPIs in Streamlit.
12. Detect anomalies and send alerts.
13. Export metrics to Prometheus and Grafana.

The repository contains source files for most of those concepts, but the components are not currently connected into a working end-to-end system.

### What actually exists

- A functional Docker Compose infrastructure definition.
- A synthetic order event generator.
- An Avro Kafka producer.
- Kafka topic configuration code.
- Spark Bronze, Silver, and Gold job code.
- Data-quality validation functions.
- PostgreSQL schema and two PostgreSQL writer functions.
- A Redis client.
- A Streamlit dashboard.
- Alert rule and statistical detection functions.
- A Prometheus exporter.
- Prometheus and Grafana provisioning files.

### What does not currently exist

- A working end-to-end data path.
- Spark service orchestration.
- Application Dockerfiles.
- A working alert daemon.
- A working infrastructure verification module.
- Tests.
- Grafana dashboards.
- Gold writers for all documented Gold tables.
- Redis integration from the Gold layer.
- PostgreSQL ingestion and DQ logging.
- Prometheus scraping of the custom metrics exporter.
- A real event trace through the platform.

### Most serious finding

The producer writes Avro-serialized binary Kafka values, while the Bronze Spark reader casts Kafka values to strings and parses them as JSON:

- Producer: `producer/kafka_publisher.py`
- Consumer: `streaming/bronze_writer.py`

This breaks the primary data pipeline before Bronze can receive valid order fields.

### Runtime conclusion

The infrastructure layer can start successfully on Docker Desktop:

- Kafka: healthy
- Schema Registry: healthy
- Kafka UI: healthy
- PostgreSQL: healthy
- Redis: healthy
- Prometheus: healthy
- Grafana: healthy
- MinIO: functionally reachable but has no Docker healthcheck
- MinIO initialization job: exited successfully

However:

- Only Kafka internal topics exist.
- No application events exist.
- All PostgreSQL application tables are empty.
- Redis contains zero keys.
- Grafana has no dashboards.
- Prometheus has most configured targets down.
- No application service is running under Compose.

The project is therefore best classified as a **partially implemented local prototype**, not an integrated platform.

---

## 2. One-Paragraph Explanation

### As if explaining it to a 5-year-old

This project is supposed to watch imaginary online orders. When somebody buys something, the project writes down what happened, checks whether the information looks correct, counts the money and orders, and shows the results on a screen. If something strange happens, it is supposed to warn somebody.

### Technical explanation

This is intended to be a Kafka/Spark medallion architecture for order lifecycle analytics. Python generates lifecycle events such as `ORDER_CREATED`, `ORDER_PAID`, `ORDER_SHIPPED`, `ORDER_DELIVERED`, `ORDER_CANCELLED`, and `ORDER_REFUNDED`. The producer serializes them with Avro and publishes them to Kafka. Spark is intended to consume them, write raw Parquet to MinIO Bronze, validate and deduplicate records into Silver, aggregate revenue and product metrics into Gold, and persist selected aggregates into PostgreSQL. Redis is intended to serve dashboard hot-path data. Streamlit queries PostgreSQL and optionally Redis. Alert logic reads Gold PostgreSQL tables. A standalone Prometheus exporter reads PostgreSQL and Kafka state. The actual implementation currently stops short of connecting these pieces reliably.

---

## 3. Business Purpose

The intended business problem is real-time operational and commercial visibility for an e-commerce platform.

The system is supposed to answer questions such as:

- How much revenue was generated in the last minute?
- How many orders were paid?
- Which products are selling fastest?
- Which geographic regions are generating revenue?
- How many orders were cancelled or refunded?
- What is the current average order value?
- Are order events arriving with poor quality?
- Has revenue dropped unexpectedly?
- Is revenue spiking because of duplicate events?
- Are order failures increasing?
- Is there a regional outage or traffic drop?
- Is the data pipeline lagging?
- Are bad events being routed to a DLQ?

Likely users:

- Operations teams
- E-commerce business analysts
- Revenue and finance teams
- Data engineers
- Platform engineers
- Incident responders
- Product and regional managers

The business model described by the repository is:

```text
Net revenue =
    ORDER_PAID amount
  - ORDER_REFUNDED amount
  - ORDER_CANCELLED amount where was_paid = true
```

---

## 4. Intended End-to-End Flow

### Documented intended architecture

```text
Order Generator
      |
      | Avro
      v
Kafka order-events
      |
      | Schema Registry contract
      v
Spark Structured Streaming
      |
      +--------------------+
      |                    |
      v                    v
MinIO Bronze        Data Quality / DLQ
      |
      v
MinIO Silver
      |
      v
Gold Aggregations
      |
      +---------------------+--------------------+
      |                     |                    |
      v                     v                    v
MinIO Gold          PostgreSQL              Redis
      |                     |                    |
      +---------------------+--------------------+
                            |
                            v
                      Streamlit Dashboard
                            |
                            v
                   Alerts / Notifications

Prometheus <--- exporters / application metrics
Grafana     <--- Prometheus
```

### Actual verified architecture

```text
Python order generator
      |
      v
KafkaPublisher
      |
      | Avro binary value
      +----------------------------+
      |                            |
      v                            v
Schema Registry                 Kafka
      |                         order-events
      |                            |
      |                            v
      |                    Spark Bronze reader
      |                            |
      |                            | casts Avro binary to string
      |                            | attempts JSON parsing
      |                            v
      |                    NULL / invalid parsed rows
      |                            |
      v                            v
Schema subject                  MinIO Bronze
exists only if producer          likely receives invalid records
actually runs
                                   |
                                   v
                            Silver DQ rejects records
                                   |
                                   v
                             DLQ attempt to Kafka

Valid Silver-to-Gold path:
NOT PROVEN

Gold PostgreSQL:
Only two writer functions exist

Redis:
Alert cooldown functions are connected.
Gold KPI cache functions are not connected.

Dashboard:
Reads PostgreSQL directly.
Several SQL queries are broken.

Alerts:
Detection functions exist.
No working daemon exists.

Metrics:
Exporter exists.
Prometheus does not scrape it.

Grafana:
Prometheus datasource exists.
No dashboard JSON exists.
```

### Connection audit

| Source | Destination | Protocol / Address | Implementing code | Configuration | Current status |
|---|---|---|---|---|---|
| Order generator | Kafka publisher | In-process Python calls | `producer/kafka_publisher.py` | `event_stream()` | Implemented |
| Kafka publisher | Kafka | Kafka TCP, default `localhost:9092` | `KafkaPublisher.publish()` | `KAFKA_BOOTSTRAP_SERVERS` | Not tested with producer |
| Kafka publisher | Schema Registry | HTTP, default `http://localhost:8081` | `SchemaRegistryClient`, `AvroSerializer` | `SCHEMA_REGISTRY_URL` | Registry API verified; no subject registered |
| Kafka | Bronze Spark | Spark Kafka connector | `bronze_writer.read_kafka_stream()` | `spark.readStream.format("kafka")` | Broken contract |
| Bronze Spark | MinIO Bronze | S3A / Parquet | `write_to_bronze()` | `s3a://bronze/order-events/` | Not tested with Spark |
| Bronze | Data Quality | Spark DataFrame | `silver_transformer.write_to_silver()` | `validate(batch_df)` | Implemented in code |
| Data Quality | DLQ Kafka | Spark Kafka sink | `_write_to_dlq()` | `order-events-dlq` | Not tested |
| Silver | Gold | Spark Parquet streaming | `gold_aggregator.run()` | `s3a://silver/order-events/` | Broken/incomplete |
| Gold | PostgreSQL | SQLAlchemy | `postgres_writer.py` | `POSTGRES_*` | Two tables only; not tested with data |
| Gold | Redis | Intended pipeline | `storage/redis_client.py` | `REDIS_*` | Not connected |
| PostgreSQL | Dashboard | SQLAlchemy | `dashboard/app.py` | `POSTGRES_*` | Dashboard code exists; SQL problems |
| PostgreSQL | Alert engine | SQLAlchemy | `rule_engine.py`, `statistical_detector.py` | `POSTGRES_*` | No daemon |
| Redis | Alert cooldown | Redis TCP | `is_alert_active()`, `set_alert_active()` | `REDIS_*` | Code exists; no alerts observed |
| Metrics exporter | Prometheus | HTTP `/metrics` | `metrics_exporter.py` | Port `8000` | Exporter not deployed or scraped |
| Prometheus | Grafana | HTTP datasource | Grafana provisioning | `http://prometheus:9090` | Verified datasource |
| Grafana | Dashboard JSON | Filesystem provisioning | `dashboards.yml` | `/etc/grafana/provisioning/dashboards` | No dashboard files |

---

## 5. Repository Inventory

### Root files

| Directory/File | Purpose | Important? | Status |
|---|---|---:|---|
| `README.md` | Project intent, architecture, setup, roadmap | Yes | Aspirational and materially inaccurate |
| `PHASE_STATUS.md` | Declares Phases 1–11 complete | Yes | Contradicts source/runtime evidence |
| `docker-compose.yml` | Infrastructure services | Yes | Functional infrastructure definition with gaps |
| `Makefile` | Setup, lifecycle, verification, test commands | Yes | Several commands are broken |
| `main.py` | Service dispatcher | Yes | Contains missing/broken entry points |
| `requirements.txt` | Python dependency pins | Yes | Pinned, but not installed in audited WSL environment |
| `.env` | Active environment configuration | Yes | Contains development credentials; CRLF formatted |
| `.env.example` | Environment template | Yes | Missing variables used by code |
| `.gitignore` | Ignore rules | Yes | `.env` is ignored |
| `.vscode/settings.json` | VS Code Python configuration | Low | Minimal |
| `.github/workflows/ci.yml` | GitHub Actions workflow | Yes | Placeholder only |
| `datalake/__init__.py` | Data lake package marker | Low | Empty; no implementation |
| `tests/unit/__init__.py` | Unit test package marker | Yes | Empty |
| `tests/integration/__init__.py` | Integration test package marker | Yes | Empty |
| `tests/e2e/__init__.py` | End-to-end test package marker | Yes | Empty |

### Producer

| File | Purpose | Status |
|---|---|---|
| `producer/order_generator.py` | Generates order lifecycle events | Substantial implementation |
| `producer/kafka_publisher.py` | Publishes Avro messages to Kafka | Implemented but not runtime verified |
| `producer/schemas/order_event.avsc` | Avro event contract | Present |
| `producer/__init__.py` | Package marker | Empty |

### Kafka

| File | Purpose | Status |
|---|---|---|
| `kafka/topic_config.py` | Creates/list/deletes four Kafka topics | Implemented but dispatcher integration broken |
| `kafka/__init__.py` | Package marker | Empty |

### Streaming

| File | Purpose | Status |
|---|---|---|
| `streaming/spark_session.py` | Spark session and package configuration | Implemented but not runtime verified |
| `streaming/bronze_writer.py` | Kafka to Bronze Parquet | Broken Avro/JSON contract |
| `streaming/silver_transformer.py` | Bronze to Silver with DQ and DLQ | Partial and broken |
| `streaming/gold_aggregator.py` | Silver to Gold metrics | Partial and data-correctness risks |
| `streaming/__init__.py` | Package marker | Empty |

### Data quality

| File | Purpose | Status |
|---|---|---|
| `data_quality/validators.py` | Spark validation rules | Partial implementation |
| `data_quality/__init__.py` | Package marker | Empty |

### Storage

| File | Purpose | Status |
|---|---|---|
| `scripts/postgres/init.sql` | PostgreSQL schemas, tables, indexes, grants | Runtime verified |
| `storage/postgres_writer.py` | PostgreSQL upserts | Only two Gold tables implemented |
| `storage/redis_client.py` | Redis cache and alert state client | Partially implemented and mostly unwired |
| `storage/__init__.py` | Package marker | Empty |

### Dashboard

| File | Purpose | Status |
|---|---|---|
| `dashboard/app.py` | Streamlit dashboard | Partial and contains broken SQL |
| `dashboard/__init__.py` | Package marker | Empty |

### Alerts

| File | Purpose | Status |
|---|---|---|
| `alerts/rule_engine.py` | Threshold alert checks | Functions exist; no daemon |
| `alerts/statistical_detector.py` | Z-score and EWMA checks | Functions exist; no baseline producer or daemon |
| `alerts/alert_router.py` | Slack/PagerDuty notifications | Implemented but integrations disabled |
| `alerts/__init__.py` | Package marker | Empty |
| `alerts/daemon.py` | Expected alert daemon | Missing |

### Observability

| File | Purpose | Status |
|---|---|---|
| `observability/metrics_exporter.py` | Prometheus metrics HTTP server | Implemented but not deployed/scraped |
| `observability/prometheus/prometheus.yml` | Prometheus scrape config | Contains many nonexistent targets |
| `observability/grafana/provisioning/datasources/prometheus.yml` | Grafana datasource | Runtime verified |
| `observability/grafana/provisioning/dashboards/dashboards.yml` | Dashboard provider | Present, but no dashboards |
| `observability/__init__.py` | Package marker | Empty |

### Scripts

| File | Purpose | Status |
|---|---|---|
| `scripts/generate_cluster_id.sh` | Generates KRaft cluster ID | Bash-only |
| `scripts/wait_for_services.sh` | Waits for healthchecks | Cannot recognize MinIO because no healthcheck |
| `scripts/postgres/init.sql` | Database initialization | Runtime verified |
| `scripts/verify.py` | Expected by `main.py` | Missing |

### Non-application metadata

`.remember/` contains tool/session state and logs. It is not part of the application architecture and was excluded from implementation authority.

There is no `docs/architecture.md`, despite the README architecture badge linking to that path.

There are no Dockerfiles.

There are no dashboard JSON files.

There are no application YAML, JSON, or TOML configuration files beyond the listed provisioning files and Avro schema.

---

## 6. Component Inventory

| Component | Technology | Purpose | Entry point | Status |
|---|---|---|---|---|
| Infrastructure | Docker Compose | Runs local dependencies | `docker compose up -d` | Runtime verified partially |
| Kafka | Confluent Platform 7.7.1 / KRaft | Event broker | Compose | Healthy during probe |
| Schema Registry | Confluent 7.7.1 | Avro schema registry | Compose | Healthy; registry empty |
| Kafka UI | Provectus `v0.7.2` | Kafka inspection UI | Compose | Healthy |
| MinIO | Versioned 2024 release | Object storage | Compose | Reachable; no healthcheck |
| PostgreSQL | `16-alpine` | Gold analytics store | Compose | Healthy; empty tables |
| Redis | `7-alpine` | Hot cache and alert cooldown | Compose | Healthy; zero keys |
| Prometheus | `v2.54.1` | Metrics collection | Compose | Healthy; most targets down |
| Grafana | `11.3.0` | Metrics visualization | Compose | Healthy; no dashboards |
| Order generator | Python/Faker | Synthetic lifecycle events | `python -m producer.order_generator` | Dependencies absent in audited environment |
| Kafka producer | `confluent-kafka` | Avro publication | `python main.py producer` | Implemented, not proven |
| Topic admin | `confluent-kafka` AdminClient | Topic creation | `python -m kafka.topic_config` | Implemented; dispatcher call broken |
| Bronze writer | PySpark | Kafka to MinIO Bronze | `python main.py bronze` | Broken serialization contract |
| Silver transformer | PySpark | DQ, deduplication, DLQ | `python main.py silver` | Partial/broken |
| Gold aggregator | PySpark | Windowed metrics | `python main.py gold` | Partial/broken |
| PostgreSQL writer | SQLAlchemy | Gold upserts | Imported by Gold | Two tables only |
| Redis client | redis-py | Cache and alert state | Imported by dashboard/alerts | Mostly unwired |
| Dashboard | Streamlit | KPI visualization | `streamlit run dashboard/app.py` | Partial/broken |
| Rule engine | SQLAlchemy/Python | Threshold alerts | No valid daemon | Not operational |
| Statistical detector | SQLAlchemy/Python | Z-score/EWMA alerts | No valid daemon | Not operational |
| Alert router | httpx/Tenacity | Slack/PagerDuty | Called by alert logic | Integrations disabled |
| Metrics exporter | prometheus-client | Custom metrics | `python -m observability.metrics_exporter` | Not deployed/scraped |
| CI | GitHub Actions | Automation | `.github/workflows/ci.yml` | Placeholder |

---

## 7. Phase-by-Phase Status

| Phase | Intended goal | Implementation | Runtime test | Status | Evidence |
|---|---|---|---|---|---|
| 1 | Infrastructure | Compose defines Kafka, Schema Registry, UI, MinIO, PostgreSQL, Redis, Prometheus, Grafana | Docker startup and APIs tested | **IMPLEMENTED BUT BROKEN** | MinIO has no healthcheck; wait script expects one |
| 2 | Order event generator | Generator and Avro publisher exist | Producer not run; dependencies absent | **IMPLEMENTED BUT BROKEN** | Producer output contract conflicts with Bronze |
| 3 | Kafka infrastructure | Topic definitions and AdminClient exist | Only internal topics exist; project topic creation not run | **IMPLEMENTED BUT BROKEN** | `main.py topics` calls function without required argument |
| 4 | Bronze ingestion | Spark Kafka reader and Parquet writer exist | Not tested | **IMPLEMENTED BUT BROKEN** | Avro bytes are parsed as JSON |
| 5 | Data quality | Validator functions exist | Not tested | **PARTIALLY IMPLEMENTED** | Nine failure flags; no DQ database logging |
| 6 | Silver | DQ, deduplication, Silver Parquet, DLQ code exists | Not tested | **IMPLEMENTED BUT BROKEN** | Empty Bronze fails schema inference; no cross-batch dedup |
| 7 | Gold | Revenue and product aggregations exist | Not tested | **IMPLEMENTED BUT BROKEN** | Only two of five documented Gold outputs implemented |
| 8 | PostgreSQL | Nine tables and two upsert writers exist | Database schema verified; no application rows | **PARTIALLY IMPLEMENTED** | No writers for region, lifecycle, baseline, DQ, ingestion logs |
| 9 | Dashboard | Streamlit app and PostgreSQL queries exist | App not launched | **IMPLEMENTED BUT BROKEN** | Interval SQL and DQ aggregate query are invalid |
| 10 | Alerting | Rule and statistical functions exist | No daemon run | **IMPLEMENTED BUT BROKEN** | `alerts.daemon` missing; baselines never populated |
| 11 | Observability | Exporter and Prometheus config exist | Prometheus target API tested | **IMPLEMENTED BUT BROKEN** | Exporter not composed or scraped; most targets nonexistent |
| 12 | Dockerization | Intended Dockerfiles per service | No Dockerfiles | **NOT IMPLEMENTED** | `docker-compose.yml` only runs infrastructure |
| 13 | CI/CD | Intended lint/test/build/deploy workflow | Workflow runs only `echo` | **NOT IMPLEMENTED** | `.github/workflows/ci.yml` is explicitly a placeholder |

No phase qualifies as `IMPLEMENTED AND LOCALLY VERIFIED` or `IMPLEMENTED AND END-TO-END VERIFIED`.

---

## 8. Actual Data Flow and One-Event Trace

### Required trace result

A real event could not be traced end-to-end.

No producer was run. No application topic existed. No event ID, partition offset, Bronze record, Silver record, Gold row, Redis entry, dashboard value, or alert was observed.

| Trace item | Result |
|---|---|
| Event ID | UNKNOWN |
| Order ID | UNKNOWN |
| Kafka topic | Application topic absent |
| Kafka partition | UNKNOWN |
| Kafka offset | UNKNOWN |
| Schema Registry subject | None registered; registry returned `[]` |
| Bronze location | No application data |
| Bronze record | No application data |
| DQ result | No application data |
| Silver record | No application data |
| Gold aggregation | No application data |
| PostgreSQL row | Tables empty |
| Redis key | Redis contained zero keys |
| Dashboard representation | No event-backed value |
| Alert representation | No alert data |
| Prometheus metric | No application metric |

### Static trace of what would happen

1. `order_generator.py` creates an event with an `order_id`.
2. `kafka_publisher.py` serializes it using `AvroSerializer`.
3. The Avro serializer produces binary bytes.
4. Kafka receives the binary value if `order-events` exists.
5. `bronze_writer.py` executes:

   ```python
   F.from_json(F.col("value").cast("string"), ORDER_EVENT_SCHEMA)
   ```

6. Avro binary bytes are not JSON.
7. Parsed fields become null or invalid.
8. Bronze may write records with null business fields.
9. Silver validation rejects them.
10. No valid Silver record is proven.
11. Gold cannot produce valid metrics.
12. PostgreSQL, Redis, dashboard, alerts, and application metrics remain empty.

This is the main data-path break.

---

## 9. Infrastructure Status

The infrastructure was started once using existing persistent volumes and then stopped.

| Service | Container | Port | Runtime probe | Health | Functional result |
|---|---|---:|---|---|---|
| Kafka | `oip-kafka` | 9092, 29093 | Broker API succeeded | Healthy | Kafka broker functional |
| Schema Registry | `oip-schema-registry` | 8081 | `/subjects` returned `[]` | Healthy | API functional; no application schema |
| Kafka UI | `oip-kafka-ui` | 8080 | `/actuator/health` returned `{"status":"UP"}` | Healthy | Functional |
| MinIO | `oip-minio` | 9000, 9001 | `/minio/health/live` succeeded | No healthcheck | API functional; health integration incomplete |
| MinIO init | `oip-minio-init` | None | Container exited 0 | Not applicable | Logs show all four buckets created |
| PostgreSQL | `oip-postgres` | 5432 | `pg_isready` succeeded | Healthy | Database functional; tables empty |
| Redis | `oip-redis` | 6379 | `PONG` | Healthy | Functional; zero keys |
| Prometheus | `oip-prometheus` | 9090 | `/-/ready` succeeded | Healthy | Functional; scrape gaps |
| Grafana | `oip-grafana` | 3000 | `/api/health` succeeded | Healthy | Functional; no dashboards |

### Runtime observations

- `docker.exe compose config` succeeded.
- Compose defines nine services including the one-shot `minio-init`.
- The README repeatedly describes eight services and omits the init container.
- At report close, OIP containers were stopped by the audit. Persistent volumes remain.
- Historical stopped containers showed Kafka, Schema Registry, and Kafka UI had previously exited with code 137. A fresh non-destructive startup later reached healthy status.
- PostgreSQL logs showed the database was already initialized and skipped `init.sql` on subsequent starts.

---

## 10. Kafka Audit

### Topic definitions

`kafka/topic_config.py` defines:

| Topic | Partitions | Replication | Retention |
|---|---:|---:|---:|
| `order-events` | 8 | 1 | 7 days |
| `payment-events` | 4 | 1 | 7 days |
| `order-events-dlq` | 1 | 1 | 30 days |
| `alerts-output` | 1 | 1 | 14 days |

### Runtime state

The verified Kafka topic list contained only:

```text
__consumer_offsets
_schemas
```

No application topic was created.

### Configuration

- KRaft mode: yes
- ZooKeeper: no
- Single combined broker/controller: yes
- Replication factor: 1
- Auto topic creation: disabled
- Broker default partitions: 1
- Application topic definition: 8 partitions for `order-events`
- Producer acknowledgements: `acks=all`
- Producer retries: 5
- Compression: Snappy
- Idempotence: not enabled explicitly
- Transactions: not used
- Delivery callback: yes
- Publish exceptions: logged and swallowed
- Producer key: `region`
- Consumer group: not explicitly configured for Spark
- Bronze starting offset: `earliest`
- Bronze data loss behavior: `failOnDataLoss=false`

### Problems

1. Topics are not automatically created by Compose.
2. `main.py topics` is broken because `create_topics()` requires `bootstrap_servers`, but the dispatcher calls it with no argument.
3. The Makefile refers to `kafka-topics.sh`, but the actual Kafka image contains `kafka-topics`, not `kafka-topics.sh`.
4. `payment-events` has no producer.
5. `alerts-output` has no producer or consumer.
6. Spark does not use Schema Registry.
7. Spark does not explicitly use a stable consumer group called `spark-cg`.
8. Metrics claims to report `spark-cg` lag but probes committed offsets for another temporary consumer group.
9. Producer failures during serialization are logged but not routed to a failure topic.
10. No idempotent producer configuration is present.
11. One broker and replication factor 1 make this local-development-only.

### Classification

**DEMO / LOCAL DEVELOPMENT / PROTOTYPE**

It is not production-like because:

- Single Kafka broker
- Replication factor 1
- Plaintext listeners
- No authentication
- No TLS
- No idempotent producer configuration
- No transactional semantics
- No operational consumer-lag enforcement
- No retry/dead-letter policy for producer serialization failures

---

## 11. Schema Audit

### Avro fields

| Field | Avro type | Required? | Producer behavior |
|---|---|---:|---|
| `order_id` | string | Yes | Normal events populated; malformed null case invalid |
| `event_type` | enum | Yes | Six lifecycle values |
| `user_id` | string | Yes | Populated |
| `product_id` | string | Yes | Populated |
| `product_name` | string | Yes | Populated |
| `region` | string | Yes | Five regions |
| `amount` | double | Yes | Floating-point monetary amount |
| `currency` | string, default INR | Yes in writer schema | INR |
| `payment_method` | nullable string | No | Usually populated |
| `status` | string | Yes | Lifecycle status |
| `event_metadata` | nullable string | No | JSON serialized into a string |
| `timestamp` | long timestamp-millis | Yes | Epoch milliseconds |
| `schema_version` | string, default 1.0 | Yes in writer schema | `"1.0"` |

### Mismatches

#### Avro producer versus Spark consumer

Producer:

```python
AvroSerializer(...)
```

Consumer:

```python
F.col("value").cast("string")
F.from_json(...)
```

This is a hard data-contract mismatch.

#### Schema Registry versus Spark

The producer uses Schema Registry. Spark does not retrieve or use the registered schema. The included Spark Avro package is not used for Kafka deserialization.

#### Monetary values

Amounts are represented as Avro `double` and Python `float`.

This is unsuitable for financial correctness at scale because binary floating point can introduce rounding errors. PostgreSQL uses `NUMERIC`, but conversion occurs through Python `float` in `postgres_writer.py`.

#### Timestamps

- Producer: epoch milliseconds
- Avro: logical timestamp-millis
- Bronze/Silver/Gold: manually divide by `1000` and cast to Spark timestamp
- No explicit UTC session timezone is configured
- Partitioning therefore depends on Spark/JVM timezone behavior

#### Malformed events

`make_malformed()` adds `_malformed` and `_bad_type` fields that are not in the Avro schema.

The null `order_id` case cannot satisfy the required Avro string field and is likely rejected before reaching Kafka. Other malformed cases can be structurally valid Avro while semantically invalid.

#### Compatibility

Schema Registry is configured for `BACKWARD`, but:

- No schema evolution process exists.
- No consumer compatibility tests exist.
- No schema version migration exists.
- No v2 schema exists.

---

## 12. Producer Audit

### Implemented behavior

`producer/order_generator.py` contains:

- Indian region distribution
- Fifteen products
- Payment methods
- Order lifecycle state
- Paid events
- Shipment events
- Delivery events
- Cancellations
- Refunds
- Malformed-event injection
- CLI parameters for TPS, duration, error rate, and pretty output

`producer/kafka_publisher.py` contains:

- Schema loading
- Schema Registry client
- Avro serializer
- Kafka producer
- Delivery callback
- Graceful signal handling
- Flush logic

### Problems

1. The order generator cannot run in the audited WSL Python environment because `faker` is not installed.
2. The Windows Python 3.11 installation also lacks `faker`.
3. No producer test exists.
4. No topic is automatically created.
5. Producer uses `region` as the Kafka key rather than `order_id`.
6. Serialization exceptions are logged but swallowed.
7. `_failed` counts delivery callback failures, not serialization failures.
8. `total` counts attempted events, not successfully delivered events.
9. No `message.timeout.ms` is configured.
10. No idempotence is explicitly enabled.
11. No producer metrics are exported despite Prometheus configuration referring to a producer target.
12. The default event rate is described as orders per second, but each order emits multiple lifecycle events, so actual Kafka message rate is much higher.
13. There is no producer container or Compose service.

### Runtime result

Producer was not executed. No application events exist in Kafka.

---

## 13. Spark Audit

### Spark session

`streaming/spark_session.py` configures:

- PySpark 3.5.3
- Spark Kafka connector 3.5.3
- Spark Avro package 3.5.3
- Hadoop AWS 3.3.4
- AWS SDK bundle
- PostgreSQL JDBC driver
- MinIO S3A endpoint
- S3A access key and secret
- Local master
- Checkpoint directory
- Eight shuffle partitions

### Compatibility

- Windows Java 17 was detected.
- PySpark 3.5.3 is broadly compatible with Java 17.
- Spark itself was not run.
- WSL has no Java command.
- Python dependencies are not installed in WSL.
- No Spark Docker image or Compose service exists.
- Spark JARs are downloaded dynamically through Maven at session startup.
- This makes execution dependent on network access and local Ivy cache state.

### Bronze

Input:

- Kafka topic
- `startingOffsets=earliest`
- `maxOffsetsPerTrigger=10000`
- `failOnDataLoss=false`

Output:

- Parquet
- S3A / MinIO
- Append mode
- Partitioned by year, month, day, hour, region

Problems:

- Avro is decoded as JSON.
- `failOnDataLoss=false` can hide data loss.
- No explicit Kafka consumer group.
- No ingestion log written to `bronze.ingestion_log`.
- No explicit UTC handling.
- No schema-registry integration.
- No Bronze-specific retention enforcement.

### Silver

Input:

- Bronze Parquet stream
- Schema inferred using:

```python
spark.read.parquet(bronze_base).schema
```

If Bronze has no existing files, startup fails before the streaming query begins.

Deduplication:

```python
dropDuplicates(["order_id", "event_type"])
```

This is only applied inside each micro-batch.

The comment claims cross-batch deduplication through watermark state, but:

- No watermark is applied in Silver.
- `dropDuplicatesWithinWatermark` is not used.
- The source is a Parquet stream.
- Cross-batch duplicates are not proven to be removed.

DLQ:

- Written as JSON to Kafka.
- No retry/idempotence control.
- No DLQ metrics are incremented.

### Gold

Gold applies a watermark before `foreachBatch`, but the actual aggregations occur inside each batch.

This means the watermark does not provide full stateful accumulation for the manually recomputed batch aggregations.

Major issue:

- Each batch writes partial window totals.
- PostgreSQL upserts replace previous window values.
- Later batches can overwrite earlier partial values instead of accumulating them.
- MinIO Gold writes append duplicate aggregates for the same window.

The Gold query is therefore not proven correct for multi-batch windows.

---

## 14. Bronze/Silver/Gold Audit

### Bronze

Implemented path:

```text
Kafka -> Spark DataFrame -> MinIO Parquet
```

Intended path is present in code but broken by Avro/JSON mismatch.

Expected path:

```text
s3a://bronze/order-events/year=YYYY/month=MM/day=DD/hour=HH/region=<region>/
```

No Bronze data was found.

### Silver

Implemented:

- Validation call
- DQ pass/fail split
- Intra-batch deduplication
- Silver metadata
- Silver Parquet write
- DLQ Kafka write

Missing or broken:

- Durable cross-batch deduplication
- DQ database log
- DQ result metrics
- Empty-source startup handling
- Provenance consistency
- Full schema enforcement

### Gold

Implemented:

- `revenue_by_minute`
- `product_rankings`

Not implemented despite schema/documentation:

- `region_heatmap`
- `lifecycle_funnel`
- `anomaly_baseline`

Gold also does not:

- Write Redis KPI keys
- Write Redis leaderboard keys
- Populate baseline statistics
- Persist Gold batch metadata
- Correctly accumulate partial windows across batches

---

## 15. Data Quality Audit

### Validation rules described

The module claims ten rules:

1. Null order ID
2. Null user ID
3. Null product ID
4. Negative amount
5. Invalid timestamp
6. Future timestamp
7. Invalid region
8. Invalid product ID
9. Invalid event type
10. Schema drift

### Actual implementation

There are nine failure columns:

```text
_fail_null_order_id
_fail_null_user_id
_fail_null_product_id
_fail_negative_amount
_fail_invalid_timestamp
_fail_invalid_region
_fail_invalid_product_id
_fail_invalid_event_type
_fail_schema_drift
```

Future timestamps are folded into `_fail_invalid_timestamp`; they do not have their own flag or failure label.

### Defects

1. `invalid_product_id` uses a regex without a final `$`, so values such as `prod-123abc` may pass.
2. Refunded events bypass the negative-amount condition entirely, including null, zero, or negative values.
3. `schema_drift` only fires when `order_id`, `event_type`, and `user_id` are all null.
4. Missing individual required fields are not necessarily classified as schema drift.
5. Currency is not validated.
6. Status is not validated against event type.
7. Payment method is not validated.
8. Event metadata JSON validity is not checked.
9. DQ results are not written to `silver.dq_run_log`.
10. DQ pass-rate metrics have no source data.
11. DQ failures are not surfaced to Prometheus.
12. A zero-record batch returns 100% from `compute_quality_score()`, while the PostgreSQL generated column returns 0% for zero records. The semantics disagree.

### Classification

**PARTIAL IMPLEMENTATION**

The validation expressions are real code, but the operational DQ system described by the architecture does not exist.

---

## 16. PostgreSQL Audit

### Runtime-verified database state

The running probe found:

- Four schemas:
  - `bronze`
  - `silver`
  - `gold`
  - `alerts`
- Nine tables
- All application table row counts: zero

### Tables

| Schema | Tables |
|---|---|
| `bronze` | `ingestion_log` |
| `silver` | `dq_run_log` |
| `gold` | `revenue_by_minute`, `product_rankings`, `region_heatmap`, `lifecycle_funnel`, `anomaly_baseline` |
| `alerts` | `alert_log`, `active_alert_state` |

### Implemented writers

`storage/postgres_writer.py` implements upserts for:

- `gold.revenue_by_minute`
- `gold.product_rankings`

No writer exists for:

- `gold.region_heatmap`
- `gold.lifecycle_funnel`
- `gold.anomaly_baseline`
- `silver.dq_run_log`
- `bronze.ingestion_log`
- `alerts.active_alert_state`

### Problems

1. Database initialization is one-shot and only runs on an empty volume.
2. Grants are hardcoded to role `oip_user`.
3. If `POSTGRES_USER` is changed, grants may target a nonexistent or wrong role.
4. No migration system exists.
5. Gold writes collect Spark DataFrames to the driver.
6. PostgreSQL writes have no retry policy.
7. PostgreSQL writes have no explicit statement timeout.
8. Float conversion can lose monetary precision.
9. Gold upserts replace partial window values rather than accumulating them.
10. Database tables are structurally broader than the active implementation.

---

## 17. Redis Audit

### Implemented key functions

`storage/redis_client.py` defines key builders for:

- Revenue KPIs
- Summary KPIs
- Product leaderboard
- Active alerts
- Active alert list
- Regional heatmap

### Runtime state

Redis responded to `PING`.

Redis key count was:

```text
0
```

### Actual integration

Alert logic uses:

- `is_alert_active()`
- `set_alert_active()`

The Gold aggregator does not call:

- `set_revenue_kpis()`
- `set_product_leaderboard()`

The dashboard imports Redis summary and leaderboard functions, but:

- Summary keys are never populated by Gold.
- Leaderboard cache is not used by the dashboard.
- `is_alert_active` is imported but not used in the dashboard.
- Heatmap key functions are unused.

### Problems

1. Redis KPI caching is effectively disconnected.
2. Redis cooldown state duplicates the PostgreSQL `active_alert_state` design.
3. `ALERT_COOLDOWN_MINUTES` is documented but not used directly.
4. Redis has no password configured.
5. Redis is exposed on host port 6379.
6. Alert check-and-set is not atomic, so concurrent alert processes could duplicate alerts.
7. No cache invalidation strategy exists beyond TTL.
8. No Redis failure metric is exported.

---

## 18. Dashboard Audit

### Implemented panels

The code attempts to display:

- Net revenue
- Order count
- Average order value
- DQ pass rate
- Revenue time series
- Regional revenue bars
- Product rankings
- Active alerts
- System information

### Actual data sources

- PostgreSQL Gold tables
- PostgreSQL DQ table
- PostgreSQL alert table
- Redis summary as an optional first lookup

The dashboard does not read Gold Parquet directly. It reads PostgreSQL Gold tables, which is reasonable if PostgreSQL is the intended analytics serving layer.

### Confirmed SQL problems

The dashboard uses queries such as:

```sql
WHERE window_start >= NOW() - INTERVAL ':minutes minutes'
```

A bind variable inside a quoted PostgreSQL interval literal is not a valid parameterization pattern.

The same issue exists in multiple dashboard queries and in the statistical detector.

The DQ query:

```sql
SELECT COALESCE(AVG(pass_rate_pct), 100.0)
FROM silver.dq_run_log
ORDER BY run_at DESC
LIMIT 10
```

is invalid because `run_at` is neither grouped nor aggregated. It also would not correctly calculate the average of the last ten rows.

### Additional problems

1. Redis availability means import success, not connectivity success.
2. PostgreSQL status is hardcoded as connected.
3. Kafka status is hardcoded as connected.
4. Spark status is hardcoded as running.
5. The dashboard has no general database error handling.
6. An unavailable database can crash the Streamlit script.
7. The documented lifecycle funnel panel is not rendered.
8. The documented regional heatmap is actually a bar chart backed by `revenue_by_minute`.
9. When Redis summary is used, `avg_order_value` is absent from the summary payload and defaults to zero.
10. No Dockerized dashboard service exists.
11. Auto-refresh uses `time.sleep()` followed by `st.rerun()`, blocking the Streamlit script between refreshes.

### Runtime result

Grafana and infrastructure APIs were verified. Streamlit itself was not launched.

---

## 19. Alerting Audit

### Rule-based engine

Implemented checks:

- Revenue drop
- Revenue spike
- Zero orders during business hours
- High failure rate

Documented but not implemented:

- No-data alert

The `no_data` rule cannot fire because `_load_current_window()` returning no rows causes the engine to return immediately.

### Statistical detector

Implemented:

- Z-score
- EWMA short/long comparison
- Redis cooldown check
- PostgreSQL alert record
- Notification routing

Missing:

- Baseline population job
- Nightly baseline computation
- Baseline maintenance
- Alert daemon
- Scheduled execution
- Auto-resolution logic

### Critical missing entry point

`main.py` maps:

```python
"alerts": "alerts.daemon:run"
```

But `alerts/daemon.py` does not exist.

Therefore:

```text
python main.py alerts
```

cannot work.

### Notification problems

- Slack webhook is empty in the active configuration.
- PagerDuty key is empty.
- Alert rows are still recorded with `channel='slack'` even when no notification was sent.
- `active_alert_state` PostgreSQL table is never written.
- Redis is used instead of the documented SQL state table.
- Cooldown is not atomic.
- Notification delivery and database recording are not transactionally coordinated.
- Failure-rate alerts pass percentages to a Slack payload that formats values as currency.

### Classification

**IMPLEMENTED BUT BROKEN**

The detection functions are real, but there is no continuously running alert system.

---

## 20. Observability Audit

### Metrics exporter

Defined metrics include:

- Revenue per minute
- Orders per minute
- DQ pass rate
- Active alerts
- Kafka consumer lag
- Spark batch duration
- DLQ record count

Actually updated:

- Revenue gauges
- Order gauges
- DQ gauge
- Active alert gauge
- Kafka lag gauge, subject to incorrect group assumptions

Never updated:

- `SPARK_BATCH_DURATION`
- `DLQ_RECORDS`

### Deployment problem

`metrics_exporter.py` starts an HTTP server on port 8000, but:

- It is not a Compose service.
- No Dockerfile exists.
- Prometheus does not scrape `localhost:8000`.
- Prometheus expects `spark-metrics:8000`, not the actual exporter.
- No exporter process was running.

### Prometheus runtime evidence

Prometheus itself was healthy.

Runtime target results:

- Prometheus: up
- Grafana: up
- Kafka: down; no exporter at `kafka:7071`
- Kafka lag monitor: down; hostname does not exist
- MinIO: down; endpoint returned 403
- Producer: down; hostname does not exist
- Alert engine: down; hostname does not exist
- DQ engine: down; hostname does not exist
- PostgreSQL exporter: down; hostname does not exist
- Redis exporter: down; hostname does not exist
- Spark metrics: down; hostname does not exist

### Grafana runtime evidence

- Grafana API was healthy.
- Prometheus datasource was provisioned.
- Dashboard search returned `[]`.
- Dashboard provisioning directory contained only `dashboards.yml`.
- No dashboard JSON exists.

### Classification

**IMPLEMENTED BUT BROKEN**

The observability code exists, but the observability architecture is not deployed.

---

## 21. Security Audit

### Credentials

The active `.env` contains development credentials. Values are intentionally not reproduced here.

Characteristics:

- MinIO uses a development root account.
- PostgreSQL uses a development password.
- Grafana uses a development admin password.
- Redis has no password.
- Kafka uses PLAINTEXT.
- Schema Registry has no authentication.
- Kafka UI authentication is disabled.
- PostgreSQL, Redis, Kafka, MinIO, Prometheus, and Grafana ports are exposed to the host.
- Slack and PagerDuty credentials are empty.

### Network security

- No TLS.
- No Kafka SASL.
- No Kafka ACLs.
- No Schema Registry authentication.
- No MinIO least-privilege user.
- PostgreSQL is exposed directly.
- Redis is exposed directly and unauthenticated.
- Kafka UI access is unrestricted.
- Grafana authentication uses a default-style local admin setup.

### Secret management

- `.env` is ignored by Git.
- `.env` exists physically in the project directory.
- No external secret manager exists.
- Passwords are assembled into SQLAlchemy DSNs.
- Compose credentials are passed through environment variables.

### Classification

Acceptable only for isolated local development. Not suitable for a shared or production environment.

---

## 22. Dependency and Version Audit

### Declared Python dependencies

| Area | Declared version |
|---|---|
| `confluent-kafka` | 2.5.3 |
| `fastavro` | 1.9.7 |
| `pyspark` | 3.5.3 |
| `boto3` | 1.35.62 |
| `psycopg2-binary` | 2.9.10 |
| SQLAlchemy | 2.0.36 |
| Redis | 5.2.0 |
| Streamlit | 1.40.1 |
| Plotly | 5.24.1 |
| Pandas | 2.2.3 |
| Faker | 31.0.0 |
| Prometheus client | 0.21.0 |
| HTTPX | 0.27.2 |
| Tenacity | 9.0.0 |
| Pydantic | 2.9.2 |
| Structlog | 24.4.0 |
| Pytest | 8.3.3 |
| Ruff | 0.7.4 |
| Black | 24.10.0 |

### Declared infrastructure versions

| Component | Version |
|---|---|
| Confluent Kafka / Schema Registry | 7.7.1 |
| Kafka UI | `v0.7.2` |
| MinIO | `RELEASE.2024-11-07T00-52-20Z` |
| MinIO client | `RELEASE.2024-11-05T11-29-45Z` |
| PostgreSQL | `16-alpine` |
| Redis | `7-alpine` |
| Prometheus | `v2.54.1` |
| Grafana | `11.3.0` |

### Runtime environment findings

WSL:

- Python 3.12.3
- No `pip`
- No Java
- No `pytest`
- No Docker command

Windows:

- Python 3.11.9 exists
- Java 17.0.19 exists
- Python 3.11 contains only basic packaging tools
- `faker` is absent
- Full declared requirements are not installed in Python 3.11
- Python 3.14 has pytest, but the repository requirements target Python 3.11+

### Dependency issues

1. `boto3` is declared but not used by application code.
2. `fastavro` is declared but not directly used.
3. Spark packages are downloaded dynamically rather than provisioned in an image.
4. No dependency lockfile beyond requirements pins exists.
5. No Dockerfile installs application dependencies.
6. No Java version is pinned in project configuration.
7. No Spark package cache or offline strategy exists.
8. `POSTGRES_JDBC_URL` is documented but not used by the PostgreSQL writer.
9. Multiple modules duplicate database DSN construction rather than using centralized settings.

---

## 23. Windows Compatibility Audit

### Works on Windows/Docker Desktop

Verified or structurally supported:

- Docker Desktop Compose configuration
- Docker volume mounting from the Windows repository path
- Kafka container
- Schema Registry container
- Kafka UI container
- PostgreSQL
- Redis
- Prometheus
- Grafana
- MinIO API
- Java 17 availability
- PowerShell access to Docker Desktop

### Requires WSL, Git Bash, or another Bash environment

- `scripts/generate_cluster_id.sh`
- `scripts/wait_for_services.sh`
- Makefile recipes
- `make setup`
- `make wait`
- `make verify`
- `make generate-cluster-id`

### Makefile issues on Windows

- Make is not installed as a PowerShell command.
- `SHELL := /bin/bash` requires Bash.
- Uses `grep`, `awk`, `sed`, `tr`, `cp`, `read`, and Unix shell syntax.
- PowerShell cannot run the Makefile directly.
- `make help` produced `Makefile` as the target label for every entry instead of the actual target names.
- Make variable expansion breaks verification commands containing `${VAR:-default}`.
- `make -n verify` expanded URLs such as `localhost:/subjects`, omitting the port.
- `make -n verify` expanded PostgreSQL credentials and names incorrectly in shell commands.
- `make verify` does not fail the overall target when checks fail.
- `make kafka-topics` uses `kafka-topics.sh`, absent from the actual Kafka image.
- `make logs-spark` references a nonexistent Compose service named `spark`.

### `.env` line endings

`.env` contains CRLF line endings.

Sourcing it in WSL Bash generated carriage-return errors such as:

```text
$'\r': command not found
```

Docker Compose parsed it successfully through Docker Desktop, but direct Bash workflows are affected.

### Host/container hostname confusion

The `.env` values use internal Docker names:

```text
kafka:9092
schema-registry:8081
minio:9000
postgres:5432
redis:6379
```

These are correct inside the Compose network but not generally correct for Python processes launched directly from Windows or WSL.

The Kafka broker advertises:

```text
PLAINTEXT://kafka:9092
```

This is an internal Docker hostname. It creates a host-client interoperability risk even when the host bootstraps through `localhost:9092`.

---

## 24. Test Audit

### Repository test inventory

There are no actual test functions.

The test tree contains only:

```text
tests/unit/__init__.py
tests/integration/__init__.py
tests/e2e/__init__.py
```

Test count:

```text
0
```

### Executed checks

| Check | Result |
|---|---|
| Python AST parsing of 31 Python files | PASS |
| Shell syntax check with `bash -n` | PASS |
| Docker Compose config validation | PASS |
| Docker infrastructure startup | PASS for most infrastructure |
| Kafka broker API | PASS |
| Schema Registry API | PASS, returned empty registry |
| Kafka UI health | PASS |
| PostgreSQL health | PASS |
| Redis ping | PASS |
| Prometheus readiness | PASS |
| Grafana health | PASS |
| MinIO health endpoint | PASS |
| Kafka application topic existence | FAIL/ABSENT; only internal topics |
| PostgreSQL application data | No data; all nine tables empty |
| Redis application data | No data; zero keys |
| Grafana dashboards | None |
| Producer execution in WSL | ERROR: `faker` absent |
| Main dispatcher in WSL | ERROR: `structlog` absent |
| Pytest collection | `0 items`; no tests ran |
| `make test` in WSL | ERROR: `pytest` command not found |
| `make test-unit` in WSL | ERROR: `pytest` command not found |
| `make wait` before Docker Desktop route | TIMEOUT/blocked by missing WSL Docker command |
| End-to-end event trace | NOT TESTED / impossible without producer and app services |
| Spark jobs | NOT TESTED |
| Dashboard | NOT TESTED |
| Alert daemon | NOT TESTED; entry point missing |
| Custom metrics exporter | NOT TESTED |
| Topic creation through project code | NOT TESTED |

No test demonstrates that an event can reach Gold.

---

## 25. Documentation Accuracy

| Documentation claim | Actual result |
|---|---|
| `PHASE_STATUS.md` says Phases 1–11 are complete | Source and runtime show partial/broken phases |
| README roadmap says Phases 2–13 are pending | `PHASE_STATUS.md` contradicts it and claims Phases 2–11 complete |
| README says eight services become healthy | Compose has nine services including `minio-init`; MinIO has no healthcheck |
| README says Spark is part of the platform runtime | No Spark Compose service exists |
| README says all services run through Compose | Application services must be run manually and some have no working entry point |
| README says Schema Registry enforces the pipeline contract | Spark does not use Schema Registry |
| README says raw Avro events flow into Spark | Spark parses Kafka values as JSON |
| README says ten DQ checks | Code has nine failure flags |
| README says DLQ routing is operational | DLQ code exists but was not connected to a functioning source path |
| README lists five Gold outputs | Code writes only two outputs |
| README says Gold dual-writes all metrics | PostgreSQL writers exist for only two tables |
| README says Redis serves hot-path KPIs | Gold never populates KPI Redis keys |
| README says dashboard contains lifecycle funnel | No lifecycle funnel panel is rendered |
| README says alerts are active | No daemon exists; baselines are never populated |
| README says Prometheus/Grafana observability exists | Most Prometheus targets are nonexistent/down; no Grafana dashboard JSON |
| README says `make verify` verifies the platform | It prints failures but returns success and has broken Make variable expansion |
| README says `make wait` waits for all services | It cannot ever mark MinIO healthy because MinIO has no healthcheck |
| README says default ports can be changed with `.env` | Application services do not consistently consume the host-port variables |
| README says retention exists for MinIO buckets | No lifecycle policy is configured |
| README says CI performs lint/test/build/deploy | CI only echoes a placeholder message |
| README links to `docs/architecture.md` | `docs/` does not exist |

The implementation and runtime evidence are authoritative. The documentation is largely a design document, not a current system description.

---

## 26. Broken Things

### B-001

| Field | Finding |
|---|---|
| Severity | CRITICAL |
| Component | Kafka-to-Bronze data contract |
| File | `producer/kafka_publisher.py`, `streaming/bronze_writer.py` |
| Function | `KafkaPublisher.publish()`, `read_kafka_stream()` |
| Problem | Producer emits Avro binary; Bronze parses JSON |
| Evidence | AvroSerializer in producer; `from_json(value.cast("string"))` in Bronze |
| Impact | Valid events cannot be decoded into Bronze business fields |

### B-002

| Field | Finding |
|---|---|
| Severity | CRITICAL |
| Component | Alert startup |
| File | `main.py` |
| Function | `_run()` |
| Problem | References missing `alerts.daemon` |
| Evidence | `alerts/daemon.py` does not exist |
| Impact | `python main.py alerts` cannot start alerting |

### B-003

| Field | Finding |
|---|---|
| Severity | HIGH |
| Component | Verification entry point |
| File | `main.py` |
| Function | `_run()` |
| Problem | References missing `scripts.verify` |
| Evidence | No `scripts/verify.py` exists |
| Impact | `python main.py verify` cannot start |

### B-004

| Field | Finding |
|---|---|
| Severity | HIGH |
| Component | Topic creation entry point |
| File | `main.py`, `kafka/topic_config.py` |
| Function | `_run()`, `create_topics()` |
| Problem | Dispatcher invokes `create_topics()` without required `bootstrap_servers` |
| Evidence | `create_topics(bootstrap_servers: str, ...)` requires an argument |
| Impact | `python main.py topics` fails before topic creation |

### B-005

| Field | Finding |
|---|---|
| Severity | HIGH |
| Component | Compose health orchestration |
| File | `docker-compose.yml`, `scripts/wait_for_services.sh` |
| Problem | MinIO has no healthcheck, but wait script requires health status |
| Evidence | MinIO service has no `healthcheck`; wait list includes `oip-minio` |
| Impact | `make wait` cannot reliably succeed |

### B-006

| Field | Finding |
|---|---|
| Severity | HIGH |
| Component | Gold aggregation correctness |
| File | `streaming/gold_aggregator.py`, `storage/postgres_writer.py` |
| Function | `write_to_gold()`, `upsert_revenue_by_minute()` |
| Problem | Partial micro-batch aggregates overwrite earlier values |
| Evidence | Batch-local grouping plus `ON CONFLICT DO UPDATE` |
| Impact | Window totals can be incomplete or incorrect |

### B-007

| Field | Finding |
|---|---|
| Severity | HIGH |
| Component | Gold completeness |
| File | `streaming/gold_aggregator.py` |
| Function | `write_to_gold()` |
| Problem | Only revenue and product metrics are written |
| Evidence | No writes for region heatmap, funnel, or baseline |
| Impact | Documented business outputs do not exist |

### B-008

| Field | Finding |
|---|---|
| Severity | HIGH |
| Component | Dashboard |
| File | `dashboard/app.py` |
| Function | `load_revenue_timeseries()`, `load_platform_kpis()`, `load_region_breakdown()` |
| Problem | PostgreSQL interval bind parameters are embedded inside quoted literals |
| Evidence | `INTERVAL ':minutes minutes'` |
| Impact | Dashboard queries fail when executed |

### B-009

| Field | Finding |
|---|---|
| Severity | HIGH |
| Component | DQ observability |
| File | `streaming/silver_transformer.py`, `scripts/postgres/init.sql` |
| Function | `write_to_silver()` |
| Problem | DQ results are logged only, not persisted |
| Evidence | No insert into `silver.dq_run_log` |
| Impact | DQ dashboard and metrics have no source data |

### B-010

| Field | Finding |
|---|---|
| Severity | HIGH |
| Component | Observability |
| File | `observability/prometheus/prometheus.yml` |
| Problem | Most targets refer to nonexistent services |
| Evidence | Runtime target API shows DNS failures and connection refusals |
| Impact | Application observability is absent |

### B-011

| Field | Finding |
|---|---|
| Severity | HIGH |
| Component | Test and release controls |
| File | `tests/`, `.github/workflows/ci.yml` |
| Problem | No tests; CI is placeholder |
| Evidence | Pytest collected zero items; CI only echoes text |
| Impact | No automated proof of correctness |

### B-012

| Field | Finding |
|---|---|
| Severity | HIGH |
| Component | Make verification |
| File | `Makefile` |
| Problem | Shell variable syntax is consumed by GNU Make |
| Evidence | `make -n verify` produced blank ports and blank fallback values |
| Impact | Verification commands are malformed |

### B-013

| Field | Finding |
|---|---|
| Severity | HIGH |
| Component | Make verification |
| File | `Makefile` |
| Problem | Verification prints failures but exits successfully |
| Evidence | `make verify` returned exit code 0 while every check printed FAIL |
| Impact | CI or operators can receive false success |

### B-014

| Field | Finding |
|---|---|
| Severity | MEDIUM |
| Component | Kafka utilities |
| File | `Makefile`, Kafka image |
| Problem | Makefile calls nonexistent `kafka-topics.sh` |
| Evidence | Container contains `kafka-topics`; `kafka-topics.sh` execution returned exit 127 |
| Impact | Documented Kafka utility command fails |

### B-015

| Field | Finding |
|---|---|
| Severity | MEDIUM |
| Component | Redis integration |
| File | `streaming/gold_aggregator.py`, `storage/redis_client.py` |
| Problem | Cache write functions are never called |
| Evidence | No calls to `set_revenue_kpis()` or `set_product_leaderboard()` |
| Impact | Dashboard hot path is not implemented |

### B-016

| Field | Finding |
|---|---|
| Severity | MEDIUM |
| Component | Alerting |
| File | `alerts/rule_engine.py`, `alerts/statistical_detector.py` |
| Problem | Baseline tables have no producer |
| Evidence | Runtime table empty; no baseline computation job |
| Impact | Revenue drop, spike, and z-score checks normally skip |

### B-017

| Field | Finding |
|---|---|
| Severity | MEDIUM |
| Component | Windows/WSL operation |
| File | `.env` |
| Problem | Active `.env` uses CRLF and cannot be sourced by Bash |
| Evidence | Bash produced `$'\\r': command not found` errors |
| Impact | WSL shell workflows can receive corrupted environment values |

### B-018

| Field | Finding |
|---|---|
| Severity | MEDIUM |
| Component | PostgreSQL permissions |
| File | `scripts/postgres/init.sql` |
| Problem | Grants are hardcoded to `oip_user` |
| Evidence | `GRANT ... TO oip_user` regardless of `POSTGRES_USER` |
| Impact | Custom database usernames can break initialization or permissions |

### B-019

| Field | Finding |
|---|---|
| Severity | MEDIUM |
| Component | Security |
| File | `docker-compose.yml`, `.env` |
| Problem | Development credentials and unauthenticated plaintext services are exposed |
| Evidence | PLAINTEXT Kafka, unrestricted Kafka UI, unauthenticated Redis, development passwords |
| Impact | Unsafe outside isolated local development |

---

## 27. Missing Things

Required by the intended architecture but absent or incomplete:

1. `alerts/daemon.py`
2. `scripts/verify.py`
3. Spark Dockerfile/service
4. Producer Dockerfile/service
5. Silver Dockerfile/service
6. Gold Dockerfile/service
7. Dashboard Dockerfile/service
8. Alert service
9. Metrics exporter service
10. Application-level process supervisor
11. Real Avro consumer/deserializer
12. Schema Registry integration in Spark
13. DQ run logger
14. Bronze ingestion logger
15. Region heatmap aggregator
16. Lifecycle funnel aggregator
17. Anomaly baseline computation
18. Redis Gold cache writes
19. Redis leaderboard consumption in dashboard
20. Alert auto-resolution
21. No-data alert
22. Alertmanager
23. Grafana dashboard JSON
24. Kafka exporter
25. PostgreSQL exporter
26. Redis exporter
27. Spark metrics integration
28. Producer metrics integration
29. Unit tests
30. Integration tests
31. End-to-end tests
32. Schema compatibility tests
33. Migration framework
34. MinIO lifecycle configuration
35. Secret management
36. External Kafka listener design
37. Production Kafka replication strategy
38. Application-level health endpoints
39. Retry and timeout policy across all database operations
40. Data replay and backfill workflow

---

## 28. Dead or Unused Things

These appear unused or disconnected:

- `datalake/__init__.py`
- `utils/config.py` singleton settings
- `boto3` dependency
- `fastavro` direct usage
- `storage.redis_client.set_revenue_kpis()`
- `storage.redis_client.set_product_leaderboard()`
- `storage.redis_client._alert_list_key()`
- `storage.redis_client._heatmap_key()`
- `dashboard.app.get_leaderboard` import
- `dashboard.app.is_alert_active` import
- `storage.postgres_writer.check_connection()`
- `gold_aggregator.apply_revenue_sign()` result column is not used for the actual aggregate writes
- `SPARK_BATCH_DURATION` metric
- `DLQ_RECORDS` metric
- `alerts.active_alert_state` table
- `bronze.ingestion_log` table
- `gold.region_heatmap` table
- `gold.lifecycle_funnel` table
- `gold.anomaly_baseline` table
- `payment-events` topic
- `alerts-output` topic
- Prometheus targets for nonexistent exporters
- Grafana dashboard provider without dashboard JSON
- `POSTGRES_JDBC_URL`
- `MINIO_CHECKPOINT_BUCKET`
- `PROMETHEUS_SCRAPE_INTERVAL`
- `ALERT_COOLDOWN_MINUTES`
- README architecture badge pointing to missing `docs/architecture.md`

---

## 29. Duplicate or Conflicting Implementations

### Configuration duplication

Database DSNs are constructed independently in:

- `storage/postgres_writer.py`
- `dashboard/app.py`
- `alerts/rule_engine.py`
- `alerts/statistical_detector.py`
- `observability/metrics_exporter.py`

The centralized `utils/config.py` is not used consistently.

### Status duplication

- `README.md`: Phases 2–13 pending
- `PHASE_STATUS.md`: Phases 1–11 complete

### Kafka CLI duplication

- `main.py topics`
- `python -m kafka.topic_config`
- `Makefile kafka-topics`
- Kafka container CLI

These paths do not all work consistently.

### Alert state duplication

- PostgreSQL `alerts.active_alert_state`
- Redis alert cooldown keys

Only Redis is used.

### Gold storage duplication

- MinIO Gold Parquet
- PostgreSQL Gold tables
- Redis intended cache

Only parts of MinIO and PostgreSQL are implemented, and Redis is disconnected.

### Runtime model duplication

- Infrastructure through Compose
- Application services through `main.py`
- Direct module execution
- Streamlit direct execution
- Prometheus targets for nonexistent service containers

There is no single authoritative runtime topology.

---

## 30. Architecture Problems

1. The primary producer/consumer serialization contract is incompatible.
2. Schema Registry is present but not used by Spark.
3. Application services are outside Compose.
4. There is no process supervisor.
5. There is no reliable startup dependency model for Spark jobs.
6. No application health endpoints exist.
7. Gold aggregation is batch-local rather than truly stateful.
8. PostgreSQL upserts overwrite partial windows.
9. The medallion layers are not transactionally coordinated.
10. Bronze metadata tables are never populated.
11. Silver DQ logs are never populated.
12. Redis is described as a serving layer but not connected to Gold.
13. Alerting is described as continuous but implemented only as callable functions.
14. Observability configuration assumes services that do not exist.
15. Grafana is provisioned without dashboards.
16. The dashboard is coupled directly to tables with fragile SQL.
17. No schema evolution strategy is implemented.
18. No replay/backfill strategy is implemented.
19. No production-grade Kafka semantics are present.
20. No deployment architecture exists beyond local infrastructure.

---

## 31. Runtime Problems

1. WSL does not have the `docker` command on PATH.
2. Docker Desktop is available through `docker.exe`.
3. The project’s Bash scripts assume `docker`, not `docker.exe`.
4. WSL Python lacks project dependencies.
5. Windows Python 3.11 lacks the declared project dependencies.
6. No Spark runtime is available in WSL.
7. No application containers are defined.
8. MinIO has no healthcheck.
9. `make wait` cannot treat MinIO as healthy.
10. Historical OIP containers exited with code 137.
11. Kafka and Schema Registry had historical startup connectivity failures.
12. Kafka application topics are absent.
13. Prometheus has many DNS targets for services that do not exist.
14. MinIO metrics are rejected with HTTP 403 by Prometheus.
15. Grafana has no dashboards.
16. The application cannot be started through all advertised `main.py` commands.

---

## 32. Data Problems

1. Avro bytes are parsed as JSON.
2. Monetary data uses floating-point types.
3. Timestamp timezone handling is implicit.
4. DQ has nine implemented failure flags despite ten documented rules.
5. Future timestamps are not independently classified.
6. Refund amount validation is too permissive.
7. Product ID validation regex is incomplete.
8. Schema drift detection is too narrow.
9. Event status consistency is not validated.
10. Currency consistency is not validated.
11. DQ results are not persisted.
12. DLQ writes are not idempotent.
13. Cross-batch deduplication is not implemented.
14. Gold windows can be overwritten with partial results.
15. MinIO Gold can contain duplicate partial aggregates.
16. Cancellation and refund events can both subtract from revenue, potentially double-reversing a paid cancellation.
17. `failed_count` counts cancellations, not DQ failures.
18. Anomaly baselines are never calculated.
19. PostgreSQL tables remain empty without a complete running pipeline.
20. Redis contains no business data.

---

## 33. Testing Gaps

The following are unproven:

- Avro serialization success
- Schema registration
- Kafka producer delivery
- Topic creation through project code
- Kafka consumer reading
- Spark Kafka connector
- Spark Avro decoding
- Bronze Parquet output
- Bronze immutability
- DQ pass/fail behavior against real data
- DLQ output
- Silver deduplication
- Cross-batch replay
- Gold window correctness
- PostgreSQL idempotence
- Redis cache population
- Dashboard query correctness
- Streamlit rendering
- Alert scheduling
- Alert cooldown behavior
- Slack notification behavior
- PagerDuty notification behavior
- Prometheus exporter collection
- Kafka lag calculation
- Grafana dashboards
- Windows direct execution
- WSL direct execution
- Clean-environment setup
- Persistent restart behavior with application state

There is no unit, integration, or end-to-end test suite.

---

## 34. Technical Debt

- Missing application orchestration
- Missing migrations
- Duplicated configuration logic
- Hardcoded defaults throughout modules
- Inconsistent use of `.env`
- Internal hostnames mixed with host execution
- Missing retries and timeouts
- Broad exception handling
- Swallowed producer errors
- No graceful Spark process supervisor
- No shutdown coordination between streaming jobs
- No structured lineage records
- No data contract tests
- No schema evolution tests
- No production-like Kafka configuration
- No security boundary
- No test fixtures
- No test data management
- No dashboard provisioning
- No alert state resolution
- No metric ownership model
- No service ownership documentation
- No authoritative architecture document
- Contradictory phase documentation
- Placeholder CI
- Empty package modules
- Unused dependencies and functions
- Hardcoded SQL assumptions
- Driver-side DataFrame collection for database writes
- No backfill strategy
- No retention/lifecycle enforcement

---

## 35. Current System Maturity

| Area | Maturity | Evidence |
|---|---|---|
| Docker infrastructure | FUNCTIONAL | Compose starts most infrastructure healthy |
| Kafka broker | FUNCTIONAL | Broker API succeeded |
| Schema Registry | FUNCTIONAL | `/subjects` returned `[]` |
| Kafka application topics | NOT STARTED | Only internal topics exist |
| Producer | PROTOTYPE | Source exists; no runtime delivery proven |
| Bronze | PARTIAL | Writer exists; serialization contract broken |
| Data quality | PARTIAL | Validation expressions exist; no operational logging |
| Silver | PARTIAL | Transform code exists; startup/dedup issues |
| Gold | PARTIAL | Two aggregates exist; three missing |
| PostgreSQL schema | FUNCTIONAL | Four schemas and nine tables verified |
| PostgreSQL data pipeline | NOT STARTED | All tables empty |
| Redis infrastructure | FUNCTIONAL | PING succeeded |
| Redis business cache | NOT STARTED | Zero keys; Gold does not write cache |
| Dashboard | PARTIAL | UI source exists; SQL and service integration unproven |
| Alert detection | PROTOTYPE | Functions exist; no daemon or baselines |
| Alert delivery | NOT STARTED | Integrations disabled and never tested |
| Metrics exporter | PROTOTYPE | Source exists; not deployed/scraped |
| Prometheus | PARTIAL | Server works; most targets down |
| Grafana | PARTIAL | Datasource exists; no dashboards |
| Tests | NOT STARTED | Zero tests |
| CI/CD | NOT STARTED | Placeholder workflow |
| Windows workflow | PARTIAL | Docker Desktop works; Bash/Make path is inconsistent |
| End-to-end platform | NOT STARTED | No real event trace |

---

## 36. What We Actually Have

Right now the project has:

- A working local infrastructure Compose file.
- A Kafka broker that can start.
- A Schema Registry that can start.
- A Kafka UI that can start.
- A MinIO server and bucket initialization job.
- A PostgreSQL schema with nine empty application tables.
- A Redis server with no application keys.
- A Prometheus server with mostly hypothetical targets.
- A Grafana server with a datasource but no dashboard.
- A synthetic order generator source.
- An Avro producer source.
- Kafka topic-admin source.
- Spark job source that does not consume the producer’s encoding correctly.
- Partial DQ, Silver, and Gold source code.
- Partial PostgreSQL writer source.
- Partial Redis source.
- Partial dashboard source.
- Partial alert source.
- Partial metrics source.
- No proven application pipeline.
- No test suite.
- No complete runtime command.
- No event successfully traced end-to-end.

The platform infrastructure is the most mature part. The application pipeline is still a collection of partially connected modules.

---

## 37. What We Thought We Had

The documentation suggests that the project has:

- A complete medallion architecture.
- A working Kafka-to-Spark pipeline.
- Avro enforcement end-to-end.
- Ten DQ checks.
- Bronze, Silver, and Gold layers.
- Five Gold datasets.
- PostgreSQL dual writes.
- Redis hot-path caching.
- A live dashboard.
- Rule and statistical alerting.
- Slack and PagerDuty routing.
- Prometheus metrics.
- Grafana dashboards.
- Complete Phases 1–11.
- CI/CD preparation.

The source and runtime demonstrate that these are mostly design intentions or partial implementations.

The largest documentation-to-reality gaps are:

1. Avro is not consumed correctly.
2. Spark is not orchestrated.
3. Gold is incomplete.
4. Redis is not integrated with Gold.
5. Alerting has no daemon.
6. Observability targets do not exist.
7. Grafana has no dashboards.
8. No tests exist.
9. Several advertised entry points are missing or broken.
10. Phase status is materially overstated.

---

## 38. Gap Analysis

| Requirement | Expected | Actual | Gap |
|---|---|---|---|
| Infrastructure | All dependencies start and become healthy | Most start healthy; MinIO has no healthcheck | Health orchestration gap |
| Event generator | Generates valid order events | Source exists; dependencies absent in audited environment | Runtime proof missing |
| Kafka topics | Four application topics exist | Only internal topics exist | Topic provisioning gap |
| Schema Registry | Producer and consumer share Avro contract | Producer only; Spark parses JSON | Critical contract gap |
| Bronze | Immutable raw event archive | Writer exists; no valid event proven | Integration gap |
| DQ | Ten observable checks | Nine flags; no DQ persistence | DQ completeness gap |
| Silver | Validated, deduplicated data | Partial code; cross-batch dedup absent | Correctness gap |
| Gold | Five business datasets | Two outputs only | Functional gap |
| PostgreSQL | All Gold data and metadata populated | Empty tables; two writers | Storage gap |
| Redis | KPI and leaderboard cache | Zero keys; writers unwired | Serving-layer gap |
| Dashboard | Live KPIs from working Gold | Source exists; SQL issues; no runtime proof | UI integration gap |
| Alerts | Continuous rules and statistical alerts | No daemon; no baselines | Operational gap |
| Metrics | Application metrics scraped | Exporter absent from Compose; targets down | Observability gap |
| Grafana | Provisioned business dashboards | No dashboard JSON | Visualization gap |
| CI/CD | Lint, test, build, deploy | Placeholder echo | Automation gap |
| Windows | PowerShell-compatible setup | Bash/Make dependency | Developer workflow gap |
| Security | Environment-safe credentials | Dev credentials/plaintext/no auth | Security gap |
| Testing | Unit/integration/E2E tests | Zero tests | Verification gap |

---

## 39. Recommended Fix Order

No fixes were applied during this audit.

### Step 1 — Establish the authoritative target architecture

Decide whether this remains:

- A local prototype with direct Python processes, or
- A fully Compose-orchestrated platform.

Do not implement further features until the runtime model is explicit.

### Step 2 — Repair the primary data contract

Choose one consistent path:

- Avro-aware Spark deserialization using Schema Registry, or
- JSON producer and JSON consumer.

The producer and Bronze reader must agree before any downstream work continues.

### Step 3 — Make startup and entry points truthful

Resolve:

- `alerts.daemon`
- `scripts.verify`
- `main.py topics`
- topic creation
- application process startup
- dashboard startup
- metrics startup

### Step 4 — Prove Kafka independently

Verify:

- Application topic creation
- Partitions
- Producer delivery
- Schema registration
- Consumer offsets
- External host connectivity
- Container hostname connectivity

### Step 5 — Prove Bronze with one real event

Confirm:

- Non-null business fields
- Correct schema
- Kafka metadata
- Parquet output
- Immutable append behavior

### Step 6 — Repair and persist DQ

Implement:

- Exact rule count
- Deterministic semantics
- Failure breakdown
- DQ database logging
- DLQ observability
- DQ tests

### Step 7 — Repair Silver semantics

Implement:

- Empty-source handling
- Cross-batch deduplication
- Replay behavior
- Stable event identity
- DLQ retry/idempotence policy

### Step 8 — Redesign Gold aggregation semantics

Choose either:

- Stateful Spark aggregations with proper output mode/checkpointing, or
- Batch-local aggregation with additive/idempotent database writes.

Do not overwrite partial windows with partial results.

### Step 9 — Populate all required Gold datasets

Implement and test:

- Revenue by minute
- Product rankings
- Region heatmap
- Lifecycle funnel
- Anomaly baselines

### Step 10 — Repair PostgreSQL and Redis serving paths

Verify:

- All writers
- Numeric precision
- Idempotent replay
- Redis population
- Cache fallback
- Key TTLs
- Cache consistency

### Step 11 — Repair dashboard SQL and runtime errors

Use correct interval parameterization and test every query against the real database.

### Step 12 — Implement a real alert daemon

Add:

- Scheduler
- Baseline job
- No-data detection
- Alert persistence
- Cooldown
- Resolution
- Notification result tracking

### Step 13 — Implement observability

Deploy:

- Metrics exporter
- Kafka lag collection
- DQ metrics
- DLQ metrics
- Spark metrics
- PostgreSQL exporter
- Redis exporter
- Valid Prometheus targets
- Grafana dashboards

### Step 14 — Add meaningful tests

Minimum required:

- Unit tests for generator and validators
- Avro contract tests
- Kafka integration test
- Spark Bronze/Silver/Gold integration tests
- PostgreSQL idempotence tests
- Redis cache tests
- Dashboard query tests
- Alert cooldown tests
- End-to-end event test

### Step 15 — Resolve Windows workflow

Provide either:

- A PowerShell-native workflow, or
- A clearly supported WSL/Git Bash workflow.

Do not require undocumented environment switching.

### Step 16 — Security hardening

After correctness:

- Remove default credentials
- Add authentication
- Add TLS where appropriate
- Restrict exposed ports
- Use least-privilege users
- Move secrets out of `.env` for shared environments

### Step 17 — Rewrite documentation from verified behavior

Only after the system passes phase verification should `README.md` and `PHASE_STATUS.md` be updated.

---

## 40. The Single Correct Way to Run the System

There is currently no truthful command sequence that runs the complete platform end-to-end successfully. The following is the least-incorrect operational sequence for the current repository.

### Windows PowerShell

Run from the repository root.

```powershell
Set-Location '<repository-root>'
```

Create and activate a Python environment:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Validate and start infrastructure:

```powershell
```

Check infrastructure:

```powershell
Invoke-WebRequest http://localhost:8080/actuator/health
Invoke-WebRequest http://localhost:8081/subjects
Invoke-WebRequest http://localhost:9000/minio/health/live
Invoke-WebRequest http://localhost:9090/-/ready
Invoke-WebRequest http://localhost:3000/api/health
```

For direct host processes, explicitly set host-accessible endpoints in each PowerShell session:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
$env:SCHEMA_REGISTRY_URL = 'http://localhost:8081'
$env:MINIO_ENDPOINT = 'http://localhost:9000'
$env:POSTGRES_HOST = 'localhost'
$env:POSTGRES_PORT = '5432'
$env:REDIS_HOST = 'localhost'
$env:REDIS_PORT = '6379'
```

Create topics using the module entry point, not `main.py topics`:

```powershell
python -m kafka.topic_config --bootstrap localhost:9092
```

The following processes would need separate PowerShell windows:

```powershell
python main.py bronze
```

```powershell
python main.py silver
```

```powershell
python main.py gold
```

```powershell
python main.py producer --tps 1 --duration 60 --error-rate 0
```

Dashboard:

```powershell
streamlit run dashboard/app.py
```

Metrics exporter:

```powershell
python -m observability.metrics_exporter
```

Alerting does not have a valid command because `alerts/daemon.py` is missing.

### Linux/WSL

The current WSL environment does not expose Docker as `docker`; Docker Desktop is available only through Windows `docker.exe`.

Use PowerShell for Docker commands unless Docker Desktop WSL integration is enabled.

The Bash scripts require Bash:

```bash
bash -n scripts/generate_cluster_id.sh
bash -n scripts/wait_for_services.sh
```

The Makefile requires GNU Make and `/bin/bash`:

```bash
make help
```

The active `.env` is CRLF formatted and cannot safely be sourced by Bash without line-ending normalization.

The current WSL environment also lacks:

- Docker
- Java
- pip
- pytest
- declared Python dependencies

Therefore WSL cannot currently run the full application directly.

---

## 41. Phase Verification Plan

| Phase | Command | Expected result | What to send | Pass condition |
|---|---|---|---|---|
| 1 | `docker compose config` | Valid Compose configuration | Full output/status | Exit 0 |
| 1 | `docker compose up -d` then `docker compose ps -a` | Infrastructure starts | Service table | All required services healthy or functionally verified |
| 1 | Health URLs and `pg_isready`, Redis `PING` | APIs respond | Command output | Every required endpoint succeeds |
| 2 | `python -m producer.order_generator --tps 1 --duration 5 --error-rate 0` | Valid JSON events printed | Sample redacted output | Events contain all required fields |
| 2 | Producer with Kafka | Delivery callbacks report success | Producer logs | Delivered count greater than zero |
| 3 | `python -m kafka.topic_config --list --bootstrap localhost:9092` | Four application topics listed | Topic list | Names and partitions match definition |
| 3 | `curl http://localhost:8081/subjects` after producer | Avro subject appears | Registry response | Subject exists and schema is valid |
| 4 | Start Bronze, publish one event | Bronze batch written | Bronze logs and MinIO listing | Non-null event record exists |
| 5 | Publish valid and malformed events | Pass/fail split occurs | DQ logs and DLQ records | Valid event passes; malformed event reaches DLQ |
| 6 | Start Silver on Bronze data | Silver Parquet appears | Silver logs and MinIO listing | Valid records only; duplicate test passes |
| 7 | Start Gold across multiple batches | Gold metrics update | Gold logs and sample rows | Window values accumulate correctly |
| 8 | Query PostgreSQL | Gold rows exist | SQL query results | Rows match Gold calculations |
| 8 | Replay same batch | No duplicate rows or incorrect totals | Before/after SQL output | Idempotence proven |
| 9 | Start Redis and dashboard | Dashboard loads | Browser/API output and logs | No SQL errors; KPIs match PostgreSQL |
| 10 | Run alert daemon against fixture data | Alert is persisted and routed | Alert table and notification logs | Cooldown and resolution work |
| 11 | Start metrics exporter | `/metrics` returns application metrics | Metrics endpoint | Values are non-empty and meaningful |
| 11 | Query Prometheus targets | Application targets are up | `/api/v1/targets` response | Expected targets are UP |
| 11 | Open Grafana | Dashboards load | Screenshot/API output | Provisioned panels show real metrics |
| 12 | Build/start application services | All services run under Compose | `docker compose ps -a` | No direct manual process required |
| 13 | Push to CI branch | Lint/tests/build execute | GitHub Actions run | All required checks pass |

### Required event trace evidence

For a successful end-to-end run, send:

- Producer log containing event/order ID
- Kafka topic/partition/offset
- Schema Registry subject
- MinIO Bronze object path
- Bronze record
- DQ result
- Silver record
- Gold row
- PostgreSQL row
- Redis key/value
- Dashboard screenshot or query result
- Alert table row if triggered
- Prometheus metric sample

---

## 42. Final Truth Table

| Question | Answer |
|---|---|
| What is this project? | A partially implemented local real-time order intelligence prototype using Kafka, Spark, MinIO, PostgreSQL, Redis, Streamlit, Prometheus, and Grafana. |
| What problem does it solve? | It is intended to provide real-time order lifecycle, revenue, product, regional, data-quality, alerting, and operational visibility. |
| What is actually implemented? | Infrastructure, event-generation code, Avro producer code, topic-admin code, partial Spark jobs, validators, partial PostgreSQL writers, partial Redis client, dashboard source, alert functions, metrics exporter, and infrastructure provisioning. |
| What works? | Docker Compose parses. Kafka, Schema Registry, Kafka UI, PostgreSQL, Redis, Prometheus, Grafana, and MinIO APIs can start/functionally respond. PostgreSQL schemas/tables initialize. |
| What doesn't work? | The Avro-to-Spark data path, full application startup, alert daemon, verification dispatcher, complete Gold layer, Redis KPI path, dashboard SQL, Prometheus application scraping, Grafana dashboards, and end-to-end processing. |
| What is incomplete? | Phases 2–13 are incomplete or broken despite `PHASE_STATUS.md` claiming Phases 2–11 complete. |
| What is untested? | Producer delivery, Spark jobs, Bronze/Silver/Gold data, DQ behavior, PostgreSQL writes, Redis cache behavior, dashboard execution, alert execution, metrics exporter, and end-to-end flow. |
| What is misleading in documentation? | Phase completion status, Avro/Spark integration claims, ten-check DQ claim, five-output Gold claim, Redis integration claim, alerting claim, observability claim, Grafana dashboard claim, and the “fully runnable” statement. |
| What is the next fix? | Establish one authoritative runtime/data-contract design, then repair the Avro/Kafka/Spark boundary before implementing downstream fixes. |

**Final classification:** **PARTIAL PROTOTYPE — INFRASTRUCTURE FUNCTIONAL, APPLICATION PIPELINE NOT END-TO-END VERIFIED.**
