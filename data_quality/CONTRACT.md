# Bronze → DQ → Silver → DLQ contract (v1)

Sources: `producer/schemas/order_event.avsc`, generator constructors/reference sets,
the previous validator's 24-hour age / 60-second skew limits, and the ISO 4217
maintenance agency's currency list. No order-state inference or monetary rounding.
Python/Spark/Kafka/Avro/Pydantic versions stay at the verified Milestone 1 baseline;
this implementation needs no new runtime dependency.

## Validation and routing

All rules have severity ERROR. Every applicable reason is retained, never a null
boolean. A new source event passes only when there are no reasons. Passing events
go to Silver; failing events go to Kafka DLQ. Both outcomes are persisted in MinIO.
Repeated source coordinates are separately recorded as REPLAY, not another event.

| Rule / failure reason | Inputs | Valid condition (otherwise invalid) |
|---|---|---|
| null_order_id | order_id | Non-null, non-blank string; no invented ID format |
| null_user_id | user_id | Non-null, non-blank string |
| null_product_id | product_id | Non-null, non-blank string |
| invalid_product_name | product_name | Non-null, non-blank string |
| invalid_amount | amount, event_type, event_metadata | Finite positive double; zero allowed for ORDER_CANCELLED with JSON boolean was_paid=false, as emitted by make_cancelled; refunds must be positive |
| null_timestamp | timestamp | Present |
| malformed_timestamp | timestamp | Spark long / Avro long; no numeric-string coercion |
| invalid_timestamp | timestamp | Epoch milliseconds in (0, 253402300799999], representable through UTC year 9999 |
| future_timestamp | timestamp, _ingested_at | At most 60 seconds after Bronze ingestion (inclusive) |
| old_timestamp | timestamp, _ingested_at | At most 24 hours before Bronze ingestion (inclusive) |
| invalid_region | region | Existing five generator regions |
| invalid_product_id | product_id | Entire string matches prod-[0-9]+; no catalog-only restriction |
| invalid_event_type | event_type | One of six Avro lifecycle symbols |
| schema_drift | all event fields, decode status | Expected field types, every required value present, successfully decoded record; optional missing fields become null |
| invalid_currency | currency | Exact uppercase ISO 4217 active code, pinned 2026-09-17 list; INR is not the only allowed currency |
| invalid_payment_method | payment_method | Null (Avro optional) or one of UPI, Credit Card, Debit Card, Net Banking, Wallet, COD |
| invalid_status | status, event_type | Corresponding CREATED/PAID/SHIPPED/DELIVERED/CANCELLED/REFUNDED |
| invalid_event_metadata | event_metadata | Null or strict JSON object, preserving arbitrary keys; no required payment/refund IDs invented |
| invalid_schema_version | schema_version | Exactly 1.0, the only implemented version |
| invalid_provenance | source topic, partition, offset | Nonblank topic; nonnegative partition and offset |
| event_ts_mismatch | timestamp, event_ts | Exact epoch-ms equivalence when timestamp is representable |

Timestamp classes are distinct. Missing/wrong-type/out-of-range values do not also
receive future/old reasons. UTC is mandatory. Age is measured at ingestion so
processing delays and later Silver replay do not change the decision. Standalone
validation without an ingestion timestamp uses an explicitly supplied clock or
processing time. The retained age policy quarantines >24-hour arrivals rather
than discarding them. There is no Silver watermark or event-time state eviction.

Malformed Confluent headers, unknown schema IDs, bad Avro, incompatible record
types, and trailing bytes are retained in Bronze with raw base64 and a bounded
decode-error code. Schema Registry outages/authentication failures still fail the
batch for retry, not quarantine valid data. Historical Bronze files without new
envelope columns remain readable; their source topic is the configured prefix.

## Silver

Business fields retain the Avro types: strings, amount double, timestamp long;
only payment_method and event_metadata may be null in passing rows. No trimming,
case folding, currency conversion, or amount rounding mutates original values.
Derived event_ts is UTC and uses exact timestamp_millis; partition helpers use
integers year/month/day and strings event_type_lower/region. Layout:

`s3a://<silver>/<source-topic>/silver_batch=<key>/year=/month=/day=/event_type_lower=/region=/`

Rows retain Bronze provenance/ingestion/batch metadata, explicit `_event_id`,
`_event_fingerprint`, `_dq_passed`, `_failure_reason`, `_dq_checked_at`,
`_dq_contract_version`, `_quality_passed`, `_cleaned_at`, `_silver_batch_id`.
Individual `_fail_*` flags and raw/original payload copies stay in the audit/DLQ,
not Silver. `_batch_id` continues to mean the Bronze batch ID.

## Identity, durability, and delivery

Identity is `topic:partition:offset`, not order_id/event_type. No stable business
event ID exists. Republishing at a different offset is a distinct event, even if
the payload matches; it might be a legitimate repeated payment/refund. All
lifecycle stages and repeated stages at different offsets are retained.

One writer per source-topic/output pair is required. A durable per-record audit
under `s3a://<silver>/_dq/<topic>/batches/` records SILVER, DLQ or REPLAY. Completed
run summaries under `runs/` provide the cross-batch identity index. Duplicate
coordinates within a batch are also recorded. Conflicting payloads at the same
coordinates fail explicitly instead of silently selecting one.

Batch keys derive from checkpoint location, source topic and Spark batch ID.
Stage the evaluated outcomes, write Silver once to a deterministic batch path,
publish DLQ, then persist the run summary. Repeating a completed batch is a no-op;
retrying a staged batch reuses its decisions and committed Silver output.
Only successful Parquet directories (_SUCCESS) are accepted. Partial directories
block with their recovery location; no existing data is deleted/overwritten.
An unfinished older batch must be recovered with its original checkpoint before
a fresh checkpoint can process that output. Completed history also protects a
fresh checkpoint replay. History scans are appropriate for this local milestone,
not a scalable transactional index.

Kafka DLQ remains the existing JSON diagnostic envelope (order-event transport
remains Confluent Avro). Key/event_id = source identity. Includes reasons, contract
version, original event JSON, raw base64 only for decode failures, source
topic/partition/offset, schema ID, Bronze file, event/ingestion/check times and
Silver batch ID. It excludes Kafka keys, infrastructure configuration and stack
traces. Original event fields can include application identifiers/metadata, so
diagnostics are not emitted to routine logs. Producer acks=all/idempotence handles
producer retries, but **DLQ is at-least-once across crashes before run completion**;
consumers can deduplicate by event_id. Parquet and Kafka are not one transaction.

Run summaries contain input, DQ-pass/fail, Silver, DLQ and replay counts, failure
breakdown and quality percentage. DQ counts include replayed input; routing counts
count only new source events. Zero input quality is 0%, matching the existing SQL
definition; an empty callback records a zero run. PostgreSQL/metrics integration
is a later milestone. Existing data, topics, schemas and checkpoints are retained.
