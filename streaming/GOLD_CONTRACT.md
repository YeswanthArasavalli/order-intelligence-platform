# Gold contract v1 — additive event ledger

## Evidence and semantic decision

`make_paid` emits the charged amount after discounts; lifecycle state is then
updated to that amount. `make_cancelled` copies it when `state.paid` is true and
emits zero otherwise. `make_refunded` emits a positive partial/full amount (50%,
75%, 100%), with a refund ID and percentage. The generator can emit both a paid
cancellation and a refund. Silver also explicitly preserves repeated payments
and refunds at different Kafka offsets; metadata IDs are optional.

README, previous Gold implementation, SQL generated net revenue and downstream
consumers all specify **paid − refund − paid cancellation**. There is no reversal
correlation ID or rule saying that the refund settles an already-recognized
cancellation. The smallest evidence-based assumption is to retain the documented
**additive event-ledger** rule: both reduce revenue, without matching, capping,
deduplicating by order, or netting them against one another. Thus paid 100,
cancelled-paid 100, refunded 30 produces **−30**, explicitly tested. This is a
signed event-flow KPI, NOT a proven accounting settlement balance. A future
business-contract change must specify overlapping reversals before changing it.
Tax, fees, gross-before-discount and settlement timing are not defined. Here
gross_revenue means sum of charged PAID amounts, not pre-discount catalog value.

For cancellation, `was_paid` must be an actual JSON boolean. Missing/null/string
values are financially ambiguous and block Gold publication with an explicit
error (Silver permits these optional metadata shapes). Gold neither guesses nor
silently drops these rows. Optional metadata/payment method on other events is
allowed. Refunds/payments without a corresponding observed lifecycle event remain
independent signed events, including negative-only windows.

## Verified Silver input

Use the actual M2 schema: 13 Avro business fields, amount double, timestamp long,
event_ts timestamp, Kafka topic/string, partition/int, offset/long, event identity
and fingerprint/string, `_dq_passed`, `_quality_passed`, `_route`, failure reason
and DQ contract version. Require true quality flags, SILVER route, empty reason,
version 1.0, valid identity and exact UTC unix_millis(event_ts)=timestamp. Verify
the fingerprint against original business fields. Raw Bronze and DLQ frames fail
closed, never publish a partial aggregate. Source topic must match configured
KAFKA_TOPIC_ORDERS. Read only completed `silver_batch` directories (_SUCCESS);
partial Silver output blocks rather than being silently consumed.

## Metrics (only the two existing implemented metrics)

Every currency is a separate dimension; no exchange rates or cross-currency sum.
All metrics use unique source topic/partition/offset, never order/type/amount dedup.
The ledger retains every lifecycle event; unused created/shipped/delivered amounts
have no revenue effect. No invented unique-user/product/order statistics.

| Output | Identity / window | Metric definitions |
|---|---|---|
| revenue_by_minute | UTC 1-minute tumbling [window_start, window_end), region, currency | gross_revenue=sum PAID amount; refund_amount=sum REFUNDED amount; cancellation_amount=sum CANCELLED amount where was_paid=true; net_revenue=sum signed contributions; order_count=count PAID events (historical name, not distinct orders); failed_count=count CANCELLED events; refunded_event_count=count REFUNDED; avg_order_value=mean PAID amounts or null if none |
| product_rankings | UTC 5-minute tumbling [window_start, window_end), product_id, product_name, currency | units_sold=count PAID events (no quantity field); revenue=sum PAID amounts. All products, no top-N filtering/rank number; consumers may sort within currency/window. Product name remains a dimension as in original aggregation. |

Windows align to Unix epoch multiples, start inclusive/end exclusive. Each event
contributes in its own event-time window, not the payment's original window or
processing window. Summing same-currency minute rows yields the cumulative event
flow; do not sum averages. No windows are fabricated for absent data. Observed
nonfinancial-event windows have zero financial sums/counts and null paid average.
Empty input/all-DLQ produce no Gold business rows and no new committed snapshot.
Unpaid cancellation amount zero contributes zero, while failed_count increases.

## Time and lateness

Use verified event_ts unchanged. Ingestion and Kafka timestamps are provenance,
not business time. M2 already applies ingestion-relative 24-hour age / 60-second
future-skew limits. Gold does not rerun those checks against processing time.
No watermark: durable full-history recomputation has no evictable streaming
aggregate state; a watermark would discard valid Silver history/replays without
solving persistence. Every newly observed Silver event can revise an old window,
however late Gold sees it. Outputs are revisable snapshots, not final closed windows.

## Money

Source remains double. Convert its shortest round-trip decimal spelling using
Python Decimal(str(value)), require exact representability in Decimal(28,8)
(20 integral digits, 8 fractional digits). No event amount is rounded: excessive
precision, nonfinite/negative amounts or overflow fail before publication. This
is an explicit Gold representability limit, not currency minor-unit enforcement.
Eight fractional digits accommodate all generated values and the active ISO
currencies, including codes with >2 minor digits. It cannot recover precision
already lost upstream. 0.1+0.2 as separate events sums exactly to 0.30000000;
a precomputed binary-artifact value 0.30000000000000004 is rejected explicitly.

Financial sums/net/revenue are Decimal(38,8); counts long. Average is computed
from decimal inputs and rounded HALF_UP to eight fractional digits. It alone is
an explicitly rounded derived metric. Any aggregate overflow blocks publication.
Gold sets ANSI decimal overflow checks and disables decimal precision loss for
its Spark session. Currency codes remain exact; no 2-decimal truncation/FX.

## Persistence and replay

Root: `s3a://<MINIO_GOLD_BUCKET>/<KAFKA_TOPIC_ORDERS>/`.
Single writer per root, stable source identity namespace; no concurrent-writer
guarantee or topic-recreation/cluster-migration identity inference.

Merge each accepted micro-batch with the current committed event ledger, detecting
conflicting payloads for identical coordinates. Distinct offsets remain distinct.
Pure replay/empty input returns current commit without another snapshot. For new
events, recompute BOTH complete metric tables from the complete unique ledger.
Write immutable Parquet under `_snapshots/<attempt-id>/events/`,
`revenue_by_minute/`, `product_rankings/`. Check successful writes, then publish ONE
small JSON object `_commits/<20-digit-sequence>.json` using S3A create/close. MinIO
atomic object visibility plus a single writer is the commit boundary, not rename.
The manifest pins all three paths, parent version, contract version, counts,
batch/checkpoint identity and commit time. Reader rejects incomplete/corrupt latest
commit; it never silently falls back to stale state.

Consumers MUST use `read_gold` / `current_manifest`, selecting the highest valid
committed version (or pin its snapshot paths). Do not recursively union historical
snapshots. A logical metric identity appears once in the current table. Old
snapshots preserve audit/time travel; no blind append or destructive overwrite.

Failure before manifest publication leaves the previous version visible; retry
rebuilds using a fresh attempt path, leaving orphan files unreferenced. Failure
after manifest publication but before Spark checkpoint commit is a source replay
and a no-op. No distributed transaction coordinator/database is introduced.
Full ledger/snapshot recomputation is deliberately bounded local-milestone design;
history size, orphan cleanup, retention and compaction require later scaling work.

## Deferred architecture

No PostgreSQL call/import remains in the Gold execution path. Existing SQL/cache/
dashboard schemas omit currency and use smaller decimals; they need an explicit
serving-contract migration later, not automatic writes from this milestone.
15-minute heatmap, hourly funnel, anomaly baseline and other unique-count metrics
were inspected but are not required to correct the two existing aggregations.
The dependency tuple from M1/M2 is retained without new application dependencies.
