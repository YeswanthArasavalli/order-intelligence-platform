# MILESTONE 3 — SILVER → GOLD

## 1. Executive result

Implemented a real Gold layer on the verified Silver pipeline, preserving the
existing one-minute revenue and five-minute product metric families. Gold now
uses event-time windows, currency-separated decimal arithmetic, source-coordinate
replay identity and committed complete snapshots instead of batch-local appends.

Final runtime fixture: **35 Kafka/Bronze records → 33 unique Silver/Gold events +
2 DLQ records → 18 revenue rows and 13 product rows**. Multi-batch refunds,
late events, separate-process restart, repeated replay and pre-publication failure
recovery all passed. All 26 relevant tests and fresh M1/M2 regressions passed.

## 2. PASS / PARTIAL / BLOCKED

**PASS**, verified 2026-10-03, for the explicitly documented local, single-writer,
additive event-flow Gold contract in `streaming/GOLD_CONTRACT.md`.

This verdict does not claim reconciled settlement accounting, concurrent writers,
unbounded scalability or completed PostgreSQL serving. The cancellation/refund
ambiguity and operational limits are explicit below. No critical acceptance item
failed. Container-wide pip consistency has a pre-existing unrelated OS-package
issue, disclosed in sections 18/24/26.

## 3. Repository state before changes

- M1 and M2 implementations/reports were present; this was not a new project.
- Actual MinIO Gold bucket was empty. Actual M2 Silver had 12 rows and the verified
  source-coordinate/DQ metadata, including two identical refunds at different offsets.
- `gold_aggregator.py` implemented two batch-local groupBy operations: one-minute
  regional revenue and five-minute paid-product totals. It appended results to
  MinIO and called PostgreSQL replacement upserts.
- A two-minute watermark was attached before foreachBatch, but aggregation took
  place on static batch frames. It did not accumulate windows across batches.
- The revenue sign helper's output was not used by persisted aggregates; MinIO
  revenue rows lacked explicit net_revenue. Currency was not a dimension.
- SQL used NUMERIC(14,2), omitted currency in identities and replaced rows on
  conflicts. Product SQL keyed only by product ID while Spark also grouped name.
- README/headers referenced additional heatmap/funnel/baseline metrics that were
  not implemented by Gold. Existing consumers read PostgreSQL/cache, not a committed
  MinIO Gold view. No Gold tests existed.
- Workspace is not a Git repository; no commit or destructive baseline reset occurred.

## 4. Files inspected

Before implementation, inspected:

- `streaming/silver_transformer.py`, `streaming/gold_aggregator.py`,
  `streaming/spark_session.py`, relevant Bronze and DQ interfaces/contracts.
- `storage/postgres_writer.py`, `storage/redis_client.py`, `datalake/` layout,
  Gold table definitions in `scripts/postgres/init.sql`.
- `producer/schemas/order_event.avsc`, event dataclass, generator reference sets,
  all lifecycle constructors and lifecycle branching; Kafka publisher/verifiers.
- `utils/config.py`, `utils/logger.py`, `main.py`, `requirements.txt`, relevant
  `.env.example`/active non-secret settings, Compose services and Spark runtime.
- `tests/unit/test_milestone2.py`, existing test directories,
  `scripts/verify_milestone1_bronze.py`, `scripts/verify_milestone2.py`, case fixtures,
  capture script and Spark pytest runner.
- README, PHASE_STATUS and prior forensic/M1/M2 reports. The README architecture
  link has no corresponding `docs/architecture.md` file.
- Gold consumers: dashboard SQL loaders, Redis KPI/product builders, alerts'
  current-revenue/baseline queries and observability revenue queries.
- Actual stored Silver schema/rows and MinIO Gold object listing.

Searches covered Gold, revenue, lifecycle types, windows/watermarks, timestamps,
batch/replay/checkpoint identifiers, writers/upserts and append/overwrite/groupBy
usage across application/configuration/documentation. No other Gold writer or
independent implemented metric family was found. The shell lacked ripgrep;
dedicated search/read tools supplied the inspection.

## 5. Files changed

| File | Change / reason |
|---|---|
| `streaming/gold_aggregator.py` | Corrected existing Gold execution: verified Silver boundary, exact money, currency dimensions, complete-history aggregation, source replay handling; removed PostgreSQL dependency/calls and ineffective watermark |
| `storage/gold_store.py` — new | Immutable Parquet snapshots, single-object commit manifest and authoritative current-table reader |
| `streaming/GOLD_CONTRACT.md` — new | Evidence-based revenue/lifecycle, metric, window, money, replay and storage contracts |
| `tests/unit/test_gold.py` — new | 19 meaningful Spark/Parquet tests, including parameterized lifecycle cases and persistence/failure boundaries |
| `scripts/milestone3_cases.py` — new | Deterministic three-arrival controlled events |
| `scripts/verify_milestone3.py` — new | Real initial/second/third/replay/recovery verification with independent decimal/window oracle |
| `scripts/verify_milestone3_m1.py` — new | Fresh actual generator/publisher/Registry/wire/Bronze regression |
| `scripts/capture_milestone3_check.py` — new | Append-only command/log/JSON evidence capture; invokes existing M2 verifier |
| `scripts/run_spark_tests.py` | Optional pytest targets; existing default remains M2 |
| `.env.example` | Explain legacy watermark variable is unused by M3 Gold |
| `README.md`, `PHASE_STATUS.md` | Link current Gold contract/report and clarify additive revenue/serving status |
| `milestone3_evidence/**`, this report — new | Preserve actual outputs, commands and findings |

No changes to M1/M2 production implementations, producer/Avro schema, requirements,
active `.env`, Spark dependency tuple, Compose deployment or downstream subsystems.

## 6. Architecture before/after

Before:

```text
Silver file stream → ineffective watermark → current-batch aggregates
  → append partial MinIO rows
  → PostgreSQL replacement upserts
```

After:

```text
Verified, completed Silver batch files
  → validate M2 quality/provenance/fingerprint and UTC timestamp contract
  → exact money + explicit lifecycle contribution
  → merge current committed ledger by topic:partition:offset
  → recompute complete event-time/currency metric tables
  → immutable Parquet attempt
  → one committed manifest exposing ledger + both tables together
  → read_gold() / pinned snapshot paths
```

Existing Spark factory, M2 source identity/fingerprints, Silver path and Hadoop
filesystem helpers are reused. Existing metric names/windows and event inclusion
remain recognizable. No database, broker, orchestrator or framework was added.

## 7. Silver contract used

Actual input uses the 13 Avro business fields (strings except amount double and
timestamp bigint), event_ts timestamp, source topic/string, partition/int,
offset/bigint, identity/fingerprint/string and M2 quality metadata.

Gold explicitly checks types, `_dq_passed=true`, `_quality_passed=true`,
`_route=SILVER`, empty `_failure_reason`, `_dq_contract_version=1.0`, configured
source topic, nonnegative coordinates, correct source identity, recomputed
business-payload fingerprint and `unix_millis(event_ts)=timestamp`.

Streaming input is `s3a://silver/<topic>/silver_batch=<key>/...`; the callback
requires its enclosing batch `_SUCCESS`. Bronze/DLQ/unverified/corrupt input fails
closed. Gold does not reclassify old valid Silver against current processing time.
Optional payment method and metadata remain optional except ambiguous cancellation
metadata, which cannot safely define its financial contribution.

## 8. Gold contract

Only the existing two implemented metric families are produced. A separate event
ledger supports durability/recomputation; it is not another aggregate metric.

**Exact revenue_by_minute schema:**

```text
window_start          timestamp
window_end            timestamp
region                string
currency              string
order_count           bigint
failed_count          bigint
refunded_event_count  bigint
gross_revenue         decimal(38,8)
refund_amount         decimal(38,8)
cancellation_amount   decimal(38,8)
net_revenue           decimal(38,8)
avg_order_value       decimal(38,8)
```

Logical identity: `(window_start, window_end, region, currency)`.

**Exact product_rankings schema:**

```text
window_start  timestamp
window_end    timestamp
product_id    string
product_name  string
currency      string
units_sold    bigint
revenue       decimal(38,8)
```

Logical identity: `(window_start, window_end, product_id, product_name, currency)`.
Product name remains a dimension as in the existing code. No top-N truncation or
ordinal ranking is invented; consumers may sort within a currency/window.

| Metric | Source / arithmetic / included events |
|---|---|
| gross_revenue | Sum exact amount of ORDER_PAID |
| refund_amount | Sum exact amount of ORDER_REFUNDED |
| cancellation_amount | Sum exact amount of ORDER_CANCELLED with boolean was_paid=true |
| net_revenue | Sum signed event contributions: paid − refunds − paid cancellations |
| order_count | Count PAID events; historical name, **not distinct orders** |
| failed_count | Count all CANCELLED events |
| refunded_event_count | Count REFUNDED events |
| avg_order_value | Exact gross paid total / paid-event count, rounded once HALF_UP to 8 places; null when count=0 |
| units_sold | Count PAID events; no quantity field exists |
| product revenue | Sum PAID amounts per five-minute product/name/currency group |

All metrics deduplicate only source identity, use each event's event_ts and separate
currencies. No amount null is accepted. No-event windows are absent. Observed
nonfinancial-only windows have zero financial sums and null paid average. Empty
input/all-DLQ produces no business row or new snapshot. Refunds without payments
remain negative flow, rather than being silently dropped or assigned fabricated payments.

## 9. Revenue semantics

Evidence chain:

1. `make_paid` applies a discount and emits the charged amount; its comment says
   revenue is recognized there. Lifecycle state amount is updated to that charge.
2. `make_cancelled` emits the state amount if paid and zero otherwise; `was_paid`
   records the state.paid boolean.
3. `make_refunded` emits a positive 50%/75%/100% refund amount, not an updated balance.
4. README, old Gold code, SQL generated net column and consumers independently
   specify subtraction of both refunds and paid cancellations.
5. The generator **can emit both** for one paid order. No schema field establishes
   whether the refund settles the cancellation reversal or is another reversal.

Smallest defensible assumption: retain that explicit **additive event-ledger**
contract and expose its consequence. PAID 100 + paid cancellation 100 + refund 30
equals **−30**, not a silently capped zero. Tests and runtime prove this behavior.

This is an event-flow KPI, **not a reconciled settlement balance**. Whether real
accounting should subtract both remains an unresolved upstream semantic risk;
changing it needs a versioned rule/correlation contract. No tax, fee or exchange-
rate semantics are established. “Gross” here means charged paid total, not
pre-discount catalog total. No currency conversion is performed.

## 10. Lifecycle semantics

- CREATED, SHIPPED and DELIVERED: retained in ledger, zero revenue contribution.
- PAID: positive charged amount; repeated source-distinct payments count separately.
- REFUNDED: negative amount; partial/multiple/repeated refunds are independent events.
- CANCELLED with JSON boolean true: negative amount; false: no revenue reduction.
- Missing/null/string was_paid on a cancellation: explicit Gold contract failure,
  no new snapshot. M2 validates optional JSON shape but does not establish this
  financial boolean; guessing would invent a rule.
- No matching, capping, deduplication by order/type/amount, or “one refund only” rule.
- Refund-only windows can be negative; multiple full refunds can exceed a payment
  under the existing additive-event contract.

All six lifecycle types, paid/unpaid cancellation, multiple refunds, source-distinct
duplicates and cancellation-plus-refund were exercised.

## 11. Window semantics

Retained **one-minute revenue** and **five-minute product** tumbling windows.
UTC, aligned to Unix epoch multiples, `[window_start, window_end)`.
The exact upper boundary belongs to the next window. No sliding/daily/hourly or
additional cumulative output table was introduced.

Events use their own occurrence windows; a later refund does not move backward to
the original payment window unless its own event_ts lies there. Cumulative net
flow can be obtained by summing same-currency minute rows. Averages must be
recomputed from paid totals/counts, not summed or naively averaged.

Tests cover millisecond boundary `+59999` versus `+60000`, five-minute boundaries,
multiple product windows and distinct currencies. Live Case 11 produced separate
15:40 and 15:41 minute rows while combining in the same five-minute product row.

## 12. Timestamp semantics

Gold preserves verified Silver event_ts and checks exact equivalence to epoch-ms
timestamp. Session timezone must be UTC. Kafka/ingestion times are upstream
provenance, not business time. Processing/manifest commit time is operational only.

The final fixture's base was `1791038700000` (2026-10-03 14:45 UTC). Business
events spanned 14:50–16:30 UTC while Gold processing occurred around 17:19–17:22 UTC.

## 13. Late-event semantics

No watermark is used. The old watermark neither accumulated batch-local state nor
made writes idempotent. This implementation retains a durable ledger and recomputes
complete snapshots, so evicting old event-time state would only lose valid data.

M2's ingestion-relative 24-hour past / 60-second future allowance remains in force
before Silver. Gold accepts every newly observed verified Silver event regardless
of later processing delay, revising older windows indefinitely within retained history.
Outputs are revisable snapshots, not declared-final closed windows.

Live offset 30 (`m3-10`, refund 15, event time 15:35 UTC) arrived after the initial
snapshot already contained events through 16:30 UTC; the existing 15:35 minute
changed from 100 to **85**. No unrelated window was lost.

## 14. Replay semantics

Source identity remains **topic + partition + offset**. The M2 business-payload
fingerprint is rechecked and used to detect conflicting content at the same identity.
Different offsets remain distinct even when order, type, amount and timestamp match.

Incoming events are merged with the currently committed ledger. Empty/pure-replay
input produces no new manifest or aggregate snapshot. This covers callback retry,
Spark checkpoint restart, a new checkpoint and repeated verification. Upstream
Bronze replay is also covered by the rerun M2 regression and Gold source identity.

The namespace assumes a stable Kafka cluster/topic incarnation. Topic recreation
or migration requires a new source namespace/output, rather than reusing coordinates.

## 15. Persistence/idempotency design

Root: `s3a://<gold-bucket>/<source-topic>/`.

1. Read the latest committed manifest and ledger.
2. Validate incoming Silver and merge unique identities; reject conflicts.
3. For new events, recompute both complete tables from all unique events.
4. Write immutable Zstd Parquet attempt paths for ledger and both metrics.
5. Verify successful outputs; publish a **single JSON manifest object** containing
   their paths/counts/schema, parent/version, batch/checkpoint and commit timestamp.

```text
<root>/_snapshots/<attempt-id>/events/
<root>/_snapshots/<attempt-id>/revenue_by_minute/
<root>/_snapshots/<attempt-id>/product_rankings/
<root>/_commits/00000000000000000003.json
```

On MinIO, S3A closes/publishes one object atomically; no directory rename or
distributed coordinator is used. **One writer per root is required.** Parent-version
checks detect some accidental conflicts but are not a concurrent-writer transaction.

`current_manifest` / `read_gold` expose only manifest-pinned complete tables.
Consumers must not recursively union snapshots: historical versions intentionally
contain earlier revisions. There is exactly one row per logical key in the current
table. No blind business-row append or historical-window overwrite occurs.

Failure before publication leaves an unreferenced attempt; prior committed state
remains visible. Retry writes a fresh attempt. Failure after publication but before
Spark checkpoint completion is a replay no-op. Corrupt/missing latest committed
output fails closed rather than silently falling back. Orphans/history are retained;
automatic deletion/retention/compaction was not introduced.

## 16. Data type/money handling

- Avro/Silver remains double; Gold converts its shortest decimal spelling with
  `Decimal(str(amount))` and requires exact Decimal(28,8) representability.
- Per-event limit: 20 integral and 8 fractional digits; excess precision/range,
  nonfinite/negative values fail before publication. Event values are never rounded.
- Financial totals/output: **Decimal(38,8)**. Counts: bigint. Currency: string dimension.
- ANSI overflow checks enabled; decimal precision loss disabled. Null overflow sums
  fail, rather than silently becoming zero.
- Average: high-precision decimal sum/count, rounded **once** HALF_UP to 8 places.
  A 10,001-event boundary test catches Spark avg's potential double-rounding behavior.
- Live 0.1 + 0.2 → **0.30000000**. Unit tests cover BHD 1.234 and explicit rejection
  of `0.30000000000000004`; no invented two-decimal or minor-unit truncation.
- This cannot recover precision already lost in the upstream double. Decimal
  migration of the authoritative event schema remains future contract work.

## 17. Test matrix

All required cases have automated assertions; real-stack evidence is in the final
`order-events-m3-20261003b` family. All source coordinates below are partition 0.

| Case | Source offsets / mechanism | Verified outcome |
|---|---|---|
| 1 Single paid | 2, m3-1 | net 100 |
| 2 Paid + refund | 3, 26, separate arrivals | net 100 → 70 |
| 3 Paid + multiple refunds | 4, 27, 33, three arrivals | net 100 → 80 → 50 |
| 4 Paid cancellation | 5, 28 | net 100 → 0; cancellation 100; failed_count 1 |
| 5 Unpaid cancellation | 6 | net 0; no paid event; failed_count 1; average null |
| 6 Same order/type at different offsets | refunds 8 and 9 | both counted; refund total 40, count 2, net 60 |
| 7 Exact source replay | committed Silver copies and callback retries | version/digest unchanged |
| 8 Spark restart | independent second/third/replay processes | zero new input before next arrival; no duplicate logical row |
| 9 Multi-batch accumulation | 10, 29 | net 100 → 75 |
| 10 Late event | 11, 30 | old 15:35 window 100 → 85 |
| 11 Multiple windows | 12, 13 | minute values 100 and 40; five-minute product total 140 |
| 12 Empty input | initial all-DLQ batch + empty unit frame | no Gold snapshot/row; clean query completion |
| 13 Invalid/DLQ exclusion | 0 invalid amount, 1 malformed wire | DLQ=2, absent from Gold source identities |
| 14 Mixed lifecycle | 14–17, 31 | created/paid/shipped/delivered/refund: gross 100, refund 30, net 70, paid count 1; product 1/100 |
| 15 Repeated verification | replay command run twice | same version 3 and logical digest |
| Additional precision | 18, 19 | exact 0.3 total, average 0.15 |
| Additional currencies | 20 INR, 21 USD | separate 10 and 20 rows |
| Overlapping reversal ambiguity | 22, 32, 34 | additive documented result −30 |
| Unmatched refund | 23 | net −7, no fabricated paid event |
| Nonfinancial-only window | 25 | zero financial sums, null average; no product row |
| Publication failure | isolated recovery root | no visible uncommitted attempt; retry commits once |

## 18. Automated test results

Final command:

```bash
python3 -m scripts.capture_milestone3_check --step tests --topic order-events-m3-20261003b
```

This executes the entire existing `tests/unit` directory through spark-submit and
the installed pytest, on actual Spark 3.5.9. **26 total: 26 passed, 0 failed,
0 skipped; 1 warning; 102.83 seconds.** Includes 19 Gold and 7 M2 tests.

Evidence: `tests-1791047933282451427.log` and `.json` under the final fixture directory.
The warning is Authlib's HTTPX deprecation. Earlier successful iterations (17 Gold,
then 25 combined) remain recorded; the final run includes the extra average test.

Coverage includes all lifecycle arithmetic, actual aggregation, event-time
boundaries, late historical updates, money/currencies, invalid-boundary rejection,
conflicting-source protection, local Parquet manifests, replay, empty state,
failure before publication with/without prior state and corrupt latest manifest.

Targeted Ruff, compileall, host imports, host pip check and Compose validation
passed. Initial lint found one unused verifier import; fixed and rerun. Container
pip check remains nonzero for the pre-existing pygobject/pycairo OS-package issue;
it is not hidden or represented as a clean environment check.

## 19. Runtime test results

Final implementation tested on the real stack in independent Spark processes.
Kafka topic **order-events-m3-20261003b**, partition 0, offsets **0–34**.

| Stage | Bronze | Physical Silver | Unique Gold ledger | Revenue rows | Product rows | DLQ | Gold version |
|---|---:|---:|---:|---:|---:|---:|---:|
| All-DLQ first batch | 2 | 0 | 0 | 0 | 0 | 2 | none |
| Initial valid arrival | 26 | 24 | 24 | 18 | 13 | 2 | 1 |
| Second arrival | 33 | 31 | 31 | 18 | 13 | 2 | 2 |
| Third arrival | 35 | 33 | 33 | 18 | 13 | 2 | 3 |
| First deliberate Silver-file replay | 35 | 66 | 33 | 18 | 13 | 2 | 3 |
| Repeated verification/file replay | 35 | 132 | 33 | 18 | 13 | 2 | 3 |

The extra Silver rows were deliberately added **only to this isolated fixture** to
exercise Gold's replay boundary. They are not a regression in the production Silver sink.
Every output metric on every row was checked against an independent Python decimal/
integer-epoch oracle. Query status, IDs/run IDs, batch progress and checkpoints are
captured in each JSON. Queries terminated normally: active=false, exception=None.

Spark progress input counts can be multiplied by multiple callback actions (for
example 132 physical replay rows reported 528 reads); Kafka offsets, explicit
DataFrame counts and unique ledger identities establish cardinality.

Exact final evidence files under `milestone3_evidence/order-events-m3-20261003b/`:

- `initial-1791047973034905790.json`
- `second-1791048009056275334.json`
- `third-1791048047727162973.json`
- `replay-1791048096189853943.json`
- `replay-1791048130457555125.json`
- `recovery-1791048166546929246.json`

Each has a corresponding full `.log` and exact Docker/spark-submit command.
The earlier `20261003a` fixture and evidence are also retained.

Reproduce with a fresh unique topic for initial setup:

```bash
docker.exe compose up -d kafka schema-registry minio
docker.exe start oip-modernization-verify
python3 -m scripts.capture_milestone3_check --step initial --topic order-events-m3-<new-run>
python3 -m scripts.capture_milestone3_check --step second --topic order-events-m3-<new-run>
python3 -m scripts.capture_milestone3_check --step third --topic order-events-m3-<new-run>
python3 -m scripts.capture_milestone3_check --step replay --topic order-events-m3-<new-run>
python3 -m scripts.capture_milestone3_check --step replay --topic order-events-m3-<new-run>
python3 -m scripts.capture_milestone3_check --step recovery --topic order-events-m3-<new-run>
```

Initial setup refuses existing topics instead of resetting them. `replay` is the
repeatable verification path. The retained Linux verifier container supplies
cached packages and internal endpoints; its original creation details are in the
M1 modernization report. Windows bind mount is `<repository-root>:/opt/oip:ro`.

## 20. Actual Gold outputs

Final committed manifest:

```text
s3a://gold/order-events-m3-20261003b/_commits/00000000000000000003.json
committed_at = 2026-10-03T17:20:43.007198+00:00
version = 3, parent = 2, Gold batch_id = 2, new_events = 2
```

Pinned snapshot base:

```text
s3a://gold/order-events-m3-20261003b/_snapshots/57373867342d400784ae6dcbf06912cd/
```

All 18 revenue rows below have region north-india, date 2026-10-03 and end=start+1
minute. Amounts are Decimal(38,8); compact decimal formatting is used here.

| UTC start | Currency | Gross | Refund | Paid cancel | Net | Paid events | Cancel events | Refund events | Paid average |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 14:50 | INR | 100 | 0 | 0 | 100 | 1 | 0 | 0 | 100 |
| 14:55 | INR | 100 | 30 | 0 | 70 | 1 | 0 | 1 | 100 |
| 15:00 | INR | 100 | 50 | 0 | 50 | 1 | 0 | 2 | 100 |
| 15:05 | INR | 100 | 0 | 100 | 0 | 1 | 1 | 0 | 100 |
| 15:10 | INR | 0 | 0 | 0 | 0 | 0 | 1 | 0 | null |
| 15:15 | INR | 100 | 40 | 0 | 60 | 1 | 0 | 2 | 100 |
| 15:30 | INR | 100 | 25 | 0 | 75 | 1 | 0 | 1 | 100 |
| 15:35 | INR | 100 | 15 | 0 | 85 | 1 | 0 | 1 | 100 |
| 15:40 | INR | 100 | 0 | 0 | 100 | 1 | 0 | 0 | 100 |
| 15:41 | INR | 40 | 0 | 0 | 40 | 1 | 0 | 0 | 40 |
| 15:55 | INR | 100 | 30 | 0 | 70 | 1 | 0 | 1 | 100 |
| 16:05 | INR | 0.3 | 0 | 0 | 0.3 | 2 | 0 | 0 | 0.15 |
| 16:10 | INR | 10 | 0 | 0 | 10 | 1 | 0 | 0 | 10 |
| 16:10 | USD | 20 | 0 | 0 | 20 | 1 | 0 | 0 | 20 |
| 16:15 | INR | 100 | 30 | 100 | −30 | 1 | 1 | 1 | 100 |
| 16:20 | INR | 0 | 7 | 0 | −7 | 0 | 0 | 1 | null |
| 16:25 | INR | 0 | 0 | 0 | 0 | 0 | 1 | 0 | null |
| 16:30 | INR | 0 | 0 | 0 | 0 | 0 | 0 | 0 | null |

Representative product rows (all 13 and every source identity are in the JSON):

| UTC window | Product / name | Currency | units_sold | revenue |
|---|---|---|---:|---:|
| 14:50–14:55 | prod-3001 / Case 1 | INR | 1 | 100.00000000 |
| 15:40–15:45 | prod-3011 / Case 11 | INR | 2 | 140.00000000 |
| 16:05–16:10 | prod-3016 / Case 16 | INR | 2 | 0.30000000 |
| 16:10–16:15 | prod-3017 / Case 17 | INR | 1 | 10.00000000 |
| 16:10–16:15 | prod-3017 / Case 17 | USD | 1 | 20.00000000 |

## 21. Restart/replay evidence

Gold query ID remained `0fcb7306-8ec9-4a9d-94b6-e0578f850e96` across same-checkpoint
process restarts; run IDs changed. Restarts with no new input reported zero input.

Logical output digest after third arrival, both replay runs and recovery inspection:

```text
8263508ee79077227be47a2a550111b4b65e45daf01e28bed149eae5c0e57579
```

Version remained 3. Physical Silver input increased 33→66→132 deliberately, but
ledger/metrics stayed 33/18/13. Direct completed-callback retry and fresh file-source
checkpoints also left version and digest unchanged.

Verified checkpoint metadata/source/offset/commit files under:

```text
s3a://checkpoints/order-events-m3-20261003b/{bronze,silver,gold}/
s3a://checkpoints/order-events-m3-20261003b-fresh-1791048079857303746/gold/
s3a://checkpoints/order-events-m3-20261003b-fresh-1791048112722254302/gold/
```

Recovery fixture `s3a://gold/order-events-m3-20261003b-recovery/`: injected failure
after all Parquet writes but before manifest publication; no committed snapshot
visible. Retry produced version 1, a subsequent callback retry stayed version 1,
and both metric tables matched the main fixture exactly. Local tests also prove
an interrupted update preserves a pre-existing visible snapshot. This is controlled
fault injection, not a claim that every OS/process/storage failure was exercised.

## 22. Milestone 1 regression

**PASS — fresh regression after final implementation refinement.**

Topic `order-events-m1-m3-20261003b`, actual generator constructors and KafkaPublisher:
**4 delivered, 0 failed**, offsets 0–3. Schema ID 1, subject version 1, Confluent
header `0000000001`, registered schema equals repository Avro. Direct binary decode
and the existing M1 Spark verifier passed. Bronze read-back **4 rows**, required
fields non-null, unique coordinates, exact UTC timestamp equivalence and checkpoint
files verified.

Evidence: `milestone3_evidence/order-events-m1-m3-20261003b/m1-1791048187613727733.json`
and `.log`. Actual runtime tuple: Spark 3.5.9, Python 3.10.12, Java 17.0.19,
Hadoop 3.3.4, AWS SDK 1.12.262.

## 23. Milestone 2 regression

**PASS — existing M2 verifier rerun on fresh isolated data after final changes.**

Topic `order-events-m2-m3-20261003b`:

- Initial controlled cases: Bronze 32, Silver 11, DLQ 19, audit 32.
- Separate-process restart and later source replay/new event: Bronze 35, Silver 12,
  DLQ 19; complete fresh-checkpoint replay raised audit to 70 without new outputs.
- Recovery phase: one Silver/one DLQ, completed-batch no-op, unfinished-checkpoint
  guard, partial-output guard, empty-source startup and zero-record quality passed.
- Seven M2 pytest tests passed in the complete final suite.

Evidence under `milestone3_evidence/order-events-m2-m3-20261003b/`:
`m2-initial-1791048226891666509`, `m2-restart-1791048270923296277`,
`m2-recovery-1791048299781294340` (`.json` and `.log`). Original M1/M2 historical
data/checkpoints remain intact; regression used new namespaces.

## 24. Dependency review

| Component | Actual current version | Decision | Reason |
|---|---|---|---|
| Spark / PySpark | 3.5.9 | **KEEP** | Actual Gold and regression runtime pass |
| Spark Kafka / Avro | 3.5.9 / Scala 2.12 | **KEEP** | Exact compatibility tuple retained |
| Hadoop / hadoop-aws | 3.3.4 | **KEEP** | S3A snapshot/manifest operations verified |
| AWS SDK bundle | 1.12.262 | **KEEP** | Hadoop-aligned dependency, no independent upgrade |
| fastavro | 1.12.2 | **KEEP** | Fresh M1/M2 binary paths pass |
| confluent-kafka | 2.15.1 | **KEEP** | Actual fixture publishing, registry and consumption pass |
| Java / Scala | 17.0.19 / 2.12 | **KEEP** | Verified Spark image tuple |
| Python host / container | 3.11.9 / 3.10.12 | **UPDATE LATER** | Runtime works; patch/lifecycle image work remains |
| Decimal / JSON / UUID | Python standard library | **NO CHANGE** | No new monetary/storage framework required |
| Pydantic / settings | 2.9.2 / 2.6.1 | **NO CHANGE** | No settings-model migration needed |
| pytest / structlog | 8.3.3 / 24.4.0 | **KEEP** | Existing tests/logging work |
| Confluent Kafka/Registry | 7.7.12 paired | **UPDATE LATER** | Prior support-lifecycle migration remains out of scope |
| MinIO server/client | Existing pinned 2024 releases | **UPDATE LATER** | Real reads/writes pass; prior maintenance/distribution issue remains |
| Container OS pygobject/pycairo | 3.42.1 / missing | **UPDATE LATER** | Pre-existing container pip-check issue unrelated to milestone modules |

No UPDATE NOW required. **No dependencies were changed or installed for M3.**
Host project pip check passed; container-wide pip check did not, as explicitly
recorded. PostgreSQL JDBC remains in the shared Spark package tuple for baseline
compatibility, but Gold performs no PostgreSQL import/connection/write.

## 25. Remaining limitations

- Full-ledger recomputation and immutable full snapshots trade performance/storage
  for clear correctness in the current local architecture; large-scale compaction,
  partition pruning, indexing and retention are not implemented.
- Single writer per Gold root; no concurrent-writer or multi-cluster guarantee.
- Consumers must use committed manifests, not union historical directories.
- Gold representability/ambiguous-cancellation failures block the batch explicitly;
  no additional quarantine workflow was invented or valid record silently dropped.
- Original amount is double; Gold cannot reconstruct already-lost precision.
- Product names remain part of product identity; units_sold/order_count count events,
  not quantities or distinct orders. This is explicit compatibility semantics.
- No business event ID; newly published offsets remain independent events.
- Bronze append/checkpoint crash atomicity and DLQ at-least-once limits from M2
  remain. Gold protects itself using source identity.
- Region heatmap, lifecycle funnel, unique users/products and anomaly baseline were
  investigated but are not needed to correct the two existing implemented metrics.
- No performance, load, concurrent-writer, live broker outage or hard-process-kill
  acceptance claim is made. Tests exercised bounded real infrastructure and explicit
  publication failures; all data and orphan attempts were retained.

## 26. Known risks

**Business interpretation:** cancellation and refund may represent the same real-world
reversal, but the current schema cannot establish that. Additive −30 is a tested
contract assumption, not proof of accounting correctness. A future versioned
business contract must resolve overlapping reversal semantics before treating this
KPI as settled revenue.

**Serving compatibility:** existing PostgreSQL keys omit currency, use narrower
decimals, and product identity omits name. Redis/dashboard/alerts/metrics similarly
assume older unqualified financial rows. They were inspected but not changed or
invoked. They must not consume new Gold by blind schema mapping. SQL interval,
transaction/partial-upsert and other historical downstream defects remain future work.

**Operational:** MinIO object atomicity and one writer underpin publication;
accidental simultaneous writers are unsupported. Retaining snapshots/orphans
increases storage. Corrupt latest commits stop readers instead of hiding damage.
Timestamp-sensitive initial fixtures should be generated fresh; repeat verification
uses the original accepted Silver data. The source ledger namespace must change
if Kafka topic offsets are reset/recreated.

**Environment:** Python/container, Confluent and MinIO lifecycle findings from the
prior modernization review remain; the unrelated container pycairo dependency
issue persists. These do not invalidate the observed local acceptance results.

**Preserved scope/data:** producer, Avro schema, Bronze/DQ/Silver production code,
Kafka topic admin, requirements, active environment/cluster identity, Compose,
Spark factory, PostgreSQL/Redis/dashboard/alerts/observability/CI/security code
were intentionally unchanged. Only isolated new fixtures/checkpoints/outputs were
created. No historical data, bucket, topic, database table or volume was deleted.
After verification, the four services started for this work were stopped to
restore their initial state; containers and all retained data remain available.

## 27. Exact next milestone

**Milestone 4 — committed Gold → PostgreSQL serving with currency-aware identities,
decimal-safe schema and idempotent complete-snapshot upserts.**

Consume the authoritative manifest/snapshot contract and verify replay-safe serving.
Implement only that single next milestone; Redis/dashboard/alerts remain later work.
