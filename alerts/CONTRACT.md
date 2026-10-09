# M7 authoritative analytical alerts — contract 1.0

## Entry point and source

With the existing process environment loaded, run once:

```bash
python main.py alerts --rules alerts/rules.example.json
# Equivalent:
python -m alerts.evaluator --rules alerts/rules.example.json
```

`ALERT_RULES_FILE` may supply the rules file; there are no implicit rules or
currency thresholds. `ALERTS_SCHEMA` defaults to `alerts_serving` and may select an
isolated `m7_<slug>` result schema. `ALERT_SOURCE_TOPIC` optionally pins the expected
source topic. Existing `POSTGRES_*` environment variables supply the connection;
`POSTGRES_PASSWORD` is required. No secret or DSN is logged.

The CLI source is **always `gold_serving`**. It ignores a historical test schema
override for analytical selection. The Python verification API additionally accepts
`m7_source_<slug>` for generated isolated fixtures. No legacy `gold.*`, Redis,
dashboard, Kafka or data-lake fallback exists. M7 reuses M5's public read-only
PostgreSQL snapshot reader and validation, not its cache functions.

## Five distinct concepts

1. **Definition:** immutable explicit configuration and its SHA-256 digest.
2. **Evaluation:** one rule's result on the latest stored comparable dimension row
   within one authoritative M4 serving version. Clear/unknown/suppressed results
   are persisted too.
3. **Event:** a persisted `FIRED` record. No notification or delivery occurs.
4. **State:** last observed condition and last actual firing time per rule/scope.
5. **History:** immutable evaluations, input evidence, source snapshots and firing
   events; state is only the latest projection.

## Rule definition

Every definition requires `rule_id`, `kind`, `currency`, `dimensions`, `metric`,
`comparison`, decimal-string `threshold`, integer `cooldown_seconds`, `severity`,
and boolean `enabled`. Unknown fields/types, duplicate IDs and arbitrary comparison
or metric expressions fail configuration validation. Definitions are never SQL.

Supported kinds:

| Kind | Metric | Exact dimensions | Comparison | Window / baseline |
|---|---|---|---|---|
| `revenue_threshold` | stored `net_revenue` | region + currency | lt/lte/gt/gte threshold | latest stored 60-second row for that dimension |
| `revenue_drop` | stored `net_revenue` | region + currency | current < previous × configured retained-revenue ratio | latest stored minute; immediately adjacent prior minute in same snapshot/dimension, with strictly positive prior net |
| `product_threshold` | stored `revenue` or `units_sold` | product_id + product_name + currency | lt/lte/gt/gte threshold | latest stored 300-second row for that full dimension |

“Latest” means maximum `(window_start, window_end)` **within the stored snapshot**,
not wall-clock now and not an assertion that a window is finalized forever. M4
supports later revisions. M7 does not backfill/fire every historical window, invent
zero rows for missing dimensions, interpolate missing minutes, recompute net,
aggregate currencies, or introduce FX. Dimensions are exact values, not wildcards.
If the prior adjacent minute is missing/nonpositive, drop is `NO_BASELINE`.

Money thresholds must be exactly representable as NUMERIC(38,8). Unit thresholds
are nonnegative signed-BIGINT integers. Drop thresholds are ratios strictly between
0 and 1 with `lt` comparison. Decimal precision 80 is used for exact multiplication;
no rounded division or financial float calculation is used. Current and previous
source rows are persisted as exact JSON evidence (money strings, UTC timestamps).

`INFO`, `WARNING`, `CRITICAL` are labels copied from the definition, with no implicit
escalation or delivery mapping. Examples select WARNING for low net, CRITICAL for
drop and INFO for product performance; operators must choose their own explicit
thresholds/dimensions. Example thresholds are verification configuration, not a
business-wide policy.

`rule_id` is immutable within a result schema. Any changed configuration, including
enabled/severity/cooldown, must use a new explicit ID; attempts to overwrite an ID
fail atomically. A new ID has independent state/cooldown. To stop evaluating an
existing definition, omit it from invocation; persisted state remains a labeled
last observation, not a live-health assertion. A configured disabled definition
records `DISABLED`/UNKNOWN and does not fire. There is no rule-management daemon.

## Deterministic identity and provenance

Canonical JSON: sorted keys, compact separators, ASCII escaping, no NaN.

```text
scope_id = SHA256({rule_id, currency, dimensions,
                   source_database, source_schema, source_topic, source_root})
definition_digest = SHA256(canonical definition incl. contract/window policy)
evaluation_id = SHA256({scope_id, definition_digest, full M4 provenance,
                        window:[window_start,window_end] or null})
event_id = SHA256({evaluation_id,event_type:"FIRED"})
```

Analytical identity includes both window boundaries, currency, and full region
or product ID/name dimensions. No random ID is the basis of deduplication. Separate
isolated result schemas may contain the same business IDs; they are independent
verification stores, not additional global emissions.

Full M4 metadata is recorded in `source_snapshots` and joined via version: source
database/schema/topic/root, Gold version/contract, manifest path/digest, snapshot
identity/digest, counts, served_at and status. `cache_contract_version` is also
retained because the shared M5 PostgreSQL reader includes it; **the data comes
from PostgreSQL, not Redis**. Evidence preserves the exact current/prior rows,
comparison boundary, qualification, previous/resulting state, database evaluation
time and pre-decision cooldown deadline. These inputs make decisions reproducible
even after M4 complete replacement removes a row.

## Cooldown and idempotency

- Scope: one rule ID + exact dimensions + currency + authoritative source identity.
  Windows and serving versions are deliberately excluded from cooldown scope.
- Duration: explicit integer 1–604800 seconds per definition.
- Clock: PostgreSQL `clock_timestamp()` after locks/source validation, once per
  invocation. No client clock, Redis TTL or event-time clock drives cooldown.
- New qualifying evaluation fires if no previous firing exists or DB time is
  **greater than or equal to** last_fired_at + cooldown. Otherwise it persists a
  `SUPPRESSED` evaluation without a firing event.
- Only an actual firing advances last_fired_at. Clear/recovery/unknown/suppression
  do not extend or reset the deadline.
- A new source version, late revision, or different window does **not** bypass
  cooldown. A new rule ID is an explicit independent policy, not an implicit retry.
- Identical rule/version/window/provenance is always `NOOP`, even after the
  cooldown expires. No periodic reminder is generated from one old snapshot.
  Expiry permits firing on the **next distinct qualifying evaluation**.
- Restart reads persisted state and immutable evaluations. Replaying a prior
  completed evaluation cannot generate another event or transition.
- PostgreSQL unavailability fails; there is no in-memory fallback or fabricated
  cooldown decision. A backwards DB clock is rejected for new state transitions.

## State machine

Stored state has three values:

| Observation | State after | Evaluation outcome | Event? |
|---|---|---|---|
| Matching rule, outside cooldown | ACTIVE | FIRED | yes |
| Matching rule, inside cooldown | ACTIVE | SUPPRESSED | no |
| False condition after ACTIVE | OK | RECOVERED | no |
| False condition otherwise | OK | OK | no |
| No row / no baseline / disabled | UNKNOWN | NO_DATA / NO_BASELINE / DISABLED | no |
| Identical completed evaluation | unchanged | invocation NOOP (original outcome returned) | no |

Unknown is not recovered and not zero. Recovery is the observed ACTIVE→OK
transition; UNKNOWN→OK records OK rather than asserting an unobserved recovery.
Every state includes its last evaluation reference/time. Absence of a rule from a
later invocation does not silently change state or delete history.

## Transactions, coherence and concurrency

Initialization runs idempotent additive DDL from `alerts/persistence.sql` in its
own transaction, serialized per target. It never changes analytical/legacy tables.
Both initialization and evaluation use a ten-second lock timeout.

Evaluation uses an alert write transaction with:

1. Transaction advisory exclusive lock `oip.m7.<alert-schema>`.
2. Transaction advisory **shared** lock on existing M4 writer key
   `oip.gold-serving.<source-schema>`. M4 takes that same key exclusively for
   complete replacement; M7 holds the shared lock through alert commit.
3. A second connection calls the frozen M5 PostgreSQL reader in its own
   REPEATABLE READ READ ONLY transaction. This avoids copying its validated read
   implementation. The M4 lock prevents a cooperating M4 writer from publishing
   between source read and alert commit.
4. Validate current `serving_history` equality and highest successfully served
   source version; compare the target's prior source/version high-water mark.
   Reject missing/uncommitted/older/conflicting source metadata or changed source
   binding. Check one-/five-minute analytical window durations.
5. Persist definitions, snapshot provenance, evaluations, event and state in the
   **one alert transaction**. Primary/unique/FK constraints enforce one rule/version
   evaluation and at most one event per evaluation.

Two evaluators targeting one schema serialize. SELECT FOR UPDATE protects existing
scope state; first creation is serialized by the transaction lock and unique keys.
There is no lease/distributed infrastructure. Each invocation needs two available
pooled SQL connections; a caller-supplied engine must provide that capacity. The
default engine has pool_size 2, max_overflow 1. Separate workers should use their
own engines. This is bounded local evaluation, not a high-scale distributed claim.

All source writes must follow M4's established writer contract. Arbitrary external
SQL that bypasses that protocol is not serialized by its advisory lock; observed
corruption fails metadata/count/digest validation. M7 does not inspect MinIO commit
objects: it trusts only successfully served PostgreSQL state/history.

## Persistence and backend results

Default schema `alerts_serving`, five tables:

- `alert_definitions`: immutable rule JSON/digest keyed by rule_id.
- `source_snapshots`: provenance/high-water history keyed by Gold version; target
  schema binds to exactly one analytical source.
- `alert_evaluations`: immutable decision/transition/input history; SHA identity and
  UNIQUE(rule_id,gold_version), currency/window/dimension evidence.
- `alert_state`: latest condition + last_fired_at and last evaluation FK per scope.
- `alert_events`: only actual firings, unique evaluation FK, severity/time.

Evaluation scope and event-time indexes support bounded result lookup. JSON evidence stores decimal
text instead of reducing financial precision in legacy NUMERIC(18,4) columns.

`read_results(engine, alert_schema, limit=100)` performs a read-only coherent
bounded read of events joined to definition/evidence/provenance plus latest states.
It returns Python dictionaries and is the backend consumer boundary. No HTTP API,
notification provider, dashboard panel or metrics collector is added.

The one-shot result includes per-rule IDs/outcomes, new FIRED/SUPPRESSED/NOOP/etc.
counts, source provenance and duration. `evaluated_at` in a NOOP invocation is its
attempt time; stored original evaluation/state timestamps remain unchanged.
Failures raise explicit errors and CLI exits nonzero without printing credentials
or chained driver exceptions. If the connection fails after COMMIT, outcome may be
ambiguous; rerun resolves by deterministic IDs. No failed state overwrites the last
valid result. Persistent failure metrics/notification delivery are outside M7.

## Operational limits and frozen boundaries

The reused source reader's row bounds and memory scope remain unchanged. Up to
1000 explicit rules per invocation; rows are selected in Python without new Gold
aggregation. No automatic run loop. Only latest stored rows are evaluated; there
is no source-age SLA, full-history backfill or real-time guarantee on retained
historical fixtures. Freshness is visible through source provenance.

Legacy `rule_engine.py`, `statistical_detector.py`, `alert_router.py`, legacy SQL
tables and Redis cooldown helpers remain historical code. M7 does not import them;
the repaired `main.py alerts` entrypoint invokes only `alerts.evaluator`. No M1–M6
production semantic changes, analytics writes, notifications, dashboard changes,
Prometheus/Grafana, scheduling, ML, deployment automation or dependency upgrades.
