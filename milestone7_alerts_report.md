# Milestone 7 — Authoritative Gold-serving Alerts

## 1. Executive result

**PASS — 2026-10-04.** M7 implements one-shot alert evaluation from authoritative
M4 PostgreSQL `gold_serving`, with explicit currency-scoped rules, deterministic
evaluation/event IDs, persisted cooldown and state, immutable decision history,
source provenance, atomic commits and restart-safe deduplication.

Actual PostgreSQL runtime verification passed, including known-condition fixtures,
Redis independence, PostgreSQL outage/recovery and separate-process restart.
The final full relevant test suite passed **137 tests**, including **40 M7 tests**.
M1–M6 runtime regressions passed. No notification providers, metrics system,
dashboard alert UI, scheduler or deployment automation were implemented.

Default CLI certification occurred at `2026-10-04T12:41:41.687942+00:00`:
four explicit rules produced two persisted firing events, one clear condition and
one `NO_BASELINE`. A separate process returned four NOOP results without changing
stored events/state/history. Cooldown suppression and expiry were separately
proved with a controlled, isolated PostgreSQL dataset and the real database clock.

## 2. Current-world inspection

Inspection preceded code changes; findings and design were recorded in the working
conversation before implementation. Reviewed the existing alert engines/router,
Redis cooldown helpers, alert SQL, M4 serving transaction/DDL/state/history, M5
read-only PostgreSQL API, M6 dashboard adapter, main dispatcher, configuration,
requirements, Compose service settings, README and phase status. Repository
searches covered alert/threshold/anomaly/cooldown/notification, serving/version,
manifest/digest, source and state/history references in source, SQL and docs.
No existing alert tests or alert verification script existed; no separate docker/
alert-service implementation was discovered. Compose supplies PostgreSQL/Redis,
not an alert worker.

Observed runtime before implementation:

```text
PostgreSQL: 16.14
gold_serving version: 3
revenue rows: 18
product rows: 13
source status: SERVED
legacy alerts.alert_log rows: 0
legacy alerts.active_alert_state rows: 0
```

| Inspection question | Actual finding |
|---|---|
| Existing code | `alerts/rule_engine.py`, `statistical_detector.py`, `alert_router.py`, empty package initializer |
| Legacy? | Yes: predates currency-aware M4 and couples analytics to notification routing |
| Tables read | `gold.revenue_by_minute`, `gold.anomaly_baseline`; not `gold_serving` |
| Redis use | `is_alert_active` / `set_alert_active` for cooldown, keyed by type/region |
| Gold recomputation | Recomputes net from gross/refund/cancellation in SQL; converts to floats |
| Currency | No currency dimension; hard-coded INR messages; dimensions can conflate currencies |
| Cooldown | Redis TTL (legacy 900s); checking and setting are separate from SQL commit |
| Persistence | Legacy SQL log exists; defined PostgreSQL active state is not used by engines |
| Deduplication | Random log UUID; no deterministic rule/window/version uniqueness |
| Provenance | No M4 serving version/manifest/digest captured |
| Startup | `main.py alerts` pointed to nonexistent `alerts.daemon:run` |
| Tests | No alert-specific tests; M1–M6 tests and verifiers are available for regression |
| Safe reuse | M5's public validated PostgreSQL reader/pooled engine and M4's advisory-lock contract |
| Replacement required | Active alert entrypoint, rule semantics, authoritative cooldown/history path |
| Preserved | Legacy files/tables/helpers, all M1–M6 production semantics, notification router unimported |

## 3. Existing alert system problems

The legacy threshold layer compared revenue to an unverified seven-day baseline,
computed failure ratios and business-hour zero-order conditions with magic numbers,
and omitted currency. The statistical layer required unverified baselines and used
float EWMA/Z-scores. The legacy log schema has NUMERIC(18,4), region-only scope,
notification timestamps/channels and random identifiers; it cannot faithfully
represent the M4/M7 contract.

Redis cooldown could disappear on cache loss, and SQL commit followed by Redis
update/notification was not an atomic state transition. Concurrent invocations
could both pass the cooldown test. Existing “active state” SQL was unused.
Errors in the legacy notification router were logged/swallowed, and alert
evaluation directly attempted Slack/PagerDuty delivery. M7 does not invoke it.

Those files were retained as historical code. The main alert entrypoint now selects
only the new evaluator, so normal M7 execution cannot reach legacy notification
providers. No unrelated refactoring or migration of consumers was needed.

## 4. M7 architecture

```text
Explicit rules JSON
        ↓
alerts.definitions.Rule (validated immutable configuration)
        ↓
alerts.evaluator.evaluate (one invocation)
        ├─ M4 shared writer lock
        ├─ existing M5 read-only PostgreSQL snapshot reader
        ├─ current serving_history + prior evaluated-version validation
        ├─ exact threshold / adjacent-window comparison
        └─ one PostgreSQL alert transaction
             ├─ definitions
             ├─ source provenance
             ├─ evaluations/history
             ├─ current state/cooldown
             └─ firing events
        ↓
Structured result + read_results() backend contract
```

The read API reused from the M5 module performs PostgreSQL SELECTs only. Importing
that module loads the installed redis-py library, but M7 makes no Redis connection,
read or write. Actual evaluation with the Redis container stopped passed.

`python main.py alerts --rules <file>` and `python -m alerts.evaluator --rules <file>`
are one-shot commands, not daemons. The main dispatcher change is limited to the
previously broken alert selector; other service selectors are unchanged.

Full operational contract: [alerts/CONTRACT.md](alerts/CONTRACT.md).

## 5. Data source and coherent-read boundary

Production/CLI source is always `gold_serving`; there is no `gold_m4_final`, legacy
Gold, cache or data-lake fallback. The Python test API permits only specifically
named generated `m7_source_*` schemas for isolated M4-shaped fixtures.

The existing M5 reader validates M4 contracts, columns, counts, Decimal values,
identities and the entire snapshot digest in one REPEATABLE READ READ ONLY
transaction. M7 also validates:

- `SERVED` state matches its successful `serving_history` entry, including timestamp.
- Current version equals the maximum successful source-history version.
- Source root/topic/manifest path identity agrees.
- Windows have the expected 60-second revenue / 300-second product duration.
- The alert schema remains bound to one database/schema/root/topic.
- Source version does not move backward relative to persisted M7 history.
- Identical versions do not change provenance.

The alert write transaction holds a shared advisory lock using M4's existing key
`oip.gold-serving.<source-schema>` through commit. M4's writer takes this lock
exclusively. This keeps the committed analytical source stable across the separate
read-only connection and the alert write transaction, while reusing frozen M5 code.
M7 itself never mutates analytical tables/state/history.

This is cooperation with the M4 writer protocol, not a claim to prevent arbitrary
external SQL that bypasses it. Observed corrupt data fails validation. M7 does not
read MinIO manifests to independently attest them; authoritative PostgreSQL's
successful state/history is its committed-source boundary.

## 6. Alert rule contract

Each rule requires:

```text
rule_id, kind, currency, dimensions, metric, comparison,
threshold (decimal string), cooldown_seconds, severity, enabled
```

Configuration is validated; arbitrary SQL, callable expressions, unknown metrics,
comparisons and fields are not accepted. One invocation accepts 1–1000 unique IDs.
No rules are implicitly enabled without an explicit file. The example file is
controlled verification configuration, not a universal business threshold policy.

| Rule kind | Metric / dimensions | Comparison | Evaluation window |
|---|---|---|---|
| revenue_threshold | stored net_revenue / exact region, currency | lt/lte/gt/gte configured monetary threshold | latest stored 1-minute row for that dimension |
| revenue_drop | stored net_revenue / exact region, currency | current < prior × configured retained-revenue ratio | latest stored minute versus immediately adjacent previous minute in same snapshot/dimension |
| product_threshold | stored revenue or units_sold / product_id, product_name, currency | lt/lte/gt/gte configured monetary/integer threshold | latest stored 5-minute row for that full product dimension |

For drop, prior net must be positive and its window_end must equal current
window_start. Missing/nonpositive baseline yields NO_BASELINE, never interpolation
or a guessed zero. Drop is an exact Decimal multiplication comparison, not rounded
percentage division. Ratio must be strictly between 0 and 1; comparison is lt.

No net recomputation, cross-currency totals or Gold reaggregation occurs. Current
and baseline source rows are recorded as exact evidence. M7 evaluates latest stored
rows, not all historical windows or wall-clock “current” data; the output always
identifies the source version/time. Late M4 revisions are distinct evaluations.

Severity is explicitly INFO, WARNING or CRITICAL, copied to events. No automatic
escalation/delivery hierarchy. Rule IDs are immutable: changed policy requires a new
ID. A new ID is an explicit independent rule with independent cooldown. Omitted
rules are not evaluated; their stored state remains their last observation.
Disabled definitions persist DISABLED/UNKNOWN decisions without firing.

## 7. Currency semantics

Every rule must name exactly one uppercase three-letter currency. Monetary
thresholds are therefore explicitly currency-specific. No FX or cross-currency
aggregation exists. Revenue scope is region/currency; product scope includes ID,
name and currency. Both window boundaries enter evaluation identity.

Runtime uses independent INR and USD definitions. With retained data, INR net 0
is below its explicit threshold 10 while USD net 20 is not below its separately
configured threshold 10. Tests also make both currencies qualify for the same
window and verify two separate events/scopes, including full negative
NUMERIC(38,8) preservation. Product tests distinguish names, currencies and windows.

Financial values use Decimal, no floats. JSON evidence preserves the original
eight-place source monetary strings. Thresholds are exactly NUMERIC(38,8); counts
are nonnegative BIGINT integers. Comparison arithmetic uses precision 80.

## 8. Provenance semantics

Each evaluation references a persisted `source_snapshots` record containing the
existing M4 metadata, rather than inventing a second analytical version:

```text
source_database: order_intelligence
source_schema: gold_serving
source_topic: order-events-m3-20261003b
gold_version: 3
gold_contract_version: 1.0
serving_status: SERVED
served_at: 2026-10-04T06:55:08.813378+00:00
revenue_rows: 18
product_rows: 13
manifest_path: s3a://gold/order-events-m3-20261003b/_commits/00000000000000000003.json
manifest_digest: 999d54805e0eef990514cfaa34101a88bcdee239e73c584be2424db6e7d5e424
snapshot_identity: ee26b6e28a8a3d5a8f1b52970af4dcfed5accac0b530c675818a1016d533420f
snapshot_digest: 26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231
```

The M5 PostgreSQL reader's additional cache-contract label is copied as provenance
metadata only; Redis is not the source. Alert evaluations separately persist DB
evaluation time, exact current/prior inputs, comparison boundary, decision and
pre/post state. These support reproduction after analytical rows are replaced.

## 9. Alert identity

All hashes use SHA-256 of sorted-key, compact ASCII JSON without NaN:

```text
scope = hash(rule_id, currency, exact dimensions, source database/schema/topic/root)
definition_digest = hash(canonical immutable rule definition)
evaluation_id = hash(scope, definition_digest, full provenance, window_start/end)
event_id = hash(evaluation_id, event_type=FIRED)
```

No random UUID is the sole deduplication key. SQL also enforces unique
`(rule_id,gold_version)` evaluations and one firing event per evaluation. An alert
schema belongs to one source. Separate isolated result schemas may contain the
same business IDs; counts below are per schema, not combined global emissions.

Default persisted firing IDs:

- INR low net: `01de64a0c0cc91d90f15b6c3b561a9fd078ea010fcab679575cf0797081c691d`
- INR product: `803b29a5dfa9cdea43dc3d115d04393f612b2862c2d6c20298e006ca2ade8af5`

Independent SQL verification recomputed definition, scope, evaluation and event
hashes without using the production hash helper.

## 10. Cooldown semantics

| Question | Implemented answer |
|---|---|
| Scope | Rule ID + exact dimensions/currency + authoritative source; window/version excluded |
| Duration | Required integer 1–604800 seconds per definition |
| Clock | PostgreSQL clock_timestamp after locking/source validation; one value per invocation |
| Identical evaluation | Always NOOP, including after expiry; no duplicate record/event |
| New source version | Distinct evaluation, but does not bypass cooldown |
| Different analytical window | Distinct evaluation, but same rule/dimension cooldown still applies |
| Repeated true condition | New evaluation within deadline is SUPPRESSED; last_fired_at unchanged |
| Exact expiry | A new qualifying evaluation can fire at or after last_fired_at + duration |
| Recovered condition | Changes observed state to OK, records RECOVERED; does not erase last-fire deadline |
| Missing/no baseline | UNKNOWN, not recovered; preserves last firing |
| Restart | Reads persisted state/evaluations, retains deadline |
| Database outage | Explicit failure; no in-memory cooldown or fabricated event |
| Clock regression | Reject new transitions when DB clock precedes last evaluated time |

This is event-driven snapshot evaluation, not a periodic reminder service: an
unchanged source evaluation does not fire again just because time passed. Expiry
allows a firing only for the next distinct qualifying evaluation. A caller choosing
a new rule ID intentionally creates an independent policy; definitions cannot be
silently changed under an existing ID.

## 11. State machine

Persisted states are **UNKNOWN**, **OK**, **ACTIVE**. Cooldown is a persisted
timestamp decision, not a misleading second condition state.

| Condition | Outcome | Resulting state | Firing event |
|---|---|---|---|
| Qualifies; no active deadline | FIRED | ACTIVE | yes |
| Qualifies; deadline active | SUPPRESSED | ACTIVE | no |
| False after ACTIVE | RECOVERED | OK | no |
| False otherwise | OK | OK | no |
| No matching row | NO_DATA | UNKNOWN | no |
| Missing/nonpositive adjacent baseline | NO_BASELINE | UNKNOWN | no |
| Definition disabled | DISABLED | UNKNOWN | no |
| Identical completed evaluation | invocation NOOP | unchanged | no |

UNKNOWN→OK records OK, not an unobserved recovery. State includes last-evaluation
ID/time and last-firing time; it represents an observation of a particular source,
not proof of current live business health. Rule omission does not delete history
or invent recovery. Restart requires no in-memory reconstruction of cooldown.

## 12. Persistence and transactions

Additive default schema: **alerts_serving**. Legacy `alerts.*` and analytical
`gold_serving.*` are not altered by M7.

| Table | Role / principal constraints |
|---|---|
| alert_definitions | Immutable rule JSON/digest; rule_id PK |
| source_snapshots | Full provenance and monotonic source history; Gold version PK |
| alert_evaluations | Immutable input/decision/transition history; deterministic PK, UNIQUE(rule_id,version), definition/source FKs |
| alert_state | Latest observation and last_fired_at; scope PK, unique rule ID, last evaluation FK |
| alert_events | Actual firing only; deterministic PK, unique evaluation FK, severity/time |

SQL template: `alerts/persistence.sql`; schema substitution accepts only validated
quoted identifiers. Schema initialization has its own idempotent transaction and
advisory lock. Alert business writes occur together in one later transaction.
Both initialization and evaluation set a ten-second lock timeout.

Evaluation obtains `oip.m7.<target-schema>` exclusive transaction lock, then the
shared M4 source lock. The separate source read is read-only. Definition/source
checks precede decision persistence. Existing state is locked FOR UPDATE. Definition,
source snapshot, evaluation, firing and state updates commit together; injected
failures after a rule or before commit roll back all business records.

Unique constraints plus serialization prevent concurrent duplicate transitions.
Two genuinely concurrent evaluator invocations were tested: one EVALUATED and one
NOOP, one persisted firing. Another test held M7 before commit and proved that
M4's exclusive source advisory lock could not be acquired during the interval.

Runtime SQL inspection found 5 primary keys, 5 foreign keys and 3 unique
constraints in `alerts_serving`, plus check constraints and lookup indexes.
`read_results(engine, alert_schema, limit)` is a bounded read-only backend API
joining events to definitions/evidence/provenance and returning state. It is not
a notification API or dashboard integration.

## 13. Failure semantics

| Failure/condition | Behavior proved |
|---|---|
| PostgreSQL available | Evaluation/persistence succeeds |
| PostgreSQL unavailable | Sanitized explicit error; no successful alert transaction claimed |
| Empty valid source | NO_DATA/UNKNOWN, zero new firings, source provenance retained |
| Missing/invalid serving_state | Reject; no new state/event/history |
| Version without successful history | Reject as uncommitted/inconsistent serving source |
| Stale source | Reject against successful source history / M7 high-water mark |
| Same-version provenance change | Reject |
| Wrong counts/money/digest | Reused source validation rejects |
| Repeated evaluation | NOOP, no durable record growth |
| Process restart | Persisted results/deadlines retained |
| Cooldown active | SUPPRESSED without event or deadline extension |
| Cooldown expired + distinct qualifying evaluation | FIRED again |
| Redis unavailable | Alert evaluation still succeeds from PostgreSQL |
| Crash/exception before commit | Transaction rollback; retry safely evaluates |
| Connection lost after commit | Client outcome can be ambiguous; retry resolves by deterministic identity |

“Uncommitted” here means not successfully served in authoritative PostgreSQL;
M7 never attempts to validate or load uncommitted Gold files. Source age alone is
not a failure: this contract intentionally evaluates the latest stored snapshot
and exposes its timestamp, without claiming that retained test data is real time.

## 14. Runtime verification and evidence table

### Exact runtime facts

```text
PostgreSQL:                16.14
Serving schema:           gold_serving
Serving version:          3
Source topic:             order-events-m3-20261003b
Manifest:                 s3a://gold/order-events-m3-20261003b/_commits/00000000000000000003.json
Revenue rows:             18 (17 INR, 1 USD)
Product rows:             13 (12 INR, 1 USD)
Currencies:               INR, USD
Rules evaluated:          4
Alerts fired:             2
Alerts suppressed:        0 on initial retained-snapshot run
Other results:            1 OK, 1 NO_BASELINE
Alerts persisted:         2 events / 4 evaluations / 4 states in alerts_serving
Repeat evaluation result: NOOP × 4, no additional events
PostgreSQL failure:       explicit failure; persisted history identical after recovery
Restart result:           separate process NOOP; state/history retained
```

Initial isolated target `m7_verify_20261004a` evaluated at
`2026-10-04T12:28:31.269024+00:00`. Default CLI target `alerts_serving` evaluated at
`2026-10-04T12:41:41.687942+00:00`; separate-process retry at
`2026-10-04T12:41:42.518969+00:00` returned four NOOPs. Source `served_at` remained
`2026-10-04T06:55:08.813378+00:00` and source digest stayed unchanged.

| Example rule | Exact selected input | Result |
|---|---|---|
| inr-net-low | north-india, INR, 2026-10-03 16:30–16:31 UTC; net 0 < 10 | FIRED / WARNING |
| usd-net-low | north-india, USD, 2026-10-03 16:10–16:11 UTC; net 20 < 10 is false | OK |
| inr-net-drop | latest INR minute 16:30 has no adjacent 16:29 minute | NO_BASELINE; no invented baseline |
| inr-product-low | prod-3017 / Case 17 / INR, 16:10–16:15 UTC; revenue 10 <= 10 | FIRED / INFO |

The default and isolated stores intentionally contain the same deterministic
business IDs; they are separate verification targets, not four default events.
Legacy alert tables remained at zero rows before/after. Analytical state/rows/
serving_history were compared before/after each retained-source runtime evaluation.

### Controlled runtime state/cooldown proof

Source: `m7_source_runtime_20261004a`; target: `m7_runtime_20261004a`.
This fixture is synthetic M4-shaped PostgreSQL data published through the existing
M4 row-serving test API, with local serving_history. It is not a claimed MinIO
Gold commit. Production analytical tables were never modified to force conditions.
Two explicit region rules, INR/USD, use a **5-second** cooldown here.

| Source version / attempt | Database evaluation UTC | INR result | USD result |
|---|---|---|---|
| 1: net 20 | 12:28:37.768255 | OK | OK |
| 2: INR net 5 | 12:28:37.835084 | FIRED | OK |
| 2 repeated | 12:28:37.862541 | NOOP | NOOP |
| 3: new window, INR net 4 | 12:28:37.915342 | SUPPRESSED | OK |
| 4: new window, INR net 3 | 12:28:43.242355 | FIRED after expiry | OK |
| 5: INR net 10 | 12:28:43.426571 | RECOVERED | OK |
| 6: empty committed SQL snapshot | 12:28:43.654090 | NO_DATA | NO_DATA |

All dates are 2026-10-04. The verifier waited 5.1 seconds against actual PostgreSQL
time; no production timestamp was altered. Final controlled history: **12
evaluations, 2 firings, 1 suppression, 1 recovery, 6 OK, 2 NO_DATA**, 2 state rows.
Firing IDs:

```text
9f7aa2a9274e85afefe7b9997ca4aa5e73bf452a16d9ec600d18a7f6e5fe85d2
442adfa0b9d0f2121c7e553d64953b1a5c865fa135da022caab7236fd2cc9449
```

An independent SQL verifier recomputed conditions, identity hashes, currency
dimensions and cooldown inequalities from persisted definitions/evidence, and
checked event presence/absence. It does not trust only evaluator return counts.

### Commands and retained artifacts

Workspace root commands (all capture-wrapper commands below exited **0**):

```bash
python3 -m scripts.capture_milestone7_check --step tests
python3 -m scripts.capture_milestone7_check --step runtime
python3 -m scripts.capture_milestone7_check --step full-tests
python3 -m scripts.capture_milestone7_check --step checks
python3 -m scripts.capture_milestone7_check --step cli
```

Capture records exact Docker/Python commands, UTC start/end times and return codes
in JSON, with paired stdout/stderr logs. Credentials remain in the retained
container environment. Runtime orchestration stops/starts only the OIP Redis and
PostgreSQL containers and restores them in `finally`, awaiting healthchecks.

Artifacts under `milestone7_evidence/20261004a/` (each stem has JSON/log):

| Requirement | Evidence | Result |
|---|---|---|
| Authoritative PostgreSQL source | `production-1791116911478976737`: source gold_serving, before/after SQL equality | PASS |
| Serving version captured | Same artifact: version 3 plus full manifest/digest/served_at | PASS |
| Currency isolation | Retained INR/USD decisions; full-suite two-qualifying-currencies test | PASS |
| Alert thresholds | Independent SQL oracle on production + controlled fixture | PASS |
| Deterministic identity | `default-persistence-1791117703441741810`: independent hash reconstruction | PASS |
| Cooldown | `controlled-1791116923893176892`: version 3 SUPPRESSED within five-second deadline | PASS |
| Cooldown expiry | Same artifact: version 4 FIRED at 12:28:43.242355 | PASS |
| Duplicate evaluation | `restart-1791116915987562199` and `default-cli-retry-1791117702606714211` | PASS |
| Persistence | Default SQL read: 4 evaluations, 2 events, 4 states | PASS |
| Restart recovery | `restart-1791116965268324566`: separate process, unchanged persisted records | PASS |
| PostgreSQL outage | `postgres-down-1791116945595817339`: explicit failure; recovery history equals baseline | PASS |
| Redis independence | `redis-down-1791116926892956576`: evaluator succeeds with Redis stopped | PASS |
| Invalid/uncommitted/stale source | Final full suite: six real-PG corruption/restore fixtures reject safely | PASS |
| Recovery / empty source | Controlled versions 5/6, persisted RECOVERED / NO_DATA | PASS |
| Atomic rollback | Final full suite: after-rule / before-commit injection rolls back all business records | PASS |
| Concurrent duplicate protection | Full suite: concurrent workers produce one event, one NOOP | PASS |
| Runtime evaluation / CLI | `default-cli-1791117701802252202` and `default-persistence-1791117703441741810` | PASS |
| M1 regression | Section 16, actual producer/registry/Bronze fixture | PASS |
| M2 regression | Section 16, initial/restart/recovery | PASS |
| M3 regression | Section 16, retained replay / unchanged Gold | PASS |
| M4 regression | Section 16, fresh serving/rollback/stale/corrupt phases | PASS |
| M5 regression | Section 16, serving/retry/rebuild/actual outages | PASS |
| M6 regression | Section 16, production UI/equality and all failure paths | PASS |

PostgreSQL outage verification ran from `2026-10-04T12:28:57.040448+00:00` to
`2026-10-04T12:29:05.595759+00:00`. The wrapper's exit 0 means its assertion that
the evaluator failed safely passed; it does not mean an alert was generated while
PostgreSQL was down. Recovery artifacts `production-1791116957587538983` and
`restart-1791116965268324566` confirmed unchanged history.

Default entrypoint actually executed inside the container:

```bash
python3 /opt/oip/main.py alerts --rules /opt/oip/alerts/rules.example.json
python3 -m alerts.evaluator --rules /opt/oip/alerts/rules.example.json
```

Both returned 0. SQL queries confirmed default persisted events, current state,
unchanged Gold version/digest/counts, and zero legacy alert-table rows.

## 15. Test results and code-quality checks

Final full suite: **137 passed, 1 warning in 105.15 seconds**. No tests were deleted,
skipped or assertions weakened. Breakdown: 26 M2/M3, 10 M4, 24 M5, 37 M6, **40 M7**.
The earlier focused capture had 39 M7 cases; an additional simultaneous-INR/USD
qualification/full-negative-precision test was added and passed in the final suite.

Final artifact: `full-tests-1791118044279974494.{json,log}`.
Actual wrapper time: `2026-10-04T12:45:34.143526+00:00` through
`2026-10-04T12:47:24.279765+00:00`. Command inside the container:

```bash
/opt/spark/bin/spark-submit /opt/oip/scripts/run_spark_tests.py tests/unit
```

M7 tests cover rule boundaries lt/lte/gt/gte, exact money/negative range, adjacent
drop conditions, missing/nonpositive baseline, product identity, currency scope,
deduplication, cooldown at exact expiry, recovery without cooldown reset,
persistence/process restart, provenance, malformed/stale/uncommitted source,
empty/disabled results, actual SQL availability errors, rollback, concurrent
evaluators, M4-lock exclusion, invalid configuration, immutable definitions,
wrong-topic rejection and backwards clock behavior. Generated test schemas alone
are dropped in teardown; historical data is not reset.

Checks actually run:

```bash
".venv/Scripts/python.exe" -m ruff check alerts/definitions.py alerts/evaluator.py scripts/milestone7_cases.py scripts/verify_milestone7.py scripts/capture_milestone7_check.py tests/unit/test_alerts.py main.py
".venv/Scripts/python.exe" -m compileall -q alerts/definitions.py alerts/evaluator.py scripts/milestone7_cases.py scripts/verify_milestone7.py scripts/capture_milestone7_check.py tests/unit/test_alerts.py main.py
".venv/Scripts/python.exe" -m pip check
docker.exe compose config --quiet
docker.exe exec oip-modernization-verify python3 -m pip check
```

Ruff, compile/import, host pip check and Compose validation passed (exit 0).
Initial lint identified one unused import; it was removed and checks rerun.
Container pip check exits 1 for the previously documented unrelated
`pygobject 3.42.1 requires pycairo, which is not installed`. No M7 dependency
installation occurred. The full suite's existing Authlib/httpx deprecation warning
remains. M7 imports do not load `alerts.alert_router`; real Redis service shutdown
and a test that forbids Redis commands demonstrate no Redis correctness dependency.

## 16. M1–M6 regression results

All commands below actually ran and returned **0**. Existing frozen verifiers were
used without edits. The source digest remained
`26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231`.

### M1

```bash
python3 -m scripts.capture_milestone3_check --step m1 --topic order-events-m1-m7-20261004a
```

PASS: four generated/published Avro records, registry/wire/Bronze verification.
Artifact: `milestone3_evidence/order-events-m1-m7-20261004a/m1-1791117284612897512.json`.

### M2

```bash
python3 -m scripts.capture_milestone3_check --step m2-initial --topic order-events-m2-m7-20261004a
python3 -m scripts.capture_milestone3_check --step m2-restart --topic order-events-m2-m7-20261004a
python3 -m scripts.capture_milestone3_check --step m2-recovery --topic order-events-m2-m7-20261004a
```

PASS: initial 32 Bronze / 11 Silver / 19 DLQ / 32 audit; restart 35 / 12 / 19 / 70;
recovery 1 Silver / 1 DLQ. Evidence under
`milestone3_evidence/order-events-m2-m7-20261004a/`:
`m2-initial-1791117344124162667`, `m2-restart-1791117393338986778`,
`m2-recovery-1791117421813285787`.

### M3

```bash
python3 -m scripts.capture_milestone3_check --step replay --topic order-events-m3-20261003b
```

PASS: 35 Bronze, 2 DLQ, 33 unique Gold events, 18 revenue / 13 product rows,
version 3 retained. Physical Silver rows rose to 4224 only because this verifier
deliberately appends duplicate files; Gold logical deduplication remained correct.
M3's separately normalized oracle digest stayed
`8263508ee79077227be47a2a550111b4b65e45daf01e28bed149eae5c0e57579`.
Artifact: `milestone3_evidence/order-events-m3-20261003b/replay-1791117480373922754.json`.

### M4

```bash
python3 -m scripts.capture_milestone4_check --phase initial --topic order-events-m3-20261003b --schema gold_m4_m7reg20261004
python3 -m scripts.capture_milestone4_check --phase retry --topic order-events-m3-20261003b --schema gold_m4_m7reg20261004
python3 -m scripts.capture_milestone4_check --phase failure --topic order-events-m3-20261003b --schema gold_m4_m7reg20261004
python3 -m scripts.capture_milestone4_check --phase old --topic order-events-m3-20261003b --schema gold_m4_m7reg20261004
python3 -m scripts.capture_milestone4_check --phase corrupt --topic order-events-m3-20261003b --schema gold_m4_m7reg20261004
```

PASS: fresh isolated version 1→3, retry NOOP, rollback, stale rejection,
corrupt/uncommitted-source rejection, independent equality. Evidence under
`milestone4_evidence/order-events-m3-20261003b/`:
`initial-1791117498902075927`, `retry-1791117513326000694`,
`failure-1791117525579281751`, `old-1791117536805055374`,
`corrupt-1791117547979494638`.

### M5

```bash
python3 -m scripts.capture_milestone5_check --step serve --schema gold_serving --namespace oip:m5:verify-m7-20261004a
python3 -m scripts.capture_milestone5_check --step serve --schema gold_serving --namespace oip:m5:verify-m7-20261004a
python3 -m scripts.capture_milestone5_check --step rebuild --schema gold_serving --namespace oip:m5:verify-m7-20261004a
python3 -m scripts.capture_milestone5_check --step outages --schema gold_serving --namespace oip:m5:verify-m7-20261004a
```

PASS: 18/13 equality, one hash/32 fields, exact retry, cache-loss rebuild,
actual Redis/SQL outages and recovery; PostgreSQL unchanged.
Evidence: `milestone5_evidence/oip-m5-verify-m7-20261004a/`, including
`serve-1791117593702363024`, `serve-1791117594551797613`,
`rebuild-1791117595445067584`, `redis-down-1791117604908819397`,
`postgres-down-1791117620493800320`, `serve-1791117627534106520`.

### M6

```bash
python3 -m scripts.capture_milestone6_check --step production --run 20261004m7
python3 -m scripts.capture_milestone6_check --step runtime --run 20261004m7
```

PASS: actual production namespace `oip:m5:gold-serving`, SQL/Redis/dashboard/rendered
table equality and Streamlit HTTP startup; isolated cache missing/corrupt/rebuild,
Redis down, SQL down with cached display, both down explicit error, and recovery.
No dashboard implementation changed.

Evidence: `milestone6_evidence/20261004m7/`, including
`production-1791117630815384659`, `missing-1791117635702969936`,
`corrupt-1791117640228105775`, `redis-down-1791117655998667686`,
`postgres-down-1791117667084024161`, `both-down-1791117691563078820`,
`healthy-1791117700441950992`.

## 17. Dependency review

No dependencies were added, upgraded or installed for M7. `requirements.txt` is
unchanged. Existing SQLAlchemy/psycopg2 and standard-library Decimal/JSON/hash tools
are sufficient.

| Candidate / verified component | Version | Classification |
|---|---|---|
| SQLAlchemy | 2.0.36 | KEEP |
| psycopg2-binary | 2.9.10 | KEEP |
| PostgreSQL | 16.14 | KEEP |
| redis-py / Redis | 5.2.0 / retained 7.4.9 | NO CHANGE; no M7 Redis operations |
| Streamlit / Pandas / Plotly | 1.40.1 / 2.2.3 / 5.24.1 | NO CHANGE |
| Spark, Kafka/Avro connectors | 3.5.9 | NO CHANGE |
| Hadoop / AWS SDK | 3.3.4 / 1.12.262 | NO CHANGE |
| fastavro / confluent-kafka | 1.12.2 / 2.15.1 | NO CHANGE |
| Java / Scala | 17 / 2.12 | NO CHANGE |
| pytest | 8.3.3 | KEEP |
| Notification/ML/metrics libraries | No M7 candidate required | NO CHANGE |

No UPDATE NOW or UPDATE LATER was justified by this milestone. Runtime Python
remains 3.10.12 in the retained verification container. Installed versions and
M7 import success were recorded under `imports-versions-1791118045216798158`;
additional direct inspection confirmed the notification router is not imported.
The pre-existing container dependency issue is described in section 15 with its
actual failing pip-check result rather than hidden.

## 18. Final file-change inventory

### NEW

| File | Reason |
|---|---|
| `alerts/definitions.py` | Explicit validated rule model, exact comparisons and deterministic identity utilities |
| `alerts/evaluator.py` | Authoritative transactional evaluation, persisted cooldown, backend results and one-shot CLI |
| `alerts/persistence.sql` | Additive M7 schema with uniqueness/FK/check constraints and indexes |
| `alerts/rules.example.json` | Explicit per-currency controlled example rules, no implicit thresholds |
| `alerts/CONTRACT.md` | Complete source/rule/state/cooldown/provenance/failure contract |
| `scripts/milestone7_cases.py` | Deterministic isolated PostgreSQL source fixtures |
| `scripts/verify_milestone7.py` | Real-service and independent persisted-decision oracle |
| `scripts/capture_milestone7_check.py` | Append-only commands/logs/evidence and safe service-outage orchestration |
| `tests/unit/test_alerts.py` | Forty business-contract/real-PG tests |
| `milestone7_alerts_report.md` | This evidence-backed report/certification |
| `milestone7_evidence/20261004a/*` | Actual M7 command/results/logs |
| Additional M3/M4/M5/M6 evidence files listed above | Actual frozen-milestone regressions |

### MODIFIED

| File | Reason |
|---|---|
| `main.py` | Route the existing broken alerts selector to M7 one-shot evaluator; update its usage line |
| `.env.example` | Document M7 rule-file/output-schema/topic configuration and distinguish legacy settings |
| `README.md` | Scoped M7 invocation and contract/report links |
| `PHASE_STATUS.md` | Record scoped M7 acceptance boundary |

### DELETED

None.

### UNCHANGED — inspected relevant files

`alerts/rule_engine.py`, `alerts/statistical_detector.py`, `alerts/alert_router.py`;
`storage/redis_client.py`, `storage/postgres_redis_serving.py`,
`storage/gold_postgres_serving.py`, `storage/POSTGRES_SERVING_CONTRACT.md`;
`dashboard/read_service.py`; `scripts/postgres/init.sql` and M4 schema DDL;
`utils/config.py`, `docker-compose.yml`, `requirements.txt`; existing M1–M6
verification/test entrypoints used for regression. Alert references in streaming
and tests were inspected by search; no frozen analytical implementation was edited.
Active credentials/environment were not rewritten. No Git commits were made;
the workspace is not a Git repository.

## 19. Known limitations and operational risks

- No notification delivery, external channels, webhook dispatch or delivery retry
  system. Existing legacy providers are not imported by M7.
- No Prometheus/Grafana implementation. Structured results expose decision counts,
  failures by exception/nonzero exit and invocation duration for later consumption.
- No scheduler, daemon, distributed scheduling or high-scale distributed evaluator.
- One result schema serializes invocations; source shared locking can briefly delay
  M4 publication. Two available SQL connections per invocation are required by the
  reused read API; caller-owned engines must have sufficient capacity.
- Bounded source reads follow the existing M5 row limit/in-memory scope. Up to 1000
  explicit rules; no unrelated performance optimization or throughput claim.
- Latest-stored-window rules are not historical backfill or freshness/SLA detectors.
  Retained old data can produce a version-labeled alert; no claim it is live now.
- No ML anomaly detection, baseline training, FX or unsupported business metrics.
- Rule IDs are immutable; new IDs intentionally create independent cooldowns.
  Omitted/disabled rule state is last-observed/unknown, not automatic recovery.
- Connection loss after commit is ambiguous until deterministic retry. Arbitrary
  external writes bypassing M4 advisory locking remain outside its writer contract.
- No retention/purge policy for alert history, authentication redesign, Kubernetes,
  cloud/production deployment or CI/CD implementation.

Secrets come from environment. SQL data values are bound parameters; identifiers
and rule fields are validated. No credential-bearing DSNs are logged, no shell
commands are built from rule text, and no global Redis flushing or volume resets
were used. Evidence search found no password/credential-bearing URL values in M7
JSON/log artifacts. Synthetic fixtures and verification schemas are clearly named;
only disposable unit fixture schemas are removed in teardown.

## 20. Exact M7 status / final certification

**PASS. M7 COMPLETE.** Implementation, actual PostgreSQL persistence, deterministic
identity, currency/Decimal rules, provenance, cooldown/expiry/recovery, restart,
failure behavior, concurrent duplicate protection and M1–M6 regressions are proven
by the evidence above. No unresolved M7 blocker remains. The full test suite has
137 passing cases; runtime and default CLI checks completed with actual services.

M1–M6 remain frozen. The only shared entrypoint edit repairs the alert selector;
it does not change their contracts. No notification or observability feature was
added. M7 is frozen after verification and this report.

### Verification-environment shutdown

Final cleanup completed at `2026-10-04T13:01:14.255782+00:00` with exit 0:

```bash
docker.exe stop oip-modernization-verify oip-schema-registry oip-kafka oip-minio oip-redis oip-postgres
```

Subsequent targeted Docker inspection confirmed all six containers `exited`.
PostgreSQL, Redis and MinIO exited 0; Kafka and Schema Registry exited 143;
the verification container exited 137 during shutdown. These are container-stop
statuses, distinct from the successful test and verifier command return codes.
Containers, volumes, analytical snapshots, persisted alerts, cache and evidence
were retained. A final running-container listing contained only unrelated services.

Shutdown evidence, under the same M7 evidence directory:
`freeze-stop-1791118874255882283.{json,log}` and
`freeze-status-1791118874460537659.{json,log}`.

## 21. Next milestone

**M8 — Notifications.** Not implemented or started by M7.
