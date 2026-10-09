# Milestone 8 — Notifications

## 1. Executive result

**PASS — M8 COMPLETE — 2026-10-04.** Persisted M7 firing events now feed a bounded,
restart-safe notification engine with deterministic requests, durable attempt
history, fenced claims, bounded retries and explicit permanent/ambiguous outcomes.
Actual PostgreSQL and real loopback HTTP were used for verification.

**External provider delivery not certified in this environment.** The sole concrete
provider is `controlled_http`, a clearly identified CONTROLLED/TEST transport. Its
durable receiver acknowledgements prove local acceptance, not Slack/email delivery
or a human reading a message. PASS uses the explicitly permitted controlled-provider
certification boundary.

Final suite: **179 passed, 1 existing warning**, including **42 M8 tests** and all
137 frozen tests. M1–M7 real-service regressions passed. All five retained M7 tables
match pre-M8 hashes/counts; 64 frozen source/contract/test files match their baseline.

The default path consumed two retained M7 events, created two requests and persisted
two deliveries. Restart/repeat created zero requests and made zero provider calls.
Independent controlled scenarios proved retry, exhaustion, permanent rejection,
provider outage/recovery, receiver restart, process death after acceptance, stale
claim recovery and concurrent-worker deduplication.

## 2. Current-world inspection

Inspection preceded implementation. Read `alerts/`, the active M7 evaluator,
SQL and contract, legacy router/consumers, main dispatcher, storage APIs, SQL init,
configuration, requirements, Compose, tests/verifiers, README and phase status.
Repository searches covered notification/notifier/notify, provider/channel names,
webhooks/email/SMTP, retry/backoff, delivery/status/timestamps and `alert_events`.
No separate notification package or service existed.

Findings were documented before implementation in `notifications/CONTRACT.md` and
the working conversation. The evidence baseline is
`milestone8_evidence/20261004a/inspect-1791121851463825006.json`, captured at
2026-10-04 13:50:49–13:50:51 UTC.

| Question | Inspected finding |
|---|---|
| Existing notification code | `alerts/alert_router.py` |
| Existing providers | Slack incoming webhook and PagerDuty Events API |
| Active callers | Legacy `rule_engine.py` / `statistical_detector.py`; not M7 |
| Real delivery currently enabled | No usable configured provider found |
| Credentials | Active dotenv/process and container inspection showed empty Slack/PagerDuty credentials; no SMTP/Telegram configuration; only presence flags printed |
| Persistent delivery state | No reliable request/attempt/result model; legacy log timestamp is not delivery evidence |
| Retry | Three in-memory Tenacity attempts; no durable retry schedule/restart continuation |
| Deduplication | PagerDuty type/region key, legacy Redis cooldown; not M7-event identity |
| Evaluation/delivery coupling | Legacy engines invoke router directly |
| Safe reuse | Existing pooled PostgreSQL engine; installed httpx; frozen M7 persisted event contract |
| Required isolation | Legacy provider calls and credential-bearing exception logging remain outside active M8 execution |
| Frozen files | M7 evaluator, definitions, SQL, contract, fixtures, tests and verifiers unchanged |

No paid service or external account was provisioned. No credential was invented.

## 3. Existing notification implementation

Legacy `_send_slack()` silently skips an empty webhook, otherwise posts through
httpx; `_send_pagerduty()` behaves similarly. The router catches/logs failures.
It does not record attempts or coordinate notification acceptance with PostgreSQL.
Legacy payloads take floats and hard-code INR, and legacy `alerts.alert_log`
defaults `notified_at` even when no provider send happened.

Those historical files are retained. The active M7→M8 path does not import them.
M8 has an independent `notifications` package and dispatcher selector; M7 continues
to generate firing records without notification dependencies.

## 4. Architecture implemented

```text
alerts_serving.alert_events (immutable M7 firings)
    + alert_evaluations / alert_definitions / source_snapshots
        ↓ bounded source-event mapping
notification_deliveries (deterministic PENDING request committed)
        ↓ short PostgreSQL claim transaction
notification_attempts + SENDING / lease / fencing token
        ↓ transaction/connection released
Provider.send(payload) — one CONTROLLED HTTP provider
        ↓ short fenced result transaction
DELIVERED / RETRYABLE_FAILURE / PERMANENT_FAILURE + attempt history
```

Default target: `notifications_serving`. Isolated targets: `m8_*`.
Three additive tables:

| Table | Purpose |
|---|---|
| source_binding | Bind one M8 schema to one database/M7 schema |
| notification_deliveries | Immutable request/configuration/payload plus current delivery state |
| notification_attempts | Numbered claim/request/result history |

Foreign key to the actual M7 event, deterministic primary key, logical-scope unique
constraint, attempt-number primary key, unique fencing token, state/check constraints
and due/lease indexes enforce the persistence boundary. The FK prevents an orphaned
M8 reference; no M7 rows or DDL files are rewritten.

Commands with process environment loaded:

```bash
python main.py notifications
python -m notifications.service --mode generate
python -m notifications.service --mode deliver
```

Default batch size 100; valid range 1–1000. One invocation generates at most that
many requests and handles at most that many claim/recovery actions. It returns
structured counts, per-attempt outcomes, provider latency and processing duration.

## 5. M7 → M8 contract / payload

CLI input is fixed to `alerts_serving.alert_events`. The Python verification API
allows explicitly named `m7_*` fixture sources. No Kafka, Gold, Redis, dashboard or
analytical-table input exists in M8. No M7 rule is evaluated or imported by M8.

M8 joins persisted events to their evaluation/definition/provenance, validates
structural consistency and maps the already-fired event. Missing requested IDs or
invalid evidence fail generation atomically. An empty valid event set creates zero
work. A bounded anti-join selects unmaterialized events first so batch processing
progresses beyond already-delivered rows.

The provider-neutral payload contains notification/event/evaluation/rule IDs,
severity, title, concise message, currency/dimensions, selected metric/value,
threshold/unit, stored comparison boundary, event/evaluation/window times and
source version/digest/manifest reference. It copies exact evidence; no threshold,
revenue or net recomputation occurs.

Actual default messages:

```text
INR net_revenue: INR 0E-8 < INR 10.00000000
INR revenue: INR 10.00000000 <= INR 10.00000000
```

`0E-8` is the exact decimal text persisted by M7. Currency is explicit; binary
floating point is never used for money. Drop thresholds are labeled ratios and
their stored comparison boundary carries currency. Unit metrics are labeled units.
Tests additionally deliver independent INR/USD events and preserve full negative
NUMERIC(38,8) text. Financial semantics remain M7's responsibility.

## 6. Notification identity

Exact formula:

```text
SHA256(canonical_json({
  "contract_version": "1.0",
  "source_database": <database>,
  "source_schema": <M7 schema>,
  "alert_event_id": <M7 event ID>,
  "provider": <provider>,
  "channel": <channel>,
  "destination": <logical destination alias>
}))
```

Canonical JSON: sorted keys, compact separators, ASCII escaping, no NaN.
Destination aliases are explicit configuration, not private addresses. Different
destinations create different requests. Endpoint fingerprint and retry policy are
persisted and checked so an existing route cannot silently change configuration.
Claim tokens are random fencing identifiers, not notification identity.

Actual default IDs:

| M7 event / rule | Notification ID |
|---|---|
| `01de64a0c0cc91d90f15b6c3b561a9fd078ea010fcab679575cf0797081c691d` / inr-net-low | `f58502ba9835ffb7929b14ef7b67e24a5cbf8dcad4bc7c39855d2aaf6ba2f6d7` |
| `803b29a5dfa9cdea43dc3d115d04393f612b2862c2d6c20298e006ca2ade8af5` / inr-product-low | `450f3104095da5f690c37ed5d74938749d057980d349224e287eb087af477b94` |

The SQL verifier recomputes identities independently of the production helper.

## 7. Provider abstraction and selected provider

`Provider` defines route metadata and `send(payload) -> DeliveryResult`.
`DeliveryResult` carries outcome, optional provider status/reference and safe error
class. Provider-specific response objects do not enter the engine.

The only concrete implementation is `ControlledHTTPProvider` (`controlled_http`,
channel `http`). It uses existing httpx against a configured loopback `/notify`
endpoint, with redirects/proxy environment disabled and no hidden HTTP retries.
There is no M8 Slack/PagerDuty/email implementation or multi-provider routing.

`scripts/milestone8_receiver.py` is the controlled transport's test receiver. It
stores requests/receipts in SQLite, serializes acceptance, rejects conflicting
payloads for the same key, and commits before returning a receipt. Response plans
support success, transient/permanent HTTP errors, malformed acknowledgement,
timeout and accepted-but-response-lost cases. This exercises actual HTTP, not
mocked success flags.

`Idempotency-Key` is the notification ID. Success requires HTTP 200 and a matching
durable-acceptance receipt/reference. Repetition returns the same receipt. This
provides one logical acceptance while the receiver ledger is retained; it is not
an exactly-once external-delivery claim.

## 8. Delivery state machine

| From / event | To | Persistence |
|---|---|---|
| Valid new M7 firing | PENDING | Immutable request committed |
| Due work claimed | SENDING | Increment attempt, record token/deadline, commit |
| Valid acceptance acknowledgement | DELIVERED | Reference, delivered_at, completed attempt |
| Retryable result, budget remains | RETRYABLE_FAILURE | Safe failure and next_attempt_at |
| Permanent rejection | PERMANENT_FAILURE | failed_at/reason, no schedule |
| Retry budget exhausted | PERMANENT_FAILURE | Explicit attempts_exhausted reason |
| Expired in-flight claim | RETRYABLE_FAILURE or PERMANENT_FAILURE | Old attempt UNKNOWN; retry or exhausted-outcome-unknown |
| Terminal record processed again | Unchanged | No provider call |

UNKNOWN belongs to attempt history, not a sixth delivery state. It honestly
records an interrupted call whose provider outcome was not committed. On exhausted
ambiguous outcomes, the terminal reason retains that uncertainty rather than
asserting that external acceptance did not happen.

## 9. Retry semantics

Defaults: max attempts **3**, base delay **2 seconds**, cap **30 seconds**, HTTP
phase timeout **2 seconds**, lease **30 seconds**. Controlled failure verification
uses 1-second base/2-second cap/1-second lease/0.1-second timeout, with actual DB
time and real waiting. No database timestamps are rewritten to force deadlines.

After attempt n: `min(max_delay, base_delay * 2**(n-1))` seconds from PostgreSQL
result/recovery time. Due work is claimed by a subsequent bounded processing call;
there is no infinite retry or background scheduler. Every committed claim consumes
an attempt, including interruption before the HTTP call.

| Provider result | Classification |
|---|---|
| HTTP 200 + matching acceptance receipt | Success |
| HTTP 408, 425, 429, 5xx | Retryable |
| Network/timeout | Retryable, externally ambiguous |
| Invalid success receipt | Retryable, externally ambiguous |
| Other HTTP statuses, including redirects/400/401/403/404/409 | Permanent |
| Unexpected provider exception | Sanitized retryable provider_exception, bounded by budget |

No raw error body, credential, URL or exception string is stored. Permanent errors
do not loop. Automatic retry never changes notification identity or destination.

## 10. Failure and transaction semantics

Generation, claiming and result recording use separate short transactions.
Provider calls occur with no checked-out SQL connection; a focused test verifies
that boundary. Generation uses an M8 advisory lock, unique constraints and bound
SQL values. Source reads and request creation occur together; explicit-ID batches
are atomic. Initialization is additive and source-bound.

Failure before request/claim commit rolls back and makes no provider call.
Failure after claim leaves a leased, recoverable SENDING record. Failure after
provider acceptance but before result commit remains ambiguous until retry with
the same idempotency key. PostgreSQL outage before claim produces explicit failure
and no fabricated send. Redis is not used for delivery correctness.

Failure hooks tested after create, before generation commit, before claim commit,
after claim commit, after provider return and before result commit. A runtime child
also actually terminates with `os._exit(73)` after provider return; its nonzero exit
is expected crash-injection evidence, not a hidden test failure.

## 11. Restart semantics

Requests, attempts, counts, deadlines and tokens are PostgreSQL state. Process
memory is not authoritative. Restart after successful delivery makes no send.
Restart after claim waits for its lease, marks the previous attempt UNKNOWN, and
schedules the next bounded retry. The next send reuses the deterministic key.

Runtime `crash-inbox` first obtained a durable provider receipt and then exited 73
before SQL completion. A new process recovered the expired claim; another due
invocation received the original receipt. There were two HTTP calls but exactly
one acceptance for that notification. Receiver restart preserved its SQLite ledger.

## 12. Concurrency semantics

Due work uses `SELECT ... FOR UPDATE SKIP LOCKED`. Claim token and state fence
result updates; late results cannot overwrite a newer claim. Concurrent generation
is serialized per M8 target and constrained by deterministic/logical unique keys.

Two actual workers processed the same event/destination: **one request, one attempt,
one HTTP request, one acceptance**. Unit tests additionally verify stale-worker
fencing and bounded batches progressing to subsequent events.

Lease expiry can permit a second call while a stalled old worker is still alive.
Receiver idempotency is the external duplicate boundary; SQL fencing alone cannot
prevent all duplicate external requests. No distributed throughput claim is made.

## 13. Provider verification

**External provider delivery not certified in this environment.** Slack/PagerDuty
credentials are absent. No paid service was used, and no mock was presented as an
external provider.

Controlled verification used real loopback HTTP at port 18088 in the verification
container, logical destination aliases, and durable receipt records. Default
delivery completed at:

- INR net: `2026-10-04T14:04:43.864045+00:00`.
- INR product: `2026-10-04T14:04:43.919448+00:00`.

Provider references are `controlled-<notification_id>` and are independently
matched against stored receiver receipts, payloads and PostgreSQL attempts.
The ledger is retained at `/tmp/omnirush/m8-20261004a.sqlite` inside the retained
verification container. It survived the demonstrated receiver/process restarts.
Its filesystem must be retained across container restarts; deletion/recreation of
that container or ledger is outside the verified durability boundary.

## 14. Security and evidence hygiene

Database credentials use the existing environment/engine. Endpoint and destination
configuration are required; placeholders in `.env.example` are empty. The single
provider accepts only explicit `http://127.0.0.1:<port>/notify`, without embedded
credentials, queries, fragments or redirects. It does not execute arbitrary URLs
from M7 events. Destination aliases are validated; SQL values are parameterized and
schema identifiers constrained. Response bodies/headers and exception text are
excluded from persisted errors and normal output.

Tests verify response-body redaction, unexpected-exception redaction and invalid
CLI configuration redaction. Evidence review found that pytest's initial automatic
parameter IDs printed a **synthetic literal credential placeholder** from a negative
configuration test. No real credential was exposed. Explicit descriptive test IDs
fixed this diagnostic issue without weakening any assertion; the final full suite
was recaptured. Earlier logs are retained as historical evidence, not rewritten.
Final full-suite and runtime evidence scans found no credential-bearing values or
redaction canaries. No full security-hardening milestone was attempted.

## 15. Runtime evidence / exact summary

Primary evidence directory: `milestone8_evidence/20261004a/`.
Every capture has paired JSON/log files, exact command, UTC timestamps and exit code.

| Fact | Verified result |
|---|---|
| PostgreSQL version | 16.14 |
| M7 alert schema | alerts_serving |
| M7 alert events available | 2 |
| Default notifications created | 2 in notifications_serving |
| Provider / channel | controlled_http / http (CONTROLLED/TEST) |
| Default successful deliveries | 2 |
| Default retryable/permanent failures | 0 / 0 |
| Default retries | 0 |
| Duplicate processing | 0 new requests, 0 calls; 2 existing logical requests recognized |
| Restart | Completed work stays terminal; interrupted accepted work recovers/deduplicates |
| PostgreSQL outage | Explicit failure, 0 provider calls during outage |
| Redis outage | 2 actual deliveries succeeded using PostgreSQL |
| Concurrent processing | 1 request + 1 attempt + 1 HTTP call + 1 acceptance |
| M7 source data unchanged | YES; all five table hashes/counts equal baseline |
| Real external provider certified | **NO** |

Controlled scenario target `m8_runtime_20261004a`:

| Scenario destination | Attempts | Final state | Evidence |
|---|---:|---|---|
| retry-inbox | 2 | DELIVERED | 503 → not-yet-due NOOP → real-delay retry succeeds |
| permanent-inbox | 1 | PERMANENT_FAILURE | 400, repeat makes no call |
| exhausted-inbox | 3 | PERMANENT_FAILURE | Three 503s; attempts_exhausted |
| accepted-timeout | 2 | DELIVERED | Receipt committed before timeout; retry deduplicated |
| crash-inbox | 2 | DELIVERED | Actual process exit73; UNKNOWN attempt; stale recovery and original receipt |
| concurrent-inbox | 1 | DELIVERED | Two workers, one actual send |
| offline-inbox | 2 | DELIVERED | Receiver stopped: connection refusal; restart/retry succeeds |

Controlled totals: **7 requests, 13 attempts, 5 delivered, 2 terminal permanent
failures**. Attempt outcomes: **5 DELIVERED, 6 RETRYABLE_FAILURE, 1 PERMANENT_FAILURE,
1 UNKNOWN**. There were six attempts beyond the first attempts. Final pending /
retryable / sending delivery counts are zero in these retained scenarios.

Outage target `m8_outage_20261004a` contains two additional delivered requests,
created while Redis was stopped. Across the three retained M8 targets: **11 logical
requests, 17 attempts, 9 delivered, 2 permanent**, **9 provider receipts and 16
received HTTP requests**. One attempt failed before HTTP receipt due to a stopped
receiver. Different logical destination aliases account for the additional
notifications; there are still only two authoritative M7 source events.

Commands actually run:

```bash
python3 -m scripts.capture_milestone8_check --step inspect
python3 -m scripts.capture_milestone8_check --step tests
python3 -m scripts.capture_milestone8_check --step runtime
python3 -m scripts.capture_milestone8_check --step outages
python3 -m scripts.capture_milestone8_check --step full-tests
python3 -m scripts.capture_milestone8_check --step checks
```

| Evidence key | Artifact stem (under the M8 evidence directory) |
|---|---|
| I | inspect-1791121851463825006 |
| R | runtime-1791122698234727641 |
| RD | redis-down-1791122800216512683 |
| PD | postgres-down-1791122817328978461 |
| REC | recovery-1791122825314829271 |
| T | full-tests-1791126144855036953 |
| H | final-integrity-1791126207047546415 |

Runtime R ran `2026-10-04T14:04:42.129307+00:00` through
`2026-10-04T14:04:58.231566+00:00`. It independently queried SQL, recomputed IDs,
checked source copies, consecutive attempt numbers and actual retry-time inequalities,
and compared delivered payloads/references against durable receiver records.

### Required evidence matrix

| Requirement | Evidence | Result |
|---|---|---|
| M7 alert event consumed | R default_history joins to actual two events | PASS |
| Notification created | R default_cli, PostgreSQL rows | PASS |
| Deterministic identity | R independent SHA reconstruction; T identity test | PASS |
| Duplicate processing | R default_restart/final_default_repeat: no extra calls | PASS |
| Successful delivery | R HTTP200 + exact durable receipts + SQL result | PASS, controlled provider |
| Retryable failure | R retry_failure, accepted_timeout, provider_offline | PASS |
| Retry | R retry_not_due and retry_success, SQL timing checks | PASS |
| Permanent failure | R permanent, no second call | PASS |
| Max retry limit | R exhausted: three attempts then terminal | PASS |
| Provider recovery | R provider_offline/provider_restored with actual stopped receiver | PASS |
| Restart recovery | R exit73, stale_recovery, restart_delivery | PASS |
| PostgreSQL outage | PD explicit error, zero receiver calls; REC persisted results | PASS |
| Redis independence | RD real Redis shutdown, two deliveries | PASS |
| Concurrency | R concurrent worker results, one request/attempt/receipt; T fencing | PASS |
| Currency preservation | T actual INR/USD receipt payloads and full precision | PASS |
| Provenance | R SQL-to-payload version/digest comparisons | PASS |
| M7 immutability | I versus H; five table hashes/counts equal | PASS |
| Rollback | T generation/claim/result transaction failures | PASS |
| Configuration/error redaction | T response/exception/CLI tests, safe test IDs | PASS |
| M1 regression | Section 17 M1 actual producer/Bronze artifact | PASS |
| M2 regression | Section 17 initial/restart/recovery artifacts | PASS |
| M3 regression | Section 17 actual retained-topic replay | PASS |
| M4 regression | Section 17 five serving/validation phases | PASS |
| M5 regression | Section 17 serve/rebuild/actual outages | PASS |
| M6 regression | Section 17 real dashboard equality/fallback/recovery | PASS |
| M7 regression | Section 17 production/restart/cooldown/actual outages | PASS |
| Real external-provider delivery | No configured external provider | NOT CERTIFIED |

## 16. Test results and quality checks

Final suite: **179 passed, 1 warning in 134.51 seconds**. Capture exit 0,
2026-10-04 15:00:07.650095–15:02:24.854897 UTC. Breakdown:

| Suite | Passing tests |
|---|---:|
| M2/M3 | 26 |
| M4 | 10 |
| M5 | 24 |
| M6 | 37 |
| M7 | 40 |
| M8 | 42 |
| Total | 179 |

Earlier focused evidence: 39 passed (`tests-1791122403268712017`); after adding
no-open-SQL-during-HTTP, real provider outage/recovery and CLI redaction coverage,
42 passed (`tests-1791122728609322404`). Initial full suite also passed179;
final recapture follows the descriptive-test-ID evidence correction. No test was
deleted, skipped or assertion weakened. The existing Authlib/httpx deprecation
warning remains in the frozen Avro test.

Final quality artifacts:

| Check | Artifact / exit |
|---|---|
| Ruff | ruff-1791126007928192608 / 0 |
| compileall | compile-1791126008114457980 / 0 |
| Host pip check | host-pip-1791126008881529202 / 0 |
| Container pip check | container-pip-1791126009926548064 / **1** |
| Compose validation | compose-1791126010205562215 / 0 |
| Final source/history integrity | H / 0 |

Container pip check reports exactly the pre-existing issue:
`pygobject 3.42.1 requires pycairo, which is not installed.` This was already recorded
before M8. No package installation occurred. M8 SQL/HTTP imports, real-service
execution and all tests passed; the environment-wide pip check is not mislabeled
as passing. No application runtime/test failure remains unresolved.

## 17. M1–M7 runtime regression

Frozen verifiers were not edited. Commands below all returned 0.

### M1 / M2 / M3

```bash
python3 -m scripts.capture_milestone3_check --step m1 --topic order-events-m1-m8-20261004a
python3 -m scripts.capture_milestone3_check --step m2-initial --topic order-events-m2-m8-20261004a
python3 -m scripts.capture_milestone3_check --step m2-restart --topic order-events-m2-m8-20261004a
python3 -m scripts.capture_milestone3_check --step m2-recovery --topic order-events-m2-m8-20261004a
python3 -m scripts.capture_milestone3_check --step replay --topic order-events-m3-20261003b
```

- M1: 4 Avro producer/registry/Bronze records. Artifact
  `milestone3_evidence/order-events-m1-m8-20261004a/m1-1791123064068075900.json`.
- M2: initial32Bronze/11Silver/19DLQ/32audit; restart35/12/19/70; recovery1Silver/1DLQ.
  Under `milestone3_evidence/order-events-m2-m8-20261004a/`:
  `m2-initial-1791123100852199023`, `m2-restart-1791123141514995528`,
  `m2-recovery-1791123168863849023`.
- M3: 35Bronze/2DLQ/33Gold events/18revenue/13product; version3/digest preserved.
  `milestone3_evidence/order-events-m3-20261003b/replay-1791125613693131972.json`.
  Physical Silver8448 reflects the frozen verifier's deliberate duplicate-file
  replay, not duplicate logical Gold. M3 oracle digest remains
  `8263508ee79077227be47a2a550111b4b65e45daf01e28bed149eae5c0e57579`.

### M4

```bash
python3 -m scripts.capture_milestone4_check --phase initial --topic order-events-m3-20261003b --schema gold_m4_m8reg20261004
python3 -m scripts.capture_milestone4_check --phase retry --topic order-events-m3-20261003b --schema gold_m4_m8reg20261004
python3 -m scripts.capture_milestone4_check --phase failure --topic order-events-m3-20261003b --schema gold_m4_m8reg20261004
python3 -m scripts.capture_milestone4_check --phase old --topic order-events-m3-20261003b --schema gold_m4_m8reg20261004
python3 -m scripts.capture_milestone4_check --phase corrupt --topic order-events-m3-20261003b --schema gold_m4_m8reg20261004
```

PASS: isolated publication1→3, repeat NOOP, rollback, old/corrupt/uncommitted
rejection. Artifacts under `milestone4_evidence/order-events-m3-20261003b/`:
`initial-1791125628627093063`, `retry-1791125639692225387`,
`failure-1791125654640464018`, `old-1791125665382635041`,
`corrupt-1791125678783400967`.

### M5

```bash
python3 -m scripts.capture_milestone5_check --step serve --schema gold_serving --namespace oip:m5:verify-m8-20261004a
python3 -m scripts.capture_milestone5_check --step serve --schema gold_serving --namespace oip:m5:verify-m8-20261004a
python3 -m scripts.capture_milestone5_check --step rebuild --schema gold_serving --namespace oip:m5:verify-m8-20261004a
python3 -m scripts.capture_milestone5_check --step outages --schema gold_serving --namespace oip:m5:verify-m8-20261004a
```

PASS: SQL/cache18/13 equality, one hash32fields, retry, rebuild, actual Redis/SQL
outages/recovery. Artifacts under `milestone5_evidence/oip-m5-verify-m8-20261004a/`:
`serve-1791125704181960527`, `serve-1791125705074625812`,
`rebuild-1791125706159084354`, `redis-down-1791125715446719288`,
`postgres-down-1791125731087575135`, `serve-1791125738330667120`.

### M6

```bash
python3 -m scripts.capture_milestone6_check --step production --run 20261004m8
python3 -m scripts.capture_milestone6_check --step runtime --run 20261004m8
```

PASS: production source/cache/rendered dashboard equality, HTTP/AppTest, missing/
corrupt cache, fallback, Redis down, SQL down, both down and recovery. Artifacts
under `milestone6_evidence/20261004m8/`: `production-1791125741851172744`,
`missing-1791125746597191062`, `corrupt-1791125751196874946`,
`redis-down-1791125767424635326`, `postgres-down-1791125779200442815`,
`both-down-1791125810299641159`, `healthy-1791125819337506994`.

### M7

```bash
python3 -m scripts.capture_milestone7_check --step runtime --run 20261004m8
```

PASS: isolated result schema `m7_verify_20261004m8`, production source v3, initial
FIRED2/OK1/NO_BASELINE1, separate-process NOOP4, actual Redis independence and SQL
failure/recovery with unchanged history. New isolated controlled source/result
schemas prove 12 evaluations, 2 fires, suppression, real cooldown expiry, recovery
and missing-data transition. Production `alerts_serving` is not reevaluated or
changed by that isolated regression.

Artifacts under `milestone7_evidence/20261004m8/`:
`production-1791125820530044618`, `restart-1791125822256457612`,
`controlled-1791125828824370963`, `redis-down-1791125830338331255`,
`postgres-down-1791125844657562106`, `production-1791125851976786555`,
`restart-1791125853632445772`.

Authoritative M4/M5/M6 snapshot digest stayed
`26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231`.

### M7 data integrity

Before implementation (I) and after runtime, regressions and final full suite (H):

| alerts_serving table | Rows | Identical SHA-256 before/after |
|---|---:|---|
| alert_events | 2 | `187b4a9f7a87550f29b9eed48e660ca08859ed751903901e85db339de9c597d2` |
| alert_evaluations | 4 | `8dffe8e25efd27ac68743f1db7bce9b8661f3aced2947ceaaf35790736b8f618` |
| alert_state | 4 | `56accab7f8b644b0eea55ae4af2eb3bec11b66730cce194e565b99381cddfd83` |
| alert_definitions | 4 | `8efc7ebfc7c4a3d885a292f1d9ed8494813717df837a3c2f2ecf63c50d95f84f` |
| source_snapshots | 1 | `0a049d22a49d2acc6cf8bddd8ca25440290d99ca05362a5b19a21cc1246490b8` |

Independent reader uses REPEATABLE READ READ ONLY, deterministic ordering and
SHA-256 of canonical JSON with exact timestamp text. No M7 mutation exception was
allowed. Final H also verifies retained M8 SQL rows against all nine receiver
receipts after the regression service restarts.

## 18. Dependency review

| Component | Version / role | Classification |
|---|---|---|
| httpx | 0.27.2, actual controlled HTTP transport | KEEP |
| SQLAlchemy | 2.0.36, transactional PostgreSQL | KEEP |
| psycopg2-binary | 2.9.10, PostgreSQL driver | KEEP |
| PostgreSQL | 16.14, authoritative state | KEEP |
| Python standard library | SQLite test receipts, HTTP test server, SHA/JSON/Decimal | KEEP |
| Tenacity | 9.0.0, legacy dependency; M8 retry is persisted | NO CHANGE |
| Redis, Spark/Kafka/Avro, dashboard libraries | Frozen M1–M7 stack, regression verified | NO CHANGE |
| New provider libraries / paid services | None required | NO CHANGE |

No UPDATE NOW or UPDATE LATER was justified. `requirements.txt` is unchanged;
no installation/upgrade was performed. Existing container pip issue is explicitly
recorded in section16, not concealed by the passing milestone status.

## 19. Final file inventory

### NEW

| File | Reason |
|---|---|
| notifications/__init__.py | Isolated M8 package |
| notifications/model.py | Domain policy, canonical identity and persisted-event payload mapping |
| notifications/provider.py | Provider-neutral interface/result and single controlled HTTP implementation |
| notifications/service.py | Bounded generation/claim/send/result engine and one-shot CLI |
| notifications/persistence.sql | Additive request/attempt/source-binding constraints/indexes |
| notifications/CONTRACT.md | Findings, source/identity/retry/claim/provider/operational contract |
| scripts/milestone8_receiver.py | Durable controlled transport and failure/receipt evidence |
| scripts/verify_milestone8.py | Real-service phases, independent SQL/receipt oracle and integrity hashing |
| scripts/capture_milestone8_check.py | Append-only evidence and bounded service-outage orchestration |
| tests/unit/test_notifications.py | 42 meaningful SQL/HTTP/identity/retry/restart/concurrency tests |
| milestone8_notifications_report.md | This evidence-backed acceptance report |
| milestone8_evidence/20261004a/* | Actual commands, tests, provider results, outage/recovery and integrity |
| Additional milestone3–7 evidence files listed above | Frozen-milestone runtime regressions |

### MODIFIED

| File | Reason |
|---|---|
| main.py | Add notifications selector/usage, preserving existing service selectors |
| .env.example | Safe empty provider/destination/endpoint placeholders and bounded policy settings |
| README.md | M8 invocation and explicit controlled-provider certification boundary |
| PHASE_STATUS.md | Record M8 scoped acceptance/freeze |

### DELETED

None.

### UNCHANGED

64 source/SQL/contract/test files under alerts, storage, streaming, data_quality,
dashboard, producer, kafka, scripts and tests are hash-verified against I. This
includes `alerts/evaluator.py`, `alerts/definitions.py`, `alerts/persistence.sql`,
`alerts/CONTRACT.md`, legacy router/engines, all frozen milestone test modules and
all frozen verifiers. Their exact paths/hashes are in I.

Inspected `requirements.txt`, `docker-compose.yml`, `utils/config.py`, active
environment configuration and frozen reports were not changed. Main's additional
selector is the shared entrypoint addition; there is no M1–M7 compatibility change.
The workspace is not a Git repository; no commit was created.

## 20. Known limitations

- External-provider delivery is **not certified**. Controlled acceptance is not
  human receipt or delivery through Slack/PagerDuty/email.
- Provider idempotency requires retaining its SQLite ledger. A future provider
  without idempotency has an unavoidable duplicate window after ambiguous acceptance.
- Lease expiry may overlap calls; token fencing prevents stale SQL updates, not
  all network calls. HTTP timeouts are per phase, not a hard total deadline.
- Max-attempt exhaustion is terminal, including outcome-unknown exhaustion.
  No operator replay/repair UI or automatic reopening exists.
- Retry progress requires another invocation. No scheduler/daemon is included.
- Route/policy configuration is immutable for existing requests; changing endpoint
  or policy is rejected rather than silently rerouting/resetting history.
- PostgreSQL generation scans for unmaterialized events, bounded in returned data;
  no load/performance or distributed-scale certification is claimed.
- No delivery-history retention/purge policy, multi-channel routing, notification
  dashboard, observability backend, deployment, CI/CD or security-hardening project.
- The pre-existing container pygobject/pycairo pip-check issue remains documented.

## 21. Final certification

**PASS. M8 COMPLETE.** M7 input/persistence/identity/idempotency, delivery states,
bounded retry, provider recovery, restart/stale claims, concurrency, PostgreSQL
failure, Redis independence, currency/provenance and M7 immutability are proven.
M1–M7 regressions pass. No unresolved critical M8 blocker remains.

**External provider delivery not certified in this environment.** This is a
controlled-provider engine certification, as allowed by the milestone acceptance
rule. M8 is frozen at this verified scope. M1–M7 remain frozen.

### Verification-environment shutdown

After final tests, regression, integrity verification and report review, the
verification services were stopped at `2026-10-04T15:09:52.421554+00:00`:

```bash
python3 -m scripts.capture_milestone8_check --step freeze
```

The captured `docker stop` command returned 0. Targeted inspection confirmed all
six OIP containers exited: verification runner137, Kafka/Schema Registry143,
MinIO/Redis/PostgreSQL0. These are shutdown statuses, not test command failures.
Containers, database/data-lake/cache volumes, M7/M8 history, receiver ledger and
evidence were retained. The final running-container listing contains only unrelated
services. Shutdown artifacts: `freeze-stop-1791126592421581627.{json,log}` and
`freeze-status-1791126592606420825.{json,log}` in the M8 evidence directory.

## 22. Next milestone

**M9 — Observability.** Not implemented by M8.
