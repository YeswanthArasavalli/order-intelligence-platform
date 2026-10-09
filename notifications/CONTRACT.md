# M8 notification contract 1.0

## Current-world findings / selected provider

Inspection on 2026-10-04 found only `alerts/alert_router.py` with Slack incoming
webhook and PagerDuty Events API calls. Legacy rule/statistical engines import it;
M7's active evaluator does not. It skips missing credentials, uses three in-memory
Tenacity attempts, swallows/logs provider exceptions, formats money as INR floats,
and has no durable request/attempt/result model. The old SQL `notified_at` default
does not prove a send. PagerDuty's type/region dedup key is not an M7 event identity.
No separate notification package, worker, tests or Compose service existed.

Active environment inspection found Slack/PagerDuty credentials empty, and no
configured SMTP, Telegram or notification endpoint. Only presence flags were
printed. Existing httpx 0.27.2, SQLAlchemy 2.0.36 and psycopg2 2.9.10 are sufficient;
no dependency upgrade is needed. Legacy delivery code remains isolated.

**Exactly one concrete provider: `controlled_http`, channel `http`.** It sends real
HTTP to an explicitly configured loopback receiver. The receiver is a TEST /
CONTROLLED transport with a durable SQLite idempotency ledger, not an external
messaging service. **External provider delivery not certified in this environment.**
The receiver is verification infrastructure, not a second provider implementation.

## Source and separation

CLI input is always `alerts_serving.alert_events`, joined to its evaluations,
definitions and source snapshots. Only persisted FIRED events generate requests.
M8 never imports M7's evaluator/rule engine, reads analytical rows or uses Redis.
The Python fixture API additionally permits `m7_*` sources. M8 never changes the
five M7 tables. No M7 DDL or rule semantics change.

Provider-neutral payloads copy event/evaluation/rule IDs, severity, dimensions,
currency, selected metric/value, configured threshold, persisted comparison
boundary, times and source version/digest/reference. Money is decimal text. A drop
ratio is explicitly labeled as a ratio; its persisted comparison boundary has the
currency. Unit counts are labeled units. No thresholds or Gold values are recomputed.

## Identity / generation

Canonical JSON uses sorted keys, compact separators, ASCII escaping and no NaN.
`notification_id = SHA256({contract_version:"1.0", source_database, source_schema,
alert_event_id, provider, channel, destination})`.

Destination is an explicit non-private logical alias; the controlled endpoint is
configuration, not payload. A changed destination is a different notification.
Endpoint fingerprint and retry policy are bound to each persisted request: config
changes cannot silently redirect or reset already-created deliveries. Keep the
same receiver ledger and address across restarts. IDs do not include random UUIDs;
random claim tokens fence workers only.

Generation and delivery are separate operations. Generation commits a bounded
batch of requests before any sends. SQL uniqueness and an M8 generation advisory
lock serialize creation. Unmaterialized events are selected first, so repeated
batches progress beyond previously processed events. An optional explicit event
list must all exist and be valid; otherwise the generation transaction rolls back.

## Persistence / claims

Default schema `notifications_serving`; isolated fixtures `m8_*`. Requests are in
`notification_deliveries`; meaningful attempts are in `notification_attempts`.
Each target schema binds to one M7 source with a source-event FK and metadata.

Claim transaction: SELECT FOR UPDATE SKIP LOCKED on due work; record SENDING,
attempt number, claim token, DB timestamps and lease deadline; commit. Then call
the provider **outside the transaction**. A short fenced result transaction
records the attempt and final/retry state. No connection is held during HTTP.

States: PENDING → SENDING → DELIVERED / RETRYABLE_FAILURE / PERMANENT_FAILURE.
DELIVERED is the chosen controlled receiver's durable acceptance acknowledgement,
not human receipt. Permanent failure includes retries exhausted; its reason is
explicit. Terminal records never auto-reopen.

Expired SENDING claims are recovered under row lock: close the old attempt as
UNKNOWN (`lease_expired`), preserving uncertainty; schedule a bounded retry or
mark exhausted permanent failure. The recovery invocation does not pretend the
old call failed externally. Late results from superseded tokens cannot update the
delivery. Every claim, including an interrupted pre-send claim, consumes an attempt.
On the final ambiguous attempt, the terminal record remains outcome-unknown;
operator investigation is necessary, not an invented delivery/failure result.

## Retry / provider boundary

Defaults: 3 attempts, base delay 2s, maximum delay 30s, HTTP phase timeout 2s,
lease 30s, batch 100 (maximum 1000). Policy is validated and persisted. Delay after
attempt n is `min(max_delay, base_delay * 2**(n-1))`, based on PostgreSQL result time;
the processor never sleeps/retries indefinitely. Subsequent one-shot invocation
claims due work. No automatic internal HTTP retries or redirects.

Controlled transport: 200 plus valid matching receipt is success; 408/425/429 and
5xx are retryable; other statuses (including redirects, 400/401/403/404/409) are
permanent. Timeouts/network errors and invalid success receipts are retryable
ambiguous outcomes. Errors are fixed safe codes/messages, never response bodies,
URLs, headers or exception text. Idempotency-Key is the deterministic notification ID.

The receiver atomically records the ID + request digest + stable receipt before
acknowledgement; repeats return the same receipt, conflicting payloads return 409.
This proves one logical acceptance while its ledger is retained, not universal
exactly-once external delivery. After provider acceptance but before SQL commit,
lease recovery retries the same key. Lost/deleted receiver state would lose that
guarantee. A future non-idempotent provider has an unavoidable duplicate window.

## Bounds / failures / scope

One invocation generates at most batch requests and claims at most batch attempts.
Provider/destination-scoped workers can run concurrently using SKIP LOCKED and
claim fencing; no distributed throughput claim. Database failure before claim
means no provider call. Failure after acceptance leaves a recoverable claim and
ambiguous attempt, not a fabricated success. Redis availability is irrelevant.

Configuration is explicit environment/CLI input. Only http://127.0.0.1:<port>/notify
is accepted for this controlled transport; no credentials, query, fragments or
redirects. HTTP proxy environment is ignored. Destination aliases are validated.
No arbitrary untrusted endpoint execution or external credentials are required.

Structured results expose created, delivered, retryable/permanent failure,
duplicate-suppression and stale-recovery counts, provider latency and duration.
No scheduler, notification/dashboard UI, Prometheus/Grafana, extra providers,
paid service, deployment or M9 work is included.

## Commands and configuration

Load the existing PostgreSQL environment. Start the controlled receiver in the
same network namespace as the processor, choosing an explicit local ledger path
and port; retain that ledger across restarts:

```bash
python -m scripts.milestone8_receiver --ledger <local-ledger-path> --port <local-port>
```

Set `NOTIFICATION_PROVIDER=controlled_http`, `NOTIFICATION_ENDPOINT` to that
receiver's `http://127.0.0.1:<local-port>/notify`, and `NOTIFICATION_DESTINATION`
to a reviewed logical alias. Empty configuration is rejected, never treated as a
successful send. `NOTIFICATIONS_SCHEMA` defaults to `notifications_serving`.

```bash
python main.py notifications
python -m notifications.service --mode generate
python -m notifications.service --mode deliver
```

`--schema`, `--batch-size` and `--mode` are optional CLI selectors. The CLI cannot
override its M7 source. Environment policy names/defaults are documented in
`.env.example`: `NOTIFICATION_MAX_ATTEMPTS`, `NOTIFICATION_BASE_DELAY_SECONDS`,
`NOTIFICATION_MAX_DELAY_SECONDS`, `NOTIFICATION_LEASE_SECONDS`,
`NOTIFICATION_TIMEOUT_SECONDS`, `NOTIFICATION_BATCH_SIZE`. A lease must exceed
four HTTP phase timeouts. This is not a hard total HTTP deadline; idempotency and
fencing cover overlapping calls after expiry. No lease-renewal daemon is added.

Generation's duplicate-suppression count is the bounded number of already-existing
matching logical requests (not additional provider calls). Delivery counters are
per invocation; a recovered orphan is counted separately and its resulting state
is counted, while attempts increment only for actual claimed send work in that
invocation. Full historical attempt counts must be read from PostgreSQL.

`DELIVERED` and `PERMANENT_FAILURE` rows have no next-attempt schedule. A retryable
transport result can remain in attempt history while the parent is terminal due
to exhaustion. `attempts_exhausted_outcome_unknown` distinguishes exhausted
ambiguous timeout/network/receipt/exception or orphan outcomes. Manual replay of
terminal work is intentionally not implemented. Inspect retained receipts/history
before any future operator repair; changing IDs to bypass deduplication is not a
delivery-recovery protocol.
