# M5 — authoritative PostgreSQL → Redis cache v1

## Source, scope and entrypoints

`storage.postgres_redis_serving.serve_postgres_snapshot()` reads only PostgreSQL.
It copies the M4 `revenue_by_minute` and `product_rankings` rows and their
`serving_state` metadata. No analytics are recomputed, no extra summary is built,
and no Kafka/data-lake/manifest reader is imported. Manifest references in Redis
are provenance strings copied from PostgreSQL, never file access instructions.

The default PostgreSQL schema is `gold_serving`, selected by
`POSTGRES_GOLD_SCHEMA` or `--schema`. `gold_m4_final` is used only when explicitly
selected for verification. One Redis namespace belongs to one database/schema/
source-root/topic combination. There is no fallback to legacy `gold.*` tables.

With the existing environment loaded:

```bash
python -m storage.postgres_redis_serving
python -m storage.postgres_redis_serving --schema gold_serving --namespace oip:m5:gold-serving
```

This is a one-shot refresh API/CLI. Invoke it after successful M4 serving or on an
operator-controlled refresh schedule. M4 production semantics are not changed by
M5. Errors propagate; the CLI returns a nonzero exit code and a sanitized message.
No automatic infinite retries or implicit SQL schema creation/migration occur.

## Exact Redis representation

Namespace grammar: `oip:m5:[a-z0-9][a-z0-9_-]{0,79}`. Colons, wildcards, braces and
path separators in the slug are rejected. Default: `oip:m5:gold-serving`.

Redis keys:

```text
<namespace>:current                  HASH — the entire active snapshot
<namespace>:stage:<32 hex UUID>       HASH — one private refresh attempt
```

Hash fields:

```text
@metadata
revenue:<b64(window_start)>:<b64(window_end)>:<b64(region)>:<currency>
product:<b64(window_start)>:<b64(window_end)>:<b64(product_id)>:<b64(product_name)>:<currency>
```

`b64(x)` is UTF-8 URL-safe Base64 without `=` padding, with `_` for the empty
string. These are **hash field identities**, not independent top-level Redis
keys. All M4 dimensions are present and independently encoded. Currency remains
three literal uppercase ASCII letters. Colons/Unicode in dimensions cannot
change namespace/key structure. Fields are unique per full M4 identity.

The entire snapshot is one eviction unit. The existing Redis `allkeys-lru`
configuration can evict the hash, producing a whole-cache miss. It cannot evict
one row while retaining this hash's completion metadata.

## Exact value serialization

Each value is JSON encoded with `sort_keys=True`, separators `(',', ':')`,
`ensure_ascii=True`, and `allow_nan=False`.

- Monetary columns: JSON **strings**, exactly eight fractional places, using
  Python Decimal with precision 80 for validation/quantization. No rounding is
  accepted; no float conversion. Range ±999999999999999999999999999999.99999999.
- Only `net_revenue` may be negative. `avg_order_value` may be JSON `null`.
- Counts: nonnegative JSON integers within PostgreSQL signed BIGINT range.
- Timestamps: UTC strings, six microsecond digits, explicit `+00:00` offset.
- Region, product ID/name and currency: JSON strings.
- Rows contain exactly the M4 columns; financial values are copied, not derived.

Revenue example (canonical serialization):

```json
{"avg_order_value":"100.00000000","cancellation_amount":"0.00000000","currency":"INR","failed_count":0,"gross_revenue":"100.00000000","net_revenue":"100.00000000","order_count":1,"refund_amount":"0.00000000","refunded_event_count":0,"region":"north-india","window_end":"2026-10-03T14:51:00.000000+00:00","window_start":"2026-10-03T14:50:00.000000+00:00"}
```

Product JSON has exactly `window_start`, `window_end`, `product_id`,
`product_name`, `currency`, `units_sold`, `revenue` (serialized sorted by name).

`@metadata` contains:

```text
cache_contract_version = "1.0"
serving_status = "SERVED"
source_database, source_schema, source_topic, source_root
gold_contract_version, gold_version
manifest_path, manifest_digest, snapshot_identity, snapshot_digest
revenue_rows, product_rows, served_at
```

`served_at` is the **PostgreSQL** serving timestamp. There is no fake refresh
timestamp; retries and rebuilds preserve identical logical metadata. The metadata
describes this cached version, not a guarantee that PostgreSQL has not advanced.

## Source validation and refresh

1. Validate schema and namespace; WATCH the exact active Redis key.
2. In PostgreSQL `REPEATABLE READ` plus `SET TRANSACTION READ ONLY`, read
   `serving_state`, both analytical tables, and database identity. No writes/DDL.
3. Validate successful state, contract/version/digests, columns, types, counts,
   currency, windows, Decimal range/scale, nonnegative metrics and duplicate keys.
   Recompute the M4 logical digest and compare with `serving_state.snapshot_digest`.
4. Check cached source/version metadata. Reject another source, older source
   version, or different metadata for the same version. Compare **all hash
   fields** for NOOP, then execute a watched read to detect overlapping changes.
   A missing/corrupt row with intact metadata is repaired by full replacement.
   Malformed/conflicting metadata requires explicit cache invalidation.
5. Build a unique staging hash, in chunks of at most 500 fields, with status
   `STAGING`. Readers never use staging keys. Verify the complete staging hash
   against the source fields before marking its metadata `SERVED`.
6. WATCH the staged hash too. A single `RENAME(stage, current)` inside watched
   `MULTI/EXEC` atomically replaces the entire active hash, including metadata.
   There is no separate mutable pointer or completion key.
7. In `finally`, delete only this attempt's staging key. RENAME has already removed
   that key on success; cleanup never deletes a completed active refresh.

Old row fields are removed by replacement, not by per-row TTLs. A newer empty
snapshot has exactly one field, `@metadata`, and zero analytical rows.

## Reads and idempotency

Use `read_active_snapshot(client, namespace)`. It issues one `HGETALL`, validates
identity fields, row counts and digest, and returns metadata plus the two lists
of JSON-safe row dictionaries. Financial values remain strings; callers needing
arithmetic must use `Decimal`, never `float`. `None` means the entire cache is
missing. Invalid contents raise `InvalidSnapshot`, rather than serving partial
data or falling back silently. Do not combine independent HGET calls across
refresh boundaries to assemble a snapshot.

An exact retry returns `NOOP`, with unchanged bytes, version and metadata. The
cache is not considered correct merely because version/digest metadata match.
Restarting the serving process follows the same comparison and recovery path.

## Concurrency and failure policy

One scheduled refresh worker per namespace is recommended. WATCH provides tested
optimistic conflict detection for overlapping invocations; it is not a
distributed lock or lease. If another worker publishes or a watched key is
evicted/deleted/changed, publication aborts and the caller retries from PostgreSQL.
Attempt-specific UUID staging keys prevent cleanup from removing another worker's
completed state. Redis Cluster support is not claimed.

There is **no PostgreSQL/Redis distributed transaction**. PostgreSQL may advance
after the consistent source read. Redis retains the copied source version until
the next successful refresh. Redis never updates PostgreSQL or acknowledges a
new PostgreSQL version before complete publication.

- Redis unavailable: fail clearly; PostgreSQL unchanged. Retry after recovery.
- PostgreSQL unavailable/invalid/unserved: no publication or fake freshness update.
  An existing complete cached version remains available and explicitly labeled.
- Partial staging failure: active hash remains the previous complete version (or
  absent if there was none). Metadata in an inactive staging key is not a public
  serving declaration.
- Lost response after RENAME: publication may have committed; retry resolves by
  full comparison. PostgreSQL is unaffected.
- Current hash deleted/evicted: miss, rebuild from PostgreSQL. Cache deletion also
  loses Redis's prior version guard; authoritative PostgreSQL then defines the
  rebuild. Redis is not a backup or source-of-truth version ledger.

## TTL and keyspace policy

No M5 TTL is configured: `TTL(current) == -1`. Source changes, deletion or eviction
determine refresh needs. Legacy 600/300/900/60-second TTLs apply only to the old
helpers and alert cooldowns. Redis AOF/RDB are operational conveniences, not
business durability; Redis can be lost entirely and reconstructed.

`delete_cache(client, namespace)` deletes exactly `<namespace>:current`. This
invalidates concurrent WATCH attempts. It never scans/deletes other namespaces,
alert keys or legacy KPI keys. No database-wide flushing is used.

Completed refreshes leave no historical snapshots; RENAME replaces the previous
hash. A killed process or unavailable Redis during cleanup may leave an inactive
`<namespace>:stage:<UUID>` hash. With refresh workers stopped, an operator may
remove these exact staging keys using a namespace-qualified SCAN and DEL. They
are never read as active. No automatic wildcard cleanup can race another worker.

## Configuration, limits and compatibility

Existing `POSTGRES_HOST/PORT/DB/USER/PASSWORD` and `REDIS_HOST/PORT` apply. M5 adds
optional `REDIS_DB` (0), `REDIS_USERNAME`/`REDIS_PASSWORD` (unset),
`REDIS_GOLD_NAMESPACE`, `REDIS_GOLD_MAX_ROWS` (100000 per table), and
`REDIS_GOLD_MAX_BYTES` (16777216 combined serialized field/value bytes).
Limits must be positive. SQL reads are row bounded; loading/serialization is in
process memory. The byte limit excludes Python/Redis overhead and is not a
large-dataset scaling guarantee. Full-hash reads and atomic rename fit this
bounded implementation; larger workloads need a separately scoped design.

Pooled clients have bounded connection counts and five-second connect/socket
timeouts. Owned clients/engines are closed/disposed; caller-owned resources remain
open. Credentials come only from the environment. SQL values are bound, schema
identifiers validated, and CLI errors do not print chained driver exceptions.

Legacy `storage.redis_client` remains unchanged. The dashboard's currency-free
summary and INR labels, alerts' cooldowns and legacy SQL, and observability's SQL
are not migrated. No compatible alias to currency-collapsing keys is published.
This milestone adds revenue/product snapshot caching plus metadata only, not new
KPIs, heatmaps, funnels, baselines or user metrics.
