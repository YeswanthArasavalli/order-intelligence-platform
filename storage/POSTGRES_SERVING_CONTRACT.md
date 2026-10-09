# Committed Gold → PostgreSQL serving v1

## Authoritative source and data

`serve_current_gold` is the supported entry point. It resolves `current_manifest`
once, pins the exact two table paths and validates their physical schemas, row
counts, keys and data types. Both tables come from that same manifest. No metrics
are calculated in PostgreSQL. net_revenue and nullable average are copied exactly;
the additive cancellation/refund interpretation from M3 is unchanged.

A root must be `s3a://<bucket>/<source-topic>/`. Every table path must belong to
one immutable snapshot attempt under that root; successful outputs and a valid
commit object are required. Serving checks that the pinned manifest is unchanged
after both table reads and before the PostgreSQL serving transaction begins. The
immutable publisher can advance immediately afterward: PostgreSQL represents the
recorded committed snapshot, not a distributed transaction with the Gold
publisher. Subsequent serving advances it.
An explicit expected version is a guard, not permission to load historical data.

## Additive schema and identities

Default schema `gold_serving`; isolated schemas via POSTGRES_GOLD_SCHEMA or CLI.
One source root/topic per schema; other roots need other serving schemas. Legacy
`gold.*` is preserved, not renamed, altered, truncated, backfilled or exposed through
misleading compatibility views. SQL migration is `scripts/postgres/m4_gold_serving.sql`;
the runtime uses that same DDL, with validated/quoted schema substitution. An
existing incompatible schema fails before replacement, rather than an in-place
automatic destructive migration. Reserved legacy schemas cannot be selected.

Revenue columns exactly match M3: timestamptz window_start/window_end, text region/
currency, bigint order_count/failed_count/refunded_event_count, numeric(38,8)
gross_revenue/refund_amount/cancellation_amount/net_revenue/avg_order_value.
Only avg_order_value is nullable. PK=(window_start,window_end,region,currency).

Product columns exactly match M3: timestamptz window_start/window_end, text
product_id/product_name/currency, bigint units_sold, numeric(38,8) revenue.
PK=(window_start,window_end,product_id,product_name,currency). All non-null.
Strings have no arbitrary varchar truncation; PostgreSQL B-tree key-size limits
can still reject unusually large dimension strings, explicitly and transactionally.

Every financial value is a Python Decimal and must be exactly representable at
38 digits/8 fractional places. Full range ±999999999999999999999999999999.99999999
is preserved. No float conversion, FX, rounding or recomputed net/average.
NaN/infinity/excess precision/range are rejected. Decimal context precision 80
is used for validation/canonicalization. Counts retain bigint range. UTC values
are explicitly timezone-aware before PostgreSQL insertion. No timestamp truncation.

## Serving transaction

Schema initialization is its own idempotent additive transaction. Business serving
uses one engine.begin transaction containing:

1. A transaction advisory lock scoped to database/schema; overlapping serving or
   initialization attempts fail clearly rather than claiming general multiwriter safety.
2. Schema validation and state/version check under the lock.
3. Per-session temporary staging tables (ON COMMIT DROP), parameterized Decimal/
   UTC inserts and bounded batch loading.
4. DELETE and complete INSERT of BOTH authoritative tables. DELETE is MVCC-safe;
   no TRUNCATE or DDL table swap is used for business replacement.
5. State/history update recording source root/topic, Gold contract/version,
   manifest path/SHA256, snapshot identity/content digest, counts and served time.
6. Current-Gold guard and commit. Any exception rolls back every business change.

Staging is not visible to other sessions. An ordinary SELECT sees previous committed
rows during replacement, then the complete new table after commit. To read multiple
tables plus state coherently across separate SELECTs, consumers MUST use one
REPEATABLE READ READ ONLY transaction (or a single SQL statement). READ COMMITTED
does not promise the same version across multiple statements straddling commit.

Every newer complete snapshot removes absent keys, including an explicitly committed
empty snapshot. M3's ordinary empty input does not publish a snapshot: absence of
a manifest means NO_COMMITTED_MANIFEST, never deletion of PostgreSQL state.

## Idempotency and failure

Same version/root/manifest hash/snapshot identity/content digest => NOOP, leaving
rows, served_at and history unchanged. Current table digest is checked on retry
so external mutation is reported, not silently accepted. Older version => reject;
same version with different content => reject; switching roots/topics => reject.
No user-facing rollback option. A failed new version can be retried unchanged.

No FAILED state is committed over the last valid SERVED state. Errors propagate,
transactions release locks, and engines owned by the operation are disposed.
Connection loss after commit is ambiguous to the client; retry resolves against
the committed serving_state. No distributed transaction or automatic broad retries.

## Resource and compatibility limits

Rows are collected to the driver (matching M3's bounded local scope). Explicit
POSTGRES_GOLD_MAX_ROWS defaults to 100000 per table, checked against the committed
manifest before collection;
larger inputs fail for an intentional scaling change. Executemany is chunked.
The bounded max rows does not bound individual string sizes. One source per schema.
No credentials/DSN logged; existing POSTGRES_* environment variables are used and
POSTGRES_PASSWORD is required. Values are bound parameters; identifiers validated
and quoted; engines hide SQL parameter values in exceptions.

Dashboard/Redis/alerts/metrics still query legacy tables or omit currency, coerce
floats, recalculate net or aggregate averages. They are not M4-compatible and are
not modified here. Serving schema needs a deliberate downstream read-contract
adoption; success here does not make legacy consumers correct.
