# Milestone 4 — Authoritative Gold PostgreSQL Serving Report

**Verification date:** 2026-10-04  
**Final verdict:** **PASS**  
**Serving schema:** `gold_m4_final` for the final runtime evidence; the default deployment schema is `gold_serving`.  
**Source topic:** `order-events-m3-20261003b`  
**Authoritative Gold version:** `3`  
**Manifest:** `s3a://gold/order-events-m3-20261003b/_commits/00000000000000000003.json`  
**Snapshot digest:** `26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231`

## 1. Exact Gold source

M4 reads the M3 committed Gold root for the source topic:

```text
s3a://gold/order-events-m3-20261003b/
```

The serving entry point is `storage.gold_postgres_serving.serve_current_gold()`. It calls `storage.gold_store.current_manifest()`, validates the exact current commit object, then reads only the manifest-pinned `revenue_by_minute` and `product_rankings` paths. It does not scan `_snapshots/*`, infer a latest attempt, or re-aggregate Silver/Bronze data.

## 2. Required Gold tables and columns

The serving contract uses the verified M3 tables:

### `revenue_by_minute`

```text
window_start, window_end, region, currency,
order_count, failed_count, refunded_event_count,
gross_revenue, refund_amount, cancellation_amount,
net_revenue, avg_order_value
```



### `product_rankings`

```text
window_start, window_end, product_id, product_name,
currency, units_sold, revenue
```

The committed manifest also contains the `events` ledger entry. M4 validates the manifest includes all three M3 table entries, while PostgreSQL serving stores the two analytical tables.

## 3. Source manifest/version used

The final runtime used:

```text
version: 3
contract_version: 1.0
manifest: s3a://gold/order-events-m3-20261003b/_commits/00000000000000000003.json
Gold revenue rows: 18
Gold product rows: 13
```

M4 first served a controlled version 1 snapshot into the isolated schema and then served the committed version 3 snapshot. The final `serving_history` contains versions 1 and 3, and `serving_state` points to version 3.

## 4. Exact PostgreSQL schema DDL

The additive migration is `scripts/postgres/m4_gold_serving.sql`. Runtime creation and additive upgrades are implemented by `ensure_schema()` in `storage/gold_postgres_serving.py`.

The final inspected `gold_m4_final` schema is:

```sql
CREATE SCHEMA IF NOT EXISTS gold_serving;

CREATE TABLE gold_serving.revenue_by_minute (
    window_start         TIMESTAMPTZ    NOT NULL,
    window_end           TIMESTAMPTZ    NOT NULL,
    region               TEXT           NOT NULL,
    currency             TEXT           NOT NULL,
    order_count          BIGINT         NOT NULL,
    failed_count         BIGINT         NOT NULL,
    refunded_event_count BIGINT         NOT NULL,
    gross_revenue        NUMERIC(38,8)  NOT NULL,
    refund_amount        NUMERIC(38,8)  NOT NULL,
    cancellation_amount  NUMERIC(38,8)  NOT NULL,
    net_revenue           NUMERIC(38,8)  NOT NULL,
    avg_order_value       NUMERIC(38,8),
    PRIMARY KEY (window_start, window_end, region, currency),
    CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE TABLE gold_serving.product_rankings (
    window_start TIMESTAMPTZ   NOT NULL,
    window_end   TIMESTAMPTZ   NOT NULL,
    product_id   TEXT          NOT NULL,
    product_name TEXT          NOT NULL,
    currency     TEXT          NOT NULL,
    units_sold   BIGINT        NOT NULL,
    revenue      NUMERIC(38,8) NOT NULL,
    PRIMARY KEY (window_start, window_end, product_id, product_name, currency),
    CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE TABLE gold_serving.serving_state (
    singleton_id         SMALLINT      PRIMARY KEY CHECK (singleton_id = 1),
    source_topic         TEXT          NOT NULL,
    source_root          TEXT          NOT NULL,
    gold_contract_version TEXT         NOT NULL,
    gold_version         BIGINT        NOT NULL,
    manifest_path        TEXT          NOT NULL,
    manifest_digest      CHAR(64)      NOT NULL,
    snapshot_identity    TEXT          NOT NULL,
    snapshot_digest      CHAR(64)      NOT NULL,
    revenue_rows         BIGINT        NOT NULL,
    product_rows         BIGINT        NOT NULL,
    served_at            TIMESTAMPTZ   NOT NULL,
    serving_status       VARCHAR(16)   NOT NULL CHECK (serving_status = 'SERVED')
);

CREATE TABLE gold_serving.serving_history (
    source_topic          TEXT          NOT NULL,
    source_root           TEXT          NOT NULL,
    gold_contract_version TEXT          NOT NULL,
    gold_version          BIGINT        NOT NULL,
    manifest_path         TEXT          NOT NULL UNIQUE,
    manifest_digest       CHAR(64)      NOT NULL,
    snapshot_identity     TEXT          NOT NULL,
    snapshot_digest       CHAR(64)      NOT NULL,
    revenue_rows          BIGINT        NOT NULL,
    product_rows          BIGINT        NOT NULL,
    served_at             TIMESTAMPTZ   NOT NULL,
    serving_status        VARCHAR(16)   NOT NULL CHECK (serving_status = 'SERVED'),
    PRIMARY KEY (source_topic, gold_version)
);
```

`idx_m4_history_served_at` is also created. Existing legacy `gold.*` tables are not altered.

## 5. Why the existing PostgreSQL Gold schema was not reused

The existing `gold.revenue_by_minute` and `gold.product_rankings` tables predate M3 and cannot represent the verified contract without destructive or ambiguous changes:

- currency is absent from both analytical identities;
- legacy monetary columns use `NUMERIC(14,2)`;
- legacy product identity does not include currency;
- downstream dashboard, Redis, alert and observability queries use those legacy tables and have INR/float assumptions.

M4 therefore uses an additive serving schema and leaves the legacy schema intact.

## 6. Currency representation

Currency is stored as a separate `TEXT` column and validated as exactly three uppercase ASCII letters. It is part of both analytical identities. There is no implicit conversion, default currency or cross-currency aggregation.

The final runtime representative rows include `INR`; the unit suite also serves `USD` rows in the same windows and verifies they remain separate.

## 7. Decimal mapping and float handling

All monetary values are converted through `Decimal(str(value))` before validation and insertion. No Python float arithmetic is used by the serving path. PostgreSQL monetary columns are `NUMERIC(38,8)`.

Validation rejects non-finite values, values with more than eight fractional places, values outside the full `NUMERIC(38,8)` range and negative values for nonnegative metrics. Negative `net_revenue` remains valid. The unit suite serves and reads the maximum supported `NUMERIC(38,8)` value exactly.

## 8. Product identity

Product identity is:

```text
(window_start, window_end, product_id, product_name, currency)
```

`product_name` is retained in the identity because it is part of the verified M3 Gold contract. It is stored as `TEXT` so the serving layer does not introduce arbitrary truncation.

## 9. Primary keys and uniqueness

The revenue primary key is:

```text
(window_start, window_end, region, currency)
```

The product primary key is:

```text
(window_start, window_end, product_id, product_name, currency)
```

The serving code also detects duplicate logical keys before PostgreSQL insertion.

## 10. Idempotency

Serving state records the source topic, Gold version, manifest path/digest, snapshot identity/digest and row counts. The same version is accepted only when the identity, manifest path and row digest agree. A matching request does not rewrite analytical tables.

The same-manifest runtime verifier returned `NOOP` on both calls and preserved 18 revenue rows and 13 product rows.

## 11. Same-manifest retry behavior

Final evidence:

```text
first_status:  NOOP
second_status: NOOP
same_manifest_noop: true
snapshot_digest: 26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231
```

An older isolated schema also exercised the additive migration path and returned `NOOP_MIGRATED` once before subsequent retries returned `NOOP`.

## 12. Stale manifest rejection

After version 3 was served, an explicit read of the committed version 1 manifest was rejected with `StaleGoldManifestError`. PostgreSQL remained at version 3 with the same row counts and digest.

## 13. Transaction boundaries

Each accepted version is served under one SQLAlchemy `engine.begin()` transaction. A transaction-scoped PostgreSQL advisory lock serializes writers for the serving schema. State, history and both analytical table replacements are committed together.

## 14. Atomic complete-snapshot replacement

The serving sequence is:

1. acquire the transaction advisory lock;
2. lock and inspect `serving_state`;
3. create transaction-local staging tables;
4. insert the complete validated manifest-pinned snapshot;
5. delete the current analytical rows;
6. insert the complete staged revenue and product snapshots;
7. update `serving_state` and append `serving_history`;
8. commit.

There is no row-by-row upsert path and no visible mixed-version state after commit.

## 15. Temporary staging tables

The serving transaction creates `pg_temp.m4_revenue_stage` and `pg_temp.m4_product_stage` using the target table definitions. They are `ON COMMIT DROP`, are populated in bounded batches of 1,000 rows, and are never visible as durable business tables.

## 16. Rollback behavior

The real verifier injected a failure after the target rows were deleted. Before and after independent PostgreSQL/Gold equality checks were identical:

```text
version:          3
revenue rows:     18
product rows:     13
snapshot digest:  26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231
failure preserved: true
```

The unit suite injects failures after staging, after deletion, after insertion and immediately before commit; all four cases roll back and can be retried.

## 17. Multi-writer behavior

The implementation supports one serving writer per isolated schema. The transaction advisory lock serializes concurrent invocations within that schema. Source Gold itself remains governed by the existing one-writer-per-root manifest contract.

## 18. Partial-batch protection

The serving API requires both complete manifest row counts to match the rows supplied. It rejects missing columns, invalid keys, duplicate logical identities, invalid currency, invalid Decimal values and configured oversized snapshots before replacement. The real injected partial replacement was rolled back, so a partial batch never became visible.

## 19. No committed manifest behavior

`serve_current_gold()` returns:

```text
NO_COMMITTED_MANIFEST
```

when the requested root has no commit object. It does not read orphan snapshot attempts and does not alter the existing PostgreSQL state.

## 20. Corruption behavior

The verifier created a corrupt latest commit object containing invalid JSON. The serving reader failed closed while parsing that latest commit and did not fall back to an older commit. The existing PostgreSQL version 3 state remained unchanged.

## 21. Uncommitted attempt behavior

An isolated root containing no committed manifest returned `NO_COMMITTED_MANIFEST`. Since table paths are accepted only from a validated manifest and must remain beneath the committed Gold root, an orphan/uncommitted attempt cannot be served through `serve_current_gold()`.

## 22. Observability and state tracking

`serving_state` exposes the current topic, source root, contract version, Gold version, manifest path/digest, snapshot identity/digest, row counts, serving timestamp and `SERVED` status. `serving_history` records each successfully served version. The verifier also performs independent digest and row equality checks using psycopg2.

The final inspected state was:

```text
gold_version:          3
source_root:           s3a://gold/order-events-m3-20261003b
revenue_rows:          18
product_rows:          13
serving_status:        SERVED
snapshot_digest:       26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231
```



## 23. Credentials and secret handling

`get_engine()` requires `POSTGRES_PASSWORD` from the existing environment and reads the remaining `POSTGRES_*` settings from environment configuration. No password fallback is present. Capture evidence commands contain no password values, and generated JSON evidence was checked/redacted where earlier runs had included credentials.

## 24. Unit and static tests

Final checks:

- `tests/unit/test_postgres_serving.py`: **10 passed** in the PostgreSQL container.
- `tests/unit/test_milestone2.py` plus `tests/unit/test_gold.py`: **26 passed** through Spark 3.5.9.
- Combined focused coverage: **36 passed**.
- Ruff: passed on changed Python files.
- Python compile check: passed for `storage`, `scripts` and `tests/unit`.
- `docker compose config --quiet`: passed.

The container-wide `pip check` reports the pre-existing unrelated environment issue `pygobject 3.42.1 requires pycairo, which is not installed`; M4 does not import or use that dependency.

## 25. Real M4 verification

Final real verification used the Spark/MinIO/PostgreSQL container and `scripts/capture_milestone4_check.py`:


| Phase       | Result                                                                                 |
| ----------- | -------------------------------------------------------------------------------------- |
| initial     | Served controlled version 1, then version 3; stale rows removed; 18/13 equality passed |
| retry       | Same-manifest `NOOP`; 18/13 equality passed                                            |
| failure     | Injected failure after deletion; prior state preserved                                 |
| old         | Version 1 rejected after version 3                                                     |
| corrupt     | Corrupt latest manifest rejected; no fallback                                          |
| uncommitted | `NO_COMMITTED_MANIFEST`                                                                |


Evidence is under `milestone4_evidence/order-events-m3-20261003b/`.

## 26. M1–M3 regression results

Final regression runs after the M4 refinements:

- M1 fresh topic `order-events-m1-m4-final-20261004a`: producer/schema/Bronze verification passed with 4 records.
- M2 fresh topic `order-events-m2-m4-final-20261004a`:
  - initial: 32 Bronze, 11 Silver, 19 DLQ, 32 audit rows;
  - restart: 35 Bronze, 12 Silver, 19 DLQ, 70 audit rows;
  - recovery: 1 Silver, 1 DLQ with fault/checkpoint/partial-output guards passed.
- M3 replay on `order-events-m3-20261003b`: 33 Gold events, 18 revenue rows, 13 product rows, 2 DLQ rows, unchanged committed Gold version 3 and unchanged M4 snapshot digest. The increased Silver file count is from the deliberate duplicate committed-file replay fixture; Gold identity deduplication remained correct.



## 27. Unchanged downstream consumers

The following were inspected and intentionally not modified:

- `dashboard/app.py`
- `storage/redis_client.py`
- `alerts/rule_engine.py`
- `alerts/statistical_detector.py`
- `observability/metrics_exporter.py`
- legacy `storage/postgres_writer.py`
- legacy `scripts/postgres/init.sql`

This keeps M4 additive and avoids silently changing existing consumers that query the incompatible legacy `gold.*` schema.

## 28. Limitations and operational notes

- `gold_serving` is the intended additive deployment schema; `gold_m4_final` is an isolated final-verification schema.
- The standalone SQL migration is intentionally additive and does not rewrite legacy `gold.*` tables. Runtime `ensure_schema()` applies equivalent idempotent DDL and upgrades isolated prior M4 schemas without dropping business data.
- The serving writer is one writer per schema; the lock provides serialization, not distributed multi-writer ownership beyond the existing Gold one-writer contract.
- Existing dashboards, Redis readers, alerts and observability consumers still use legacy Gold assumptions and require a separate compatibility migration if they are later moved to the currency-aware serving schema.
- No retention, archival or deletion policy for orphan Gold snapshot attempts was introduced.



## 29. Final verdict

**PASS.** M4 now serves only the authoritative committed M3 Gold snapshot into an additive PostgreSQL schema. The final PostgreSQL rows match the manifest-pinned Gold rows independently, currency and product-name identities are preserved, Decimal(38,8) values are validated without float conversion, complete replacement is transactional, retries are idempotent, stale/corrupt/uncommitted inputs fail closed, rollback preserves the previous valid state, and M1–M3 regressions remain passing.