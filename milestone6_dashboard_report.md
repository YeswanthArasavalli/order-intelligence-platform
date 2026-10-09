# Milestone 6 — Currency-aware dashboard over M5 Redis / M4 PostgreSQL

## 1. Executive verdict

**PASS — 2026-10-04.** The Streamlit dashboard now consumes a coherent M5 Redis
snapshot, with read-only authoritative `gold_serving` fallback. Currency, full
product/revenue identity, Decimal values and provenance survive the read and
display path. Redis remains disposable; the dashboard neither writes caches nor
computes Gold analytics.

The mandatory production-path qualification was actually executed: the missing
`gold_serving` schema was populated through the existing M4 current-committed-Gold
entrypoint, then the default M5 namespace `oip:m5:gold-serving` was published and
independently verified. There is no production dependency on `gold_m4_final`.

Final production verification: `2026-10-04T07:42:52.109527+00:00`.

| Property | Verified value |
|---|---|
| PostgreSQL schema | `gold_serving` |
| Redis namespace | `oip:m5:gold-serving` |
| Source topic | `order-events-m3-20261003b` |
| Gold version | 3 |
| Revenue | 18 rows: 17 INR, 1 USD |
| Products | 13 rows: 12 INR, 1 USD |
| Redis active data | 1 hash, 32 fields, TTL -1 |
| PostgreSQL served_at | `2026-10-04T06:55:08.813378+00:00` |
| SQL / Redis / dashboard / rendered-table digest | `26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231` |

## 2. PASS / PARTIAL / BLOCKED

**PASS**, supported by actual read-layer, Streamlit, source qualification,
failure/recovery and regression evidence. All critical acceptance criteria were
demonstrated. Full relevant suite: **97 passed**, including **37 M6 tests**.
M1–M5 runtime regressions passed. No M1–M5 production implementation was modified.

Validation followed inspect → design → implement → tests → real-service checks →
regressions → report → freeze. No subsequent milestone implementation was added.

## 3. Current-world findings

Inspection was completed before editing the dashboard. Reviewed:

- `dashboard/app.py` and its empty package initializer; no other dashboard module
  or dashboard tests existed.
- All dashboard query functions, rendered metrics, imports and startup path.
- `storage/redis_client.py`, M5 serving/read code and `REDIS_SERVING_CONTRACT.md`.
- M4 serving implementation, `gold_store.py`, and `scripts/postgres/m4_gold_serving.sql`.
- M5 tests and independent verifier.
- Relevant environment/configuration, requirements, Compose service settings,
  README/status references and actual installed packages/services.

Findings from code, not just prior reports:

- Dashboard called `get_summary()` on `kpi:summary:latest`; leaderboard/cooldown
  functions were imported but unused.
- Revenue/product/region queries read legacy `gold.*`. Revenue queries recomputed
  net, used invalid parameterized interval syntax, and omitted currency.
- Platform KPI fallback summed currencies together, averaged averages and converted
  money to float. INR symbols and labels were hard-coded throughout.
- Product display omitted product ID/currency/window boundaries and formatted
  revenue to whole INR units.
- DQ card queried `silver.dq_run_log`; alert banner queried `alerts.alert_log`.
  Neither is part of the verified M4 analytical snapshot.
- Kafka/Spark/PostgreSQL health indicators were unconditional, and Redis status
  reflected import success rather than a validated read.
- No UI filters existed. Time ranges were hard-coded separately for each query.
- Startup was direct Streamlit, outside `main.py`; a blocking sleep/rerun loop
  refreshed every ten seconds. The dashboard did not load `.env` itself.
- M5's public readers validate keys/row shapes/counts/digest. Consumer-specific
  expected database/schema/topic and complete provenance checks belong in M6.
- Actual `gold_serving` had no tables at inspection; existing M4 verification data
  and committed Gold remained intact.

## 4. Dashboard architecture before M6

```text
Streamlit app.py
  ├─ legacy Redis summary helper → currency-free summary
  ├─ inline SQL → legacy gold.* revenue/product tables
  ├─ inline SQL → Silver DQ log / alert log
  └─ locally recomputed net, mixed-currency sums, floats and hard-coded INR
```

Displayed metrics were net revenue, order count, average order value, DQ pass rate,
revenue series, regional revenue, top products and alert messages. A lifecycle
funnel was mentioned in a docstring but not actually rendered.

## 5. Dashboard architecture after M6

```text
dashboard/app.py (Streamlit presentation)
        ↓
dashboard/read_service.py — DashboardReadService
        ├─ M5 read_active_snapshot() — preferred, one HGETALL
        └─ M5 read_postgres_snapshot() — read-only authoritative fallback
                     ↓
        gold_serving.serving_state
        gold_serving.revenue_by_minute
        gold_serving.product_rankings
```

One refresh chooses one complete source snapshot; Redis and PostgreSQL rows are
never merged. The read layer converts validated monetary strings to Decimal and
timestamps to aware UTC datetimes. The UI only filters, orders and presents rows.

Preserved useful UI concepts: title, metric cards, revenue time series, product
tables, status/provenance and periodic refresh. Metric cards now explicitly select
**one stored revenue window/region row per currency**, displaying its net, orders
and average exactly. No platform total or averaged-average is invented. The
regional distinction is retained as separate traces/rows within each currency.

DQ/alert panels and fabricated health indicators were removed from this dashboard.
Their underlying implementations and alert cooldown helpers remain unchanged.
Products show complete source identities and stored values, not an ambiguous
cross-window top-ten ranking.

## 6. Redis read contract

Default namespace `oip:m5:gold-serving`; active key `<namespace>:current`.
The dashboard uses the existing M5 `read_active_snapshot()` public primitive.
This is one coherent HGETALL including `@metadata`, `revenue:*` and `product:*`.
M5 validates row fields, encoded identity dimensions, money, windows, counts and
digest. M6 additionally requires:

- Object metadata with `SERVED`, cache/Gold contract `1.0`.
- Expected database and exactly `gold_serving` as source schema.
- Nonempty topic/root/manifest provenance.
- Positive integer Gold version, correctly shaped SHA-256 metadata.
- Aware `served_at` and exact nonnegative integer row counts.
- Optional configured expected source topic.

M6 contains malformed metadata shape errors from the decoder and treats them as
invalid cache, triggering fallback. No M5 writer/decoder logic was duplicated or
modified. No legacy analytical Redis keys are read. Namespace validation reuses
the existing `oip:m5:<lowercase-slug>` grammar.

## 7. PostgreSQL fallback contract

Fallback calls the existing read-only M5 PostgreSQL snapshot primitive with the
explicit schema `gold_serving`. It reads the two M4 analytical tables plus
`serving_state` metadata in one REPEATABLE READ transaction with READ ONLY set.
There are no dashboard SQL writes, schema migrations, cache rebuilds, legacy
`gold.*` fallbacks or data-lake access. The dashboard configuration rejects any
other source schema, including `gold_m4_final`.

All dimensions, Decimal values and timestamps are preserved. Source digest and
counts must match `serving_state`. Missing/unserved/invalid source data is an error,
not an empty financial result.

## 8. Currency behavior

Default UI selection is **All currencies (separate views)**. INR and USD appear
in separate revenue/product sections, chart axes/trace names, cards and table
currency columns. No mixed-currency sum, FX conversion or implicit INR default.

An optional `DASHBOARD_DEFAULT_CURRENCY` selects an explicit uppercase currency.
If absent from the snapshot, the UI warns and shows separate currency views.
Each product identity retains product ID, product name, currency and both window
boundaries. Same-window INR/USD rows and same-ID/different-name rows remain distinct.

## 9. Decimal behavior

The read layer holds Python Decimal for all money, including negative net and
nullable averages. Tables convert Decimal to exact eight-place **strings** before
Pandas/Arrow serialization. Cards format Decimal directly with eight places;
`None` average shows “not applicable”, not zero. No business arithmetic is done.

The only money-to-float conversion is the Plotly **y coordinate** in
`revenue_chart()`. Browser chart coordinates require approximate numeric values.
This value never feeds cards, tables, filtering, rankings or calculations. Exact
eight-place text is retained in custom hover data and the full detail table. The
UI discloses the presentation approximation. Full NUMERIC(38,8), negative values
and UTC microseconds are covered by tests.

## 10. Filter semantics

- Currency: all as separate groups, INR only, USD only, or any valid source currency.
- Region: exact match for revenue only. Product snapshots have no region column;
  the UI explicitly says the region selector does not filter products.
- Time: all snapshot windows by default, last 30 minutes relative to UTC now, or
  custom ISO-8601 aware timestamps. Inclusion uses `window_start >= start` and
  `window_start < end`. Window boundaries remain unchanged; no prorating occurs.
- KPI source row: selects a full revenue window/region within each currency,
  defaulting to the latest sorted matching row. It is not a platform-wide total.
- There was no existing product-selection filter to preserve. Full product rows
  remain separate across windows/names/currencies.

Invalid/naive time bounds or end <= start produce a filter error. Valid filters
with no matches show a distinct no-matching-rows message, not “0 revenue”.

## 11. Empty/error semantics

| Situation | Display/read behavior |
|---|---|
| Valid empty Redis/SQL snapshot | Provenance plus “Valid empty committed snapshot: zero revenue and product rows” |
| Redis key missing | PostgreSQL fallback, Redis status `missing` |
| Redis malformed/inconsistent | PostgreSQL fallback, Redis status `invalid` |
| Redis connection unavailable | PostgreSQL fallback, Redis status `unavailable` |
| Filter matches no rows | Explicit no-matching-rows messages for that currency/table |
| No valid source available | `DashboardUnavailable`, visible error; no tables or financial cards |

No exception is converted into an apparently valid empty dataset. UI errors are
sanitized; driver exception details and credential-bearing DSNs are not displayed.

## 12. Cache failure behavior

Validated cache is preferred and does not require a PostgreSQL connection. Missing,
malformed, wrong-source, incorrect-count, invalid-money, invalid-key or wrong-digest
cache is rejected, and a complete SQL snapshot is read. The dashboard never repairs
or repopulates Redis; explicit M5 serving handles rebuild outside the read path.

Actual cache deletion and corruption were performed only in
`oip:m5:verify-m6-20261004a`, then restored by M5. Redis stop/start exercised real
connection failure and recovery, not just mocked exceptions. After restoration,
the reader/UI returned to the Redis path with unchanged business values.

## 13. PostgreSQL failure behavior

With valid Redis, PostgreSQL is intentionally not queried. The UI says:

> Showing the cached source version. Current PostgreSQL freshness/availability
> has not been checked.

It displays source schema/topic, Gold version, digest and PostgreSQL `served_at`.
Actual PostgreSQL-container shutdown preserved cached dashboard availability.
This is a cached-version view, not a live-freshness claim.

With both services stopped, the reader raised an explicit failure; Streamlit
rendered an error with zero analytical tables and no financial cards. Unit tests
also cover missing/invalid Redis plus unavailable SQL. Recovery restored the
normal Redis path. No distributed transaction/freshness guarantee is claimed.

## 14. Production gold_serving verification

Initial read-only inspection:

```bash
docker.exe exec oip-postgres psql -U oip_user -d order_intelligence -c "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema='gold_serving';"
```

Returned zero tables. The verification-only script `scripts/qualify_m6_source.py`
therefore ran the unchanged M4 `serve_current_gold()` API into `gold_serving`
using the retained topic's current committed version 3, then independently checked
Gold/SQL equality using the existing M4 oracle. It did not copy `gold_m4_final`,
reset historical tables or enter the dashboard's dependency graph.

```bash
python3 -m scripts.capture_milestone6_check --step qualify
python3 -m scripts.capture_milestone6_check --step production
```

Qualification: **exit 0**, `2026-10-04T06:54:40.415332+00:00` through
`2026-10-04T06:55:09.491280+00:00`; M4 status `SERVED`, version 3, 18/13 rows.
M5 default production serving then returned `SERVED`, and later `NOOP`.
Production read and dashboard/UI verification all passed.

Provenance:

```text
manifest_path = s3a://gold/order-events-m3-20261003b/_commits/00000000000000000003.json
manifest_digest = 999d54805e0eef990514cfaa34101a88bcdee239e73c584be2424db6e7d5e424
snapshot_identity = ee26b6e28a8a3d5a8f1b52970af4dcfed5accac0b530c675818a1016d533420f
snapshot_digest = 26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231
```

These path values are provenance copied into the dashboard, never dashboard file reads.

## 15. Tests

`tests/unit/test_dashboard.py` adds **37 tests** (including parameterization).
No pre-existing dashboard tests were replaced. M5 raw Redis fixtures are encoded
independently, so the real M5 decoder is exercised. PostgreSQL failure cases use
controlled read errors; actual SQL/network fallback is separately exercised below.
Streamlit AppTest runs the real app and interacts with widgets.

| Required case | Evidence |
|---|---|
| 1 Valid Redis preferred | Preferred source, no PostgreSQL query |
| 2 Redis unavailable | Fallback with unavailable status |
| 3 Redis missing | Fallback with missing status |
| 4 Malformed metadata | Broken JSON / nonobject / missing-invalid provenance rejected |
| 5 Invalid digest | Fallback, no cache data displayed |
| 6 Invalid row | Float/NaN money, lowercase currency, invalid timestamp/count rejected |
| 7 SQL down + valid cache | No SQL read; explicit cached freshness statement |
| 8 Both unavailable | Explicit error, sanitized exception, no financial tables/cards |
| 9 INR/USD separate | Separate sections and typed rows |
| 10 Same-window currencies | Distinct INR and USD identities |
| 11 Product identity | Same ID, different names/currencies/window ends preserved |
| 12 Decimal exactness | Full precision/range and exact table text |
| 13 Negative net | Exact `INR -30.00000001` display |
| 14 Nullable average | None retained; not-applicable display |
| 15 Empty committed snapshot | Valid empty message, not failure |
| 16 Currency filter | INR/USD and all-currency results |
| 17 Region filter | Exact revenue match; products correctly unaffected |
| 18 Time filtering | Inclusive start/exclusive end, aware bounds, empty and malformed ranges |
| 19 Provenance | Schema/topic/version/digest/served_at retained and visible |
| 20 No legacy keys | Only `<namespace>:current` accessed |

Additional tests reject nonproduction/unsafe configuration, unexpected topics and
verify explicit default-currency UI behavior. During development, a Streamlit
interaction test found a loop-lambda capture bug in KPI row labels; binding each
formatter to its own currency/row list fixed it. Final interactions pass.

Commands actually run (workspace root):

```bash
python3 -m scripts.capture_milestone6_check --step tests
python3 -m scripts.capture_milestone6_check --step full-tests
python3 -m scripts.capture_milestone6_check --step checks
".venv/Scripts/python.exe" -m ruff check dashboard tests/unit/test_dashboard.py scripts/qualify_m6_source.py scripts/verify_milestone6.py scripts/capture_milestone6_check.py
".venv/Scripts/python.exe" -m compileall -q dashboard tests/unit/test_dashboard.py scripts/qualify_m6_source.py scripts/verify_milestone6.py scripts/capture_milestone6_check.py
".venv/Scripts/python.exe" -m pip check
docker.exe compose config --quiet
```

Final full relevant suite: **97 passed, 1 warning in 83.54s**, exit 0.
Breakdown: 26 M2/M3 + 10 M4 + 24 M5 + 37 M6. Ran via the retained container's
`spark-submit /opt/oip/scripts/run_spark_tests.py tests/unit` from
`2026-10-04T06:57:47.346622+00:00` to `2026-10-04T06:59:12.951312+00:00`.
Ruff/compile/Compose checks returned 0; host pip check returned 0.

## 16. Real runtime evidence

Ran actual PostgreSQL, Redis, Streamlit app rendering and Streamlit server startup.
`scripts/verify_milestone6.py` uses AppTest against real service clients, with no
mocked dashboard read results. It independently reconstructs rendered table data,
checks visible provenance and interacts with the USD selector. A separate real
Streamlit server subprocess was probed at `/` and `/_stcore/health` (HTTP 200,
health `ok`), then terminated. This is not a browser screenshot/pixel test.

Actual server command:

```bash
python3 -m streamlit run /opt/oip/dashboard/app.py --server.headless=true --server.address=127.0.0.1 --server.port=18506 --browser.gatherUsageStats=false
```

Runtime orchestration:

```bash
python3 -m scripts.capture_milestone6_check --step production
python3 -m scripts.capture_milestone6_check --step runtime
```

Evidence directory: `milestone6_evidence/20261004a/`. Each stem below has JSON
and paired log; JSON records exact commands, start/end timestamps and return code.
All listed commands returned **0**.

| Scenario | Evidence stem | Verified outcome |
|---|---|---|
| Production source qualification | `qualify-1791096909492879378` | Current Gold → gold_serving, 18/13 equality |
| A/J/K Production Redis/UI | `production-1791096920483196812` | Default namespace/schema; four-way digest match; HTTP server |
| A Isolated healthy | `healthy-1791096922805155765` | Redis preferred, INR/USD separate |
| B Deleted cache | `missing-1791096925100117955` | SQL fallback, Redis missing |
| C Rebuilt cache | `rebuild-1791096927529039673` | M5 rebuild, Redis preferred |
| Corrupt cache | `corrupt-1791096929609560551` | SQL fallback, Redis invalid; isolated cache restored |
| D Redis stopped | `redis-down-1791096947008070474` | Actual SQL fallback; complete equality |
| E Redis restored | `healthy-1791096955433947244` | Redis preferred again |
| F PostgreSQL stopped | `postgres-down-1791096958395437498` | Cached view with explicit provenance; digest unchanged |
| G Both stopped | `both-down-1791096980762795507` | Explicit unavailable UI, zero displayed tables |
| Recovery | `healthy-1791096989481611283` | Services restored, Redis preferred |
| H/I Currency/version | Healthy/production results | INR/USD views; version 3 and source visible |
| Final production after regressions | `production-1791099772267881934` | M5 NOOP; unchanged digest; HTTP/UI pass |
| Full test suite | `full-tests-1791097152951391322` | 97 passed |

Both-down verification ran from `06:55:58.888741` through `06:56:20.762536` UTC.
The final production run recorded `2026-10-04T07:42:52.109527+00:00`.
Redis/PostgreSQL stop/start commands are captured separately; recovery uses
`finally` and waits for healthchecks. No named volumes were removed or reset.

## 17. Independent equality evidence

The independent M5 verifier supplies direct psycopg2 reads and separately decoded
Redis keys/JSON; it does not use dashboard filter/display transformations.
The M6 verifier independently normalizes the typed dashboard rows and table values
rendered through Streamlit, comparing every source column and full logical identity.
It checks row counts, currency, timestamps, Decimal values, net/product revenue,
counts, provenance and deterministic SHA-256, not just row counts.

```text
PostgreSQL = Redis = DashboardReadService = Streamlit exact tables
SHA256: 26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231
revenue_by_minute: 18
product_rankings: 13
```

Fallback results match authoritative SQL. When SQL is deliberately stopped, the
cache/reader/UI are compared to the previously captured independent SQL baseline
digest and raw Redis contents. Production SQL tables/state/history are compared
before/after dashboard reads to assert no mutations.

Representative actual source values include INR net `100.00000000`, INR refunded
net `70.00000000`, USD net `20.00000000`, negative INR net `-30.00000000` and
`-7.00000000`, nullable average, and products retaining ID/name/window/currency.
The JSON evidence contains representative arrays plus exact provenance.

## 18. M1 regression

**PASS**, actual fresh-topic producer/registry/wire/Bronze checks, four records.

```bash
python3 -m scripts.capture_milestone3_check --step m1 --topic order-events-m1-m6-20261004a
```

Exit 0. Evidence:
`milestone3_evidence/order-events-m1-m6-20261004a/m1-1791097204787177104.json`.

## 19. M2 regression

**PASS**, fresh topic `order-events-m2-m6-20261004a`.

```bash
python3 -m scripts.capture_milestone3_check --step m2-initial --topic order-events-m2-m6-20261004a
python3 -m scripts.capture_milestone3_check --step m2-restart --topic order-events-m2-m6-20261004a
python3 -m scripts.capture_milestone3_check --step m2-recovery --topic order-events-m2-m6-20261004a
```

All exit 0. Initial 32 Bronze / 11 Silver / 19 DLQ / 32 audit;
restart 35 / 12 / 19 / 70; recovery 1 Silver / 1 DLQ with recovery guards.
Evidence under `milestone3_evidence/order-events-m2-m6-20261004a/`:
`m2-initial-1791097243714858877`, `m2-restart-1791097281901111331`,
`m2-recovery-1791097306835127127` (JSON/log).

## 20. M3 regression

**PASS**, retained topic replay. 35 Bronze, 2 DLQ, 33 unique Gold events,
18 revenue rows and 13 product rows; committed version 3 unchanged. Physical
Silver rows reached 2112 due to deliberate retained duplicate-file replay, while
Gold source identity deduplication preserved the logical state.

```bash
python3 -m scripts.capture_milestone3_check --step replay --topic order-events-m3-20261003b
```

Exit 0. Evidence:
`milestone3_evidence/order-events-m3-20261003b/replay-1791099639428445145.json`.
Its legacy oracle-normalized digest stayed
`8263508ee79077227be47a2a550111b4b65e45daf01e28bed149eae5c0e57579`;
M4/M5/M6 canonical digest stayed `26ec1b51…2a231`.

## 21. M4 regression

**PASS**, fresh isolated `gold_m4_m6reg20261004` schema; production source untouched.

```bash
python3 -m scripts.capture_milestone4_check --phase initial --topic order-events-m3-20261003b --schema gold_m4_m6reg20261004
python3 -m scripts.capture_milestone4_check --phase retry --topic order-events-m3-20261003b --schema gold_m4_m6reg20261004
python3 -m scripts.capture_milestone4_check --phase failure --topic order-events-m3-20261003b --schema gold_m4_m6reg20261004
python3 -m scripts.capture_milestone4_check --phase old --topic order-events-m3-20261003b --schema gold_m4_m6reg20261004
python3 -m scripts.capture_milestone4_check --phase corrupt --topic order-events-m3-20261003b --schema gold_m4_m6reg20261004
```

All exit 0: v1→v3, NOOP retry, transactional rollback, stale rejection,
corrupt/uncommitted rejection. Final 18/13 rows and canonical digest match.
Evidence under `milestone4_evidence/order-events-m3-20261003b/`:
`initial-1791099648791006582`, `retry-1791099667091272105`,
`failure-1791099680044271949`, `old-1791099695719923359`,
`corrupt-1791099711627677206`.

## 22. M5 regression

**PASS**, 24 automated M5 tests actually ran in the full suite, plus separate
runtime serving/retry/rebuild/outage checks sourced from production `gold_serving`.

```bash
python3 -m scripts.capture_milestone5_check --step serve --schema gold_serving --namespace oip:m5:verify-m6-m5reg-20261004a
python3 -m scripts.capture_milestone5_check --step serve --schema gold_serving --namespace oip:m5:verify-m6-m5reg-20261004a
python3 -m scripts.capture_milestone5_check --step rebuild --schema gold_serving --namespace oip:m5:verify-m6-m5reg-20261004a
python3 -m scripts.capture_milestone5_check --step outages --schema gold_serving --namespace oip:m5:verify-m6-m5reg-20261004a
```

All exit 0; PostgreSQL unchanged, row/digest equality, retry NOOP and successful
rebuild. Actual Redis and PostgreSQL outages/recovery were repeated.
Evidence directory: `milestone5_evidence/oip-m5-verify-m6-m5reg-20261004a/`.
Key stems: `serve-1791099740553490525`, `serve-1791099741290589994`,
`rebuild-1791099742038214686`, `redis-down-1791099747943752338`,
`postgres-down-1791099759703770574`, `serve-1791099769481266413`.

## 23. Dependency review

| Dependency | Version | Decision |
|---|---|---|
| Streamlit | 1.40.1 | KEEP |
| Pandas | 2.2.3 | KEEP |
| Plotly | 5.24.1 | KEEP |
| redis-py | 5.2.0 | KEEP |
| SQLAlchemy / psycopg2 | 2.0.36 / 2.9.10 | KEEP |
| Redis / PostgreSQL | 7.4.9 / 16.14 | KEEP |
| Spark, Kafka/Avro connectors | 3.5.9 | NO CHANGE |
| Hadoop / AWS SDK | 3.3.4 / 1.12.262 | NO CHANGE |
| fastavro / confluent-kafka | 1.12.2 / 2.15.1 | NO CHANGE |
| Java / Scala | 17 / 2.12 | NO CHANGE |

No dependency upgrade was required; no UPDATE NOW/LATER was selected for M6.
`requirements.txt` is unchanged. The host already had the pinned UI packages;
the minimal verification container lacked them. Installed existing project pins:

```bash
docker.exe exec oip-modernization-verify python3 -m pip install streamlit==1.40.1 pandas==2.2.3 plotly==5.24.1
docker.exe exec oip-modernization-verify python3 -m pip install tenacity==9.0.0
```

The first installation resolved transitive packages (including Streamlit-required
packaging/cachetools bounds). Its resolver initially chose tenacity 9.1.4 as a
transitive dependency; this was set to the existing project pin 9.0.0 before the
full suite/regressions. No core verified pins were upgraded. Container Python
remains 3.10.12; host uses the retained project venv.

Host pip check: exit 0, no broken requirements. Container pip check: exit 1 for
the previously recorded unrelated `pygobject 3.42.1 requires pycairo` issue.
The full suite retains the existing Authlib/httpx deprecation warning. Neither
was introduced by dashboard source changes. UI version evidence:
`milestone6_evidence/20261004a/versions-1791097154031227740.log`.

## 24. Files changed

- `dashboard/app.py` — coherent M6 Streamlit migration, exact tables/cards,
  per-currency views, filters, provenance and explicit failure states.
- `dashboard/read_service.py` — new read-only Redis-preferred/SQL-fallback adapter.
- `tests/unit/test_dashboard.py` — 37 focused contract/UI tests.
- `scripts/qualify_m6_source.py` — verification-only production M4 source qualification.
- `scripts/verify_milestone6.py` — independent equality, actual UI and HTTP verifier.
- `scripts/capture_milestone6_check.py` — append-only runtime/test/outage evidence.
- `.env.example` — optional currency/topic/refresh display configuration.
- `README.md`, `PHASE_STATUS.md` — scoped M6 usage and acceptance references.
- `milestone6_dashboard_report.md` — this report.
- Append-only `milestone6_evidence/` and M1–M5 regression evidence files.

No Git commit was created; this workspace is not a Git repository.

## 25. Files intentionally unchanged

M1–M5 production code, including `storage/redis_client.py`,
`storage/postgres_redis_serving.py`, `storage/gold_postgres_serving.py`,
`storage/gold_store.py`, their contracts/DDL, and existing tests/verifiers.
Producer, Bronze, DQ, Silver, Gold aggregation, alert engines/cooldowns,
Prometheus/Grafana, CI/deployment, Compose, active `.env`, requirements, main
dispatcher and unrelated helpers were not modified.

Legacy Redis `get_summary`, `get_leaderboard` and `is_alert_active` imports were
removed from the dashboard only; the functions still exist for legacy consumers.
No concrete M1–M5 production compatibility fix was needed: M6 adds consumer
provenance checks around the existing public reader rather than changing it.

## 26. Remaining limitations

- Cached data may lag PostgreSQL; M6 deliberately does not query PostgreSQL to
  assert freshness on valid-cache reads. Provenance exposes the displayed version.
- Full snapshots are read in memory within M5's bounded design. No new scalability
  or latency guarantee is made.
- Chart coordinates are approximate presentation-only floats; exact original money
  is available in hover/details/cards/tables. No browser pixel-accuracy claim.
- No cache refresh daemon or scheduler. Display polling does not synchronize M5.
- Products cannot be filtered by region because M4 does not provide that dimension.
- Tests use actual Streamlit AppTest rendering plus HTTP startup, not a separate
  browser automation/screenshot suite.
- Current production data is the retained verification topic's committed snapshot;
  the dashboard clearly shows its topic/time/version rather than pretending it is
  a live incoming order stream.

## 27. Known risks

Existing Redis allkeys-lru eviction can cause a full-cache miss and increased SQL
fallback reads. Repeated outages incur the existing connection timeouts. Source
changes during independent verification may require re-running comparisons, while
each dashboard read still chooses one coherent snapshot. Configuration must refer
to the intended database/namespace; optional topic pinning further constrains it.

No passwords/DSNs are embedded in source or recorded as command arguments. Source
schema/namespace/currency/refresh inputs are validated. Invalid source errors are
sanitized. No FLUSHALL/FLUSHDB, volume reset or historical data deletion occurred.
Only explicitly isolated caches were deleted/corrupted during M6 failure tests.
Named volumes and retained historical fixtures remain available.

After final verification, the six OIP verification services started for this run
were stopped with exit 0; `docker ps -a` confirmed all six stopped. Containers,
volumes, the qualified `gold_serving` source and production Redis cache were kept.
The Streamlit smoke-test subprocess had already been terminated by its verifier.

```bash
docker.exe stop oip-modernization-verify oip-schema-registry oip-kafka oip-minio oip-redis oip-postgres
docker.exe ps -a --filter name=oip --format '{{.Names}}\t{{.Status}}'
```

Startup from the project root, with existing environment loaded:

```bash
python -m streamlit run dashboard/app.py
```

Required source schema is `gold_serving`; production namespace defaults to
`oip:m5:gold-serving`. Optional `DASHBOARD_DEFAULT_CURRENCY` is blank by default;
`DASHBOARD_SOURCE_TOPIC` is an optional topic pin; `DASHBOARD_REFRESH_SECONDS`
defaults to 10 and accepts integers 1–3600. The UI never silently selects INR.

## 28. Exact next milestone

**Milestone 7 — Currency-aware alert consumption of authoritative M4 PostgreSQL
analytical data, with explicit source-version and cooldown semantics.**
This is the single next milestone; it was not implemented. M6 is frozen after
the verification and report above.
