-- =============================================================================
-- Order Intelligence Platform — PostgreSQL Initialisation Script
-- Runs automatically on first container start via docker-entrypoint-initdb.d
--
-- WARNING: This script only runs when the data directory is EMPTY.
-- To re-run after first start: make db-reset (destroys all data)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- SCHEMAS
-- Bronze: audit / lineage metadata (not raw data — that lives in MinIO)
-- Silver: data quality run logs
-- Gold:   aggregated business metrics (primary analytics tables)
-- Alerts: alert log and alert state
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS alerts;

COMMENT ON SCHEMA bronze IS 'Audit metadata for Bronze layer events stored in MinIO';
COMMENT ON SCHEMA silver IS 'Data quality run results and Silver layer metadata';
COMMENT ON SCHEMA gold   IS 'Pre-aggregated business metrics — primary dashboard source';
COMMENT ON SCHEMA alerts IS 'Alert log, active alert state, and anomaly baseline';

-- ---------------------------------------------------------------------------
-- EXTENSIONS
-- ---------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";   -- uuid_generate_v4()
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";  -- query performance

-- ---------------------------------------------------------------------------
-- GOLD LAYER — Revenue by minute
-- Primary source for dashboard KPI cards and revenue time-series chart.
-- Composite PK ensures idempotent upserts from Spark (INSERT ON CONFLICT DO UPDATE).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.revenue_by_minute (
    window_start         TIMESTAMPTZ     NOT NULL,
    window_end           TIMESTAMPTZ     NOT NULL,
    region               VARCHAR(50)     NOT NULL,
    order_count          INTEGER         NOT NULL DEFAULT 0,
    gross_revenue        NUMERIC(14, 2)  NOT NULL DEFAULT 0.00,
    refund_amount        NUMERIC(14, 2)  NOT NULL DEFAULT 0.00,
    cancellation_amount  NUMERIC(14, 2)  NOT NULL DEFAULT 0.00,
    net_revenue          NUMERIC(14, 2)  GENERATED ALWAYS AS
                             (gross_revenue - refund_amount - cancellation_amount) STORED,
    failed_count         INTEGER         NOT NULL DEFAULT 0,
    avg_order_value      NUMERIC(10, 2)  NOT NULL DEFAULT 0.00,
    inserted_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_revenue_by_minute PRIMARY KEY (window_start, region)
);

CREATE INDEX IF NOT EXISTS idx_revenue_by_minute_window
    ON gold.revenue_by_minute (window_start DESC);

CREATE INDEX IF NOT EXISTS idx_revenue_by_minute_region
    ON gold.revenue_by_minute (region, window_start DESC);

COMMENT ON TABLE gold.revenue_by_minute IS
    'Tumbling 1-minute window revenue aggregates. Net revenue = gross - refunds - paid cancellations.';

-- ---------------------------------------------------------------------------
-- GOLD LAYER — Product rankings (5-minute windows)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.product_rankings (
    window_start     TIMESTAMPTZ     NOT NULL,
    window_end       TIMESTAMPTZ     NOT NULL,
    product_id       VARCHAR(30)     NOT NULL,
    product_name     VARCHAR(150)    NOT NULL,
    units_sold       INTEGER         NOT NULL DEFAULT 0,
    revenue          NUMERIC(14, 2)  NOT NULL DEFAULT 0.00,
    rank_by_revenue  INTEGER,
    rank_by_units    INTEGER,
    inserted_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_product_rankings PRIMARY KEY (window_start, product_id)
);

CREATE INDEX IF NOT EXISTS idx_product_rankings_window
    ON gold.product_rankings (window_start DESC, rank_by_revenue ASC);

COMMENT ON TABLE gold.product_rankings IS
    'Top products by revenue and units sold per 5-minute window.';

-- ---------------------------------------------------------------------------
-- GOLD LAYER — Regional heatmap (15-minute windows)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.region_heatmap (
    window_start      TIMESTAMPTZ     NOT NULL,
    window_end        TIMESTAMPTZ     NOT NULL,
    region            VARCHAR(50)     NOT NULL,
    order_count       INTEGER         NOT NULL DEFAULT 0,
    net_revenue       NUMERIC(14, 2)  NOT NULL DEFAULT 0.00,
    pct_of_total      NUMERIC(6, 3),   -- percentage of platform revenue this window
    avg_order_value   NUMERIC(10, 2),
    inserted_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_region_heatmap PRIMARY KEY (window_start, region)
);

COMMENT ON TABLE gold.region_heatmap IS
    'Regional revenue breakdown for heatmap visualisation. 15-minute windows.';

-- ---------------------------------------------------------------------------
-- GOLD LAYER — Order lifecycle funnel (1-hour windows)
-- Tracks conversion through the order state machine.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.lifecycle_funnel (
    window_start         TIMESTAMPTZ     NOT NULL,
    window_end           TIMESTAMPTZ     NOT NULL,
    event_type           VARCHAR(30)     NOT NULL,
    event_count          INTEGER         NOT NULL DEFAULT 0,
    conversion_rate_pct  NUMERIC(5, 2),  -- vs ORDER_CREATED in same window
    inserted_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_lifecycle_funnel PRIMARY KEY (window_start, event_type)
);

COMMENT ON TABLE gold.lifecycle_funnel IS
    'Count of each lifecycle event type per hour for funnel analysis.';

-- ---------------------------------------------------------------------------
-- GOLD LAYER — Anomaly baseline (pre-computed nightly)
-- Stores statistical parameters per metric per region per hour-of-week slot.
-- Used by Layer 2 (z-score / EWMA) anomaly detection.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.anomaly_baseline (
    metric_name       VARCHAR(50)    NOT NULL,
    region            VARCHAR(50)    NOT NULL,
    day_of_week       SMALLINT       NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),
    hour_of_day       SMALLINT       NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    rolling_mean      NUMERIC(14, 2) NOT NULL,
    rolling_stddev    NUMERIC(14, 4) NOT NULL,
    p50               NUMERIC(14, 2),
    p95               NUMERIC(14, 2),
    sample_count      INTEGER        NOT NULL DEFAULT 0,
    computed_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_anomaly_baseline PRIMARY KEY (metric_name, region, day_of_week, hour_of_day)
);

COMMENT ON TABLE gold.anomaly_baseline IS
    'Pre-computed statistical baseline per metric/region/time-slot. Recomputed nightly.';

-- ---------------------------------------------------------------------------
-- SILVER LAYER — Data quality run log
-- One row per Spark micro-batch DQ evaluation.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver.dq_run_log (
    run_id           UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    batch_id         VARCHAR(100)    NOT NULL,
    run_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    total_records    INTEGER         NOT NULL DEFAULT 0,
    passed_records   INTEGER         NOT NULL DEFAULT 0,
    failed_records   INTEGER         NOT NULL DEFAULT 0,
    dlq_records      INTEGER         NOT NULL DEFAULT 0,
    pass_rate_pct    NUMERIC(5, 2)   GENERATED ALWAYS AS
                         (CASE WHEN total_records > 0
                               THEN (passed_records::NUMERIC / total_records) * 100
                               ELSE 0 END) STORED,
    failure_breakdown JSONB,          -- {"null_key": 3, "bad_timestamp": 1}
    spark_watermark  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_dq_run_log_run_at
    ON silver.dq_run_log (run_at DESC);

COMMENT ON TABLE silver.dq_run_log IS
    'Data quality run results per Spark micro-batch. Drives the DQ pass-rate Grafana panel.';

-- ---------------------------------------------------------------------------
-- ALERTS SCHEMA — Alert log
-- One row per fired alert. Used for deduplication, audit, and dashboard banner.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS alerts.alert_log (
    alert_id         UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    alert_type       VARCHAR(60)     NOT NULL,  -- revenue_drop, failed_spike, dlq_overflow
    severity         VARCHAR(10)     NOT NULL CHECK (severity IN ('critical', 'warning', 'info')),
    region           VARCHAR(50),               -- NULL means platform-wide
    window_start     TIMESTAMPTZ     NOT NULL,
    current_value    NUMERIC(18, 4)  NOT NULL,
    baseline_value   NUMERIC(18, 4),
    deviation_pct    NUMERIC(8, 2),             -- negative = drop, positive = spike
    layer            SMALLINT        NOT NULL CHECK (layer IN (1, 2, 3)),  -- detection layer
    message          TEXT,
    notified_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    channel          VARCHAR(20)     NOT NULL CHECK (channel IN ('slack', 'pagerduty', 'log')),
    resolved_at      TIMESTAMPTZ,               -- NULL = still active
    resolved_by      VARCHAR(30)                -- 'auto' or engineer name
);

CREATE INDEX IF NOT EXISTS idx_alert_log_notified_at
    ON alerts.alert_log (notified_at DESC);

CREATE INDEX IF NOT EXISTS idx_alert_log_type_region
    ON alerts.alert_log (alert_type, region, notified_at DESC);

COMMENT ON TABLE alerts.alert_log IS
    'Full audit log of every fired alert. Used for deduplication and dashboard banner.';

-- ---------------------------------------------------------------------------
-- ALERTS SCHEMA — Active alert state
-- Lightweight table checked before firing a new alert (cooldown enforcement).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS alerts.active_alert_state (
    alert_type       VARCHAR(60)     NOT NULL,
    region           VARCHAR(50)     NOT NULL DEFAULT 'all',
    last_fired_at    TIMESTAMPTZ     NOT NULL,
    alert_log_id     UUID            REFERENCES alerts.alert_log(alert_id),
    CONSTRAINT pk_active_alert_state PRIMARY KEY (alert_type, region)
);

COMMENT ON TABLE alerts.active_alert_state IS
    'Current cooldown state per alert type and region. Prevents alert storms.';

-- ---------------------------------------------------------------------------
-- BRONZE SCHEMA — Ingestion batch log
-- Records each Spark micro-batch write to MinIO Bronze.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.ingestion_log (
    batch_id           VARCHAR(100)   PRIMARY KEY,
    topic              VARCHAR(100)   NOT NULL,
    partition_offsets  JSONB,         -- {"0": 10042, "1": 9981, ...}
    record_count       INTEGER        NOT NULL DEFAULT 0,
    bronze_path        TEXT,          -- s3a://bronze/order-events/year=.../
    ingested_at        TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    spark_batch_id     BIGINT
);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_topic_time
    ON bronze.ingestion_log (topic, ingested_at DESC);

COMMENT ON TABLE bronze.ingestion_log IS
    'Metadata log for each Bronze layer write. Does not store the actual events (those are in MinIO Parquet).';

-- ---------------------------------------------------------------------------
-- GRANTS — oip_user gets full access to all schemas
-- (oip_user is the application user set via POSTGRES_USER)
-- ---------------------------------------------------------------------------

GRANT USAGE ON SCHEMA bronze, silver, gold, alerts TO oip_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO oip_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO oip_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold   TO oip_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA alerts TO oip_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze TO oip_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA silver TO oip_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA gold   TO oip_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA alerts TO oip_user;

-- Default privileges for future tables created in each schema
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES    TO oip_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES    TO oip_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT ALL ON TABLES    TO oip_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA alerts GRANT ALL ON TABLES    TO oip_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON SEQUENCES TO oip_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON SEQUENCES TO oip_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT ALL ON SEQUENCES TO oip_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA alerts GRANT ALL ON SEQUENCES TO oip_user;

-- ---------------------------------------------------------------------------
-- VERIFICATION — Confirms init script completed successfully
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    schema_count INT;
    table_count  INT;
BEGIN
    SELECT COUNT(*) INTO schema_count
    FROM information_schema.schemata
    WHERE schema_name IN ('bronze', 'silver', 'gold', 'alerts');

    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables
    WHERE table_schema IN ('bronze', 'silver', 'gold', 'alerts');

    RAISE NOTICE '=== OIP Database Init Complete ===';
    RAISE NOTICE 'Schemas created: %/4', schema_count;
    RAISE NOTICE 'Tables  created: %/9', table_count;
    RAISE NOTICE 'Database: order_intelligence';
    RAISE NOTICE '==================================';
END $$;
