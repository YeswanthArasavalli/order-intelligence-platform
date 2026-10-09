-- Milestone 4 PostgreSQL serving schema.
-- This is an additive migration. It deliberately does not alter legacy gold.*
-- tables because their keys/precision predate the verified M3 Gold contract.
-- Runtime serving also runs the equivalent idempotent DDL for isolated schemas.

CREATE SCHEMA IF NOT EXISTS gold_serving;

CREATE TABLE IF NOT EXISTS gold_serving.revenue_by_minute (
    window_start          TIMESTAMPTZ    NOT NULL,
    window_end            TIMESTAMPTZ    NOT NULL,
    region                TEXT          NOT NULL,
    currency              TEXT          NOT NULL,
    order_count           BIGINT        NOT NULL,
    failed_count          BIGINT        NOT NULL,
    refunded_event_count  BIGINT        NOT NULL,
    gross_revenue         NUMERIC(38,8) NOT NULL,
    refund_amount         NUMERIC(38,8) NOT NULL,
    cancellation_amount   NUMERIC(38,8) NOT NULL,
    net_revenue            NUMERIC(38,8) NOT NULL,
    avg_order_value       NUMERIC(38,8),
    CONSTRAINT pk_m4_revenue_by_minute
        PRIMARY KEY (window_start, window_end, region, currency),
    CONSTRAINT ck_m4_revenue_currency CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE TABLE IF NOT EXISTS gold_serving.product_rankings (
    window_start  TIMESTAMPTZ    NOT NULL,
    window_end    TIMESTAMPTZ    NOT NULL,
    product_id    TEXT           NOT NULL,
    product_name  TEXT           NOT NULL,
    currency      TEXT           NOT NULL,
    units_sold    BIGINT         NOT NULL,
    revenue       NUMERIC(38,8)  NOT NULL,
    CONSTRAINT pk_m4_product_rankings
        PRIMARY KEY (window_start, window_end, product_id, product_name, currency),
    CONSTRAINT ck_m4_product_currency CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE TABLE IF NOT EXISTS gold_serving.serving_state (
    singleton_id      SMALLINT      PRIMARY KEY CHECK (singleton_id = 1),
    source_topic      TEXT          NOT NULL,
    source_root       TEXT          NOT NULL,
    gold_contract_version TEXT      NOT NULL,
    gold_version      BIGINT        NOT NULL,
    manifest_path     TEXT          NOT NULL,
    manifest_digest   CHAR(64)      NOT NULL,
    snapshot_identity TEXT          NOT NULL,
    snapshot_digest   CHAR(64)      NOT NULL,
    revenue_rows      BIGINT        NOT NULL,
    product_rows      BIGINT        NOT NULL,
    served_at         TIMESTAMPTZ   NOT NULL,
    serving_status    VARCHAR(16)   NOT NULL CHECK (serving_status = 'SERVED')
);

CREATE TABLE IF NOT EXISTS gold_serving.serving_history (
    source_topic      TEXT          NOT NULL,
    source_root       TEXT          NOT NULL,
    gold_contract_version TEXT      NOT NULL,
    gold_version      BIGINT        NOT NULL,
    manifest_path     TEXT          NOT NULL UNIQUE,
    manifest_digest   CHAR(64)      NOT NULL,
    snapshot_identity TEXT          NOT NULL,
    snapshot_digest   CHAR(64)      NOT NULL,
    revenue_rows      BIGINT        NOT NULL,
    product_rows      BIGINT        NOT NULL,
    served_at         TIMESTAMPTZ   NOT NULL,
    serving_status    VARCHAR(16)   NOT NULL CHECK (serving_status = 'SERVED'),
    PRIMARY KEY (source_topic, gold_version)
);

CREATE INDEX IF NOT EXISTS idx_m4_history_served_at
    ON gold_serving.serving_history (served_at DESC);
