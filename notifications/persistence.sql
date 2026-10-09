-- Additive M8 tables; {schema}/{source} are validated identifiers, never values.
CREATE SCHEMA IF NOT EXISTS {schema};
CREATE TABLE IF NOT EXISTS {schema}.source_binding (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    source_database TEXT NOT NULL,
    source_schema TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS {schema}.notification_deliveries (
    notification_id CHAR(64) PRIMARY KEY,
    alert_event_id CHAR(64) NOT NULL REFERENCES {source}.alert_events(event_id),
    provider TEXT NOT NULL,
    channel TEXT NOT NULL,
    destination TEXT NOT NULL,
    configuration JSONB NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING','SENDING','DELIVERED','RETRYABLE_FAILURE','PERMANENT_FAILURE')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 10),
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    next_attempt_at TIMESTAMPTZ,
    claimed_at TIMESTAMPTZ,
    lease_until TIMESTAMPTZ,
    claim_token CHAR(32),
    delivered_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    provider_reference TEXT,
    terminal_reason TEXT,
    UNIQUE(alert_event_id,provider,channel,destination),
    CHECK (attempt_count <= max_attempts),
    CHECK ((status='SENDING') = (claim_token IS NOT NULL AND lease_until IS NOT NULL AND claimed_at IS NOT NULL)),
    CHECK ((status IN ('PENDING','RETRYABLE_FAILURE')) = (next_attempt_at IS NOT NULL)),
    CHECK ((status='DELIVERED') = (delivered_at IS NOT NULL)),
    CHECK ((status='PERMANENT_FAILURE') = (failed_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_m8_due ON {schema}.notification_deliveries(provider,channel,destination,status,next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_m8_lease ON {schema}.notification_deliveries(lease_until) WHERE status='SENDING';
CREATE TABLE IF NOT EXISTS {schema}.notification_attempts (
    notification_id CHAR(64) NOT NULL REFERENCES {schema}.notification_deliveries(notification_id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number BETWEEN 1 AND 10),
    claim_token CHAR(32) NOT NULL UNIQUE,
    provider TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    outcome TEXT NOT NULL CHECK (outcome IN ('SENDING','DELIVERED','RETRYABLE_FAILURE','PERMANENT_FAILURE','UNKNOWN')),
    provider_status INTEGER,
    provider_reference TEXT,
    error_class TEXT,
    sanitized_message TEXT,
    duration_ms DOUBLE PRECISION CHECK (duration_ms >= 0),
    retryable BOOLEAN,
    PRIMARY KEY(notification_id,attempt_number),
    CHECK ((outcome='SENDING') = (completed_at IS NULL))
);
