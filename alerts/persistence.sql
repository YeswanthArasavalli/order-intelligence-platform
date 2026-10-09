-- M7 additive schema template. {schema} is a validated, quoted identifier.
-- Analytical gold_serving tables and legacy alerts.* are never altered here.
CREATE SCHEMA IF NOT EXISTS {schema};
CREATE TABLE IF NOT EXISTS {schema}.alert_definitions (
    rule_id TEXT PRIMARY KEY,
    definition_digest CHAR(64) NOT NULL,
    definition JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS {schema}.source_snapshots (
    gold_version BIGINT PRIMARY KEY CHECK (gold_version > 0),
    provenance JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS {schema}.alert_evaluations (
    evaluation_id CHAR(64) PRIMARY KEY,
    rule_id TEXT NOT NULL REFERENCES {schema}.alert_definitions(rule_id),
    gold_version BIGINT NOT NULL REFERENCES {schema}.source_snapshots(gold_version),
    scope_id CHAR(64) NOT NULL,
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    dimensions JSONB NOT NULL,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    evidence JSONB NOT NULL,
    qualifies BOOLEAN,
    outcome TEXT NOT NULL CHECK (outcome IN ('FIRED','SUPPRESSED','OK','RECOVERED','NO_DATA','NO_BASELINE','DISABLED')),
    previous_status TEXT NOT NULL CHECK (previous_status IN ('UNKNOWN','OK','ACTIVE')),
    resulting_status TEXT NOT NULL CHECK (resulting_status IN ('UNKNOWN','OK','ACTIVE')),
    evaluated_at TIMESTAMPTZ NOT NULL,
    last_fired_before TIMESTAMPTZ,
    cooldown_until TIMESTAMPTZ,
    UNIQUE (rule_id, gold_version)
);
CREATE INDEX IF NOT EXISTS idx_m7_evaluations_scope ON {schema}.alert_evaluations(scope_id, evaluated_at);
CREATE TABLE IF NOT EXISTS {schema}.alert_state (
    scope_id CHAR(64) PRIMARY KEY,
    rule_id TEXT NOT NULL UNIQUE REFERENCES {schema}.alert_definitions(rule_id),
    currency TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    dimensions JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('UNKNOWN','OK','ACTIVE')),
    last_fired_at TIMESTAMPTZ,
    last_evaluated_at TIMESTAMPTZ NOT NULL,
    last_evaluation_id CHAR(64) NOT NULL REFERENCES {schema}.alert_evaluations(evaluation_id)
);
CREATE TABLE IF NOT EXISTS {schema}.alert_events (
    event_id CHAR(64) PRIMARY KEY,
    evaluation_id CHAR(64) NOT NULL UNIQUE REFERENCES {schema}.alert_evaluations(evaluation_id),
    severity TEXT NOT NULL CHECK (severity IN ('INFO','WARNING','CRITICAL')),
    fired_at TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type = 'FIRED')
);
CREATE INDEX IF NOT EXISTS idx_m7_events_fired ON {schema}.alert_events(fired_at, event_id);
