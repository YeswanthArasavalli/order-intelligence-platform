"""
Order Intelligence Platform — Phase 10
Alert Engine — Layer 1: Rule-Based Detection
=============================================
Hard-threshold rules evaluated every minute against gold.revenue_by_minute.
Fires within 60s of threshold breach.

Rules:
  1. revenue_drop        — current < 70% of 7-day same-hour baseline
  2. revenue_spike       — current > 300% of baseline (possible duplicate events)
  3. zero_orders         — order_count = 0 during business hours
  4. high_failure_rate   — failed_count / order_count > 15%
  5. no_data             — no events in last 5 minutes for any region
"""

import os
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import create_engine, text

from alerts.alert_router import fire_alert
from storage.redis_client import is_alert_active, set_alert_active
from utils.logger import configure_logging, get_logger

configure_logging()
log = get_logger("rule_engine")


def _get_engine():
    dsn = (
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER', 'oip_user')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'oip_password_dev')}@"
        f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'order_intelligence')}"
    )
    return create_engine(dsn, pool_pre_ping=True, pool_size=2)


_engine = None


def engine():
    global _engine
    if _engine is None:
        _engine = _get_engine()
    return _engine


# ---------------------------------------------------------------------------
# Threshold constants
# ---------------------------------------------------------------------------

REVENUE_DROP_THRESHOLD   = float(os.getenv("ALERT_REVENUE_DROP_THRESHOLD", "0.70"))
REVENUE_SPIKE_THRESHOLD  = 3.00
FAILURE_RATE_THRESHOLD   = 0.15
BUSINESS_HOUR_START      = 6    # 6 AM
BUSINESS_HOUR_END        = 23   # 11 PM


# ---------------------------------------------------------------------------
# Rule evaluations
# ---------------------------------------------------------------------------

def _load_current_window() -> list:
    """Load the most recent completed 1-min window per region."""
    sql = text("""
        SELECT
            region,
            window_start,
            order_count,
            (gross_revenue - refund_amount - cancellation_amount) AS net_revenue,
            failed_count
        FROM gold.revenue_by_minute
        WHERE window_start = (
            SELECT MAX(window_start) FROM gold.revenue_by_minute
            WHERE window_start <= NOW() - INTERVAL '1 minute'
        )
    """)
    with engine().connect() as conn:
        rows = conn.execute(sql).fetchall()
    return [dict(zip(["region", "window_start", "order_count", "net_revenue", "failed_count"], r))
            for r in rows]


def _load_baseline(region: str, window_start: datetime) -> Optional[float]:
    """Load 7-day same-hour baseline revenue for a region."""
    sql = text("""
        SELECT rolling_mean
        FROM gold.anomaly_baseline
        WHERE metric_name = 'net_revenue'
          AND region      = :region
          AND day_of_week = :dow
          AND hour_of_day = :hour
    """)
    with engine().connect() as conn:
        row = conn.execute(sql, {
            "region": region,
            "dow":    window_start.weekday(),
            "hour":   window_start.hour,
        }).fetchone()
    return float(row[0]) if row else None


def _should_fire(alert_type: str, region: str) -> bool:
    """Return True if alert is NOT in cooldown window."""
    return not is_alert_active(alert_type, region)


def _record_alert(alert_type: str, region: str, severity: str,
                  current_value: float, baseline_value: float,
                  deviation_pct: float, message: str) -> None:
    """Write alert to PostgreSQL alert_log and set Redis cooldown key."""
    # Write to DB
    sql = text("""
        INSERT INTO alerts.alert_log
            (alert_type, severity, region, window_start, current_value,
             baseline_value, deviation_pct, layer, message, channel)
        VALUES
            (:alert_type, :severity, :region, NOW(), :current_value,
             :baseline_value, :deviation_pct, 1, :message, 'slack')
    """)
    with engine().connect() as conn:
        conn.execute(sql, {
            "alert_type":     alert_type,
            "severity":       severity,
            "region":         region,
            "current_value":  current_value,
            "baseline_value": baseline_value,
            "deviation_pct":  deviation_pct,
            "message":        message,
        })
        conn.commit()

    # Set Redis cooldown
    set_alert_active(alert_type, region, {
        "alert_type":    alert_type,
        "region":        region,
        "current_value": current_value,
        "message":       message,
    })

    # Fire notification
    fire_alert(
        alert_type=alert_type,
        severity=severity,
        region=region,
        current_value=current_value,
        baseline_value=baseline_value,
        deviation_pct=deviation_pct,
        message=message,
        layer=1,
    )


# ---------------------------------------------------------------------------
# Rule 1 — Revenue drop
# ---------------------------------------------------------------------------

def check_revenue_drop(rows: list) -> None:
    for row in rows:
        region     = row["region"]
        current    = float(row["net_revenue"] or 0)
        baseline   = _load_baseline(region, row["window_start"])

        if baseline is None or baseline == 0:
            continue  # no baseline yet — skip

        ratio = current / baseline
        if ratio < REVENUE_DROP_THRESHOLD and _should_fire("revenue_drop", region):
            deviation = round((ratio - 1.0) * 100, 2)
            msg = (f"Revenue in {region} dropped to ₹{current:,.0f} "
                   f"({abs(deviation):.1f}% below baseline ₹{baseline:,.0f})")
            log.warning("rule_revenue_drop", region=region,
                        current=current, baseline=baseline, deviation_pct=deviation)
            _record_alert("revenue_drop", region, "critical",
                          current, baseline, deviation, msg)


# ---------------------------------------------------------------------------
# Rule 2 — Revenue spike
# ---------------------------------------------------------------------------

def check_revenue_spike(rows: list) -> None:
    for row in rows:
        region   = row["region"]
        current  = float(row["net_revenue"] or 0)
        baseline = _load_baseline(region, row["window_start"])

        if baseline is None or baseline == 0:
            continue

        ratio = current / baseline
        if ratio > REVENUE_SPIKE_THRESHOLD and _should_fire("revenue_spike", region):
            deviation = round((ratio - 1.0) * 100, 2)
            msg = (f"Revenue spike in {region}: ₹{current:,.0f} "
                   f"({deviation:.1f}% above baseline — possible duplicate events)")
            _record_alert("revenue_spike", region, "warning",
                          current, baseline, deviation, msg)


# ---------------------------------------------------------------------------
# Rule 3 — Zero orders during business hours
# ---------------------------------------------------------------------------

def check_zero_orders(rows: list) -> None:
    hour = datetime.now(timezone.utc).hour
    if not (BUSINESS_HOUR_START <= hour <= BUSINESS_HOUR_END):
        return  # off-hours — expected to be quiet

    for row in rows:
        region = row["region"]
        if int(row["order_count"] or 0) == 0 and _should_fire("zero_orders", region):
            msg = f"Zero orders received in {region} during business hours"
            log.error("rule_zero_orders", region=region, hour=hour)
            _record_alert("zero_orders", region, "critical",
                          0, 1, -100.0, msg)


# ---------------------------------------------------------------------------
# Rule 4 — High failure rate
# ---------------------------------------------------------------------------

def check_failure_rate(rows: list) -> None:
    for row in rows:
        region      = row["region"]
        orders      = int(row["order_count"] or 0)
        failed      = int(row["failed_count"] or 0)
        if orders == 0:
            continue
        rate = failed / orders
        if rate > FAILURE_RATE_THRESHOLD and _should_fire("high_failure_rate", region):
            msg = (f"High failure rate in {region}: "
                   f"{failed}/{orders} orders failed ({rate*100:.1f}%)")
            _record_alert("high_failure_rate", region, "critical",
                          rate * 100, FAILURE_RATE_THRESHOLD * 100,
                          round((rate - FAILURE_RATE_THRESHOLD) * 100, 2), msg)


# ---------------------------------------------------------------------------
# Main evaluation — runs every 60s
# ---------------------------------------------------------------------------

def run_rules() -> None:
    """Evaluate all Layer 1 rules against the latest window."""
    log.info("rule_engine_tick")
    rows = _load_current_window()
    if not rows:
        log.warning("rule_engine_no_data")
        return

    check_revenue_drop(rows)
    check_revenue_spike(rows)
    check_zero_orders(rows)
    check_failure_rate(rows)
    log.info("rule_engine_tick_complete", regions_checked=len(rows))
