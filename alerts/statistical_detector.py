"""
Order Intelligence Platform — Phase 10
Alert Engine — Layer 2: Statistical Detection
==============================================
Context-aware anomaly detection using:
  - Z-score (3-sigma rule) against 28-day rolling baseline
  - EWMA ratio (short MA / long MA) for trend detection

Advantages over Layer 1 rules:
  - Adapts to time-of-day patterns (no false alarms at 3 AM)
  - Detects subtle drifts that don't cross hard thresholds
  - ~0.3% false positive rate at z=3.0

Run alongside rule_engine.py — they operate independently.
"""

import math
import os
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import create_engine, text

from alerts.alert_router import fire_alert
from storage.redis_client import is_alert_active, set_alert_active
from utils.logger import configure_logging, get_logger

configure_logging()
log = get_logger("statistical_detector")

ZSCORE_THRESHOLD = float(os.getenv("ALERT_ZSCORE_THRESHOLD", "3.0"))
EWMA_RATIO_LOW   = 0.60     # short MA / long MA < 0.6 → downward trend alert
EWMA_ALPHA_SHORT = 0.4      # weight for 5-min EWMA (more reactive)
EWMA_ALPHA_LONG  = 0.1      # weight for 60-min EWMA (smoother baseline)


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
# Z-score detection
# ---------------------------------------------------------------------------

def _load_baseline_stats(region: str, dow: int, hour: int) -> Optional[dict]:
    """Load pre-computed mean and stddev for this time slot."""
    sql = text("""
        SELECT rolling_mean, rolling_stddev, sample_count
        FROM gold.anomaly_baseline
        WHERE metric_name = 'net_revenue'
          AND region      = :region
          AND day_of_week = :dow
          AND hour_of_day = :hour
    """)
    with engine().connect() as conn:
        row = conn.execute(sql, {"region": region, "dow": dow, "hour": hour}).fetchone()
    if not row:
        return None
    return {
        "mean":         float(row[0]),
        "stddev":       float(row[1]),
        "sample_count": int(row[2]),
    }


def _compute_zscore(current: float, mean: float, stddev: float) -> Optional[float]:
    if stddev == 0:
        return None
    return (current - mean) / stddev


def check_zscore(rows: list) -> None:
    """
    For each region's current window, compute z-score against baseline.
    Alert if |z| > ZSCORE_THRESHOLD.
    """
    now = datetime.now(timezone.utc)
    for row in rows:
        region  = row["region"]
        current = float(row["net_revenue"] or 0)
        stats   = _load_baseline_stats(region, now.weekday(), now.hour)

        if not stats or stats["sample_count"] < 14:
            # Need at least 14 data points for meaningful z-score
            continue

        z = _compute_zscore(current, stats["mean"], stats["stddev"])
        if z is None:
            continue

        log.debug("zscore_computed",
                  region=region,
                  z=round(z, 3),
                  current=current,
                  mean=stats["mean"])

        if abs(z) > ZSCORE_THRESHOLD:
            direction  = "drop" if z < 0 else "spike"
            alert_type = f"zscore_{direction}"
            severity   = "critical" if abs(z) > 4.0 else "warning"
            deviation  = round((current / stats["mean"] - 1.0) * 100, 2) if stats["mean"] else 0

            if not is_alert_active(alert_type, region):
                msg = (f"Statistical anomaly in {region}: "
                       f"z={z:.2f} (threshold ±{ZSCORE_THRESHOLD}). "
                       f"Current ₹{current:,.0f} vs mean ₹{stats['mean']:,.0f}")
                log.warning("zscore_alert",
                            region=region, z=round(z, 3),
                            current=current, mean=stats["mean"])

                _record_stat_alert(alert_type, severity, region,
                                   current, stats["mean"], deviation, msg, layer=2)


# ---------------------------------------------------------------------------
# EWMA trend detection
# ---------------------------------------------------------------------------

def _load_recent_revenue(region: str, minutes: int = 60) -> list:
    """Load net_revenue per minute for the last N minutes (ordered oldest→newest)."""
    sql = text("""
        SELECT (gross_revenue - refund_amount - cancellation_amount) AS net_revenue
        FROM gold.revenue_by_minute
        WHERE region = :region
          AND window_start >= NOW() - INTERVAL ':minutes minutes'
        ORDER BY window_start ASC
    """)
    with engine().connect() as conn:
        rows = conn.execute(sql, {"region": region, "minutes": minutes}).fetchall()
    return [float(r[0] or 0) for r in rows]


def _ewma(values: list, alpha: float) -> float:
    """Compute Exponentially Weighted Moving Average."""
    if not values:
        return 0.0
    result = values[0]
    for v in values[1:]:
        result = alpha * v + (1 - alpha) * result
    return result


def check_ewma_trend(regions: list) -> None:
    """
    Compute short (5-min) and long (60-min) EWMA.
    Alert if short_ma / long_ma < EWMA_RATIO_LOW (downward trend).
    """
    for region in regions:
        values = _load_recent_revenue(region, minutes=60)
        if len(values) < 10:
            continue  # not enough history

        short_ma = _ewma(values[-5:],  EWMA_ALPHA_SHORT)
        long_ma  = _ewma(values,       EWMA_ALPHA_LONG)

        if long_ma == 0:
            continue

        ratio = short_ma / long_ma
        log.debug("ewma_computed",
                  region=region,
                  short_ma=round(short_ma, 2),
                  long_ma=round(long_ma, 2),
                  ratio=round(ratio, 3))

        if ratio < EWMA_RATIO_LOW and not is_alert_active("ewma_downtrend", region):
            deviation = round((ratio - 1.0) * 100, 2)
            msg = (f"EWMA downtrend in {region}: "
                   f"5-min avg ₹{short_ma:,.0f} vs 60-min avg ₹{long_ma:,.0f} "
                   f"(ratio {ratio:.2f} < threshold {EWMA_RATIO_LOW})")
            log.warning("ewma_downtrend_alert",
                        region=region, ratio=round(ratio, 3))
            _record_stat_alert("ewma_downtrend", "warning", region,
                               short_ma, long_ma, deviation, msg, layer=2)


# ---------------------------------------------------------------------------
# Shared alert recorder
# ---------------------------------------------------------------------------

def _record_stat_alert(alert_type: str, severity: str, region: str,
                       current: float, baseline: float,
                       deviation_pct: float, message: str, layer: int) -> None:
    sql = text("""
        INSERT INTO alerts.alert_log
            (alert_type, severity, region, window_start, current_value,
             baseline_value, deviation_pct, layer, message, channel)
        VALUES
            (:alert_type, :severity, :region, NOW(), :current,
             :baseline, :deviation_pct, :layer, :message, 'slack')
    """)
    with engine().connect() as conn:
        conn.execute(sql, {
            "alert_type":   alert_type,
            "severity":     severity,
            "region":       region,
            "current":      current,
            "baseline":     baseline,
            "deviation_pct": deviation_pct,
            "layer":        layer,
            "message":      message,
        })
        conn.commit()

    set_alert_active(alert_type, region, {"message": message})
    fire_alert(alert_type, severity, region, current, baseline,
               deviation_pct, message, layer)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_statistical_checks(rows: list) -> None:
    """Evaluate all Layer 2 statistical checks."""
    log.info("statistical_detector_tick")
    check_zscore(rows)
    check_ewma_trend([r["region"] for r in rows])
    log.info("statistical_detector_tick_complete")
