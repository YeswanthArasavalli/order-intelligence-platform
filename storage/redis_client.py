"""
Order Intelligence Platform — Phase 9
Redis Client
============
Hot-path cache for dashboard KPIs, product leaderboard, and alert state.

Read pattern (cache-aside):
    1. Try Redis (sub-millisecond)
    2. On miss → query PostgreSQL → write to Redis with TTL
    3. Dashboard always gets fresh data within TTL window

Write pattern:
    After each Spark batch → pipeline SET all affected keys atomically.
    Using pipeline batches all SET commands in one round-trip.

Key design:
    kpi:revenue:1min:{region}:{window_start_epoch}   TTL: 600s
    kpi:summary:latest                                TTL: 60s
    leaderboard:products:5min:{window_start_epoch}   TTL: 300s
    alert:active:{alert_type}:{region}               TTL: 900s (= cooldown)
    dashboard:region_heatmap:{window_start_epoch}    TTL: 60s
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import redis

from utils.logger import configure_logging, get_logger

configure_logging()
log = get_logger("redis_client")

# ---------------------------------------------------------------------------
# Redis connection (lazy singleton)
# ---------------------------------------------------------------------------

_client: Optional[redis.Redis] = None


def _get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=0,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )
        _client.ping()
        log.info("redis_connected",
                 host=os.getenv("REDIS_HOST", "localhost"),
                 port=os.getenv("REDIS_PORT", "6379"))
    return _client


# ---------------------------------------------------------------------------
# TTL constants (seconds)
# ---------------------------------------------------------------------------

TTL_KPI          = int(os.getenv("REDIS_TTL_KPI",         "600"))
TTL_LEADERBOARD  = int(os.getenv("REDIS_TTL_LEADERBOARD", "300"))
TTL_ALERT        = int(os.getenv("REDIS_TTL_ALERT",       "900"))
TTL_DASHBOARD    = int(os.getenv("REDIS_TTL_DASHBOARD",   "60"))


# ---------------------------------------------------------------------------
# Key builders
# ---------------------------------------------------------------------------

def _kpi_key(region: str, window_start: Any) -> str:
    ts = int(window_start.timestamp()) if hasattr(window_start, "timestamp") else int(window_start)
    return f"kpi:revenue:1min:{region}:{ts}"

def _summary_key() -> str:
    return "kpi:summary:latest"

def _leaderboard_key(window_start: Any) -> str:
    ts = int(window_start.timestamp()) if hasattr(window_start, "timestamp") else int(window_start)
    return f"leaderboard:products:5min:{ts}"

def _alert_key(alert_type: str, region: str = "all") -> str:
    return f"alert:active:{alert_type}:{region}"

def _alert_list_key() -> str:
    return "alert:list:active"

def _heatmap_key(window_start: Any) -> str:
    ts = int(window_start.timestamp()) if hasattr(window_start, "timestamp") else int(window_start)
    return f"dashboard:region_heatmap:{ts}"


# ---------------------------------------------------------------------------
# KPI writes (called after Spark Gold batch)
# ---------------------------------------------------------------------------

def set_revenue_kpis(revenue_rows: List[Dict]) -> None:
    """
    Write all revenue_by_minute rows to Redis after a Spark batch.
    Uses pipeline to batch all SETs in one round-trip.

    Args:
        revenue_rows: List of dicts with keys:
            window_start, region, order_count, gross_revenue,
            refund_amount, cancellation_amount, avg_order_value
    """
    if not revenue_rows:
        return

    client = _get_client()
    pipe   = client.pipeline(transaction=False)

    for row in revenue_rows:
        net_revenue = (
            float(row["gross_revenue"])
            - float(row["refund_amount"])
            - float(row["cancellation_amount"])
        )
        payload = {
            "window_start":        str(row["window_start"]),
            "region":              row["region"],
            "order_count":         int(row["order_count"]),
            "gross_revenue":       float(row["gross_revenue"]),
            "refund_amount":       float(row["refund_amount"]),
            "cancellation_amount": float(row["cancellation_amount"]),
            "net_revenue":         round(net_revenue, 2),
            "avg_order_value":     float(row.get("avg_order_value", 0)),
        }
        key = _kpi_key(row["region"], row["window_start"])
        pipe.setex(key, TTL_KPI, json.dumps(payload))

    # Update platform-wide summary (aggregate across all regions)
    total_net = sum(
        float(r["gross_revenue"]) - float(r["refund_amount"]) - float(r["cancellation_amount"])
        for r in revenue_rows
    )
    summary = {
        "total_net_revenue": round(total_net, 2),
        "total_orders":      sum(int(r["order_count"]) for r in revenue_rows),
        "updated_at":        datetime.utcnow().isoformat(),
    }
    pipe.setex(_summary_key(), TTL_DASHBOARD, json.dumps(summary))
    pipe.execute()

    log.debug("redis_kpi_written", rows=len(revenue_rows))


def set_product_leaderboard(product_rows: List[Dict], window_start: Any) -> None:
    """Write product leaderboard (sorted by revenue desc) to Redis."""
    if not product_rows:
        return

    client  = _get_client()
    sorted_rows = sorted(product_rows, key=lambda r: float(r["revenue"]), reverse=True)
    key     = _leaderboard_key(window_start)

    client.setex(key, TTL_LEADERBOARD, json.dumps(sorted_rows[:10]))  # top 10
    log.debug("redis_leaderboard_written", products=len(sorted_rows))


# ---------------------------------------------------------------------------
# KPI reads (dashboard hot path)
# ---------------------------------------------------------------------------

def get_summary() -> Optional[Dict]:
    """Get latest platform-wide KPI summary. Returns None on cache miss."""
    try:
        raw = _get_client().get(_summary_key())
        return json.loads(raw) if raw else None
    except Exception as exc:
        log.warning("redis_get_summary_failed", error=str(exc))
        return None


def get_leaderboard(window_start: Any) -> Optional[List[Dict]]:
    """Get product leaderboard for the given window. Returns None on miss."""
    try:
        raw = _get_client().get(_leaderboard_key(window_start))
        return json.loads(raw) if raw else None
    except Exception as exc:
        log.warning("redis_get_leaderboard_failed", error=str(exc))
        return None


# ---------------------------------------------------------------------------
# Alert state (used by alert engine for cooldown enforcement)
# ---------------------------------------------------------------------------

def set_alert_active(alert_type: str, region: str = "all", payload: Dict = None) -> None:
    """Mark an alert type as active for the cooldown TTL window."""
    key  = _alert_key(alert_type, region)
    data = json.dumps(payload or {"alert_type": alert_type, "region": region})
    _get_client().setex(key, TTL_ALERT, data)
    log.info("redis_alert_active_set",
             alert_type=alert_type, region=region, ttl=TTL_ALERT)


def is_alert_active(alert_type: str, region: str = "all") -> bool:
    """Return True if this alert type is in its cooldown window."""
    key = _alert_key(alert_type, region)
    return _get_client().exists(key) > 0


def clear_alert(alert_type: str, region: str = "all") -> None:
    """Clear an active alert (called on auto-resolve)."""
    _get_client().delete(_alert_key(alert_type, region))


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

def ping() -> bool:
    try:
        return _get_client().ping()
    except Exception as exc:
        log.error("redis_ping_failed", error=str(exc))
        return False
